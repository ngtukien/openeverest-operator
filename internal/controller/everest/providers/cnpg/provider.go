// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package cnpg contains the CloudNativePG (CNPG) provider code.
// It manages postgresql.cnpg.io/v1 resources through the unstructured client, so Everest does not
// depend on the CNPG Go module and follows whatever CNPG version is installed.
package cnpg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/AlekSi/pointer"
	"github.com/Masterminds/semver/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters;scheduledbackups;poolers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=databases;publications;subscriptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusterimagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

// CloudNativePG and Barman Cloud Plugin identifiers.
const (
	apiGroup            = "postgresql.cnpg.io"
	barmanAPIGroup      = "barmancloud.cnpg.io"
	clusterKind         = "Cluster"
	backupKind          = "Backup"
	scheduledBackupKind = "ScheduledBackup"
	poolerKind          = "Pooler"
	imageCatalogKind    = "ClusterImageCatalog"
	objectStoreKind     = "ObjectStore"
	poolerNameLabel     = "cnpg.io/poolerName"
	barmanPluginName    = "barman-cloud.cloudnative-pg.io"

	// CRDs whose presence enables a feature.
	clusterCRDName     = "clusters.postgresql.cnpg.io"
	objectStoreCRDName = "objectstores.barmancloud.cnpg.io"
	podMonitorCRDName  = "podmonitors.monitoring.coreos.com"

	// ClusterImageCatalogs managed by the platform; Everest only reads them.
	imageCatalogName          = "everest-postgresql"
	timescaleImageCatalogName = "timescaledb-oss"

	// Standard installation of the operator, used to report its version.
	operatorNamespace = "cnpg-system"
	operatorAppName   = "cloudnative-pg"
)

const (
	cnpgAPIVersion = "v1"
	// Port every CNPG instance and Pooler listens on.
	postgresPort = 5432

	// CNPG Cluster condition that reports overall health.
	conditionReady = "Ready"
	// Prefix of every CNPG phase that needs manual intervention,
	// e.g. "Cluster is unrecoverable and needs manual intervention".
	phaseUnrecoverablePrefix = "Cluster is unrecoverable"
)

// ClusterGVK is the GroupVersionKind of the CNPG Cluster.
var ClusterGVK = schema.GroupVersionKind{
	Group:   apiGroup,
	Version: cnpgAPIVersion,
	Kind:    clusterKind,
}

// Provider is a provider for CloudNativePG clusters.
// It embeds *unstructured.Unstructured (the CNPG Cluster CR) to satisfy the metav1.Object
// interface required by dbProvider.
type Provider struct {
	*unstructured.Unstructured
	providers.ProviderOptions
}

// New returns a new CNPG provider.
func New(ctx context.Context, opts providers.ProviderOptions) (*Provider, error) {
	cluster := newUnstructured(ClusterGVK, opts.DB.GetNamespace(), opts.DB.GetName())
	if err := opts.C.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil && !k8serrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get CNPG Cluster: %w", err)
	}

	// The approved images come from the DatabaseEngine that the DatabaseEngine controller fills
	// from the ClusterImageCatalog. A missing DatabaseEngine must not block a running cluster;
	// Engine() reports a clear error if it needs a version that is not available.
	dbEngine, err := common.GetDatabaseEngineForType(ctx, opts.C, everestv1alpha1.DatabaseEngineCNPG, opts.DB.GetNamespace())
	if err != nil {
		dbEngine = &everestv1alpha1.DatabaseEngine{}
	}
	opts.DBEngine = dbEngine

	return &Provider{
		Unstructured:    cluster,
		ProviderOptions: opts,
	}, nil
}

// Apply returns the CNPG applier.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	in, err := parseInputs(p.DB.GetAnnotations())
	return &applier{
		Provider: p,
		ctx:      ctx,
		in:       in,
		inErr:    err,
	}
}

// Status builds the DatabaseCluster status from the current CNPG Cluster state.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.DB.Status
	status.Port = postgresPort
	status.CRVersion = cnpgAPIVersion

	desired, _, _ := unstructured.NestedInt64(p.Object, "spec", "instances")
	ready, _, _ := unstructured.NestedInt64(p.Object, "status", "readyInstances")
	status.Size = int32(desired)
	status.Ready = int32(ready)

	hostname, err := p.hostname(ctx)
	if err != nil {
		return status, false, err
	}
	status.Hostname = hostname

	phase, _, _ := unstructured.NestedString(p.Object, "status", "phase")
	readyCondition, message := readyCondition(p.Object)
	status.Message = message
	switch {
	case isUnrecoverable(phase):
		status.Status = everestv1alpha1.AppStateError
		status.Message = phase
	case readyCondition && desired > 0 && ready == desired:
		status.Status = everestv1alpha1.AppStateReady
	default:
		// Transient phases: "Setting up primary", "Failing over", "Switchover in progress", etc.
		status.Status = everestv1alpha1.AppStateCreating
		if status.Message == "" {
			status.Message = "waiting for the CloudNativePG Cluster to become ready"
		}
	}

	resizeStatus, err := getPVCResizeStatus(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace())
	if err != nil {
		return status, false, err
	}
	meta.RemoveStatusCondition(&status.Conditions, everestv1alpha1.ConditionTypeVolumeResizeFailed)
	if resizeStatus.resizing {
		status.Status = everestv1alpha1.AppStateResizingVolumes
		if resizeStatus.failed {
			meta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               everestv1alpha1.ConditionTypeVolumeResizeFailed,
				Status:             metav1.ConditionTrue,
				Reason:             everestv1alpha1.ReasonVolumeResizeFailed,
				Message:            resizeStatus.failureReason,
				ObservedGeneration: p.DB.GetGeneration(),
			})
		}
	}

	if rawStatus, found, _ := unstructured.NestedMap(p.Object, "status"); found {
		if data, err := json.Marshal(rawStatus); err == nil {
			status.Details = string(data)
		}
	}
	return status, status.Status == everestv1alpha1.AppStateReady, nil
}

// Cleanup removes the backups and restores of the cluster, then the CNPG Cluster itself.
func (p *Provider) Cleanup(ctx context.Context, db *everestv1alpha1.DatabaseCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(db, consts.DBBackupCleanupFinalizer) {
		if done, err := common.DeleteBackupsForDatabase(ctx, p.C, db.GetName(), db.GetNamespace()); err != nil || !done {
			return done, err
		}
		if done, err := common.DeleteRestoresForDatabase(ctx, p.C, db.GetName(), db.GetNamespace()); err != nil || !done {
			return done, err
		}
		controllerutil.RemoveFinalizer(db, consts.DBBackupCleanupFinalizer)
		if err := p.C.Update(ctx, db); err != nil {
			return false, err
		}
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, db, EmptyClusterObject())
}

// DBObject returns the CNPG Cluster unstructured object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.SetGroupVersionKind(ClusterGVK)
	return p.Unstructured
}

// RunPreReconcileHook is a no-op for CNPG.
func (p *Provider) RunPreReconcileHook(_ context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

// hostname returns the connection hostname of the cluster.
//
// The entrypoint is the Pooler when it is enabled, otherwise the "-rw" Service. When the
// entrypoint is exposed through a LoadBalancer, its external address is preferred. There is no
// fallback from the Pooler to "-rw": bypassing the pooler would defeat the connection limit it
// exists to enforce.
func (p *Provider) hostname(ctx context.Context) (string, error) {
	db := p.DB
	internal := db.GetName() + "-rw"
	external := externalServiceName(db.GetName())
	if poolerEnabled(db) {
		internal = poolerName(db.GetName())
		external = internal
	}
	svc := &corev1.Service{}
	err := p.C.Get(ctx, types.NamespacedName{Name: external, Namespace: db.GetNamespace()}, svc)
	if client.IgnoreNotFound(err) != nil {
		return "", err
	}
	if err == nil && svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		for _, ing := range svc.Status.LoadBalancer.Ingress {
			if ing.Hostname != "" {
				return ing.Hostname, nil
			}
			if ing.IP != "" {
				return ing.IP, nil
			}
		}
	}
	return fmt.Sprintf("%s.%s.svc", internal, db.GetNamespace()), nil
}

// EmptyClusterObject returns an unstructured object with the GVK of a CNPG Cluster set, but no
// name/namespace. Useful as a type placeholder, e.g. when registering a watch source.
func EmptyClusterObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ClusterGVK)
	return u
}

func readyCondition(object map[string]any) (bool, string) {
	conditions, _, _ := unstructured.NestedSlice(object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok || condition["type"] != conditionReady {
			continue
		}
		message, _ := condition["message"].(string)
		return condition["status"] == string(metav1.ConditionTrue), message
	}
	return false, ""
}

func isUnrecoverable(phase string) bool {
	return strings.HasPrefix(phase, phaseUnrecoverablePrefix)
}

type pvcResizeStatus struct {
	resizing      bool
	failed        bool
	failureReason string
}

// getPVCResizeStatus inspects CNPG PVC conditions and capacities. PVC conditions can be
// short-lived, so comparing the requested size with the reported capacity keeps Everest from
// missing an in-progress expansion.
func getPVCResizeStatus(ctx context.Context, c client.Client, name, namespace string) (pvcResizeStatus, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{"cnpg.io/cluster": name}); err != nil {
		return pvcResizeStatus{}, fmt.Errorf("failed to list CNPG PVCs: %w", err)
	}
	result := pvcResizeStatus{}
	for _, pvc := range pvcList.Items {
		requested, hasRequest := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		capacity, hasCapacity := pvc.Status.Capacity[corev1.ResourceStorage]
		if hasRequest && hasCapacity && !capacity.IsZero() && requested.Cmp(capacity) > 0 {
			result.resizing = true
		}
		for _, condition := range pvc.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type { //nolint:exhaustive
			case corev1.PersistentVolumeClaimResizing, corev1.PersistentVolumeClaimFileSystemResizePending:
				result.resizing = true
			case corev1.PersistentVolumeClaimControllerResizeError, corev1.PersistentVolumeClaimNodeResizeError:
				result.resizing = true
				result.failed = true
				result.failureReason = condition.Message
			}
		}
	}
	return result, nil
}

func newUnstructured(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	return u
}

var crdGVK = schema.GroupVersionKind{
	Group:   "apiextensions.k8s.io",
	Version: "v1",
	Kind:    "CustomResourceDefinition",
}

var clusterImageCatalogGVK = schema.GroupVersionKind{
	Group:   apiGroup,
	Version: "v1",
	Kind:    imageCatalogKind,
}

// CRDInstalled reports whether the CRD with the given name exists. Providers that depend on
// optional operators (CloudNativePG, Barman Cloud Plugin, Prometheus Operator) use it to decide
// whether a feature is available instead of failing on "no matches for kind".
func CRDInstalled(ctx context.Context, c client.Client, name string) (bool, error) {
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(crdGVK)
	err := c.Get(ctx, types.NamespacedName{Name: name}, crd)
	switch {
	case err == nil:
		return true, nil
	case k8serrors.IsNotFound(err), meta.IsNoMatchError(err):
		return false, nil
	default:
		return false, err
	}
}

// ImageCatalogVersions reads the ClusterImageCatalog approved by the platform and converts it
// into Everest versions.
//
// Everest reads the catalog itself and writes spec.imageName instead of using
// spec.imageCatalogRef, because imageCatalogRef only takes a major version and leaves imageName
// empty: the published version would not match the running one and the version-change guard
// could not read the current version.
//
// A missing CRD or catalog is not an error: the version list is empty and admission rejects every
// version, so no cluster is created from an image nobody approved.
func ImageCatalogVersions(ctx context.Context, c client.Client) (everestv1alpha1.Versions, error) {
	versions := everestv1alpha1.Versions{}

	catalog := &unstructured.Unstructured{}
	catalog.SetGroupVersionKind(clusterImageCatalogGVK)
	err := c.Get(ctx, types.NamespacedName{Name: imageCatalogName}, catalog)
	if err != nil {
		if k8serrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return versions, nil
		}
		return versions, fmt.Errorf("get ClusterImageCatalog %q: %w", imageCatalogName, err)
	}

	images, _, err := unstructured.NestedSlice(catalog.Object, "spec", "images")
	if err != nil {
		return versions, fmt.Errorf("read spec.images of ClusterImageCatalog: %w", err)
	}

	engine := everestv1alpha1.ComponentsMap{}
	for _, raw := range images {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		image, _ := entry["image"].(string)
		if image == "" {
			continue
		}
		version, err := versionFromImage(image)
		if err != nil {
			// One malformed entry must not disable the whole catalog.
			continue
		}
		// An entry whose declared major differs from its tag is misplaced; trusting it would
		// start the wrong PostgreSQL major.
		if major, ok := entryMajor(entry); ok && major != int64(semverMajor(version)) {
			continue
		}
		engine[version] = &everestv1alpha1.Component{
			ImagePath: image,
			ImageHash: digestFromImage(image),
			Status:    everestv1alpha1.DBEngineComponentAvailable,
		}
	}
	if len(engine) == 0 {
		return versions, nil
	}
	versions.Engine = engine

	// componentImages carries non-PostgreSQL images (pgbouncer). They do not depend on the
	// PostgreSQL version, so the image is published for every engine version.
	if pgbouncer := componentImage(catalog.Object, "pgbouncer"); pgbouncer != "" {
		proxy := everestv1alpha1.ComponentsMap{}
		for version := range engine {
			proxy[version] = &everestv1alpha1.Component{
				ImagePath: pgbouncer,
				ImageHash: digestFromImage(pgbouncer),
				Status:    everestv1alpha1.DBEngineComponentAvailable,
			}
		}
		versions.Proxy = map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
			everestv1alpha1.ProxyTypePGBouncer: proxy,
		}
	}
	return versions, nil
}

func componentImage(object map[string]any, key string) string {
	components, _, err := unstructured.NestedSlice(object, "spec", "componentImages")
	if err != nil {
		return ""
	}
	for _, raw := range components {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := entry["key"].(string); name == key {
			image, _ := entry["image"].(string)
			return image
		}
	}
	return ""
}

// entryMajor reads the "major" field of a catalog entry. Depending on the decoder the number is
// an int64 or a float64.
func entryMajor(entry map[string]any) (int64, bool) {
	switch value := entry["major"].(type) {
	case int64:
		return value, true
	case float64:
		return int64(value), true
	}
	return 0, false
}

// versionFromImage extracts the PostgreSQL version from an image reference. The tag must start
// with an X.Y version; anything after the first "-" is build metadata.
//
//	ghcr.io/cloudnative-pg/postgresql:14.24-202608310817-standard-bookworm@sha256:31221f...
//	                                  └───┘
func versionFromImage(image string) (string, error) {
	// Strip the digest first, it also contains ":".
	reference, _, _ := strings.Cut(image, "@")

	lastSlash := strings.LastIndex(reference, "/")
	lastColon := strings.LastIndex(reference, ":")
	if lastColon <= lastSlash {
		return "", fmt.Errorf("image %q has no tag", image)
	}
	tag := reference[lastColon+1:]

	version, _, _ := strings.Cut(tag, "-")
	if _, err := semver.NewVersion(version); err != nil {
		return "", fmt.Errorf("cannot determine PostgreSQL version from tag %q: %w", tag, err)
	}
	return version, nil
}

func digestFromImage(image string) string {
	_, digest, found := strings.Cut(image, "@")
	if !found {
		return ""
	}
	return digest
}

func semverMajor(version string) uint64 {
	parsed, err := semver.NewVersion(version)
	if err != nil {
		return 0
	}
	return parsed.Major()
}

// EngineStatus reports the state, operator version and available versions of the CloudNativePG
// DatabaseEngine. CNPG is detected by its CRD, not by a Deployment name that depends on how it
// was installed. Both failure modes fail closed: without the operator or the catalog the version
// list is empty and admission rejects every version.
func EngineStatus(ctx context.Context, c client.Client) (everestv1alpha1.EngineState, string, everestv1alpha1.Versions, error) {
	installed, err := CRDInstalled(ctx, c, clusterCRDName)
	if err != nil || !installed {
		return everestv1alpha1.DBEngineStateNotInstalled, "", everestv1alpha1.Versions{}, err
	}
	ready, version := operatorStatus(ctx, c)
	if !ready {
		return everestv1alpha1.DBEngineStateInstalling, version, everestv1alpha1.Versions{}, nil
	}
	versions, err := ImageCatalogVersions(ctx, c)
	return everestv1alpha1.DBEngineStateInstalled, version, versions, err
}

// operatorStatus finds the operator Deployment by its standard label. A missing Deployment (e.g.
// installed in another namespace) does not block: the CRD already proves CNPG is installed and
// the version is informational.
func operatorStatus(ctx context.Context, c client.Client) (bool, string) {
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments,
		client.InNamespace(operatorNamespace),
		client.MatchingLabels{"app.kubernetes.io/name": operatorAppName},
	); err != nil || len(deployments.Items) == 0 {
		return true, "unknown"
	}
	deployment := deployments.Items[0]
	version := "unknown"
	if containers := deployment.Spec.Template.Spec.Containers; len(containers) != 0 {
		// Not Split(image, ":")[1]: an untagged or digest-only image would panic.
		image, _, _ := strings.Cut(containers[0].Image, "@")
		if lastColon := strings.LastIndex(image, ":"); lastColon > strings.LastIndex(image, "/") {
			version = image[lastColon+1:]
		}
	}
	ready := deployment.Status.ReadyReplicas == deployment.Status.Replicas &&
		deployment.Status.UnavailableReplicas == 0 &&
		deployment.GetGeneration() == deployment.Status.ObservedGeneration
	return ready, version
}

var (
	specPath          = field.NewPath("spec")
	enginePath        = specPath.Child("engine")
	proxyPath         = specPath.Child("proxy")
	annotationsPath   = field.NewPath("metadata", "annotations")
	extensionPath     = annotationsPath.Key(AnnotationExtension)
	engineVersionPath = enginePath.Child("version")
)

// ValidateCreate validates a CloudNativePG DatabaseCluster. Everything the provider can not apply
// is rejected instead of being silently ignored.
func ValidateCreate(ctx context.Context, c client.Client, db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	if installed, err := CRDInstalled(ctx, c, clusterCRDName); err != nil {
		allErrs = append(allErrs, field.InternalError(enginePath.Child("type"),
			fmt.Errorf("could not verify that CloudNativePG is installed: %w", err)))
	} else if !installed {
		allErrs = append(allErrs, field.Forbidden(enginePath.Child("type"),
			fmt.Sprintf("CloudNativePG is not installed: CRD %q not found", clusterCRDName)))
	}

	engine := db.Spec.Engine
	if engine.Version != "" {
		if _, err := semver.NewVersion(engine.Version); err != nil {
			allErrs = append(allErrs, field.Invalid(engineVersionPath, engine.Version, "must be a PostgreSQL version such as 17.4"))
		}
	}
	if engine.CRVersion != nil {
		allErrs = append(allErrs, field.Forbidden(enginePath.Child("crVersion"), "CloudNativePG does not use CR versions"))
	}
	if engine.Replicas < 1 {
		allErrs = append(allErrs, field.Invalid(enginePath.Child("replicas"), engine.Replicas, "CloudNativePG requires at least one instance"))
	}
	if extension, found := db.GetAnnotations()[AnnotationExtension]; found && extension != ExtensionTimescaleDB {
		allErrs = append(allErrs, field.NotSupported(extensionPath, extension, []string{ExtensionTimescaleDB}))
	}

	allErrs = append(allErrs, validateProxy(db.Spec.Proxy)...)

	if in, err := parseInputs(db.GetAnnotations()); err != nil {
		allErrs = append(allErrs, field.Invalid(annotationsPath, AnnotationPrefix+"*", err.Error()))
	} else if db.Spec.DataSource != nil && (in.replicaSource != "" || in.schemaImportSource != "") {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("dataSource"),
			fmt.Sprintf("can not be combined with %s or %s", AnnotationReplicaSource, AnnotationSchemaImportSource)))
	}

	if db.Spec.Monitoring != nil && db.Spec.Monitoring.MonitoringConfigName != "" {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("monitoring"),
			"PMM monitoring is not supported by CloudNativePG; metrics are exported natively"))
	}
	if pointer.Get(db.Spec.DataSource).DataImport != nil {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("dataSource", "dataImport"),
			"data import is not supported by CloudNativePG"))
	}
	if db.Spec.EngineFeatures != nil {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("engineFeatures"), "engine features are not supported by CloudNativePG"))
	}
	if db.Spec.Paused {
		allErrs = append(allErrs, field.Forbidden(specPath.Child("paused"), "pausing is not supported by CloudNativePG yet"))
	}
	for i, schedule := range db.Spec.Backup.Schedules {
		if schedule.RetentionCopies != 0 {
			allErrs = append(allErrs, field.Forbidden(specPath.Child("backup", "schedules").Index(i).Child("retentionCopies"),
				"CloudNativePG retention is the time-based recovery window of the ObjectStore"))
		}
	}
	return allErrs
}

// ValidateUpdate additionally rejects changes CNPG can not roll out: major upgrades, downgrades
// and a flavor switch.
func ValidateUpdate(ctx context.Context, c client.Client, oldDb, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	allErrs := ValidateCreate(ctx, c, newDb)
	if oldDb.GetAnnotations()[AnnotationExtension] != newDb.GetAnnotations()[AnnotationExtension] {
		allErrs = append(allErrs, field.Forbidden(extensionPath, "is immutable"))
	}
	oldVersion, oldErr := semver.NewVersion(oldDb.Spec.Engine.Version)
	newVersion, newErr := semver.NewVersion(newDb.Spec.Engine.Version)
	if oldErr != nil || newErr != nil {
		return allErrs
	}
	switch {
	case oldVersion.Major() != newVersion.Major():
		allErrs = append(allErrs, field.Forbidden(engineVersionPath, fmt.Sprintf(
			"CloudNativePG does not support major upgrades (current=%s, requested=%s)", oldVersion, newVersion)))
	case newVersion.LessThan(oldVersion):
		allErrs = append(allErrs, field.Forbidden(engineVersionPath, fmt.Sprintf(
			"CloudNativePG does not support downgrades (current=%s, requested=%s)", oldVersion, newVersion)))
	}
	return allErrs
}

// validateProxy accepts the pgbouncer pooler only, and rejects pooler settings that would be
// ignored because the pooler is not enabled.
func validateProxy(proxy everestv1alpha1.Proxy) field.ErrorList {
	var allErrs field.ErrorList
	if proxy.Type != "" && proxy.Type != everestv1alpha1.ProxyTypePGBouncer {
		allErrs = append(allErrs, field.NotSupported(proxyPath.Child("type"), proxy.Type,
			[]string{string(everestv1alpha1.ProxyTypePGBouncer)}))
	}
	if _, err := ParsePoolMode(proxy.Config); err != nil {
		allErrs = append(allErrs, field.Invalid(proxyPath.Child("config"), proxy.Config, err.Error()))
	}
	if proxy.Storage != nil {
		allErrs = append(allErrs, field.Forbidden(proxyPath.Child("storage"), "the CloudNativePG pooler does not use storage"))
	}
	if proxy.Type == "" {
		const msg = "set spec.proxy.type=pgbouncer to enable the pooler"
		if proxy.Config != "" {
			allErrs = append(allErrs, field.Forbidden(proxyPath.Child("config"), msg))
		}
		if proxy.Replicas != nil {
			allErrs = append(allErrs, field.Forbidden(proxyPath.Child("replicas"), msg))
		}
		if resources := proxy.Resources.ToResourceRequirements(); len(resources.Limits) != 0 || len(resources.Requests) != 0 {
			allErrs = append(allErrs, field.Forbidden(proxyPath.Child("resources"), msg))
		}
	}
	return allErrs
}

// Annotations of the CNPG provider. They carry only what the upstream Everest API can not express,
// one scalar value per annotation, under the AnnotationPrefix:
//
//	extension                      "timescaledb"
//	server-alt-dns-names           comma-separated DNS names for the server certificate
//	source.<src>.<field>           an external PostgreSQL; fields: host, port, dbname, user,
//	                               sslmode, password-secret ("secret" or "secret/key"),
//	                               cluster ("name" or "namespace/name" of a CNPG cluster),
//	                               client-cert-secret, ca-secret
//	database.<db>.owner            a CNPG Database; empty owner = owner of the user Secret
//	publication.<pub>.dbname       a CNPG Publication; .tables is "*" (default) or
//	                               "schema.table,..."
//	subscription.<sub>.<field>     a CNPG Subscription; fields: dbname, publication, source,
//	                               reclaim-policy ("retain" or "delete")
//	schema-import-source           <src> to import the schema of the application database from
//	replica-source                 <src> to replicate from; replica-enabled "false" promotes
//	replication-expose             CIDRs allowed to reach the primary directly for logical
//	                               replication (e.g. a migration source subscribing back);
//	                               also grants REPLICATION to the owner
const (
	// AnnotationPrefix starts every annotation of the CNPG provider.
	AnnotationPrefix = "cnpg.everest.io/"
	// AnnotationExtension selects a PostgreSQL flavor. Supported: "timescaledb".
	AnnotationExtension = AnnotationPrefix + "extension"
	// AnnotationServerAltDNSNames is a comma-separated list of extra DNS names for the server
	// certificate, e.g. the external name a DNS cutover points to.
	AnnotationServerAltDNSNames = AnnotationPrefix + "server-alt-dns-names"
	// AnnotationSchemaImportSource names the source whose schema is imported at bootstrap.
	AnnotationSchemaImportSource = AnnotationPrefix + "schema-import-source"
	// AnnotationReplicaSource names the source this cluster replicates from.
	AnnotationReplicaSource = AnnotationPrefix + "replica-source"
	// AnnotationReplicaEnabled keeps the cluster a replica ("true", default); "false" promotes it.
	AnnotationReplicaEnabled = AnnotationPrefix + "replica-enabled"
	// AnnotationReplicationExpose is a comma-separated list of CIDRs that may open replication
	// connections to the primary through a dedicated LoadBalancer. PgBouncer can not carry the
	// replication protocol, so a subscriber outside the cluster needs this path.
	AnnotationReplicationExpose = AnnotationPrefix + "replication-expose"

	// ExtensionTimescaleDB is the AnnotationExtension value for TimescaleDB.
	ExtensionTimescaleDB = "timescaledb"
)

const (
	defaultPostgresPort     = 5432
	defaultPasswordKey      = "password"
	defaultReplicaUser      = "streaming_replica"
	defaultReplicaDBName    = "postgres"
	defaultSSLMode          = "prefer"
	certAuthSSLMode         = "verify-full"
	replicationSecretSuffix = "-replication"
	caSecretSuffix          = "-ca"
	allTables               = "*"
	fieldDBName             = "dbname"
)

var (
	sourceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	// A subscription name is also the replication slot name on the publisher.
	slotNamePattern = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
)

// source is an external PostgreSQL referenced by schema import, subscriptions or a replica.
type source struct {
	name             string
	host             string
	port             int
	dbname           string
	user             string
	sslmode          string
	passwordSecret   string
	passwordKey      string
	cluster          string
	clusterNamespace string
	clientCertSecret string
	caSecret         string
}

type logicalDatabase struct {
	name  string
	owner string
}

type publication struct {
	name   string
	dbname string
	tables []string // nil publishes all tables.
}

type subscription struct {
	name          string
	dbname        string
	publication   string
	source        string
	reclaimPolicy string
}

// inputs is the parsed annotation contract of a DatabaseCluster.
type inputs struct {
	sources            map[string]*source
	databases          []logicalDatabase
	publications       []publication
	subscriptions      []subscription
	schemaImportSource string
	replicaSource      string
	replicaEnabled     bool
	// replicationExpose lists the CIDRs of AnnotationReplicationExpose; empty = not exposed.
	replicationExpose []string
}

// parseInputs reads the annotations of the CNPG provider. Unknown keys are rejected, so a typo
// fails instead of being silently ignored.
func parseInputs(annotations map[string]string) (inputs, error) {
	b := inputsBuilder{
		in:            inputs{sources: map[string]*source{}, replicaEnabled: true},
		databases:     map[string]*logicalDatabase{},
		publications:  map[string]*publication{},
		subscriptions: map[string]*subscription{},
	}
	keys := make([]string, 0, len(annotations))
	for key := range annotations {
		if strings.HasPrefix(key, AnnotationPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := b.add(key, strings.TrimSpace(annotations[key])); err != nil {
			return b.in, err
		}
	}
	return b.build()
}

// inputsBuilder collects annotations before the checks that span several of them.
type inputsBuilder struct {
	in                inputs
	databases         map[string]*logicalDatabase
	publications      map[string]*publication
	subscriptions     map[string]*subscription
	replicaEnabledSet bool
}

func (b *inputsBuilder) add(key, value string) error {
	switch key {
	case AnnotationExtension, AnnotationServerAltDNSNames:
		return nil
	case AnnotationSchemaImportSource:
		b.in.schemaImportSource = value
		return nil
	case AnnotationReplicaSource:
		b.in.replicaSource = value
		return nil
	case AnnotationReplicaEnabled:
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%s: must be true or false", key)
		}
		b.in.replicaEnabled, b.replicaEnabledSet = enabled, true
		return nil
	case AnnotationReplicationExpose:
		for cidr := range strings.SplitSeq(value, ",") {
			cidr = strings.TrimSpace(cidr)
			if _, _, err := net.ParseCIDR(cidr); err != nil {
				return fmt.Errorf("%s: %q is not a CIDR", key, cidr)
			}
			b.in.replicationExpose = append(b.in.replicationExpose, cidr)
		}
		return nil
	}

	kind, rest, _ := strings.Cut(strings.TrimPrefix(key, AnnotationPrefix), ".")
	name, fieldName, found := strings.Cut(rest, ".")
	if !found || name == "" || fieldName == "" {
		return fmt.Errorf("unknown annotation %q", key)
	}
	var err error
	switch kind {
	case "source":
		err = b.addSource(name, fieldName, value)
	case "database":
		err = b.addDatabase(name, fieldName, value)
	case "publication":
		err = b.addPublication(name, fieldName, value)
	case "subscription":
		err = b.addSubscription(name, fieldName, value)
	default:
		return fmt.Errorf("unknown annotation %q", key)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	return nil
}

func (b *inputsBuilder) addSource(name, fieldName, value string) error {
	if !sourceNamePattern.MatchString(name) {
		return errors.New("source name must be a DNS label")
	}
	src := b.in.sources[name]
	if src == nil {
		src = &source{name: name}
		b.in.sources[name] = src
	}
	return src.set(fieldName, value)
}

func (b *inputsBuilder) addDatabase(name, fieldName, value string) error {
	if fieldName != "owner" {
		return fmt.Errorf("unknown database field %q", fieldName)
	}
	if err := checkIdentifier(name, false); err != nil {
		return fmt.Errorf("database %w", err)
	}
	if err := checkIdentifier(value, true); err != nil {
		return fmt.Errorf("owner %w", err)
	}
	b.databases[name] = &logicalDatabase{name: name, owner: value}
	return nil
}

func (b *inputsBuilder) addPublication(name, fieldName, value string) error {
	if err := checkIdentifier(name, false); err != nil {
		return fmt.Errorf("publication %w", err)
	}
	pub := b.publications[name]
	if pub == nil {
		pub = &publication{name: name}
		b.publications[name] = pub
	}
	return pub.set(fieldName, value)
}

func (b *inputsBuilder) addSubscription(name, fieldName, value string) error {
	if !slotNamePattern.MatchString(name) {
		return fmt.Errorf("subscription name must match %s, it is also the replication slot name", slotNamePattern)
	}
	sub := b.subscriptions[name]
	if sub == nil {
		sub = &subscription{name: name}
		b.subscriptions[name] = sub
	}
	return sub.set(fieldName, value)
}

// build runs the checks that span several annotations.
func (b *inputsBuilder) build() (inputs, error) {
	in := b.in
	for _, name := range sortedKeys(b.databases) {
		in.databases = append(in.databases, *b.databases[name])
	}
	for _, name := range sortedKeys(b.publications) {
		pub := b.publications[name]
		if pub.dbname == "" {
			return in, fmt.Errorf("publication %q: dbname is required", name)
		}
		in.publications = append(in.publications, *pub)
	}
	for _, name := range sortedKeys(b.subscriptions) {
		sub := b.subscriptions[name]
		if sub.dbname == "" || sub.publication == "" || sub.source == "" {
			return in, fmt.Errorf("subscription %q: dbname, publication and source are required", name)
		}
		if err := in.requirePasswordSource(sub.source); err != nil {
			return in, fmt.Errorf("subscription %q: %w", name, err)
		}
		in.subscriptions = append(in.subscriptions, *sub)
	}
	if in.schemaImportSource != "" {
		if err := in.requirePasswordSource(in.schemaImportSource); err != nil {
			return in, fmt.Errorf("%s: %w", AnnotationSchemaImportSource, err)
		}
	}
	return in, b.checkReplica()
}

func (b *inputsBuilder) checkReplica() error {
	in := b.in
	if in.replicaSource == "" {
		if b.replicaEnabledSet {
			return fmt.Errorf("%s requires %s", AnnotationReplicaEnabled, AnnotationReplicaSource)
		}
		return nil
	}
	if in.schemaImportSource != "" {
		return fmt.Errorf("%s and %s are exclusive bootstraps", AnnotationReplicaSource, AnnotationSchemaImportSource)
	}
	src := in.sources[in.replicaSource]
	switch {
	case src == nil:
		return fmt.Errorf("%s: source %q is not defined", AnnotationReplicaSource, in.replicaSource)
	case src.host == "" && src.cluster == "":
		return fmt.Errorf("source %q: host or cluster is required for a replica", src.name)
	case src.passwordSecret != "" && (src.clientCertSecret != "" || src.caSecret != ""):
		return fmt.Errorf("source %q: password and certificate authentication are exclusive", src.name)
	case src.passwordSecret != "" && src.user == "":
		return fmt.Errorf("source %q: user with the REPLICATION privilege is required for password authentication", src.name)
	case src.passwordSecret == "" && src.cluster == "":
		return fmt.Errorf("source %q: password-secret is required for a replica of a PostgreSQL outside CloudNativePG", src.name)
	}
	return nil
}

// requirePasswordSource checks a source used through a regular connection (schema import,
// subscriptions).
func (in inputs) requirePasswordSource(name string) error {
	src := in.sources[name]
	switch {
	case src == nil:
		return fmt.Errorf("source %q is not defined", name)
	case src.host == "" && src.cluster == "":
		return fmt.Errorf("source %q: host or cluster is required", name)
	case src.dbname == "" || src.user == "" || src.passwordSecret == "":
		return fmt.Errorf("source %q: dbname, user and password-secret are required", name)
	}
	return nil
}

func (s *source) set(fieldName, value string) error {
	switch fieldName {
	case "host":
		s.host = value
	case "port":
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("port must be between 1 and 65535")
		}
		s.port = port
	case fieldDBName:
		s.dbname = value
	case "user":
		s.user = value
	case "sslmode":
		switch value {
		case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		default:
			return fmt.Errorf("invalid sslmode %q", value)
		}
		s.sslmode = value
	case "password-secret":
		secret, key, _ := strings.Cut(value, "/")
		if secret == "" {
			return errors.New("password-secret must be \"secret\" or \"secret/key\"")
		}
		s.passwordSecret, s.passwordKey = secret, key
	case "cluster":
		namespace, name, found := strings.Cut(value, "/")
		if !found {
			namespace, name = "", value
		}
		if name == "" {
			return errors.New("cluster must be \"name\" or \"namespace/name\"")
		}
		s.cluster, s.clusterNamespace = name, namespace
	case "client-cert-secret":
		s.clientCertSecret = value
	case "ca-secret":
		s.caSecret = value
	default:
		return fmt.Errorf("unknown source field %q", fieldName)
	}
	return nil
}

func (p *publication) set(fieldName, value string) error {
	switch fieldName {
	case fieldDBName:
		if err := checkIdentifier(value, false); err != nil {
			return fmt.Errorf("dbname %w", err)
		}
		p.dbname = value
	case "tables":
		if value == "" || value == allTables {
			p.tables = nil
			return nil
		}
		for table := range strings.SplitSeq(value, ",") {
			table = strings.TrimSpace(table)
			schemaName, tableName, ok := strings.Cut(table, ".")
			if !ok || checkIdentifier(schemaName, false) != nil || checkIdentifier(tableName, false) != nil {
				return fmt.Errorf("table %q must be in schema.table form", table)
			}
			p.tables = append(p.tables, table)
		}
	default:
		return fmt.Errorf("unknown publication field %q", fieldName)
	}
	return nil
}

func (s *subscription) set(fieldName, value string) error {
	switch fieldName {
	case fieldDBName:
		if err := checkIdentifier(value, false); err != nil {
			return fmt.Errorf("dbname %w", err)
		}
		s.dbname = value
	case "publication":
		if err := checkIdentifier(value, false); err != nil {
			return fmt.Errorf("publication %w", err)
		}
		s.publication = value
	case "source":
		s.source = value
	case "reclaim-policy":
		if value != "retain" && value != "delete" {
			return errors.New("reclaim-policy must be retain or delete")
		}
		s.reclaimPolicy = value
	default:
		return fmt.Errorf("unknown subscription field %q", fieldName)
	}
	return nil
}

func checkIdentifier(value string, optional bool) error {
	if value == "" && optional {
		return nil
	}
	if !postgresIdentifier.MatchString(value) {
		return fmt.Errorf("%q must match %s", value, postgresIdentifier)
	}
	return nil
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
