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

package cnpg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/Masterminds/semver/v3"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

// Operator-wide settings, read from the environment of the operator.
const (
	// Size of the dedicated WAL volume of new clusters; unset means no WAL volume.
	walStorageSizeEnv = "CNPG_WAL_STORAGE_SIZE"
	// Storage class of the dedicated WAL volume.
	walStorageClassEnv = "CNPG_WAL_STORAGE_CLASS"
)

const (
	defaultAppDatabase = "app"
	superuserName      = "postgres"
	// Role that groups the privileges delegated to the customer user.
	adminRoleName = "dbaas_admin_role"
	// Label that makes CNPG watch a Secret it does not own, so password changes reach
	// PostgreSQL through managed.roles.
	cnpgReloadLabel = "cnpg.io/reload"
	// Optional key of the user Secret that names the application database.
	userSecretDatabaseKey = "database"
	// First PostgreSQL major with createrole_self_grant and
	// pg_use_reserved_connections.
	minPGMajorForRoleDelegation = 16
)

var postgresIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type applier struct {
	*Provider

	ctx       context.Context //nolint:containedctx
	in        inputs
	inErr     error
	pausedErr error
}

// ResetDefaults makes sure the Cluster has a spec map; Engine() rebuilds it from scratch.
func (a *applier) ResetDefaults() error {
	if a.inErr != nil {
		return fmt.Errorf("invalid %s annotations: %w", AnnotationPrefix, a.inErr)
	}
	if a.Object == nil {
		a.Object = map[string]any{}
	}
	if _, ok := a.Object["spec"]; !ok {
		a.Object["spec"] = map[string]any{}
	}
	return nil
}

// Paused fails the reconciliation: CNPG hibernation is not wired yet, and silently ignoring
// spec.paused would leave the user believing the cluster is stopped.
func (a *applier) Paused(paused bool) {
	if paused {
		a.pausedErr = errors.New("CloudNativePG provider does not support spec.paused yet")
	}
}

// AllowUnsafeConfig is a no-op for CNPG.
func (a *applier) AllowUnsafeConfig() {}

// Metadata sets the labels and controller reference of the CNPG Cluster. DatabaseCluster
// annotations are not propagated, so a user can not set cnpg.io/* annotations through Everest.
func (a *applier) Metadata() error {
	a.SetLabels(map[string]string{
		"app.kubernetes.io/name":       a.DB.GetName(),
		"app.kubernetes.io/instance":   a.DB.GetName(),
		"app.kubernetes.io/managed-by": "everest-operator",
	})
	return controllerutil.SetControllerReference(a.DB, a.Unstructured, a.C.Scheme())
}

// Engine rebuilds the CNPG Cluster spec from the DatabaseCluster spec.
func (a *applier) Engine() error {
	if a.pausedErr != nil {
		return a.pausedErr
	}
	engine := &a.DB.Spec.Engine
	if engine.Version == "" {
		engine.Version = a.DBEngine.BestEngineVersion()
	}
	if engine.Version == "" {
		return errors.New("no PostgreSQL version is available for CloudNativePG")
	}
	if engine.Replicas < 1 {
		return errors.New("CloudNativePG requires at least one PostgreSQL instance")
	}
	desired, err := semver.NewVersion(engine.Version)
	if err != nil {
		return fmt.Errorf("invalid PostgreSQL version %q: %w", engine.Version, err)
	}

	spec := map[string]any{
		"instances":             int64(engine.Replicas),
		"primaryUpdateMethod":   "switchover",
		"primaryUpdateStrategy": "unsupervised",
		"enableSuperuserAccess": false,
	}
	if err := a.setImage(spec, desired); err != nil {
		return err
	}
	if err := a.setStorage(spec); err != nil {
		return err
	}
	resourceRequirements := engine.Resources.ToResourceRequirements()
	resources, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&resourceRequirements)
	if err != nil {
		return fmt.Errorf("convert engine resources: %w", err)
	}
	spec["resources"] = resources

	major := desired.Major()
	postgresql := map[string]any{
		"parameters":        postgresqlParameters(engine.Config, major),
		"enableAlterSystem": false,
	}
	if a.isTimescale() {
		postgresql["shared_preload_libraries"] = []any{ExtensionTimescaleDB}
	}
	spec["postgresql"] = postgresql

	var owner string
	if engine.UserSecretsName != "" {
		if owner, err = a.setBootstrapAndRoles(spec, major); err != nil {
			return err
		}
	} else if a.in.schemaImportSource != "" {
		return errors.New("schema import requires spec.engine.userSecretsName")
	}
	if names := a.serverAltDNSNames(); len(names) != 0 {
		spec["certificates"] = map[string]any{"serverAltDNSNames": names}
	}
	if err := a.setMonitoring(spec); err != nil {
		return err
	}
	a.Object["spec"] = spec
	if a.in.schemaImportSource != "" {
		if err := mergeExternalCluster(a.Object, a.externalCluster(a.in.sources[a.in.schemaImportSource])); err != nil {
			return err
		}
	}
	if err := a.reconcileDatabases(owner); err != nil {
		return err
	}
	return a.reconcileReplication()
}

// postInitSQL creates the role that carries the privileges delegated to the customer user.
// Role delegation (reserved connections, createrole_self_grant) only exists from PostgreSQL 16.
func postInitSQL(major uint64) []string {
	statements := []string{
		fmt.Sprintf("CREATE ROLE %s NOLOGIN;", adminRoleName),
		fmt.Sprintf("GRANT pg_monitor, pg_signal_backend TO %s WITH ADMIN OPTION;", adminRoleName),
		fmt.Sprintf("GRANT pg_read_all_data, pg_write_all_data TO %s WITH ADMIN OPTION;", adminRoleName),
	}
	if major >= minPGMajorForRoleDelegation {
		statements = append(statements,
			fmt.Sprintf("GRANT pg_use_reserved_connections TO %s WITH ADMIN OPTION;", adminRoleName))
	}
	return append(statements,
		// pg_maintain only exists from PG17. A single-quoted body avoids $$ quoting.
		fmt.Sprintf("DO 'BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ''pg_maintain'') "+
			"THEN GRANT pg_maintain TO %s WITH ADMIN OPTION; END IF; END';", adminRoleName),
		"REVOKE CONNECT, TEMPORARY ON DATABASE postgres, template1 FROM PUBLIC;",
	)
}

// postInitApplicationSQL restricts the application database to its owner. Identifiers are
// validated by readUserSecret before they reach this SQL.
func postInitApplicationSQL(database, owner string) []string {
	return []string{
		// REVOKE CREATE only: REVOKE ALL also strips USAGE and breaks object resolution.
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC;",
		fmt.Sprintf("REVOKE CONNECT, TEMPORARY ON DATABASE %s FROM PUBLIC;", database),
		fmt.Sprintf("GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s;", database, owner),
		fmt.Sprintf("GRANT ALL ON SCHEMA public TO %s;", owner),
	}
}

// postgresqlParameters merges the platform defaults with spec.engine.config; the user wins.
func postgresqlParameters(config string, major uint64) map[string]any {
	parameters := map[string]any{}
	if major >= minPGMajorForRoleDelegation {
		parameters["createrole_self_grant"] = "set, inherit"
		parameters["reserved_connections"] = "3"
	}
	for key, value := range ParsePostgreSQLParameters(config) {
		parameters[key] = value
	}
	return parameters
}

// EngineFeatures fails when engine features are requested: none apply to CNPG.
func (a *applier) EngineFeatures() error {
	if a.DB.Spec.EngineFeatures != nil {
		return errors.New("engineFeatures are not supported by the CloudNativePG provider")
	}
	return nil
}

// Proxy decides the entrypoint of the cluster.
//
//   - no proxy.type: clients connect to the "-rw" Service CNPG creates;
//   - proxy.type=pgbouncer: Everest creates Poolers and the entrypoint moves to the pooler.
//
// proxy.expose applies to the entrypoint. Exposing "-rw" while a pooler is enabled would open a
// path around the connection limit the pooler exists to enforce.
func (a *applier) Proxy() error {
	proxy := a.DB.Spec.Proxy
	if proxy.Storage != nil {
		return errProxyStorageUnsupported
	}
	if proxy.Type != "" && proxy.Type != everestv1alpha1.ProxyTypePGBouncer {
		return fmt.Errorf("CloudNativePG supports only %q as spec.proxy.type", everestv1alpha1.ProxyTypePGBouncer)
	}
	serviceTemplate, err := a.exposeServiceTemplate()
	if err != nil {
		return err
	}
	if err := a.reconcilePoolers(serviceTemplate); err != nil {
		return err
	}
	var additional []any
	if !poolerEnabled(a.DB) && serviceTemplate != nil {
		additional = append(additional, map[string]any{
			"selectorType":    "rw",
			"updateStrategy":  "patch",
			"serviceTemplate": serviceTemplate,
		})
	}
	if len(a.in.replicationExpose) != 0 {
		additional = append(additional, map[string]any{
			"selectorType":   "rw",
			"updateStrategy": "patch",
			"serviceTemplate": map[string]any{
				"metadata": map[string]any{"name": replicationServiceName(a.DB.GetName())},
				"spec": map[string]any{
					"type":                     string(corev1.ServiceTypeLoadBalancer),
					"externalTrafficPolicy":    string(corev1.ServiceExternalTrafficPolicyLocal),
					"loadBalancerSourceRanges": toAnySlice(a.in.replicationExpose),
				},
			},
		})
	}
	if len(additional) == 0 {
		return nil
	}
	return unstructured.SetNestedSlice(a.Object, additional, "spec", "managed", "services", "additional")
}

func externalServiceName(dbName string) string {
	return dbName + "-rw-external"
}

// replicationServiceName is the LoadBalancer of AnnotationReplicationExpose: straight to the
// primary, bypassing PgBouncer.
func replicationServiceName(dbName string) string {
	return dbName + "-rw-replication"
}

// Monitoring fails when PMM monitoring is requested; CNPG metrics are exported natively.
func (a *applier) Monitoring() error {
	if a.DB.Spec.Monitoring != nil && a.DB.Spec.Monitoring.MonitoringConfigName != "" {
		return errors.New("MonitoringConfig/PMM is not supported by the CloudNativePG provider")
	}
	return nil
}

// PodSchedulingPolicy maps the PostgreSQL engine affinity of a PodSchedulingPolicy onto the CNPG
// Cluster. CNPG reuses policies of engineType "postgresql": the affinity schema is the same.
func (a *applier) PodSchedulingPolicy() error {
	if a.DB.Spec.PodSchedulingPolicyName == "" {
		return nil
	}
	policy, err := common.GetPodSchedulingPolicy(a.ctx, a.C, a.DB.Spec.PodSchedulingPolicyName)
	if err != nil {
		return err
	}
	if policy.Spec.EngineType != everestv1alpha1.DatabaseEnginePostgresql {
		return fmt.Errorf("PodSchedulingPolicy %q has engineType %q; CloudNativePG uses %q policies",
			policy.GetName(), policy.Spec.EngineType, everestv1alpha1.DatabaseEnginePostgresql)
	}
	if policy.Spec.AffinityConfig == nil || policy.Spec.AffinityConfig.PostgreSQL == nil ||
		policy.Spec.AffinityConfig.PostgreSQL.Engine == nil {
		return nil
	}
	engineAffinity := policy.Spec.AffinityConfig.PostgreSQL.Engine
	affinity := map[string]any{}
	convert := func(key string, value any) error {
		converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
		if err != nil {
			return fmt.Errorf("convert %s: %w", key, err)
		}
		affinity[key] = converted
		return nil
	}
	if engineAffinity.NodeAffinity != nil {
		if err := convert("nodeAffinity", engineAffinity.NodeAffinity); err != nil {
			return err
		}
	}
	if engineAffinity.PodAffinity != nil {
		if err := convert("additionalPodAffinity", engineAffinity.PodAffinity); err != nil {
			return err
		}
	}
	if engineAffinity.PodAntiAffinity != nil {
		if err := convert("additionalPodAntiAffinity", engineAffinity.PodAntiAffinity); err != nil {
			return err
		}
	}
	if len(affinity) == 0 {
		return nil
	}
	return unstructured.SetNestedMap(a.Object, affinity, "spec", "affinity")
}

// Backup archives WAL and base backups through the Barman Cloud Plugin, and mirrors
// spec.backup.schedules into CNPG ScheduledBackups.
func (a *applier) Backup() error {
	storageName, err := a.backupStorageName()
	if err != nil {
		return err
	}
	if storageName != "" {
		storage, err := getBackupStorage(a.ctx, a.C, a.DB.GetNamespace(), storageName)
		if err != nil {
			return err
		}
		// The BackupStorage controller normally created the store already; reconcile it here so
		// the cluster never points to a store that does not exist.
		if err := ReconcileSharedObjectStore(a.ctx, a.C, storage); err != nil {
			return err
		}
		if err := unstructured.SetNestedSlice(a.Object,
			[]any{archiverPluginConfiguration(SharedObjectStoreName(storageName), ServerName(a.DB))},
			"spec", "plugins"); err != nil {
			return err
		}
	}
	return a.reconcileScheduledBackups()
}

// DataSource bootstraps the cluster from a backup (restore / PITR). CNPG can only restore while
// creating a cluster, never in place.
//
//   - dbClusterBackupName: reads the source cluster's directory in the shared ObjectStore;
//   - backupSource.path: reads an arbitrary path through a recovery ObjectStore owned by the
//     cluster. A path in the form ".../<serverName>/base/<backupID>" (the destination Everest
//     reports for CNPG backups) selects that exact backup.
//
// Without PITR the cluster is restored to the end of the chosen backup, as for other engines;
// PITR "latest" replays all archived WAL.
func (a *applier) DataSource() error {
	ds := a.DB.Spec.DataSource
	if a.in.replicaSource != "" {
		if ds != nil {
			return errors.New("CloudNativePG can not bootstrap from both spec.dataSource and a replica source")
		}
		return a.replicaCluster()
	}
	if ds == nil {
		return nil
	}
	if a.in.schemaImportSource != "" {
		return errors.New("CloudNativePG can not bootstrap from both spec.dataSource and a schema import")
	}
	if a.DB.Status.Status == everestv1alpha1.AppStateReady {
		// Restored: record the DatabaseClusterRestore and drop the data source once it completes.
		if err := common.ReconcileDBRestoreFromDataSource(a.ctx, a.C, a.DB); err != nil {
			return err
		}
		if a.DB.Spec.DataSource == nil {
			return nil
		}
	}

	var (
		source   recoverySource
		backup   *everestv1alpha1.DatabaseClusterBackup
		sourceDB *everestv1alpha1.DatabaseCluster
		err      error
	)
	switch {
	case ds.DBClusterBackupName != "":
		backup, sourceDB, err = a.sourceBackup(ds.DBClusterBackupName)
		if err != nil {
			return err
		}
		source, err = a.recoverySourceFromBackup(backup, sourceDB)
	case ds.BackupSource != nil:
		source, err = a.recoverySourceFromPath(ds.BackupSource)
	default:
		return errors.New("either dbClusterBackupName or backupSource must be specified")
	}
	if err != nil {
		return err
	}

	target, err := recoveryTarget(ds.PITR, backup, source.backupID)
	if err != nil {
		return err
	}
	recovery := map[string]any{"source": source.name}
	if len(target) != 0 {
		recovery["recoveryTarget"] = target
	}
	// Keep the owner and database of the user Secret, as a fresh initdb would.
	if secretName := a.DB.Spec.Engine.UserSecretsName; secretName != "" {
		owner, database, err := a.readUserSecret(secretName)
		if err != nil {
			return err
		}
		recovery["owner"] = owner
		recovery["database"] = database
		recovery["secret"] = map[string]any{"name": secretName}
	}
	if err := unstructured.SetNestedMap(a.Object, map[string]any{"recovery": recovery}, "spec", "bootstrap"); err != nil {
		return err
	}
	return mergeExternalCluster(a.Object, map[string]any{
		"name":   source.name,
		"plugin": recoveryPluginConfiguration(source.objectStore, source.serverName),
	})
}

type recoverySource struct {
	name        string
	objectStore string
	serverName  string
	backupID    string
}

// recoveryTarget builds the CNPG recoveryTarget.
//   - no PITR: stop at the end of the chosen backup (targetImmediate);
//   - PITR latest: no target, replay all WAL;
//   - PITR date: stop at that time, which can not be before the end of the backup. An upper
//     bound is only checked when latestRestorableTime was measured; otherwise CNPG reports a
//     target beyond the archived WAL.
func recoveryTarget(pitr *everestv1alpha1.PITR, backup *everestv1alpha1.DatabaseClusterBackup, backupID string) (map[string]any, error) {
	if pitr == nil {
		if backupID == "" {
			return nil, nil //nolint:nilnil
		}
		return map[string]any{"backupID": backupID, "targetImmediate": true}, nil
	}
	switch pitr.Type {
	case everestv1alpha1.PITRTypeLatest:
		return nil, nil //nolint:nilnil
	case everestv1alpha1.PITRTypeDate:
		if pitr.Date == nil {
			return nil, errors.New("pitr.date is required when pitr.type is date")
		}
		targetTime := pitr.Date.UTC()
		if backup != nil {
			if completed := backup.Status.CompletedAt; completed != nil && targetTime.Before(completed.Time) {
				return nil, fmt.Errorf("pitr.date %s is before the end of backup %q (%s)",
					targetTime.Format(time.RFC3339), backup.GetName(), completed.UTC().Format(time.RFC3339))
			}
			if latest := backup.Status.LatestRestorableTime; latest != nil && targetTime.After(latest.Time) {
				return nil, fmt.Errorf("pitr.date %s is after the latest restorable time %s",
					targetTime.Format(time.RFC3339), latest.UTC().Format(time.RFC3339))
			}
		}
		target := map[string]any{"targetTime": targetTime.Format(time.RFC3339)}
		if backupID != "" {
			target["backupID"] = backupID
		}
		return target, nil
	default:
		return nil, fmt.Errorf("unsupported pitr.type %q for CloudNativePG", pitr.Type)
	}
}

// ParseBackupPath splits a barman backup path "<destinationPath>/<serverName>/base/<backupID>".
func ParseBackupPath(path string) (string, string, string, bool) {
	prefix, backupID, found := strings.Cut(strings.TrimRight(path, "/"), "/base/")
	if !found || backupID == "" || strings.Contains(backupID, "/") {
		return "", "", "", false
	}
	lastSlash := strings.LastIndex(prefix, "/")
	if lastSlash < 0 || lastSlash == len(prefix)-1 {
		return "", "", "", false
	}
	return prefix[:lastSlash], prefix[lastSlash+1:], backupID, true
}

// BackupPath is the inverse of ParseBackupPath.
func BackupPath(destinationPath, serverName, backupID string) string {
	return fmt.Sprintf("%s/%s/base/%s", strings.TrimRight(destinationPath, "/"), serverName, backupID)
}

// DataImport fails: DataImportJobs are not supported by CNPG.
func (a *applier) DataImport() error {
	if pointer.Get(a.DB.Spec.DataSource).DataImport != nil {
		return errors.New("data import is not supported by the CloudNativePG provider")
	}
	return nil
}

// setImage selects the operand image. Plain PostgreSQL takes the digest-pinned image published by
// the ClusterImageCatalog; the TimescaleDB flavor references its own catalog by major.
func (a *applier) setImage(spec map[string]any, desired *semver.Version) error {
	if a.isTimescale() {
		currentMajor, found, _ := unstructured.NestedInt64(a.Object, "spec", "imageCatalogRef", "major")
		if found && uint64(currentMajor) != desired.Major() {
			return fmt.Errorf("CloudNativePG does not support PostgreSQL major upgrades: current=%d desired=%d",
				currentMajor, desired.Major())
		}
		spec["imageCatalogRef"] = map[string]any{
			"apiGroup": apiGroup,
			"kind":     imageCatalogKind,
			"name":     timescaleImageCatalogName,
			"major":    int64(desired.Major()),
		}
		return nil
	}

	currentImage, _, _ := unstructured.NestedString(a.Object, "spec", "imageName")
	if err := validateVersionChange(currentImage, desired); err != nil {
		return err
	}
	switch component := a.DBEngine.Status.AvailableVersions.Engine[a.DB.Spec.Engine.Version]; {
	case component != nil && component.ImagePath != "":
		spec["imageName"] = component.ImagePath
	case currentImage != "":
		// The version was withdrawn from the catalog while this cluster runs it. Withdrawing a
		// version blocks new clusters; it must not force a rolling update on running ones.
		spec["imageName"] = currentImage
	default:
		return fmt.Errorf("PostgreSQL version %q is not available in ClusterImageCatalog %q",
			a.DB.Spec.Engine.Version, imageCatalogName)
	}
	return nil
}

func (a *applier) setStorage(spec map[string]any) error {
	engine := a.DB.Spec.Engine
	currentSize, err := a.currentStorageSize()
	if err != nil {
		return err
	}
	storage := map[string]any{}
	if err := common.ConfigureStorage(a.ctx, a.C, a.DB, currentSize, engine.Storage.Size, engine.Storage.Class,
		func(size resource.Quantity, storageClass *string) {
			storage["size"] = size.String()
			if storageClass != nil && *storageClass != "" {
				storage["storageClass"] = *storageClass
			}
		}); err != nil {
		return fmt.Errorf("configure CloudNativePG storage: %w", err)
	}
	spec["storage"] = storage

	// CNPG can add a WAL volume to a running cluster but never remove it, so an existing one is
	// kept even if the operator setting was removed afterwards.
	if walStorage, found, _ := unstructured.NestedMap(a.Object, "spec", "walStorage"); found {
		spec["walStorage"] = walStorage
		return nil
	}
	if size := os.Getenv(walStorageSizeEnv); size != "" {
		if _, err := resource.ParseQuantity(size); err != nil {
			return fmt.Errorf("invalid %s=%q: %w", walStorageSizeEnv, size, err)
		}
		walStorage := map[string]any{"size": size, "resizeInUseVolumes": true}
		if class := os.Getenv(walStorageClassEnv); class != "" {
			walStorage["storageClass"] = class
		}
		spec["walStorage"] = walStorage
	}
	return nil
}

// setBootstrapAndRoles bootstraps the application database and keeps the customer user as a
// CNPG managed role, so a password change in the user Secret reaches PostgreSQL.
func (a *applier) setBootstrapAndRoles(spec map[string]any, major uint64) (string, error) {
	secretName := a.DB.Spec.Engine.UserSecretsName
	owner, database, err := a.readUserSecret(secretName)
	if err != nil {
		return "", err
	}
	initdb := map[string]any{
		"database":               database,
		"owner":                  owner,
		"dataChecksums":          true,
		"secret":                 map[string]any{"name": secretName},
		"postInitSQL":            toAnySlice(postInitSQL(major)),
		"postInitTemplateSQL":    []any{"REVOKE CREATE ON SCHEMA public FROM PUBLIC;"},
		"postInitApplicationSQL": toAnySlice(postInitApplicationSQL(database, owner)),
	}
	if a.in.schemaImportSource != "" {
		// Import only the schema of the application database; the data follows through a
		// subscription, so the source keeps serving writes until the cutover.
		initdb["import"] = map[string]any{
			"type":       "microservice",
			"schemaOnly": true,
			"databases":  []any{database},
			"source":     map[string]any{"externalCluster": externalClusterName(a.in.schemaImportSource)},
		}
	}
	role := map[string]any{
		"name":      owner,
		"ensure":    "present",
		"login":     true,
		"superuser": false,
		// A subscriber outside the cluster (replication-expose) logs in as the owner.
		"replication":     len(a.in.replicationExpose) != 0,
		"bypassrls":       false,
		"createdb":        false,
		"createrole":      major >= minPGMajorForRoleDelegation,
		"inherit":         true,
		"connectionLimit": int64(-1),
		"inRoles":         []any{adminRoleName},
		"passwordSecret":  map[string]any{"name": secretName},
	}
	if a.in.replicaSource == "" {
		spec["bootstrap"] = map[string]any{"initdb": initdb}
	} else {
		// A replica is cloned by pg_basebackup (DataSource): initdb and its postInitSQL never run,
		// so the admin role does not exist and granting it would stall CNPG's role reconciler,
		// and with it the pooler auth user, once the replica is promoted.
		delete(role, "inRoles")
	}
	spec["managed"] = map[string]any{"roles": []any{role}}
	return owner, nil
}

// readUserSecret reads the application owner and database from the user Secret and labels the
// Secret so CNPG watches it.
func (a *applier) readUserSecret(secretName string) (string, string, error) {
	secret := &corev1.Secret{}
	if err := a.C.Get(a.ctx, types.NamespacedName{Namespace: a.DB.GetNamespace(), Name: secretName}, secret); err != nil {
		return "", "", fmt.Errorf("get user secret %q: %w", secretName, err)
	}
	owner := strings.TrimSpace(string(secret.Data[corev1.BasicAuthUsernameKey]))
	switch {
	case owner == "":
		return "", "", fmt.Errorf("user secret %q must contain a %q key", secretName, corev1.BasicAuthUsernameKey)
	case owner == superuserName:
		return "", "", fmt.Errorf("user secret %q names the superuser %q; CloudNativePG requires an unprivileged owner",
			secretName, superuserName)
	case !postgresIdentifier.MatchString(owner):
		return "", "", fmt.Errorf("user secret %q: username %q is not a valid PostgreSQL identifier", secretName, owner)
	}
	database := strings.TrimSpace(string(secret.Data[userSecretDatabaseKey]))
	if database == "" {
		database = defaultAppDatabase
	}
	if !postgresIdentifier.MatchString(database) {
		return "", "", fmt.Errorf("user secret %q: database %q is not a valid PostgreSQL identifier", secretName, database)
	}
	if _, found := secret.GetLabels()[cnpgReloadLabel]; !found {
		patched := secret.DeepCopy()
		labels := patched.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[cnpgReloadLabel] = "true"
		patched.SetLabels(labels)
		if err := a.C.Patch(a.ctx, patched, client.MergeFrom(secret)); err != nil {
			return "", "", fmt.Errorf("label user secret %q with %s: %w", secretName, cnpgReloadLabel, err)
		}
	}
	return owner, database, nil
}

func (a *applier) setMonitoring(spec map[string]any) error {
	// Without the Prometheus Operator CRD, CNPG still exposes metrics on port 9187.
	installed, err := CRDInstalled(a.ctx, a.C, podMonitorCRDName)
	if err != nil {
		return fmt.Errorf("check PodMonitor CRD: %w", err)
	}
	if installed {
		spec["monitoring"] = map[string]any{"enablePodMonitor": true}
	}
	return nil
}

func (a *applier) isTimescale() bool {
	return a.DB.GetAnnotations()[AnnotationExtension] == ExtensionTimescaleDB
}

func (a *applier) serverAltDNSNames() []any {
	var names []any
	for name := range strings.SplitSeq(a.DB.GetAnnotations()[AnnotationServerAltDNSNames], ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// exposeServiceTemplate translates spec.proxy.expose. It returns nil for internal exposure, where
// the default ClusterIP Service is enough.
func (a *applier) exposeServiceTemplate() (map[string]any, error) {
	expose := a.DB.Spec.Proxy.Expose
	var serviceType corev1.ServiceType
	switch expose.Type.Normalize() {
	case "", everestv1alpha1.ExposeTypeClusterIP:
		return nil, nil //nolint:nilnil
	case everestv1alpha1.ExposeTypeLoadBalancer:
		serviceType = corev1.ServiceTypeLoadBalancer
	case everestv1alpha1.ExposeTypeNodePort:
		serviceType = corev1.ServiceTypeNodePort
	default:
		return nil, fmt.Errorf("unsupported CloudNativePG expose type %q", expose.Type)
	}

	metadata := map[string]any{"name": externalServiceName(a.DB.GetName())}
	if poolerEnabled(a.DB) {
		// A Pooler names its Service after itself; overriding it would make CNPG lose track.
		metadata = map[string]any{}
	}
	if expose.LoadBalancerConfigName != "" {
		config := &everestv1alpha1.LoadBalancerConfig{}
		if err := a.C.Get(a.ctx, types.NamespacedName{Name: expose.LoadBalancerConfigName}, config); err != nil {
			return nil, fmt.Errorf("get LoadBalancerConfig %q: %w", expose.LoadBalancerConfigName, err)
		}
		metadata["annotations"] = stringMapToAny(config.Spec.Annotations)
	}
	serviceSpec := map[string]any{
		"type": string(serviceType),
		// Keep the client source IP, so NetworkPolicies can tell external clients from pods of
		// other namespaces.
		"externalTrafficPolicy": string(corev1.ServiceExternalTrafficPolicyLocal),
	}
	if ranges := expose.IPSourceRangesStringArray(); len(ranges) != 0 {
		serviceSpec["loadBalancerSourceRanges"] = toAnySlice(ranges)
	}
	return map[string]any{"metadata": metadata, "spec": serviceSpec}, nil
}

// backupStorageName returns the single storage used by the cluster. CNPG archives WAL to one
// destination, so schedules, PITR and pending on-demand backups must agree.
func (a *applier) backupStorageName() (string, error) {
	names := map[string]struct{}{}
	for _, schedule := range a.DB.Spec.Backup.Schedules {
		if schedule.RetentionCopies != 0 {
			return "", fmt.Errorf("schedule %q: CloudNativePG does not support retentionCopies; retention is the "+
				"time-based recovery window of the ObjectStore", schedule.Name)
		}
		if schedule.Enabled {
			names[schedule.BackupStorageName] = struct{}{}
		}
	}
	if pitr := a.DB.Spec.Backup.PITR; pitr.Enabled {
		if pitr.BackupStorageName == nil || *pitr.BackupStorageName == "" {
			return "", errors.New("backup.pitr.backupStorageName is required for CloudNativePG")
		}
		names[*pitr.BackupStorageName] = struct{}{}
	}
	backups := &everestv1alpha1.DatabaseClusterBackupList{}
	if err := a.C.List(a.ctx, backups, client.InNamespace(a.DB.GetNamespace())); err != nil {
		return "", fmt.Errorf("list DatabaseClusterBackups: %w", err)
	}
	for _, backup := range backups.Items {
		if backup.Spec.DBClusterName == a.DB.GetName() && !backup.HasCompleted() {
			names[backup.Spec.BackupStorageName] = struct{}{}
		}
	}
	if len(names) > 1 {
		return "", errors.New("CloudNativePG supports one backup destination per cluster; " +
			"all schedules, PITR and backups must use the same BackupStorage")
	}
	for name := range names {
		return name, nil
	}
	return "", nil
}

// reconcileScheduledBackups creates or deletes CNPG ScheduledBackups for spec.backup.schedules.
// With backupOwnerReference "none" the produced Backups have no owner, so the backup controller
// adopts them as DatabaseClusterBackups.
func (a *applier) reconcileScheduledBackups() error {
	for _, schedule := range a.DB.Spec.Backup.Schedules {
		name := a.DB.GetName() + "-" + schedule.Name
		object := newUnstructured(ScheduledBackupGVK, a.DB.GetNamespace(), name)
		if !schedule.Enabled {
			if err := a.C.Delete(a.ctx, object); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete ScheduledBackup %q: %w", name, err)
			}
			continue
		}
		// CNPG uses six-field cron (with seconds); Everest uses five.
		cron := schedule.Schedule
		switch len(strings.Fields(cron)) {
		case 5: //nolint:mnd
			cron = "0 " + cron
		case 6: //nolint:mnd
		default:
			return fmt.Errorf("schedule %q must contain five or six cron fields", schedule.Name)
		}
		if _, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
			object.SetLabels(map[string]string{
				BackupStorageLabel:              schedule.BackupStorageName,
				ScheduleNameLabel:               schedule.Name,
				consts.DatabaseClusterNameLabel: a.DB.GetName(),
			})
			object.Object["spec"] = map[string]any{
				"cluster":              map[string]any{"name": a.DB.GetName()},
				"schedule":             cron,
				"backupOwnerReference": "none",
				"method":               "plugin",
				"pluginConfiguration":  map[string]any{"name": barmanPluginName},
			}
			return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
		}); err != nil {
			return fmt.Errorf("reconcile ScheduledBackup %q: %w", name, err)
		}
	}
	return nil
}

func (a *applier) sourceBackup(name string) (*everestv1alpha1.DatabaseClusterBackup, *everestv1alpha1.DatabaseCluster, error) {
	backup := &everestv1alpha1.DatabaseClusterBackup{}
	if err := a.C.Get(a.ctx, types.NamespacedName{Namespace: a.DB.GetNamespace(), Name: name}, backup); err != nil {
		return nil, nil, fmt.Errorf("get DatabaseClusterBackup %q: %w", name, err)
	}
	if backup.Status.State != everestv1alpha1.BackupSucceeded {
		return nil, nil, fmt.Errorf("backup %q has not succeeded yet (state: %s)", name, backup.Status.State)
	}
	sourceDB := &everestv1alpha1.DatabaseCluster{}
	if err := a.C.Get(a.ctx, types.NamespacedName{Namespace: a.DB.GetNamespace(), Name: backup.Spec.DBClusterName}, sourceDB); err != nil {
		return nil, nil, fmt.Errorf("get source DatabaseCluster %q (restore from its backup path with "+
			"backupSource if it was deleted): %w", backup.Spec.DBClusterName, err)
	}
	return backup, sourceDB, nil
}

func (a *applier) recoverySourceFromBackup(
	backup *everestv1alpha1.DatabaseClusterBackup,
	sourceDB *everestv1alpha1.DatabaseCluster,
) (recoverySource, error) {
	storage, err := getBackupStorage(a.ctx, a.C, a.DB.GetNamespace(), backup.Spec.BackupStorageName)
	if err != nil {
		return recoverySource{}, err
	}
	if err := ReconcileSharedObjectStore(a.ctx, a.C, storage); err != nil {
		return recoverySource{}, err
	}
	backupID, err := a.cnpgBackupID(backup.GetName())
	if err != nil {
		return recoverySource{}, err
	}
	return recoverySource{
		name:        sourceDB.GetName(),
		objectStore: SharedObjectStoreName(storage.GetName()),
		serverName:  ServerName(sourceDB),
		backupID:    backupID,
	}, nil
}

func (a *applier) recoverySourceFromPath(bs *everestv1alpha1.BackupSource) (recoverySource, error) {
	storage, err := getBackupStorage(a.ctx, a.C, a.DB.GetNamespace(), bs.BackupStorageName)
	if err != nil {
		return recoverySource{}, err
	}
	destinationPath, serverName, backupID := bs.Path, a.DB.GetName(), ""
	if parsedPath, parsedServer, parsedID, ok := ParseBackupPath(bs.Path); ok {
		destinationPath, serverName, backupID = parsedPath, parsedServer, parsedID
	}
	objectStore, err := a.reconcileRecoveryObjectStore(storage, destinationPath)
	if err != nil {
		return recoverySource{}, err
	}
	return recoverySource{name: serverName, objectStore: objectStore, serverName: serverName, backupID: backupID}, nil
}

// cnpgBackupID returns the barman backup ID of the CNPG Backup that shares the name of the
// Everest DatabaseClusterBackup.
func (a *applier) cnpgBackupID(name string) (string, error) {
	backup := newUnstructured(BackupGVK, a.DB.GetNamespace(), name)
	if err := a.C.Get(a.ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
		return "", fmt.Errorf("get CNPG Backup %q: %w", name, err)
	}
	id, _, _ := unstructured.NestedString(backup.Object, "status", "backupId")
	if id == "" {
		return "", fmt.Errorf("CNPG Backup %q has no backupId yet", name)
	}
	return id, nil
}

func (a *applier) currentStorageSize() (resource.Quantity, error) {
	size, found, err := unstructured.NestedString(a.Object, "spec", "storage", "size")
	if err != nil || !found || size == "" {
		return resource.Quantity{}, err
	}
	current, err := resource.ParseQuantity(size)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("parse current CloudNativePG storage size %q: %w", size, err)
	}
	return current, nil
}

// validateVersionChange rejects major upgrades and downgrades: CNPG rolling updates only
// support minor versions.
func validateVersionChange(currentImage string, desired *semver.Version) error {
	if currentImage == "" {
		return nil
	}
	currentVersion, err := versionFromImage(currentImage)
	if err != nil {
		return fmt.Errorf("cannot determine the running PostgreSQL version: %w", err)
	}
	current, err := semver.NewVersion(currentVersion)
	if err != nil {
		return err
	}
	if desired.Major() != current.Major() {
		return fmt.Errorf("CloudNativePG does not support PostgreSQL major upgrades: current=%s desired=%s",
			current, desired)
	}
	if desired.LessThan(current) {
		return fmt.Errorf("CloudNativePG does not support PostgreSQL downgrades: current=%s desired=%s", current, desired)
	}
	return nil
}

// ParsePostgreSQLParameters parses spec.engine.config (postgresql.conf lines) into CNPG
// postgresql.parameters.
func ParsePostgreSQLParameters(config string) map[string]string {
	parameters := map[string]string{}
	for line := range strings.SplitSeq(config, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		parameters[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "'\"")
	}
	return parameters
}

func stringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i := range in {
		out[i] = in[i]
	}
	return out
}

// Connection pooling (PgBouncer) through the CNPG Pooler CRD, enabled by
// spec.proxy.type=pgbouncer. The pooler is not needed for HA ("-rw" already follows the primary);
// it bounds the number of PostgreSQL connections.

const (
	poolModeSession     = "session"
	poolModeTransaction = "transaction"
	// Number of instances from which a read-only pooler is added.
	minReplicasForReadPooler = 2
)

// PoolerGVK is the GroupVersionKind of the CNPG Pooler.
var PoolerGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: poolerKind}

// poolerName is the read-write pooler, pointing to the primary.
func poolerName(dbName string) string {
	return dbName + "-pooler-rw"
}

// readPoolerName is the read-only pooler, pointing to the standbys only.
func readPoolerName(dbName string) string {
	return dbName + "-pooler-ro"
}

func poolerEnabled(db *everestv1alpha1.DatabaseCluster) bool {
	return db.Spec.Proxy.Type == everestv1alpha1.ProxyTypePGBouncer
}

// ParsePoolMode reads spec.proxy.config, which only accepts "pool_mode = session|transaction".
// Exported for the validating webhook.
func ParsePoolMode(config string) (string, error) {
	poolMode := poolModeSession
	for line := range strings.SplitSeq(config, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return "", fmt.Errorf("invalid spec.proxy.config line %q: expected key = value", line)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key != "pool_mode" {
			return "", fmt.Errorf("unsupported key %q in spec.proxy.config: only pool_mode is supported", key)
		}
		if value != poolModeSession && value != poolModeTransaction {
			return "", fmt.Errorf("invalid pool_mode %q in spec.proxy.config: must be %q or %q",
				value, poolModeSession, poolModeTransaction)
		}
		poolMode = value
	}
	return poolMode, nil
}

// reconcilePoolers creates, updates or deletes the rw and ro Poolers. Poolers are owned by the
// DatabaseCluster, so Kubernetes removes them together with the cluster.
func (a *applier) reconcilePoolers(serviceTemplate map[string]any) error {
	poolMode, err := ParsePoolMode(a.DB.Spec.Proxy.Config)
	if err != nil {
		return err
	}
	enabled := poolerEnabled(a.DB)
	if err := a.reconcilePooler(poolerName(a.DB.GetName()), "rw", enabled, poolMode, serviceTemplate); err != nil {
		return err
	}
	readEnabled := enabled && a.DB.Spec.Engine.Replicas >= minReplicasForReadPooler
	return a.reconcilePooler(readPoolerName(a.DB.GetName()), "ro", readEnabled, poolMode, serviceTemplate)
}

func (a *applier) reconcilePooler(name, selectorType string, enabled bool, poolMode string, serviceTemplate map[string]any) error {
	object := newUnstructured(PoolerGVK, a.DB.GetNamespace(), name)
	if !enabled {
		if err := a.C.Delete(a.ctx, object); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete Pooler %q: %w", name, err)
		}
		// The PDB is owned by the DatabaseCluster, not the Pooler, so it must be removed explicitly.
		return a.reconcilePoolerPDB(name, 0)
	}

	instances := int64(1)
	if a.DB.Spec.Proxy.Replicas != nil {
		instances = int64(*a.DB.Spec.Proxy.Replicas)
	}

	// Idle server connections keep a database "in use", which delays DROP DATABASE; 60s instead
	// of the 600s default keeps that short at the cost of a few ms for sparse clients.
	parameters := map[string]any{"server_idle_timeout": "60"}
	if poolMode == poolModeTransaction {
		// Without it, drivers using prepared statements (JDBC, asyncpg, pgx) fail randomly.
		parameters["max_prepared_statements"] = "200"
	}
	if serviceTemplate != nil {
		// Outside the cluster TLS is mandatory; CNPG's default "prefer" would accept plaintext.
		parameters["client_tls_sslmode"] = "require"
	}
	pgbouncer := map[string]any{"poolMode": poolMode, "parameters": parameters}
	// The pgbouncer image comes from the ClusterImageCatalog, like the operand image.
	if component := a.DBEngine.Status.AvailableVersions.
		Proxy[everestv1alpha1.ProxyTypePGBouncer][a.DB.Spec.Engine.Version]; component != nil && component.ImagePath != "" {
		pgbouncer["image"] = component.ImagePath
	}

	spec := map[string]any{
		"cluster":   map[string]any{"name": a.DB.GetName()},
		"type":      selectorType,
		"instances": instances,
		"pgbouncer": pgbouncer,
	}
	if serviceTemplate != nil {
		spec["serviceTemplate"] = serviceTemplate
	}
	resourceRequirements := a.DB.Spec.Proxy.Resources.ToResourceRequirements()
	if len(resourceRequirements.Limits) != 0 || len(resourceRequirements.Requests) != 0 {
		resources, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&resourceRequirements)
		if err != nil {
			return fmt.Errorf("convert proxy resources: %w", err)
		}
		// The container must be named "pgbouncer" for CNPG to merge the pod template.
		spec["template"] = map[string]any{
			"spec": map[string]any{
				"containers": []any{map[string]any{"name": "pgbouncer", "resources": resources}},
			},
		}
	}

	if _, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile Pooler %q: %w", name, err)
	}
	return a.reconcilePoolerPDB(name, instances)
}

// reconcilePoolerPDB keeps at least one pooler endpoint during node drains. CNPG creates PDBs for
// the Cluster but not for Poolers. With a single instance a PDB either protects nothing or blocks
// drains forever, so none is created.
func (a *applier) reconcilePoolerPDB(name string, instances int64) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: a.DB.GetNamespace()},
	}
	if instances < 2 { //nolint:mnd
		if err := a.C.Delete(a.ctx, pdb); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete Pooler PodDisruptionBudget %q: %w", name, err)
		}
		return nil
	}
	maxUnavailable := intstr.FromInt32(1)
	if _, err := controllerutil.CreateOrUpdate(a.ctx, a.C, pdb, func() error {
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{poolerNameLabel: name},
			},
		}
		return controllerutil.SetControllerReference(a.DB, pdb, a.C.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile Pooler PodDisruptionBudget %q: %w", name, err)
	}
	return nil
}

var errProxyStorageUnsupported = errors.New("the CloudNativePG pooler does not support spec.proxy.storage")

// Logical databases, logical replication and replica clusters through CNPG CRDs. They are owned
// by the DatabaseCluster and labelled with it, so entries removed from the annotations are pruned
// and deleting the cluster removes them.

// OwnerLabel names the DatabaseCluster that owns a CNPG Database, Publication or Subscription.
const OwnerLabel = "everest.percona.com/database-cluster"

var (
	// DatabaseGVK is the GroupVersionKind of the CNPG Database.
	DatabaseGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: "Database"}
	// PublicationGVK is the GroupVersionKind of the CNPG Publication.
	PublicationGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: "Publication"}
	// SubscriptionGVK is the GroupVersionKind of the CNPG Subscription.
	SubscriptionGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: "Subscription"}

	// Reports an object deleted to be re-created with a changed immutable field.
	errRecreating = errors.New("recreating because an immutable field changed; retrying")
)

// externalClusterName is the spec.externalClusters entry of a source. The prefix keeps it apart
// from the entry DataSource() generates for a restore.
func externalClusterName(sourceName string) string {
	return "source-" + sourceName
}

// objectName turns a PostgreSQL identifier into a Kubernetes name: "<cluster>-<kind>-<name>".
// PostgreSQL identifiers have no "-", so replacing "_" can not collide.
func objectName(cluster, kind, pgName string) string {
	return cluster + "-" + kind + "-" + strings.ReplaceAll(pgName, "_", "-")
}

// externalCluster builds the spec.externalClusters entry of a source. A source naming a CNPG
// cluster without password authenticates with the client certificate CNPG generates for
// streaming replication.
func (a *applier) externalCluster(src *source) map[string]any {
	namespace := src.clusterNamespace
	if namespace == "" {
		namespace = a.DB.GetNamespace()
	}
	host := src.host
	if host == "" {
		host = fmt.Sprintf("%s-rw.%s.svc", src.cluster, namespace)
	}
	port := src.port
	if port == 0 {
		port = defaultPostgresPort
	}
	parameters := map[string]any{"host": host, "port": strconv.Itoa(port)}
	entry := map[string]any{"name": externalClusterName(src.name), "connectionParameters": parameters}

	certAuth := src.passwordSecret == "" && src.cluster != ""
	sslMode := src.sslmode
	switch {
	case sslMode != "":
	case certAuth:
		sslMode = certAuthSSLMode
	default:
		sslMode = defaultSSLMode
	}
	parameters["sslmode"] = sslMode
	parameters["dbname"] = src.dbname
	if src.dbname == "" {
		// Streaming replication does not use a database; pg_basebackup still needs one to connect.
		parameters["dbname"] = defaultReplicaDBName
	}
	parameters["user"] = src.user

	if certAuth {
		if src.user == "" {
			parameters["user"] = defaultReplicaUser
		}
		clientCert := src.clientCertSecret
		if clientCert == "" {
			clientCert = src.cluster + replicationSecretSuffix
		}
		ca := src.caSecret
		if ca == "" {
			ca = src.cluster + caSecretSuffix
		}
		entry["sslKey"] = map[string]any{"name": clientCert, "key": "tls.key"}
		entry["sslCert"] = map[string]any{"name": clientCert, "key": "tls.crt"}
		entry["sslRootCert"] = map[string]any{"name": ca, "key": "ca.crt"}
		return entry
	}
	key := src.passwordKey
	if key == "" {
		key = defaultPasswordKey
	}
	entry["password"] = map[string]any{"name": src.passwordSecret, "key": key}
	return entry
}

// replicaCluster turns the cluster into a replica of the source: pg_basebackup clones it, then it
// replays WAL read-only. Setting replica-enabled to "false" promotes it. The replica inherits the
// roles of the primary, so no initdb runs.
func (a *applier) replicaCluster() error {
	src := a.in.sources[a.in.replicaSource]
	if err := mergeExternalCluster(a.Object, a.externalCluster(src)); err != nil {
		return err
	}
	name := externalClusterName(src.name)
	basebackup := map[string]any{"source": name}
	if secretName := a.DB.Spec.Engine.UserSecretsName; secretName != "" {
		// Without these CNPG defaults to database and owner "app", and after the promotion
		// fails to set the password of a role the clone does not have.
		owner, database, err := a.readUserSecret(secretName)
		if err != nil {
			return err
		}
		basebackup["database"] = database
		basebackup["owner"] = owner
		basebackup["secret"] = map[string]any{"name": secretName}
	}
	if err := unstructured.SetNestedMap(a.Object, map[string]any{
		"pg_basebackup": basebackup,
	}, "spec", "bootstrap"); err != nil {
		return err
	}
	return unstructured.SetNestedMap(a.Object, map[string]any{
		"enabled": a.in.replicaEnabled,
		"source":  name,
	}, "spec", "replica")
}

// reconcileDatabases creates a CNPG Database per database.<db>.owner annotation. With
// databaseReclaimPolicy "delete", removing the annotation drops the database.
func (a *applier) reconcileDatabases(defaultOwner string) error {
	desired := map[string]struct{}{}
	for _, database := range a.in.databases {
		owner := database.owner
		if owner == "" {
			owner = defaultOwner
		}
		if owner == "" {
			return fmt.Errorf("database %q: owner is required without spec.engine.userSecretsName", database.name)
		}
		object := newUnstructured(DatabaseGVK, a.DB.GetNamespace(), objectName(a.DB.GetName(), "db", database.name))
		desired[object.GetName()] = struct{}{}
		if err := a.applyOwned(object, map[string]any{
			"cluster":               map[string]any{"name": a.DB.GetName()},
			"name":                  database.name,
			"owner":                 owner,
			"ensure":                "present",
			"databaseReclaimPolicy": "delete",
		}); err != nil {
			return fmt.Errorf("reconcile Database %q: %w", database.name, err)
		}
	}
	return a.pruneOwned(DatabaseGVK, desired)
}

// reconcileReplication creates CNPG Publications and Subscriptions. A subscription reads its
// publisher through the spec.externalClusters entry of its source.
func (a *applier) reconcileReplication() error {
	publications := map[string]struct{}{}
	for _, pub := range a.in.publications {
		target := map[string]any{"allTables": true}
		if len(pub.tables) != 0 {
			objects := make([]any, 0, len(pub.tables))
			for _, table := range pub.tables {
				schemaName, tableName, _ := strings.Cut(table, ".")
				objects = append(objects, map[string]any{"table": map[string]any{"schema": schemaName, "name": tableName}})
			}
			target = map[string]any{"objects": objects}
		}
		object := newUnstructured(PublicationGVK, a.DB.GetNamespace(), objectName(a.DB.GetName(), "pub", pub.name))
		publications[object.GetName()] = struct{}{}
		if err := a.recreateIfImmutableChanged(object, pub.name, pub.dbname); err != nil {
			return err
		}
		if err := a.applyOwned(object, map[string]any{
			"cluster": map[string]any{"name": a.DB.GetName()},
			"name":    pub.name,
			"dbname":  pub.dbname,
			"target":  target,
		}); err != nil {
			return fmt.Errorf("reconcile Publication %q: %w", pub.name, err)
		}
	}
	if err := a.pruneOwned(PublicationGVK, publications); err != nil {
		return err
	}

	subscriptions := map[string]struct{}{}
	for _, sub := range a.in.subscriptions {
		if err := mergeExternalCluster(a.Object, a.externalCluster(a.in.sources[sub.source])); err != nil {
			return err
		}
		object := newUnstructured(SubscriptionGVK, a.DB.GetNamespace(), objectName(a.DB.GetName(), "sub", sub.name))
		subscriptions[object.GetName()] = struct{}{}
		if err := a.recreateIfImmutableChanged(object, sub.name, sub.dbname); err != nil {
			return err
		}
		spec := map[string]any{
			"cluster":             map[string]any{"name": a.DB.GetName()},
			"name":                sub.name,
			"dbname":              sub.dbname,
			"publicationName":     sub.publication,
			"externalClusterName": externalClusterName(sub.source),
		}
		// CNPG defaults to retain: removing the subscription keeps the slot on the publisher,
		// which holds WAL until its disk fills. One-off migrations set "delete".
		if sub.reclaimPolicy != "" {
			spec["subscriptionReclaimPolicy"] = sub.reclaimPolicy
		}
		if err := a.applyOwned(object, spec); err != nil {
			return fmt.Errorf("reconcile Subscription %q: %w", sub.name, err)
		}
	}
	return a.pruneOwned(SubscriptionGVK, subscriptions)
}

func (a *applier) applyOwned(object *unstructured.Unstructured, spec map[string]any) error {
	_, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		labels := object.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[OwnerLabel] = a.DB.GetName()
		object.SetLabels(labels)
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	})
	return err
}

// pruneOwned deletes the objects of this cluster that are no longer declared. A CNPG version
// without the CRD is fine as long as nothing is declared.
func (a *applier) pruneOwned(gvk schema.GroupVersionKind, desired map[string]struct{}) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err := a.C.List(a.ctx, list, client.InNamespace(a.DB.GetNamespace()),
		client.MatchingLabels{OwnerLabel: a.DB.GetName()}); err != nil {
		if len(desired) == 0 && meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("list %s: %w", gvk.Kind, err)
	}
	for i := range list.Items {
		item := &list.Items[i]
		if _, ok := desired[item.GetName()]; ok {
			continue
		}
		if err := a.C.Delete(a.ctx, item); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete %s %q: %w", gvk.Kind, item.GetName(), err)
		}
	}
	return nil
}

// recreateIfImmutableChanged deletes a Publication or Subscription whose spec.name or
// spec.dbname (immutable in CNPG) differs from the desired one; the next reconciliation creates it
// again. With the default retain policy this does not drop the object in PostgreSQL.
func (a *applier) recreateIfImmutableChanged(object *unstructured.Unstructured, pgName, dbName string) error {
	existing := newUnstructured(object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	if err := a.C.Get(a.ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !existing.GetDeletionTimestamp().IsZero() {
		return fmt.Errorf("%s %q: %w", object.GetKind(), object.GetName(), errRecreating)
	}
	currentName, _, _ := unstructured.NestedString(existing.Object, "spec", "name")
	currentDB, _, _ := unstructured.NestedString(existing.Object, "spec", "dbname")
	if currentName == pgName && currentDB == dbName {
		return nil
	}
	if err := a.C.Delete(a.ctx, existing); client.IgnoreNotFound(err) != nil {
		return err
	}
	return fmt.Errorf("%s %q: %w", object.GetKind(), object.GetName(), errRecreating)
}

// mergeExternalCluster inserts or replaces a spec.externalClusters entry by name, keeping the
// entries other steps of the same reconciliation added.
func mergeExternalCluster(object, entry map[string]any) error {
	existing, _, err := unstructured.NestedSlice(object, "spec", "externalClusters")
	if err != nil {
		return err
	}
	for i, raw := range existing {
		if item, ok := raw.(map[string]any); ok && item["name"] == entry["name"] {
			existing[i] = entry
			return unstructured.SetNestedSlice(object, existing, "spec", "externalClusters")
		}
	}
	return unstructured.SetNestedSlice(object, append(existing, entry), "spec", "externalClusters")
}
