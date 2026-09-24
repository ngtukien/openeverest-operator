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

// Backups go through the Barman Cloud Plugin (CNPG-I), not the in-tree
// spec.backup.barmanObjectStore:
//   - in-tree runs barman-cloud inside the operand image, which only the deprecated `system`
//     flavor ships; with `standard` images WAL archiving fails silently and PITR is lost;
//   - the plugin injects a sidecar that carries barman-cloud, in its own cgroup;
//   - CNPG deprecated in-tree Barman in 1.26.
//

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=barmancloud.cnpg.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete

var errInPlaceRestore = errors.New("CloudNativePG can not restore in place; " +
	"create a new DatabaseCluster with spec.dataSource")

// EmptyBackupObject returns an unstructured CNPG Backup without name, e.g. to register a watch.
func EmptyBackupObject() *unstructured.Unstructured {
	return newUnstructured(BackupGVK, "", "")
}

// ReconcileBackup creates the CNPG Backup of an on-demand DatabaseClusterBackup and releases the
// backup on deletion. The base backup is taken by the Barman Cloud Plugin; its destination comes
// from spec.plugins of the Cluster. It returns true to requeue.
func ReconcileBackup(ctx context.Context, c client.Client, backup *everestv1alpha1.DatabaseClusterBackup) (bool, error) {
	upstream := newUnstructured(BackupGVK, backup.GetNamespace(), backup.GetName())
	if !backup.GetDeletionTimestamp().IsZero() {
		// Delete the CNPG Backup before releasing the finalizer, so it is not orphaned.
		if err := c.Delete(ctx, upstream); client.IgnoreNotFound(err) != nil {
			return false, err
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(upstream), upstream); err == nil {
			return true, nil
		} else if !k8serrors.IsNotFound(err) {
			return false, err
		}
		if controllerutil.RemoveFinalizer(backup, everestv1alpha1.DBBackupStorageProtectionFinalizer) {
			return true, c.Update(ctx, backup)
		}
		return false, nil
	}
	if backup.HasCompleted() {
		return false, nil
	}

	cluster := EmptyClusterObject()
	if err := c.Get(ctx, types.NamespacedName{Namespace: backup.GetNamespace(), Name: backup.Spec.DBClusterName}, cluster); err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	// The DatabaseCluster reconciler adds the plugin to the Cluster first. A Backup created before
	// that fails permanently instead of being retried.
	if !hasBarmanCloudPlugin(cluster.Object) {
		return true, nil
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, upstream, func() error {
		upstream.SetLabels(map[string]string{
			consts.DatabaseClusterNameLabel: backup.Spec.DBClusterName,
			BackupStorageLabel:              backup.Spec.BackupStorageName,
		})
		upstream.Object["spec"] = map[string]any{
			"cluster":             map[string]any{"name": backup.Spec.DBClusterName},
			"method":              "plugin",
			"pluginConfiguration": map[string]any{"name": barmanPluginName},
		}
		if metav1.GetControllerOf(upstream) == nil {
			return controllerutil.SetControllerReference(backup, upstream, c.Scheme())
		}
		return nil
	}); err != nil {
		return false, err
	}
	// Poll until the Backup reaches a terminal phase, in case a watch event is missed.
	return true, nil
}

// AdoptBackup creates the DatabaseClusterBackup of a CNPG Backup produced by a ScheduledBackup.
func AdoptBackup(ctx context.Context, c client.Client, obj client.Object) error {
	upstream := newUnstructured(BackupGVK, obj.GetNamespace(), obj.GetName())
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), upstream); err != nil {
		return client.IgnoreNotFound(err)
	}
	clusterName, _, _ := unstructured.NestedString(upstream.Object, "spec", "cluster", "name")
	if clusterName == "" {
		return errors.New("CNPG Backup does not reference a cluster")
	}
	cluster := &everestv1alpha1.DatabaseCluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: upstream.GetNamespace(), Name: clusterName}, cluster); err != nil {
		return client.IgnoreNotFound(err)
	}
	if cluster.Spec.Engine.Type != everestv1alpha1.DatabaseEngineCNPG {
		return nil
	}
	storageName := upstream.GetLabels()[BackupStorageLabel]
	if storageName == "" {
		// Some CNPG versions do not copy the ScheduledBackup labels to the Backups.
		storageName = clusterBackupStorage(cluster)
	}
	if storageName == "" {
		return errors.New("cannot determine the BackupStorage of the CNPG Backup")
	}

	backup := &everestv1alpha1.DatabaseClusterBackup{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(upstream), backup); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return err
	}
	backup.SetName(upstream.GetName())
	backup.SetNamespace(upstream.GetNamespace())
	backup.SetLabels(map[string]string{consts.DatabaseClusterNameLabel: clusterName})
	backup.Spec.DBClusterName = clusterName
	backup.Spec.BackupStorageName = storageName
	if err := controllerutil.SetControllerReference(cluster, backup, c.Scheme()); err != nil {
		return err
	}
	return c.Create(ctx, backup)
}

// BackupStatus maps the CNPG Backup status onto the DatabaseClusterBackup status. The destination
// is reported as ".../<serverName>/base/<backupID>", the form DataSource accepts in
// backupSource.path to restore that exact backup after the source cluster is gone.
func BackupStatus(
	ctx context.Context,
	c client.Client,
	backup *everestv1alpha1.DatabaseClusterBackup,
) (everestv1alpha1.DatabaseClusterBackupStatus, error) {
	status := everestv1alpha1.DatabaseClusterBackupStatus{}
	upstream := newUnstructured(BackupGVK, backup.GetNamespace(), backup.GetName())
	if err := c.Get(ctx, client.ObjectKeyFromObject(upstream), upstream); err != nil {
		return status, client.IgnoreNotFound(err)
	}
	createdAt := upstream.GetCreationTimestamp()
	status.CreatedAt = &createdAt

	phase, _, _ := unstructured.NestedString(upstream.Object, "status", "phase")
	status.State = backupState(phase)

	if stoppedAt, _, _ := unstructured.NestedString(upstream.Object, "status", "stoppedAt"); stoppedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, stoppedAt); err == nil {
			completedAt := metav1.NewTime(parsed)
			status.CompletedAt = &completedAt
			// The end of the backup is a lower bound; keep a more accurate value set by someone else.
			status.LatestRestorableTime = backup.Status.LatestRestorableTime
			if status.LatestRestorableTime == nil {
				status.LatestRestorableTime = &completedAt
			}
		}
	}

	destinationPath, _, _ := unstructured.NestedString(upstream.Object, "status", "destinationPath")
	serverName, _, _ := unstructured.NestedString(upstream.Object, "status", "serverName")
	backupID, _, _ := unstructured.NestedString(upstream.Object, "status", "backupId")
	switch {
	case destinationPath != "" && serverName != "" && backupID != "":
		destination := BackupPath(destinationPath, serverName, backupID)
		status.Destination = &destination
	case destinationPath != "":
		status.Destination = &destinationPath
	}
	return status, nil
}

// ReconcileRestore checks a DatabaseClusterRestore. CNPG restores only while bootstrapping a new
// Cluster (spec.bootstrap is immutable), so the restore must describe the data source the cluster
// was created from. It returns true to requeue until the cluster is ready.
func ReconcileRestore(restore *everestv1alpha1.DatabaseClusterRestore, db *everestv1alpha1.DatabaseCluster) (bool, error) {
	if restore.IsComplete() {
		return false, nil
	}
	if db.Spec.DataSource == nil {
		return false, errInPlaceRestore
	}
	ds := db.Spec.DataSource.IntoDBRestoreDataSource()
	if ds.DBClusterBackupName != restore.Spec.DataSource.DBClusterBackupName ||
		!reflect.DeepEqual(ds.BackupSource, restore.Spec.DataSource.BackupSource) ||
		!reflect.DeepEqual(ds.PITR, restore.Spec.DataSource.PITR) {
		return false, errInPlaceRestore
	}
	return db.Status.Status != everestv1alpha1.AppStateReady, nil
}

// RestoreStatus derives the restore state from the cluster being bootstrapped. The first
// completion time is kept so the status does not change on every reconciliation.
func RestoreStatus(
	restore *everestv1alpha1.DatabaseClusterRestore,
	db *everestv1alpha1.DatabaseCluster,
) everestv1alpha1.DatabaseClusterRestoreStatus {
	status := everestv1alpha1.DatabaseClusterRestoreStatus{Message: db.Status.Message}
	switch db.Status.Status.WithCreatingState() {
	case everestv1alpha1.AppStateReady:
		status.State = everestv1alpha1.RestoreSucceeded
		status.CompletedAt = restore.Status.CompletedAt
		if status.CompletedAt == nil {
			now := metav1.Now()
			status.CompletedAt = &now
		}
	case everestv1alpha1.AppStateError:
		status.State = everestv1alpha1.RestoreFailed
	default:
		status.State = everestv1alpha1.RestoreRunning
	}
	return status
}

func clusterBackupStorage(cluster *everestv1alpha1.DatabaseCluster) string {
	for _, schedule := range cluster.Spec.Backup.Schedules {
		if schedule.Enabled {
			return schedule.BackupStorageName
		}
	}
	if name := cluster.Spec.Backup.PITR.BackupStorageName; name != nil {
		return *name
	}
	return ""
}

func backupState(phase string) everestv1alpha1.BackupState {
	switch strings.ToLower(phase) {
	case "completed":
		return everestv1alpha1.BackupSucceeded
	case "failed":
		return everestv1alpha1.BackupFailed
	case "running", "finalizing":
		return everestv1alpha1.BackupRunning
	case "pending", "started":
		return everestv1alpha1.BackupStarting
	default:
		return everestv1alpha1.BackupNew
	}
}

const (
	// BackupStorageLabel associates CNPG backups and ObjectStores with a BackupStorage.
	BackupStorageLabel = "everest.percona.com/backup-storage"
	// ScheduleNameLabel associates CNPG backups with a schedule.
	ScheduleNameLabel = "everest.percona.com/backup-schedule"

	// Environment variable that overrides the recovery window of shared ObjectStores.
	retentionPolicyEnv     = "CNPG_BACKUP_RETENTION_POLICY"
	defaultRetentionPolicy = "7d"

	regionSecretKey           = "AWS_REGION"
	parameterBarmanObjectName = "barmanObjectName"
	parameterServerName       = "serverName"
)

var (
	// BackupGVK is the GroupVersionKind of the CNPG Backup.
	BackupGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: backupKind}
	// ScheduledBackupGVK is the GroupVersionKind of the CNPG ScheduledBackup.
	ScheduledBackupGVK = schema.GroupVersionKind{Group: apiGroup, Version: cnpgAPIVersion, Kind: scheduledBackupKind}
	// ObjectStoreGVK is the GroupVersionKind of the Barman Cloud Plugin ObjectStore.
	ObjectStoreGVK = schema.GroupVersionKind{Group: barmanAPIGroup, Version: "v1", Kind: objectStoreKind}

	// ErrStorageUnsupported marks a BackupStorage that CNPG backups cannot use. Other engines may
	// still use it, so callers outside this provider treat it as "no shared store".
	ErrStorageUnsupported = errors.New("BackupStorage is not supported by CloudNativePG backups")
	// ErrBarmanCloudPluginMissing reports that the Barman Cloud Plugin is not installed.
	ErrBarmanCloudPluginMissing = fmt.Errorf(
		"backups require the Barman Cloud Plugin: CRD %q is not installed", objectStoreCRDName,
	)
)

// SharedObjectStoreName is the name of the shared ObjectStore of a BackupStorage.
func SharedObjectStoreName(storageName string) string {
	return storageName
}

// ServerName is the directory of a cluster inside the shared ObjectStore. It carries the UID so
// that re-creating a cluster with the same name writes to a new directory instead of colliding
// with the old WAL archive (CNPG refuses to archive into a non-empty directory).
func ServerName(db *everestv1alpha1.DatabaseCluster) string {
	return db.GetName() + "-" + string(db.GetUID())
}

func recoveryObjectStoreName(dbName string) string {
	return dbName + "-recovery"
}

func regionSecretName(objectStore string) string {
	return objectStore + "-region"
}

// BarmanObjectStore converts an Everest BackupStorage into the "configuration" of an ObjectStore.
// Region is referenced through an Everest-owned Secret, because barman only reads the region
// from a Secret while BackupStorage.spec.region is a literal.
func BarmanObjectStore(storage *everestv1alpha1.BackupStorage, regionSecret string) (map[string]any, error) {
	if pointer.Get(storage.Spec.ForcePathStyle) {
		return nil, fmt.Errorf("%w: forcePathStyle is not supported", ErrStorageUnsupported)
	}
	if storage.Spec.VerifyTLS != nil && !*storage.Spec.VerifyTLS {
		return nil, fmt.Errorf("%w: verifyTLS=false is not supported", ErrStorageUnsupported)
	}
	secret := storage.Spec.CredentialsSecretName
	result := map[string]any{}
	if storage.Spec.EndpointURL != "" {
		result["endpointURL"] = storage.Spec.EndpointURL
	}
	switch storage.Spec.Type {
	case everestv1alpha1.BackupStorageTypeS3:
		result["destinationPath"] = "s3://" + strings.Trim(storage.Spec.Bucket, "/")
		credentials := map[string]any{
			"accessKeyId":     map[string]any{"name": secret, "key": "AWS_ACCESS_KEY_ID"},
			"secretAccessKey": map[string]any{"name": secret, "key": "AWS_SECRET_ACCESS_KEY"},
		}
		if storage.Spec.Region != "" {
			credentials["region"] = map[string]any{"name": regionSecret, "key": regionSecretKey}
		}
		result["s3Credentials"] = credentials
	case everestv1alpha1.BackupStorageTypeAzure:
		if storage.Spec.EndpointURL == "" {
			return nil, fmt.Errorf("%w: Azure BackupStorage.endpointURL is required", ErrStorageUnsupported)
		}
		result["destinationPath"] = fmt.Sprintf("%s/%s",
			strings.TrimRight(storage.Spec.EndpointURL, "/"), strings.Trim(storage.Spec.Bucket, "/"))
		result["azureCredentials"] = map[string]any{
			"storageAccount": map[string]any{"name": secret, "key": "AZURE_STORAGE_ACCOUNT_NAME"},
			"storageKey":     map[string]any{"name": secret, "key": "AZURE_STORAGE_ACCOUNT_KEY"},
		}
	default:
		return nil, fmt.Errorf("%w: storage type %q", ErrStorageUnsupported, storage.Spec.Type)
	}
	return result, nil
}

// ReconcileSharedObjectStore creates or updates the shared ObjectStore of a BackupStorage.
//
// It is called by the BackupStorage controller (so the store exists as soon as the BackupStorage
// does) and by the provider (so a cluster never points to a missing store). The store is owned by
// the BackupStorage and is garbage collected with it.
func ReconcileSharedObjectStore(ctx context.Context, c client.Client, storage *everestv1alpha1.BackupStorage) error {
	name := SharedObjectStoreName(storage.GetName())
	config, err := BarmanObjectStore(storage, regionSecretName(name))
	if err != nil {
		return err
	}
	if err := ensurePluginInstalled(ctx, c); err != nil {
		return err
	}
	if storage.Spec.Region != "" {
		if err := reconcileRegionSecret(ctx, c, storage, name, storage.Spec.Region); err != nil {
			return err
		}
	}
	config["data"] = map[string]any{"compression": "gzip", "jobs": int64(2)}       //nolint:mnd
	config["wal"] = map[string]any{"compression": "zstd", "maxParallel": int64(2)} //nolint:mnd
	spec := map[string]any{
		"configuration": config,
		// The plugin retention is a time-based recovery window, not "keep N copies".
		"retentionPolicy": retentionPolicy(),
		"instanceSidecarConfiguration": map[string]any{
			"resources": map[string]any{
				"limits":   map[string]any{"memory": "256Mi"},
				"requests": map[string]any{"cpu": "30m", "memory": "128Mi"},
			},
		},
	}
	return applyObjectStore(ctx, c, storage, name, storage.GetName(), spec)
}

// reconcileRecoveryObjectStore creates a store owned by the cluster that reads backups from an
// arbitrary path (spec.dataSource.backupSource.path). It never carries a retention policy, so
// retention can not delete data outside the shared layout.
func (a *applier) reconcileRecoveryObjectStore(storage *everestv1alpha1.BackupStorage, destinationPath string) (string, error) {
	name := recoveryObjectStoreName(a.DB.GetName())
	config, err := BarmanObjectStore(storage, regionSecretName(name))
	if err != nil {
		return "", err
	}
	config["destinationPath"] = strings.TrimRight(destinationPath, "/")
	if err := ensurePluginInstalled(a.ctx, a.C); err != nil {
		return "", err
	}
	if storage.Spec.Region != "" {
		if err := reconcileRegionSecret(a.ctx, a.C, a.DB, name, storage.Spec.Region); err != nil {
			return "", err
		}
	}
	return name, applyObjectStore(a.ctx, a.C, a.DB, name, storage.GetName(), map[string]any{"configuration": config})
}

func applyObjectStore(ctx context.Context, c client.Client, owner client.Object, name, storageName string, spec map[string]any) error {
	object := newUnstructured(ObjectStoreGVK, owner.GetNamespace(), name)
	if _, err := controllerutil.CreateOrUpdate(ctx, c, object, func() error {
		object.SetLabels(map[string]string{BackupStorageLabel: storageName})
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(owner, object, c.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile Barman Cloud ObjectStore %q: %w", name, err)
	}
	return nil
}

func reconcileRegionSecret(ctx context.Context, c client.Client, owner client.Object, objectStore, region string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: regionSecretName(objectStore), Namespace: owner.GetNamespace()},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = map[string]string{regionSecretKey: region}
		return controllerutil.SetControllerReference(owner, secret, c.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile region secret for ObjectStore %q: %w", objectStore, err)
	}
	return nil
}

func ensurePluginInstalled(ctx context.Context, c client.Client) error {
	installed, err := CRDInstalled(ctx, c, objectStoreCRDName)
	if err != nil {
		return fmt.Errorf("check Barman Cloud Plugin CRD: %w", err)
	}
	if !installed {
		return ErrBarmanCloudPluginMissing
	}
	return nil
}

func retentionPolicy() string {
	if value := os.Getenv(retentionPolicyEnv); value != "" {
		return value
	}
	return defaultRetentionPolicy
}

// archiverPluginConfiguration builds the spec.plugins entry of the Cluster. The isWALArchiver flag replaces
// archive_command; it defaults to false, and forgetting it silently disables WAL archiving.
func archiverPluginConfiguration(objectStore, serverName string) map[string]any {
	return map[string]any{
		"name":          barmanPluginName,
		"isWALArchiver": true,
		"parameters": map[string]any{
			parameterBarmanObjectName: objectStore,
			parameterServerName:       serverName,
		},
	}
}

// recoveryPluginConfiguration builds the "plugin" field of a spec.externalClusters entry used to
// read the backups of another cluster. It is read-only: no isWALArchiver.
func recoveryPluginConfiguration(objectStore, serverName string) map[string]any {
	return map[string]any{
		"name": barmanPluginName,
		"parameters": map[string]any{
			parameterBarmanObjectName: objectStore,
			parameterServerName:       serverName,
		},
	}
}

// hasBarmanCloudPlugin reports whether a CNPG Cluster object archives through the plugin.
func hasBarmanCloudPlugin(cluster map[string]any) bool {
	plugins, _, _ := unstructured.NestedSlice(cluster, "spec", "plugins")
	for _, raw := range plugins {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := entry["name"].(string); name == barmanPluginName {
			return true
		}
	}
	return false
}

func getBackupStorage(ctx context.Context, c client.Client, namespace, name string) (*everestv1alpha1.BackupStorage, error) {
	if name == "" {
		return nil, errors.New("a BackupStorage name is required")
	}
	storage := &everestv1alpha1.BackupStorage{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, storage); err != nil {
		return nil, fmt.Errorf("get BackupStorage %q: %w", name, err)
	}
	return storage, nil
}
