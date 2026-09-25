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
	"testing"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

const (
	testNamespace  = "databases"
	testCluster    = "orders"
	testUserSecret = "orders-owner"
	testStorage    = "s3"
	testImage16    = "ghcr.io/cloudnative-pg/postgresql:16.4-standard-bookworm@sha256:aaa"
	testImage14    = "ghcr.io/cloudnative-pg/postgresql:14.24-standard-bookworm@sha256:bbb"
	testSourceDB   = "source"
	testBouncer    = "ghcr.io/cloudnative-pg/pgbouncer:1.25.1@sha256:ccc"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	for _, gvk := range []schema.GroupVersionKind{
		ClusterGVK, BackupGVK, ScheduledBackupGVK, ObjectStoreGVK, PoolerGVK, DatabaseGVK, PublicationGVK, SubscriptionGVK,
	} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		list := gvk
		list.Kind += "List"
		scheme.AddKnownTypeWithName(list, &unstructured.UnstructuredList{})
	}
	return scheme
}

func crd(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": name},
	}}
}

func testDB(version string) *everestv1alpha1.DatabaseCluster {
	return &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testCluster, Namespace: testNamespace, UID: types.UID("uid-1")},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type:            everestv1alpha1.DatabaseEngineCNPG,
				Version:         version,
				Replicas:        3,
				UserSecretsName: testUserSecret,
				Storage:         everestv1alpha1.Storage{Size: resource.MustParse("20Gi")},
				Config:          "max_connections = 200\nreserved_connections = 5\n",
			},
		},
	}
}

func userSecret(username, database string) *corev1.Secret {
	data := map[string][]byte{"username": []byte(username), "password": []byte("secret")}
	if database != "" {
		data["database"] = []byte(database)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testUserSecret, Namespace: testNamespace},
		Data:       data,
	}
}

func testEngine() *everestv1alpha1.DatabaseEngine {
	return &everestv1alpha1.DatabaseEngine{Status: everestv1alpha1.DatabaseEngineStatus{
		AvailableVersions: everestv1alpha1.Versions{
			Engine: everestv1alpha1.ComponentsMap{
				"16.4":  {ImagePath: testImage16},
				"14.24": {ImagePath: testImage14},
			},
			Proxy: map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
				everestv1alpha1.ProxyTypePGBouncer: {"16.4": {ImagePath: testBouncer}},
			},
		},
	}}
}

//nolint:ireturn
func newTestApplier(t *testing.T, db *everestv1alpha1.DatabaseCluster, current map[string]any, objects ...client.Object) (*applier, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(append(objects, db)...).Build()
	cluster := newUnstructured(ClusterGVK, testNamespace, testCluster)
	if current != nil {
		cluster.Object["spec"] = current
	}
	provider := &Provider{
		Unstructured:    cluster,
		ProviderOptions: providers.ProviderOptions{C: c, DB: db, DBEngine: testEngine()},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.ResetDefaults())
	return a, c
}

func nested(t *testing.T, object map[string]any, fields ...string) any {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	require.NoError(t, err)
	require.True(t, found, "field %v not found", fields)
	return value
}

func TestEngine(t *testing.T) {
	t.Parallel()
	a, c := newTestApplier(t, testDB("16.4"), nil, userSecret("orders_owner", ""))
	require.NoError(t, a.Engine())

	assert.Equal(t, testImage16, nested(t, a.Object, "spec", "imageName"))
	assert.Equal(t, int64(3), nested(t, a.Object, "spec", "instances"))
	assert.Equal(t, "20Gi", nested(t, a.Object, "spec", "storage", "size"))
	assert.Equal(t, false, nested(t, a.Object, "spec", "enableSuperuserAccess"))

	parameters := nested(t, a.Object, "spec", "postgresql", "parameters")
	assert.Equal(t, map[string]any{
		"max_connections":       "200",
		"reserved_connections":  "5", // spec.engine.config wins over the platform default.
		"createrole_self_grant": "set, inherit",
	}, parameters)

	initdb := nested(t, a.Object, "spec", "bootstrap", "initdb").(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, "orders_owner", initdb["owner"])
	assert.Equal(t, defaultAppDatabase, initdb["database"])
	assert.Contains(t, initdb["postInitSQL"], "GRANT pg_use_reserved_connections TO dbaas_admin_role WITH ADMIN OPTION;")
	assert.Contains(t, initdb["postInitApplicationSQL"], "GRANT CONNECT, TEMPORARY ON DATABASE app TO orders_owner;")

	role := nested(t, a.Object, "spec", "managed", "roles").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, true, role["createrole"])
	assert.Equal(t, []any{adminRoleName}, role["inRoles"])

	// The user Secret is labelled so CNPG propagates password changes.
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testUserSecret}, secret))
	assert.Equal(t, "true", secret.Labels[cnpgReloadLabel])

	_, found, _ := unstructured.NestedMap(a.Object, "spec", "walStorage")
	assert.False(t, found, "no WAL volume unless the operator is configured for it")
}

func TestEnginePG14SkipsRoleDelegation(t *testing.T) {
	t.Parallel()
	db := testDB("14.24")
	db.Spec.Engine.Config = ""
	a, _ := newTestApplier(t, db, nil, userSecret("orders_owner", "orders"))
	require.NoError(t, a.Engine())

	assert.Empty(t, nested(t, a.Object, "spec", "postgresql", "parameters"))
	initdb := nested(t, a.Object, "spec", "bootstrap", "initdb").(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, "orders", initdb["database"])
	assert.NotContains(t, initdb["postInitSQL"], "GRANT pg_use_reserved_connections TO dbaas_admin_role WITH ADMIN OPTION;")
	role := nested(t, a.Object, "spec", "managed", "roles").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, false, role["createrole"])
}

func TestEngineRejectsUnsafeUserSecret(t *testing.T) {
	t.Parallel()
	for name, secret := range map[string]*corev1.Secret{
		"superuser":        userSecret(superuserName, ""),
		"invalid username": userSecret("orders; DROP", ""),
		"invalid database": userSecret("orders_owner", "App-DB"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, _ := newTestApplier(t, testDB("16.4"), nil, secret)
			require.Error(t, a.Engine())
		})
	}
}

func TestEngineTimescaleDB(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Annotations = map[string]string{AnnotationExtension: ExtensionTimescaleDB}
	a, _ := newTestApplier(t, db, map[string]any{"imageCatalogRef": map[string]any{"major": int64(16)}}, userSecret("orders_owner", ""))
	require.NoError(t, a.Engine())

	assert.Equal(t, timescaleImageCatalogName, nested(t, a.Object, "spec", "imageCatalogRef", "name"))
	assert.Equal(t, int64(16), nested(t, a.Object, "spec", "imageCatalogRef", "major"))
	assert.Equal(t, []any{ExtensionTimescaleDB}, nested(t, a.Object, "spec", "postgresql", "shared_preload_libraries"))

	db.Spec.Engine.Version = "17.2"
	require.ErrorContains(t, a.Engine(), "major upgrades")
}

func TestEngineWALStorageAndAltDNSNames(t *testing.T) {
	t.Setenv(walStorageSizeEnv, "4Gi")
	t.Setenv(walStorageClassEnv, "fast")
	db := testDB("16.4")
	db.Annotations = map[string]string{AnnotationServerAltDNSNames: "orders.example.com, orders-ro.example.com"}
	a, _ := newTestApplier(t, db, nil, userSecret("orders_owner", ""))
	require.NoError(t, a.Engine())
	assert.Equal(t, "4Gi", nested(t, a.Object, "spec", "walStorage", "size"))
	assert.Equal(t, "fast", nested(t, a.Object, "spec", "walStorage", "storageClass"))
	assert.Equal(t, []any{"orders.example.com", "orders-ro.example.com"},
		nested(t, a.Object, "spec", "certificates", "serverAltDNSNames"))

	// CNPG can not remove a WAL volume: an existing one survives removing the setting.
	t.Setenv(walStorageSizeEnv, "")
	existing := map[string]any{"walStorage": map[string]any{"size": "8Gi"}}
	a, _ = newTestApplier(t, testDB("16.4"), existing, userSecret("orders_owner", ""))
	require.NoError(t, a.Engine())
	assert.Equal(t, "8Gi", nested(t, a.Object, "spec", "walStorage", "size"))
}

func TestEngineKeepsWithdrawnImage(t *testing.T) {
	t.Parallel()
	db := testDB("16.2")
	running := "ghcr.io/cloudnative-pg/postgresql:16.2-standard-bookworm@sha256:old"
	a, _ := newTestApplier(t, db, map[string]any{"imageName": running}, userSecret("orders_owner", ""))
	require.NoError(t, a.Engine())
	assert.Equal(t, running, nested(t, a.Object, "spec", "imageName"))

	b, _ := newTestApplier(t, testDB("16.2"), nil, userSecret("orders_owner", ""))
	require.ErrorContains(t, b.Engine(), "not available in ClusterImageCatalog")
}

func TestValidateVersionChange(t *testing.T) {
	t.Parallel()
	for version, wantErr := range map[string]bool{"16.4": false, "16.6": false, "16.2": true, "17.1": true} {
		desired, err := semver.NewVersion(version)
		require.NoError(t, err)
		err = validateVersionChange(testImage16, desired)
		assert.Equal(t, wantErr, err != nil, version)
	}
}

func newTestStorage() *everestv1alpha1.BackupStorage {
	return &everestv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: testStorage, Namespace: testNamespace},
		Spec: everestv1alpha1.BackupStorageSpec{
			Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", Region: "eu-1",
			CredentialsSecretName: "s3-creds",
		},
	}
}

func TestBackup(t *testing.T) {
	t.Setenv(retentionPolicyEnv, "14d")
	db := testDB("16.4")
	db.Spec.Backup.Schedules = []everestv1alpha1.BackupSchedule{
		{Name: "daily", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: testStorage},
	}
	a, c := newTestApplier(t, db, nil, newTestStorage(), crd(objectStoreCRDName))
	require.NoError(t, a.Backup())

	plugin := nested(t, a.Object, "spec", "plugins").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, true, plugin["isWALArchiver"])
	assert.Equal(t, map[string]any{parameterBarmanObjectName: testStorage, parameterServerName: "orders-uid-1"}, plugin["parameters"])

	store := newUnstructured(ObjectStoreGVK, testNamespace, testStorage)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(store), store))
	assert.Equal(t, "s3://backups", nested(t, store.Object, "spec", "configuration", "destinationPath"))
	assert.Equal(t, "14d", nested(t, store.Object, "spec", "retentionPolicy"))
	assert.Equal(t, map[string]any{"cpu": "200m", "memory": "256Mi"},
		nested(t, store.Object, "spec", "instanceSidecarConfiguration", "resources", "limits"),
		"explicit CPU limit, not the namespace LimitRange default")
	assert.Equal(t, testStorage+"-region",
		nested(t, store.Object, "spec", "configuration", "s3Credentials", "region", "name"))

	scheduled := newUnstructured(ScheduledBackupGVK, testNamespace, "orders-daily")
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(scheduled), scheduled))
	assert.Equal(t, "0 0 2 * * *", nested(t, scheduled.Object, "spec", "schedule"))
	assert.Equal(t, "none", nested(t, scheduled.Object, "spec", "backupOwnerReference"))
	assert.Equal(t, "plugin", nested(t, scheduled.Object, "spec", "method"))
}

func TestBackupRejections(t *testing.T) {
	t.Parallel()
	retention := testDB("16.4")
	retention.Spec.Backup.Schedules = []everestv1alpha1.BackupSchedule{
		{Name: "daily", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: testStorage, RetentionCopies: 3},
	}
	a, _ := newTestApplier(t, retention, nil)
	require.ErrorContains(t, a.Backup(), "retentionCopies")

	twoStorages := testDB("16.4")
	twoStorages.Spec.Backup.Schedules = []everestv1alpha1.BackupSchedule{
		{Name: "a", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: "one"},
		{Name: "b", Enabled: true, Schedule: "0 3 * * *", BackupStorageName: "two"},
	}
	b, _ := newTestApplier(t, twoStorages, nil)
	require.ErrorContains(t, b.Backup(), "one backup destination")

	pluginMissing := testDB("16.4")
	pluginMissing.Spec.Backup.Schedules = retention.Spec.Backup.Schedules[:1]
	pluginMissing.Spec.Backup.Schedules[0].RetentionCopies = 0
	d, _ := newTestApplier(t, pluginMissing, nil, newTestStorage())
	require.ErrorIs(t, d.Backup(), ErrBarmanCloudPluginMissing)
}

//nolint:ireturn
func restoreFixture(t *testing.T, pitr *everestv1alpha1.PITR) (*applier, client.Client) {
	t.Helper()
	source := testDB("16.4")
	source.Name, source.UID = testSourceDB, types.UID("uid-src")
	completed := metav1.NewTime(time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC))
	latest := metav1.NewTime(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: testNamespace},
		Spec:       everestv1alpha1.DatabaseClusterBackupSpec{DBClusterName: testSourceDB, BackupStorageName: testStorage},
		Status: everestv1alpha1.DatabaseClusterBackupStatus{
			State: everestv1alpha1.BackupSucceeded, CompletedAt: &completed, LatestRestorableTime: &latest,
		},
	}
	cnpgBackup := newUnstructured(BackupGVK, testNamespace, "nightly")
	cnpgBackup.Object["status"] = map[string]any{"backupId": "20260901T100000"}
	db := testDB("16.4")
	db.Spec.DataSource = &everestv1alpha1.DataSource{DBClusterBackupName: "nightly", PITR: pitr}
	return newTestApplier(t, db, nil, source, backup, cnpgBackup, newTestStorage(),
		userSecret("orders_owner", ""), crd(objectStoreCRDName))
}

func TestDataSourceFromBackup(t *testing.T) {
	t.Parallel()
	a, _ := restoreFixture(t, nil)
	require.NoError(t, a.DataSource())
	recovery := nested(t, a.Object, "spec", "bootstrap", "recovery").(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, testSourceDB, recovery["source"])
	assert.Equal(t, "orders_owner", recovery["owner"])
	// Without PITR the restore stops at the end of the chosen backup.
	assert.Equal(t, map[string]any{"backupID": "20260901T100000", "targetImmediate": true}, recovery["recoveryTarget"])

	external := nested(t, a.Object, "spec", "externalClusters").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, recoveryPluginConfiguration(testStorage, "source-uid-src"), external["plugin"])
}

func TestDataSourcePITR(t *testing.T) {
	t.Parallel()
	at := func(hour int) *metav1.Time {
		value := metav1.NewTime(time.Date(2026, 9, 1, hour, 0, 0, 0, time.UTC))
		return &value
	}
	a, _ := restoreFixture(t, &everestv1alpha1.PITR{Type: everestv1alpha1.PITRTypeDate, Date: &everestv1alpha1.RestoreDate{Time: *at(11)}})
	require.NoError(t, a.DataSource())
	assert.Equal(t, "2026-09-01T11:00:00Z", nested(t, a.Object, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime"))

	for _, hour := range []int{9, 13} {
		b, _ := restoreFixture(t, &everestv1alpha1.PITR{Type: everestv1alpha1.PITRTypeDate, Date: &everestv1alpha1.RestoreDate{Time: *at(hour)}})
		require.Error(t, b.DataSource(), "hour %d is outside the restorable window", hour)
	}

	latest, _ := restoreFixture(t, &everestv1alpha1.PITR{Type: everestv1alpha1.PITRTypeLatest})
	require.NoError(t, latest.DataSource())
	_, found, _ := unstructured.NestedMap(latest.Object, "spec", "bootstrap", "recovery", "recoveryTarget")
	assert.False(t, found, "latest replays all WAL")
}

func TestDataSourceFromBackupPath(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Spec.DataSource = &everestv1alpha1.DataSource{BackupSource: &everestv1alpha1.BackupSource{
		BackupStorageName: testStorage,
		Path:              "s3://backups/source-uid-src/base/20260901T100000",
	}}
	a, c := newTestApplier(t, db, nil, newTestStorage(), userSecret("orders_owner", ""), crd(objectStoreCRDName))
	require.NoError(t, a.DataSource())

	assert.Equal(t, map[string]any{"backupID": "20260901T100000", "targetImmediate": true},
		nested(t, a.Object, "spec", "bootstrap", "recovery", "recoveryTarget"))
	store := newUnstructured(ObjectStoreGVK, testNamespace, "orders-recovery")
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(store), store))
	assert.Equal(t, "s3://backups", nested(t, store.Object, "spec", "configuration", "destinationPath"))
	_, found, _ := unstructured.NestedString(store.Object, "spec", "retentionPolicy")
	assert.False(t, found, "a recovery store never applies retention")
}

func TestParseBackupPath(t *testing.T) {
	t.Parallel()
	path := BackupPath("s3://backups/prefix/", "orders-uid", "20260901T100000")
	assert.Equal(t, "s3://backups/prefix/orders-uid/base/20260901T100000", path)
	destination, server, id, ok := ParseBackupPath(path)
	assert.True(t, ok)
	assert.Equal(t, []string{"s3://backups/prefix", "orders-uid", "20260901T100000"}, []string{destination, server, id})
	_, _, id, ok = ParseBackupPath("s3://backups/prefix")
	assert.False(t, ok)
	assert.Empty(t, id)
}

func TestProxyPoolers(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Spec.Proxy = everestv1alpha1.Proxy{
		Type:     everestv1alpha1.ProxyTypePGBouncer,
		Replicas: pointer.ToInt32(2),
		Config:   "pool_mode = transaction",
		Expose:   everestv1alpha1.Expose{Type: everestv1alpha1.ExposeTypeLoadBalancer},
	}
	a, c := newTestApplier(t, db, nil)
	require.NoError(t, a.Proxy())

	rw := newUnstructured(PoolerGVK, testNamespace, "orders-pooler-rw")
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(rw), rw))
	assert.Equal(t, "rw", nested(t, rw.Object, "spec", "type"))
	assert.Equal(t, "transaction", nested(t, rw.Object, "spec", "pgbouncer", "poolMode"))
	assert.Equal(t, testBouncer, nested(t, rw.Object, "spec", "pgbouncer", "image"))
	assert.Equal(t, "require", nested(t, rw.Object, "spec", "pgbouncer", "parameters", "client_tls_sslmode"))
	assert.Equal(t, "LoadBalancer", nested(t, rw.Object, "spec", "serviceTemplate", "spec", "type"))

	ro := newUnstructured(PoolerGVK, testNamespace, "orders-pooler-ro")
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(ro), ro), "3 instances get a read pooler")
	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "orders-pooler-rw"}, pdb))

	// With a pooler the cluster opens no other external Service.
	_, found, _ := unstructured.NestedSlice(a.Object, "spec", "managed", "services", "additional")
	assert.False(t, found)

	// Disabling the pooler deletes Poolers and PDBs.
	db.Spec.Proxy = everestv1alpha1.Proxy{}
	require.NoError(t, a.Proxy())
	require.Error(t, c.Get(context.Background(), client.ObjectKeyFromObject(rw), rw))
	require.Error(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "orders-pooler-rw"}, pdb))
}

func TestProxyExposeWithoutPooler(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Spec.Proxy.Expose = everestv1alpha1.Expose{Type: everestv1alpha1.ExposeTypeLoadBalancer}
	a, _ := newTestApplier(t, db, nil)
	require.NoError(t, a.Proxy())
	service := nested(t, a.Object, "spec", "managed", "services", "additional").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, "rw", service["selectorType"])
	assert.Equal(t, "orders-rw-external", nested(t, service, "serviceTemplate", "metadata", "name"))
	assert.Equal(t, "Local", nested(t, service, "serviceTemplate", "spec", "externalTrafficPolicy"))
}

func TestParsePoolMode(t *testing.T) {
	t.Parallel()
	for config, want := range map[string]string{"": "session", "# comment\npool_mode = transaction": "transaction"} {
		got, err := ParsePoolMode(config)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	for _, config := range []string{"max_client_conn = 10", "pool_mode = statement", "garbage"} {
		_, err := ParsePoolMode(config)
		require.Error(t, err, config)
	}
}

func TestStatus(t *testing.T) {
	t.Parallel()
	status := func(t *testing.T, db *everestv1alpha1.DatabaseCluster, clusterStatus map[string]any, objects ...client.Object) everestv1alpha1.DatabaseClusterStatus {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
		cluster := newUnstructured(ClusterGVK, testNamespace, testCluster)
		cluster.Object["spec"] = map[string]any{"instances": int64(3)}
		cluster.Object["status"] = clusterStatus
		p := &Provider{Unstructured: cluster, ProviderOptions: providers.ProviderOptions{C: c, DB: db}}
		got, _, err := p.Status(context.Background())
		require.NoError(t, err)
		return got
	}
	ready := map[string]any{
		"readyInstances": int64(3),
		"conditions":     []any{map[string]any{"type": "Ready", "status": "True"}},
	}

	got := status(t, testDB("16.4"), ready)
	assert.Equal(t, everestv1alpha1.AppStateReady, got.Status)
	assert.Equal(t, "orders-rw.databases.svc", got.Hostname)

	pooled := testDB("16.4")
	pooled.Spec.Proxy.Type = everestv1alpha1.ProxyTypePGBouncer
	lb := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-pooler-rw", Namespace: testNamespace},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
			Ingress: []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}},
		}},
	}
	assert.Equal(t, "203.0.113.10", status(t, pooled, ready, lb).Hostname)
	assert.Equal(t, "orders-pooler-rw.databases.svc", status(t, pooled, ready).Hostname)

	broken := status(t, testDB("16.4"), map[string]any{"phase": "Cluster is unrecoverable and needs manual intervention"})
	assert.Equal(t, everestv1alpha1.AppStateError, broken.Status)

	creating := status(t, testDB("16.4"), map[string]any{"readyInstances": int64(1)})
	assert.Equal(t, everestv1alpha1.AppStateCreating, creating.Status)
}

const (
	testPG14Image = "ghcr.io/cloudnative-pg/postgresql:14.24-202608310817-standard-bookworm" +
		"@sha256:31221f7241b4aa568c4191d2a8e51d2bbc51b5141a2548bb9023d6e96e1b98a0"
	testPG17Image = "ghcr.io/cloudnative-pg/postgresql:17.11-202608310816-standard-bookworm" +
		"@sha256:e8ffaff9d17011fb71f264d857c3b4c54cb86ed0443a4ecb0298cb96be708d4e"
	testPgBouncerImage = "ghcr.io/cloudnative-pg/pgbouncer:1.25.1" +
		"@sha256:e6ddfe22d845e603825e235dd8334b21ecd125abea2a2172478f556b8dee2bb8"

	fieldImage = "image"
	fieldMajor = "major"
)

func catalogScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(clusterImageCatalogGVK, &unstructured.Unstructured{})
	listGVK := clusterImageCatalogGVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func newCatalog(images []any, componentImages []any) *unstructured.Unstructured {
	catalog := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"images": images},
	}}
	catalog.SetGroupVersionKind(clusterImageCatalogGVK)
	catalog.SetName(imageCatalogName)
	if componentImages != nil {
		_ = unstructured.SetNestedSlice(catalog.Object, componentImages, "spec", "componentImages")
	}
	return catalog
}

func TestImageCatalogVersions(t *testing.T) {
	t.Parallel()

	catalog := newCatalog(
		[]any{
			map[string]any{fieldMajor: int64(14), fieldImage: testPG14Image},
			map[string]any{fieldMajor: int64(17), fieldImage: testPG17Image},
		},
		[]any{
			map[string]any{"key": "pgbouncer", fieldImage: testPgBouncerImage},
		},
	)
	c := fake.NewClientBuilder().
		WithScheme(catalogScheme(t)).
		WithObjects(catalog).
		Build()

	versions, err := ImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)

	require.Len(t, versions.Engine, 2)
	require.Contains(t, versions.Engine, "14.24")
	require.Contains(t, versions.Engine, "17.11")

	// Image phải được giữ nguyên cả digest: đó là toàn bộ lý do dùng catalog.
	assert.Equal(t, testPG14Image, versions.Engine["14.24"].ImagePath)
	assert.Equal(
		t,
		"sha256:31221f7241b4aa568c4191d2a8e51d2bbc51b5141a2548bb9023d6e96e1b98a0",
		versions.Engine["14.24"].ImageHash,
	)
	assert.Equal(t, everestv1alpha1.DBEngineComponentAvailable, versions.Engine["17.11"].Status)

	// componentImages được công bố cho mọi version engine, đúng hình dạng ComponentsMap.
	proxy := versions.Proxy[everestv1alpha1.ProxyTypePGBouncer]
	require.Len(t, proxy, 2)
	assert.Equal(t, testPgBouncerImage, proxy["14.24"].ImagePath)
	assert.Equal(t, testPgBouncerImage, proxy["17.11"].ImagePath)
}

func TestImageCatalogVersionsMissingCatalog(t *testing.T) {
	t.Parallel()

	// Chưa apply catalog thì trả về danh sách rỗng chứ không phải lỗi: Everest vẫn phải chạy
	// được trên cụm chưa cài CloudNativePG. Danh sách rỗng khiến admission từ chối mọi version,
	// tức fail closed đúng như thiết kế.
	c := fake.NewClientBuilder().WithScheme(catalogScheme(t)).Build()

	versions, err := ImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)
	assert.Empty(t, versions.Engine)
	assert.Empty(t, versions.Proxy)
}

func TestImageCatalogVersionsSkipsInvalidEntries(t *testing.T) {
	t.Parallel()

	catalog := newCatalog([]any{
		// major khai trong catalog lệch với major suy ra từ tag: entry đặt nhầm chỗ, tin vào nó
		// sẽ dựng cụm bằng sai bản PostgreSQL.
		map[string]any{fieldMajor: int64(15), fieldImage: testPG14Image},
		// Image không có tag thì không suy ra được version.
		map[string]any{fieldMajor: int64(16), fieldImage: "ghcr.io/cloudnative-pg/postgresql"},
		// Entry hợp lệ vẫn phải sống sót qua các entry hỏng phía trên.
		map[string]any{fieldMajor: int64(17), fieldImage: testPG17Image},
	}, nil)
	c := fake.NewClientBuilder().
		WithScheme(catalogScheme(t)).
		WithObjects(catalog).
		Build()

	versions, err := ImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)
	require.Len(t, versions.Engine, 1)
	assert.Contains(t, versions.Engine, "17.11")
}

func TestVersionFromImage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		image   string
		want    string
		wantErr bool
	}{
		{name: "tag kèm digest", image: testPG14Image, want: "14.24"},
		{
			name:  "tag không digest",
			image: "ghcr.io/cloudnative-pg/postgresql:17.11-202608310816-standard-bookworm",
			want:  "17.11",
		},
		{name: "tag chỉ có version", image: "ghcr.io/cloudnative-pg/postgresql:18.6", want: "18.6"},
		{name: "không có tag", image: "ghcr.io/cloudnative-pg/postgresql", wantErr: true},
		{name: "tag không phải version", image: "ghcr.io/cloudnative-pg/postgresql:latest", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := versionFromImage(tc.image)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValidateProxy(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		proxy   everestv1alpha1.Proxy
		wantErr int
	}{
		{name: "no proxy"},
		{name: "pooler", proxy: everestv1alpha1.Proxy{
			Type: everestv1alpha1.ProxyTypePGBouncer, Replicas: pointer.ToInt32(2), Config: "pool_mode = transaction",
		}},
		{name: "unsupported type", proxy: everestv1alpha1.Proxy{Type: everestv1alpha1.ProxyTypeHAProxy}, wantErr: 1},
		{name: "unsupported config key", proxy: everestv1alpha1.Proxy{
			Type: everestv1alpha1.ProxyTypePGBouncer, Config: "max_client_conn = 10",
		}, wantErr: 1},
		{name: "pooler settings without pooler", proxy: everestv1alpha1.Proxy{
			Replicas: pointer.ToInt32(2), Config: "pool_mode = session",
		}, wantErr: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Len(t, validateProxy(tc.proxy), tc.wantErr)
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	t.Parallel()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(crd(clusterCRDName)).Build()
	db := func(version, extension string) *everestv1alpha1.DatabaseCluster {
		db := testDB(version)
		if extension != "" {
			db.Annotations = map[string]string{AnnotationExtension: extension}
		}
		return db
	}
	ctx := context.Background()
	assert.Empty(t, ValidateUpdate(ctx, c, db("16.4", ""), db("16.6", "")), "minor upgrade")
	assert.Len(t, ValidateUpdate(ctx, c, db("16.4", ""), db("17.1", "")), 1, "major upgrade")
	assert.Len(t, ValidateUpdate(ctx, c, db("16.4", ""), db("16.2", "")), 1, "downgrade")
	assert.Len(t, ValidateUpdate(ctx, c, db("16.4", ""), db("16.4", ExtensionTimescaleDB)), 1, "flavor switch")

	notInstalled := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	assert.Len(t, ValidateCreate(ctx, notInstalled, db("16.4", "")), 1, "CloudNativePG not installed")
}

func annotations(pairs ...string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		out[AnnotationPrefix+pairs[i]] = pairs[i+1]
	}
	return out
}

var legacySource = []string{
	"source.legacy.host", "legacy.example.internal",
	"source.legacy.dbname", "orders",
	"source.legacy.user", "migration_user",
	"source.legacy.sslmode", "verify-full",
	"source.legacy.password-secret", "migration-source/pass",
}

func TestParseInputs(t *testing.T) {
	t.Parallel()
	in, err := parseInputs(annotations(append(legacySource,
		"database.payments.owner", "orders_owner",
		"database.reports.owner", "",
		"publication.pub_all.dbname", "app",
		"publication.pub_some.dbname", "app",
		"publication.pub_some.tables", "public.orders, public.items",
		"subscription.migration_sub.dbname", "app",
		"subscription.migration_sub.publication", "dbaas_migration",
		"subscription.migration_sub.source", "legacy",
		"subscription.migration_sub.reclaim-policy", "delete",
		"schema-import-source", "legacy",
		"extension", "timescaledb",
	)...))
	require.NoError(t, err)
	assert.Equal(t, []logicalDatabase{{name: "payments", owner: "orders_owner"}, {name: "reports"}}, in.databases)
	assert.Equal(t, []publication{
		{name: "pub_all", dbname: "app"},
		{name: "pub_some", dbname: "app", tables: []string{"public.orders", "public.items"}},
	}, in.publications)
	assert.Equal(t, []subscription{{
		name: "migration_sub", dbname: "app", publication: "dbaas_migration", source: "legacy", reclaimPolicy: "delete",
	}}, in.subscriptions)
	assert.Equal(t, "pass", in.sources["legacy"].passwordKey)
	assert.Equal(t, "legacy", in.schemaImportSource)
	assert.True(t, in.replicaEnabled)
}

func TestParseInputsRejects(t *testing.T) {
	t.Parallel()
	for name, pairs := range map[string][]string{
		"unknown key":             {"databases", "a,b"},
		"unknown source field":    {"source.legacy.hostname", "x"},
		"invalid port":            {"source.legacy.port", "70000"},
		"invalid sslmode":         {"source.legacy.sslmode", "sometimes"},
		"invalid database":        {"database.Bad-Name.owner", ""},
		"slot-invalid sub name":   {"subscription.bad-name.dbname", "app"},
		"publication w/o dbname":  {"publication.pub.tables", "*"},
		"bad table":               {"publication.pub.dbname", "app", "publication.pub.tables", "orders"},
		"undefined source":        {"subscription.sub.dbname", "app", "subscription.sub.publication", "p", "subscription.sub.source", "nope"},
		"source without password": {"source.s.host", "h", "source.s.dbname", "d", "source.s.user", "u", "schema-import-source", "s"},
		"replica-enabled alone":   {"replica-enabled", "false"},
		"replica without user":    {"source.old.host", "h", "source.old.password-secret", "s", "replica-source", "old"},
		"replica without auth":    {"source.old.host", "h", "source.old.user", "u", "replica-source", "old"},
		"replica and import":      append(legacySource, "replica-source", "legacy", "schema-import-source", "legacy"),
		"mixed replica auth": {
			"source.a.cluster", "orders", "source.a.password-secret", "s", "source.a.ca-secret", "ca", "replica-source", "a",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parseInputs(annotations(pairs...))
			require.Error(t, err)
		})
	}
}

func TestEngineDatabasesAndReplication(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Annotations = annotations(append(legacySource,
		"database.payments.owner", "",
		"publication.pub_some.dbname", "app",
		"publication.pub_some.tables", "public.orders",
		"subscription.migration_sub.dbname", "app",
		"subscription.migration_sub.publication", "dbaas_migration",
		"subscription.migration_sub.source", "legacy",
		"subscription.migration_sub.reclaim-policy", "delete",
		"schema-import-source", "legacy",
	)...)
	stale := newUnstructured(DatabaseGVK, testNamespace, "orders-db-old")
	stale.SetLabels(map[string]string{OwnerLabel: testCluster})
	a, c := newTestApplier(t, db, nil, userSecret("orders_owner", ""), stale)
	a.in, a.inErr = parseInputs(db.Annotations)
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	ctx := context.Background()

	database := newUnstructured(DatabaseGVK, testNamespace, "orders-db-payments")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(database), database))
	assert.Equal(t, "orders_owner", nested(t, database.Object, "spec", "owner"), "defaults to the user Secret owner")
	assert.Equal(t, "delete", nested(t, database.Object, "spec", "databaseReclaimPolicy"))
	require.Error(t, c.Get(ctx, client.ObjectKeyFromObject(stale), stale), "undeclared databases are pruned")

	pub := newUnstructured(PublicationGVK, testNamespace, "orders-pub-pub-some")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pub), pub))
	assert.Equal(t, []any{map[string]any{"table": map[string]any{"schema": "public", "name": "orders"}}},
		nested(t, pub.Object, "spec", "target", "objects"))

	sub := newUnstructured(SubscriptionGVK, testNamespace, "orders-sub-migration-sub")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sub), sub))
	assert.Equal(t, "source-legacy", nested(t, sub.Object, "spec", "externalClusterName"))
	assert.Equal(t, "delete", nested(t, sub.Object, "spec", "subscriptionReclaimPolicy"))

	// Schema import and the subscription share one externalClusters entry for the source.
	external := nested(t, a.Object, "spec", "externalClusters").([]any) //nolint:forcetypeassert
	require.Len(t, external, 1)
	entry := external[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, map[string]any{
		"host": "legacy.example.internal", "port": "5432", "dbname": "orders", "user": "migration_user", "sslmode": "verify-full",
	}, entry["connectionParameters"])
	assert.Equal(t, map[string]any{"name": "migration-source", "key": "pass"}, entry["password"])

	imported := nested(t, a.Object, "spec", "bootstrap", "initdb", "import").(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, true, imported["schemaOnly"])
	assert.Equal(t, []any{"app"}, imported["databases"])
	assert.Equal(t, map[string]any{"externalCluster": "source-legacy"}, imported["source"])
}

func TestReplicaCluster(t *testing.T) {
	t.Parallel()
	db := testDB("16.4")
	db.Annotations = annotations(
		"source.zone-a.cluster", "zone-a/orders",
		"replica-source", "zone-a",
		"replica-enabled", "false",
	)
	a, _ := newTestApplier(t, db, nil, userSecret("orders_owner", ""))
	a.in, a.inErr = parseInputs(db.Annotations)
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	require.NoError(t, a.DataSource())

	assert.Equal(t, map[string]any{"pg_basebackup": map[string]any{
		"source": "source-zone-a", "database": "app", "owner": "orders_owner",
		"secret": map[string]any{"name": testUserSecret},
	}}, nested(t, a.Object, "spec", "bootstrap"), "no initdb; the application database comes from the user Secret")
	assert.Equal(t, map[string]any{"enabled": false, "source": "source-zone-a"}, nested(t, a.Object, "spec", "replica"))
	entry := nested(t, a.Object, "spec", "externalClusters").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, map[string]any{
		"host": "orders-rw.zone-a.svc", "port": "5432", "dbname": "postgres", "user": "streaming_replica", "sslmode": "verify-full",
	}, entry["connectionParameters"])
	assert.Equal(t, map[string]any{"name": "orders-replication", "key": "tls.crt"}, entry["sslCert"])
	assert.Equal(t, map[string]any{"name": "orders-ca", "key": "ca.crt"}, entry["sslRootCert"])

	db.Spec.DataSource = &everestv1alpha1.DataSource{DBClusterBackupName: "nightly"}
	require.Error(t, a.DataSource(), "a replica can not also restore")
}

func TestDataSourceKeepsSubscriptionSources(t *testing.T) {
	t.Parallel()
	a, _ := restoreFixture(t, nil)
	a.in, a.inErr = parseInputs(annotations(append(legacySource,
		"subscription.sub.dbname", "app", "subscription.sub.publication", "p", "subscription.sub.source", "legacy")...))
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	require.NoError(t, a.DataSource())
	assert.Len(t, nested(t, a.Object, "spec", "externalClusters"), 2, "restore source and subscription source")
}

func TestValidateCreateAnnotations(t *testing.T) {
	t.Parallel()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(crd(clusterCRDName)).Build()
	db := testDB("16.4")
	db.Annotations = annotations("databases", "a")
	assert.Len(t, ValidateCreate(context.Background(), c, db), 1)

	db.Annotations = annotations(append(legacySource, "schema-import-source", "legacy")...)
	db.Spec.DataSource = &everestv1alpha1.DataSource{DBClusterBackupName: "nightly"}
	assert.Len(t, ValidateCreate(context.Background(), c, db), 1)
}

func TestReplicaFromExternalPostgres(t *testing.T) {
	t.Parallel()
	db := testDB("14.24")
	db.Annotations = annotations(
		"source.trove.host", "192.168.250.1",
		"source.trove.user", "db_user",
		"source.trove.sslmode", "disable",
		"source.trove.password-secret", "source-postgres-credentials",
		"replica-source", "trove",
	)
	a, _ := newTestApplier(t, db, nil, userSecret("orders_owner", ""))
	a.in, a.inErr = parseInputs(db.Annotations)
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	require.NoError(t, a.DataSource())
	entry := nested(t, a.Object, "spec", "externalClusters").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, map[string]any{
		"host": "192.168.250.1", "port": "5432", "dbname": "postgres", "user": "db_user", "sslmode": "disable",
	}, entry["connectionParameters"])
	assert.Equal(t, map[string]any{"name": "source-postgres-credentials", "key": "password"}, entry["password"])
	assert.Equal(t, map[string]any{"enabled": true, "source": "source-trove"}, nested(t, a.Object, "spec", "replica"))

	// The clone has no admin role (initdb never ran): granting it would stall role reconciliation.
	role := nested(t, a.Object, "spec", "managed", "roles").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, "orders_owner", role["name"])
	assert.NotContains(t, role, "inRoles")
	assert.Equal(t, map[string]any{"name": testUserSecret}, role["passwordSecret"])
}

func TestReplicationExpose(t *testing.T) {
	t.Parallel()
	db := testDB("14.24")
	db.Annotations = annotations(
		"source.trove.host", "192.168.250.1",
		"source.trove.user", "db_user",
		"source.trove.password-secret", "source-postgres-credentials",
		"replica-source", "trove",
		"replica-enabled", "false",
		"replication-expose", "192.168.250.1/32, 10.0.0.0/24",
	)
	db.Spec.Proxy = everestv1alpha1.Proxy{
		Type: everestv1alpha1.ProxyTypePGBouncer, Replicas: pointer.ToInt32(1), Config: "pool_mode = session",
		Expose: everestv1alpha1.Expose{Type: everestv1alpha1.ExposeTypeLoadBalancer},
	}
	a, _ := newTestApplier(t, db, nil, userSecret("orders_owner", ""))
	a.in, a.inErr = parseInputs(db.Annotations)
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	require.NoError(t, a.Proxy())

	// The pooler keeps its own Service; the only additional one goes straight to the primary.
	additional := nested(t, a.Object, "spec", "managed", "services", "additional").([]any) //nolint:forcetypeassert
	require.Len(t, additional, 1)
	svc := additional[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, "rw", svc["selectorType"])
	template := svc["serviceTemplate"].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, map[string]any{"name": "orders-rw-replication"}, template["metadata"])
	assert.Equal(t, []any{"192.168.250.1/32", "10.0.0.0/24"}, nested(t, template, "spec", "loadBalancerSourceRanges"))
	assert.Equal(t, "LoadBalancer", nested(t, template, "spec", "type"))

	role := nested(t, a.Object, "spec", "managed", "roles").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, true, role["replication"], "the subscriber logs in as the owner")

	// Removing the annotation takes both away.
	delete(db.Annotations, AnnotationReplicationExpose)
	a.in, a.inErr = parseInputs(db.Annotations)
	require.NoError(t, a.inErr)
	require.NoError(t, a.Engine())
	require.NoError(t, a.Proxy())
	_, found, _ := unstructured.NestedSlice(a.Object, "spec", "managed", "services", "additional")
	assert.False(t, found)
	role = nested(t, a.Object, "spec", "managed", "roles").([]any)[0].(map[string]any) //nolint:forcetypeassert
	assert.Equal(t, false, role["replication"])

	_, err := parseInputs(annotations("replication-expose", "not-a-cidr"))
	require.Error(t, err)
}
