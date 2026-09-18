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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

const (
	testNamespace     = "databases"
	testClusterName   = "orders"
	testInstanceName  = "orders-1"
	testClusterLabel  = "cnpg.io/cluster"
	fieldMetadata     = "metadata"
	testEngineVersion = "16.4"
	testEngineImage   = "registry.example/postgresql:16.4"
	testUserSecret    = "app-creds"
	listKindSuffix    = "List"
	testPoolerName    = "orders-pooler-rw"
)

// crdObject dựng một CustomResourceDefinition tối thiểu để provider phát hiện CRD có mặt.
func crdObject(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion":  "apiextensions.k8s.io/v1",
		"kind":        "CustomResourceDefinition",
		fieldMetadata: map[string]any{fieldName: name},
	}}
}

func TestApplierEngine(t *testing.T) {
	t.Parallel()
	storageClass := "longhorn"
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type:     everestv1alpha1.DatabaseEnginePostgresql,
				Version:  testEngineVersion,
				Replicas: 3,
				Storage: everestv1alpha1.Storage{
					Size: resource.MustParse("20Gi"), Class: &storageClass,
				},
				Resources: everestv1alpha1.Resources{
					Limits: &everestv1alpha1.ResourceSpec{
						CPU: resource.MustParse("1"), Memory: resource.MustParse("2Gi"),
					},
					Requests: &everestv1alpha1.ResourceSpec{
						CPU: resource.MustParse("500m"), Memory: resource.MustParse("1Gi"),
					},
				},
				Config: "max_connections = 200\nshared_buffers = '512MB'\n",
			},
		},
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Engine: everestv1alpha1.ComponentsMap{
					testEngineVersion: {ImagePath: testEngineImage},
				},
			},
		},
	}
	c := fake.NewClientBuilder().Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: engine, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.ResetDefaults())
	require.NoError(t, a.Engine())

	assert.Equal(t, int64(3), mustNested(t, provider.Object, "spec", "instances"))
	assert.Equal(t, testEngineImage, mustNested(t, provider.Object, "spec", "imageName"))
	assert.Equal(t, "20Gi", mustNested(t, provider.Object, "spec", "storage", "size"))
	assert.Equal(t, "longhorn", mustNested(t, provider.Object, "spec", "storage", "storageClass"))
	assert.Equal(t, "200", mustNested(t, provider.Object, "spec", "postgresql", "parameters", "max_connections"))
	assert.Equal(t, "512MB", mustNested(t, provider.Object, "spec", "postgresql", "parameters", "shared_buffers"))

	_, found, err := unstructured.NestedMap(provider.Object, "spec", "monitoring")
	require.NoError(t, err)
	assert.False(t, found, "enablePodMonitor must stay unset when the PodMonitor CRD is not installed")
}

func TestApplierEngineEnablesPodMonitorWhenCRDInstalled(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type:     everestv1alpha1.DatabaseEnginePostgresql,
				Version:  testEngineVersion,
				Replicas: 1,
				Storage:  everestv1alpha1.Storage{Size: resource.MustParse("1Gi")},
			},
		},
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Engine: everestv1alpha1.ComponentsMap{testEngineVersion: {ImagePath: testEngineImage}},
			},
		},
	}
	crd := crdObject("podmonitors.monitoring.coreos.com")
	c := fake.NewClientBuilder().WithObjects(crd).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: engine, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.ResetDefaults())
	require.NoError(t, a.Engine())

	assert.Equal(t, true, mustNested(t, provider.Object, "spec", "monitoring", "enablePodMonitor"))
}

// [CUSTOM CNPG] owner của database ứng dụng lấy từ key "username" trong Secret, và KHÔNG được là
// superuser: CNPG tắt password của superuser (enableSuperuserAccess mặc định false), nên cụm sẽ
// lên xanh mà không ai kết nối được.
func TestApplierEngineInitdbOwner(t *testing.T) {
	t.Parallel()
	newDB := func(secretName string) *everestv1alpha1.DatabaseCluster {
		return &everestv1alpha1.DatabaseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
			Spec: everestv1alpha1.DatabaseClusterSpec{Engine: everestv1alpha1.Engine{
				Type: everestv1alpha1.DatabaseEnginePostgresql, Version: testEngineVersion, Replicas: 1,
				Storage:         everestv1alpha1.Storage{Size: resource.MustParse("1Gi")},
				UserSecretsName: secretName,
			}},
		}
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Engine: everestv1alpha1.ComponentsMap{testEngineVersion: {ImagePath: testEngineImage}},
			},
		},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	// runEngine chạy bước Engine() với userSecretsName trỏ vào một Secret có username cho trước.
	runEngine := func(t *testing.T, secretName, username string) (*Provider, error) {
		t.Helper()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: testNamespace},
			Data:       map[string][]byte{corev1.BasicAuthUsernameKey: []byte(username)},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		provider := &Provider{
			Unstructured:    &unstructured.Unstructured{Object: map[string]any{}},
			ProviderOptions: providers.ProviderOptions{DB: newDB(secretName), DBEngine: engine, C: c},
		}
		a := &applier{Provider: provider, ctx: context.Background()}
		require.NoError(t, a.ResetDefaults())
		return provider, a.Engine()
	}

	t.Run("owner lay tu secret", func(t *testing.T) {
		t.Parallel()
		provider, err := runEngine(t, testUserSecret, "dbadmin")
		require.NoError(t, err)
		assert.Equal(t, "dbadmin", mustNested(t, provider.Object, "spec", "bootstrap", "initdb", "owner"))
		assert.Equal(t, "app", mustNested(t, provider.Object, "spec", "bootstrap", "initdb", "database"))
	})

	t.Run("tu choi superuser", func(t *testing.T) {
		t.Parallel()
		_, err := runEngine(t, "su-creds", "postgres")
		require.ErrorContains(t, err, "superuser")
	})

	t.Run("thieu key username", func(t *testing.T) {
		t.Parallel()
		_, err := runEngine(t, "empty-creds", "")
		require.ErrorContains(t, err, "username")
	})
}

func TestStatusReady(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace}}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"instances": int64(3)},
		"status": map[string]any{
			"readyInstances": int64(3),
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "message": "Cluster in healthy state",
			}},
		},
	}}
	provider := &Provider{
		Unstructured:    cluster,
		ProviderOptions: providers.ProviderOptions{DB: db},
	}
	status, complete, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.True(t, complete)
	assert.Equal(t, everestv1alpha1.AppStateReady, status.Status)
	assert.Equal(t, int32(3), status.Size)
	assert.Equal(t, int32(3), status.Ready)
	assert.Equal(t, "orders-rw.databases.svc", status.Hostname)
}

// [CUSTOM CNPG] Bật pooler bằng spec.proxy.type=pgbouncer: Everest dựng CRD Pooler của CNPG, lấy
// image từ ClusterImageCatalog, và đường vào chuyển sang Service của pooler.
func TestProxyCreatesPooler(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	for _, gvk := range []schema.GroupVersionKind{PoolerGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk
		listGVK.Kind += listKindSuffix
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	replicas := int32(2)
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type: everestv1alpha1.DatabaseEnginePostgresql, Version: testEngineVersion,
			},
			Proxy: everestv1alpha1.Proxy{
				Type: everestv1alpha1.ProxyTypePGBouncer, Replicas: &replicas,
			},
		},
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Proxy: map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
					everestv1alpha1.ProxyTypePGBouncer: {
						testEngineVersion: {ImagePath: "ghcr.io/cloudnative-pg/pgbouncer:1.25.1@sha256:abc"},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: engine, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Proxy())

	pooler := &unstructured.Unstructured{Object: map[string]any{}}
	pooler.SetGroupVersionKind(PoolerGVK)
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testPoolerName}, pooler))
	assert.Equal(t, testClusterName, mustNested(t, pooler.Object, "spec", "cluster", fieldName))
	assert.Equal(t, "rw", mustNested(t, pooler.Object, "spec", "type"))
	assert.Equal(t, int64(2), mustNested(t, pooler.Object, "spec", "instances"))
	// Image phải tới từ catalog, không ghép chuỗi.
	assert.Equal(t, "ghcr.io/cloudnative-pg/pgbouncer:1.25.1@sha256:abc",
		mustNested(t, pooler.Object, "spec", "pgbouncer", "image"))
	// session mode: transaction phá prepared statement và advisory lock, nền tảng không tự bật thay.
	assert.Equal(t, "session", mustNested(t, pooler.Object, "spec", "pgbouncer", "poolMode"))

	// CNPG không tự tạo PDB cho Pooler nên Everest phải tạo.
	pdb := &policyv1.PodDisruptionBudget{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testPoolerName}, pdb))

	// Hợp đồng kết nối chuyển sang pooler.
	status, _, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "orders-pooler-rw.databases.svc", status.Hostname)
}

// [CUSTOM CNPG] Tắt pooler phải XOÁ hẳn Pooler, không để lại object mồ côi — cùng triết lý với
// cách Backup() xoá ScheduledBackup khi schedule.Enabled=false.
func TestProxyDeletesPoolerWhenDisabled(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(PoolerGVK, &unstructured.Unstructured{})
	listGVK := PoolerGVK
	listGVK.Kind += listKindSuffix
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})

	existing := newUnstructured(PoolerGVK, testNamespace, testPoolerName)
	existingPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: testPoolerName, Namespace: testNamespace},
	}
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, existing, existingPDB).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: &everestv1alpha1.DatabaseEngine{}, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Proxy())

	gone := &unstructured.Unstructured{Object: map[string]any{}}
	gone.SetGroupVersionKind(PoolerGVK)
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testPoolerName}, gone)
	require.Error(t, err, "Pooler phải bị xoá khi tắt")

	// PDB cũng phải biến mất: owner của nó là DatabaseCluster nên GC không dọn hộ.
	pdb := &policyv1.PodDisruptionBudget{}
	err = c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: testPoolerName}, pdb)
	require.Error(t, err, "PDB của pooler phải bị xoá khi tắt")

	status, _, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "orders-rw.databases.svc", status.Hostname)
}

// [CUSTOM CNPG] config và storage vẫn bị chặn: proxy.config là chuỗi INI tự do, cho qua nghĩa là
// cho phép đặt bất kỳ tham số nào kể cả thứ phá pool.
func TestProxyRejectsFreeFormConfig(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Proxy: everestv1alpha1.Proxy{
				Type: everestv1alpha1.ProxyTypePGBouncer, Config: "pool_mode = statement",
			},
		},
	}
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db},
	}
	err := (&applier{Provider: provider, ctx: context.Background()}).Proxy()
	require.ErrorContains(t, err, "spec.proxy.config")
}

func TestBarmanObjectStoreS3(t *testing.T) {
	t.Parallel()
	storage := &everestv1alpha1.BackupStorage{Spec: everestv1alpha1.BackupStorageSpec{
		Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", EndpointURL: "https://s3.example",
		CredentialsSecretName: "backup-creds",
	}}
	config, err := BarmanObjectStore(storage, "s3-region")
	require.NoError(t, err)
	// Gốc bucket: store dùng chung, mỗi cụm tách thư mục bằng serverName.
	assert.Equal(t, "s3://backups", config["destinationPath"])
	assert.Equal(t, "https://s3.example", config["endpointURL"])
	credentials := config["s3Credentials"].(map[string]any)
	assert.Equal(t, map[string]any{fieldName: "backup-creds", fieldKey: "AWS_ACCESS_KEY_ID"}, credentials["accessKeyId"])
}

// [CUSTOM CNPG] Barman chỉ nhận region qua secret reference. Region PHẢI trỏ vào Secret do Everest
// sở hữu, không phải Secret credential của người dùng — secret đó chỉ có access key và secret key,
// nên trỏ nhầm làm WAL archiving chết với "missing key AWS_REGION, inside secret".
func TestBarmanObjectStoreRegionUsesOwnedSecret(t *testing.T) {
	t.Parallel()
	storage := &everestv1alpha1.BackupStorage{Spec: everestv1alpha1.BackupStorageSpec{
		Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", Region: "us-east-1",
		CredentialsSecretName: "backup-creds",
	}}
	config, err := BarmanObjectStore(storage, "s3-region")
	require.NoError(t, err)
	credentials, ok := config["s3Credentials"].(map[string]any)
	require.True(t, ok)
	assert.Equal(
		t,
		map[string]any{fieldName: "s3-region", fieldKey: regionSecretKey},
		credentials["region"],
	)
	// Access key vẫn lấy từ secret của người dùng.
	assert.Equal(
		t,
		map[string]any{fieldName: "backup-creds", fieldKey: "AWS_ACCESS_KEY_ID"},
		credentials["accessKeyId"],
	)
}

// newScheduledBackupFixture dựng scheme có ScheduledBackup/ObjectStore, một DatabaseCluster với lịch
// backup hằng ngày và BackupStorage S3 mà lịch đó trỏ tới.
func newScheduledBackupFixture(t *testing.T) (*runtime.Scheme, *everestv1alpha1.DatabaseCluster, *everestv1alpha1.BackupStorage) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	for _, gvk := range []schema.GroupVersionKind{ScheduledBackupGVK, ObjectStoreGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk
		listGVK.Kind += listKindSuffix
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace, UID: types.UID("uid-1")},
		Spec: everestv1alpha1.DatabaseClusterSpec{Backup: everestv1alpha1.Backup{Schedules: []everestv1alpha1.BackupSchedule{{
			Name: "daily", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: "s3",
		}}}},
	}
	storage := &everestv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: testNamespace},
		Spec:       everestv1alpha1.BackupStorageSpec{Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", CredentialsSecretName: "creds"},
	}
	return scheme, db, storage
}

func TestBackupCreatesScheduledBackup(t *testing.T) {
	t.Parallel()
	scheme, db, storage := newScheduledBackupFixture(t)
	// Barman Cloud Plugin phải có mặt thì Everest mới dựng được ObjectStore.
	pluginCRD := crdObject(consts.BarmanCloudObjectStoreCRDName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, storage, pluginCRD).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Backup())
	created := &unstructured.Unstructured{Object: map[string]any{}}
	created.SetGroupVersionKind(ScheduledBackupGVK)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "orders-daily"}, created))
	assert.Equal(t, "0 0 2 * * *", mustNested(t, created.Object, "spec", "schedule"))
	assert.Equal(t, "none", mustNested(t, created.Object, "spec", "backupOwnerReference"))
	assert.Equal(t, testClusterName, mustNested(t, created.Object, "spec", "cluster", fieldName))
	// [CUSTOM CNPG] Base backup do Barman Cloud Plugin chụp, không phải interface in-tree.
	assert.Equal(t, "plugin", mustNested(t, created.Object, "spec", "method"))
	assert.Equal(t, consts.BarmanCloudPluginName,
		mustNested(t, created.Object, "spec", "pluginConfiguration", fieldName))

	// Cluster trỏ tới ObjectStore qua spec.plugins và KHÔNG được còn spec.backup.barmanObjectStore:
	// CNPG chặn cứng việc bật isWALArchiver khi cấu hình in-tree còn tồn tại.
	plugins, ok := mustNested(t, provider.Object, "spec", "plugins").([]any)
	require.True(t, ok)
	require.Len(t, plugins, 1)
	plugin, ok := plugins[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, consts.BarmanCloudPluginName, plugin[fieldName])
	assert.Equal(t, true, plugin["isWALArchiver"])
	parameters, ok := plugin["parameters"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3", parameters[parameterBarmanObjectName])
	assert.Equal(t, "orders-uid-1", parameters[parameterServerName])
	_, found, err := unstructured.NestedFieldNoCopy(provider.Object, "spec", "backup", "barmanObjectStore")
	require.NoError(t, err)
	assert.False(t, found, "in-tree barmanObjectStore phải vắng mặt khi dùng plugin")

	// ObjectStore dùng chung mang tên BackupStorage, trỏ gốc bucket và thuộc về BackupStorage —
	// không phải của cụm, nên xoá cụm không kéo store theo.
	store := &unstructured.Unstructured{Object: map[string]any{}}
	store.SetGroupVersionKind(ObjectStoreGVK)
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: "s3"}, store))
	assert.Equal(t, "s3://backups",
		mustNested(t, store.Object, "spec", "configuration", "destinationPath"))
	owner := metav1.GetControllerOf(store)
	require.NotNil(t, owner)
	assert.Equal(t, "BackupStorage", owner.Kind)
	assert.Equal(t, "s3", owner.Name)
}

// [CUSTOM CNPG] Chính sách backup nằm ở BackupStorage.spec.objectStore và được gộp vào ObjectStore
// dùng chung (retention, sidecar, wal/data). Cụm restore ghi WAL dưới serverName của CHÍNH nó và
// đọc backup của cụm nguồn dưới serverName của cụm nguồn, trong cùng một store.
func TestBackupObjectStorePolicyFromBackupStorage(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	for _, gvk := range []schema.GroupVersionKind{ScheduledBackupGVK, ObjectStoreGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk
		listGVK.Kind += listKindSuffix
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	schedules := []everestv1alpha1.BackupSchedule{{Name: "daily", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: "s3"}}
	source := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace, UID: types.UID("uid-1")},
		Spec:       everestv1alpha1.DatabaseClusterSpec{Backup: everestv1alpha1.Backup{Schedules: schedules}},
	}
	restored := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-restored", Namespace: testNamespace, UID: types.UID("uid-2")},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Backup:     everestv1alpha1.Backup{Schedules: schedules},
			DataSource: &everestv1alpha1.DataSource{DBClusterBackupName: "orders-backup"},
		},
	}
	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-backup", Namespace: testNamespace},
		Spec:       everestv1alpha1.DatabaseClusterBackupSpec{DBClusterName: testClusterName, BackupStorageName: "s3"},
	}
	storage := &everestv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: testNamespace},
		Spec: everestv1alpha1.BackupStorageSpec{
			Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", CredentialsSecretName: "creds",
			ObjectStore: &runtime.RawExtension{Raw: []byte(`{
				"retentionPolicy": "7d",
				"instanceSidecarConfiguration": {"retentionPolicyIntervalSeconds": 1800},
				"configuration": {
					"wal": {"compression": "zstd", "maxParallel": 2},
					"data": {"compression": "gzip", "jobs": 2}
				}
			}`)},
		},
	}
	pluginCRD := crdObject(consts.BarmanCloudObjectStoreCRDName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source, restored, backup, storage, pluginCRD).Build()
	getStore := func(name string) map[string]any {
		store := &unstructured.Unstructured{Object: map[string]any{}}
		store.SetGroupVersionKind(ObjectStoreGVK)
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, store))
		return store.Object
	}

	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: restored, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.Backup())
	require.NoError(t, a.DataSource())

	archive := getStore("s3")
	assert.Equal(t, "7d", mustNested(t, archive, "spec", "retentionPolicy"))
	assert.Equal(t, int64(1800), mustNested(t, archive, "spec", "instanceSidecarConfiguration", "retentionPolicyIntervalSeconds"))
	// "Ở đâu" vẫn do Everest sinh, "thế nào" gộp vào cùng khối configuration.
	assert.Equal(t, "s3://backups", mustNested(t, archive, "spec", "configuration", "destinationPath"))
	assert.Equal(t, "zstd", mustNested(t, archive, "spec", "configuration", "wal", "compression"))
	assert.Equal(t, int64(2), mustNested(t, archive, "spec", "configuration", "data", "jobs"))

	plugins, ok := mustNested(t, provider.Object, "spec", "plugins").([]any)
	require.True(t, ok)
	archiver, ok := plugins[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{parameterBarmanObjectName: "s3", parameterServerName: "orders-restored-uid-2"},
		archiver[fieldParameters])

	external, ok := mustNested(t, provider.Object, "spec", "externalClusters").([]any)
	require.True(t, ok)
	source0, ok := external[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{parameterBarmanObjectName: "s3", parameterServerName: "orders-uid-1"},
		mustNested(t, source0, "plugin", fieldParameters))

	// Không còn store recovery riêng khi đọc từ layout của store dùng chung.
	missing := &unstructured.Unstructured{Object: map[string]any{}}
	missing.SetGroupVersionKind(ObjectStoreGVK)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "orders-restored-recovery"}, missing)
	assert.True(t, apierrors.IsNotFound(err), "store recovery chỉ dùng cho backupSource.path")
}

// [CUSTOM CNPG] Barman Cloud Plugin cài riêng, không đi kèm CNPG operator. Thiếu nó thì lỗi thô
// từ API server là "no matches for kind ObjectStore" — không nói gì về nguyên nhân thật. Everest
// phải báo thẳng rằng plugin chưa được cài.
func TestBackupRequiresBarmanCloudPlugin(t *testing.T) {
	t.Parallel()
	scheme, db, storage := newScheduledBackupFixture(t)
	// Không có CRD của plugin trong cụm.
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, storage).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	err := (&applier{Provider: provider, ctx: context.Background()}).Backup()
	require.ErrorContains(t, err, "Barman Cloud Plugin")
	require.ErrorContains(t, err, consts.BarmanCloudObjectStoreCRDName)
}

// [CUSTOM CNPG] backupSource.path trỏ vào đường dẫn tuỳ ý ngoài layout của store dùng chung, nên
// restore đọc qua store riêng <cụm>-recovery. Store đó không nhận chính sách của BackupStorage —
// retention không được áp lên dữ liệu nằm ngoài layout Everest quản.
func TestDataSourceBackupSourcePathUsesRecoveryStore(t *testing.T) {
	t.Parallel()
	scheme, db, storage := newScheduledBackupFixture(t)
	storage.Spec.ObjectStore = &runtime.RawExtension{Raw: []byte(`{"retentionPolicy": "7d"}`)}
	db.Spec.Backup.Schedules = nil
	db.Spec.DataSource = &everestv1alpha1.DataSource{BackupSource: &everestv1alpha1.BackupSource{
		Path: "s3://legacy/orders/uid-0/", BackupStorageName: "s3",
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(db, storage, crdObject(consts.BarmanCloudObjectStoreCRDName)).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).DataSource())

	store := &unstructured.Unstructured{Object: map[string]any{}}
	store.SetGroupVersionKind(ObjectStoreGVK)
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: "orders-recovery"}, store))
	assert.Equal(t, "s3://legacy/orders/uid-0", mustNested(t, store.Object, "spec", "configuration", "destinationPath"))
	_, found, err := unstructured.NestedFieldNoCopy(store.Object, "spec", "retentionPolicy")
	require.NoError(t, err)
	assert.False(t, found, "store recovery không được mang retention")
	assert.True(t, metav1.IsControlledBy(store, db), "store recovery thuộc về cụm restore")

	external, ok := mustNested(t, provider.Object, "spec", "externalClusters").([]any)
	require.True(t, ok)
	entry, ok := external[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{parameterBarmanObjectName: "orders-recovery", parameterServerName: testClusterName},
		mustNested(t, entry, "plugin", fieldParameters))
}

// [CUSTOM CNPG] Cụm tạo trước khi chuyển sang store dùng chung còn store riêng <cụm>-<storage> và
// Secret region của nó. Backup() dọn chúng — nhưng chỉ khi chính cụm này là controller owner.
func TestBackupDeletesLegacyObjectStore(t *testing.T) {
	t.Parallel()
	scheme, db, storage := newScheduledBackupFixture(t)
	require.NoError(t, corev1.AddToScheme(scheme))
	isController := true
	ownedByDB := []metav1.OwnerReference{{
		APIVersion: everestv1alpha1.GroupVersion.String(), Kind: "DatabaseCluster",
		Name: db.Name, UID: db.UID, Controller: &isController,
	}}
	legacy := newUnstructured(ObjectStoreGVK, testNamespace, "orders-s3")
	legacy.SetLabels(map[string]string{BackupStorageLabel: "s3"})
	legacy.SetOwnerReferences(ownedByDB)
	legacyRegion := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "orders-s3-region", Namespace: testNamespace, OwnerReferences: ownedByDB,
	}}
	// Trùng tên kiểu cũ nhưng KHÔNG thuộc cụm này (người dùng tự tạo): phải giữ nguyên.
	foreign := newUnstructured(ObjectStoreGVK, testNamespace, "orders-other")
	foreign.SetLabels(map[string]string{BackupStorageLabel: "other"})

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		db, storage, crdObject(consts.BarmanCloudObjectStoreCRDName), legacy, legacyRegion, foreign,
	).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Backup())

	get := func(obj client.Object, name string) error {
		return c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, obj)
	}
	gone := newUnstructured(ObjectStoreGVK, "", "")
	assert.True(t, apierrors.IsNotFound(get(gone, "orders-s3")), "store kiểu cũ của cụm phải bị xoá")
	assert.True(t, apierrors.IsNotFound(get(&corev1.Secret{}, "orders-s3-region")), "Secret region kiểu cũ phải bị xoá")
	require.NoError(t, get(newUnstructured(ObjectStoreGVK, "", ""), "orders-other"))
	require.NoError(t, get(newUnstructured(ObjectStoreGVK, "", ""), "s3"))
}

// [CUSTOM CNPG] BackupStorage controller gọi ReconcileSharedObjectStore cho MỌI BackupStorage, kể cả
// cái chỉ phục vụ PXC/PSMDB. Hai trường hợp "không có store" phải nhận ra được bằng errors.Is để
// controller bỏ qua thay vì requeue mãi.
func TestReconcileSharedObjectStoreSentinels(t *testing.T) {
	t.Parallel()
	scheme, _, storage := newScheduledBackupFixture(t)

	noPlugin := fake.NewClientBuilder().WithScheme(scheme).WithObjects(storage).Build()
	require.ErrorIs(t, ReconcileSharedObjectStore(context.Background(), noPlugin, storage), ErrBarmanCloudPluginMissing)

	insecure := storage.DeepCopy()
	insecure.Spec.VerifyTLS = new(false)
	withPlugin := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(insecure, crdObject(consts.BarmanCloudObjectStoreCRDName)).Build()
	require.ErrorIs(t, ReconcileSharedObjectStore(context.Background(), withPlugin, insecure), ErrStorageUnsupported)
}

func TestStatusResizingVolumes(t *testing.T) {
	t.Parallel()
	// A PVC in the Resizing condition represents the online expansion window;
	// the provider should expose that transient state through Everest status.
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace}}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: testInstanceName, Namespace: testNamespace, Labels: map[string]string{testClusterLabel: testClusterName}},
		Status: corev1.PersistentVolumeClaimStatus{Conditions: []corev1.PersistentVolumeClaimCondition{{
			Type: corev1.PersistentVolumeClaimResizing, Status: corev1.ConditionTrue,
		}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"instances": int64(1)}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	status, complete, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.True(t, complete)
	assert.Equal(t, everestv1alpha1.AppState("resizingVolumes"), status.Status)
}

func TestStatusResizeCompletedReturnsToReady(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNamespace},
		Status:     everestv1alpha1.DatabaseClusterStatus{Status: everestv1alpha1.AppStateResizingVolumes},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: testInstanceName, Namespace: testNamespace, Labels: map[string]string{testClusterLabel: testClusterName},
		},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")},
		}},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	provider := &Provider{
		Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"spec": map[string]any{"instances": int64(1)},
			"status": map[string]any{
				"readyInstances": int64(1),
				"conditions":     []any{map[string]any{"type": "Ready", "status": "True"}},
			},
		}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}

	status, complete, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.True(t, complete)
	assert.Equal(t, everestv1alpha1.AppStateReady, status.Status)
}

func TestStatusResizeFailureUsesCNPGLabels(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{
		Name: testClusterName, Namespace: testNamespace, Generation: 2,
	}}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: testInstanceName, Namespace: testNamespace, Labels: map[string]string{testClusterLabel: testClusterName},
		},
		Status: corev1.PersistentVolumeClaimStatus{Conditions: []corev1.PersistentVolumeClaimCondition{{
			Type: corev1.PersistentVolumeClaimControllerResizeError, Status: corev1.ConditionTrue, Message: "quota exceeded",
		}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"instances": int64(1)}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}

	status, _, err := provider.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, everestv1alpha1.AppState(everestv1alpha1.AppStateResizingVolumes), status.Status)
	condition := meta.FindStatusCondition(status.Conditions, everestv1alpha1.ConditionTypeVolumeResizeFailed)
	require.NotNil(t, condition)
	assert.Equal(t, "quota exceeded", condition.Message)
}

func TestApplierPodSchedulingPolicyUsesCNPGSchema(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	policy := &everestv1alpha1.PodSchedulingPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "spread-postgresql"},
		Spec: everestv1alpha1.PodSchedulingPolicySpec{
			EngineType: everestv1alpha1.DatabaseEnginePostgresql,
			AffinityConfig: &everestv1alpha1.AffinityConfig{
				PostgreSQL: &everestv1alpha1.PostgreSQLAffinityConfig{Engine: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{}},
					PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
						TopologyKey: "topology.kubernetes.io/zone",
					}}},
				}},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(policy).Build()
	db := &everestv1alpha1.DatabaseCluster{Spec: everestv1alpha1.DatabaseClusterSpec{
		PodSchedulingPolicyName: policy.Name,
	}}
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}

	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).PodSchedulingPolicy())
	rawAffinity := mustNested(t, provider.Object, "spec", "affinity")
	affinity, ok := rawAffinity.(map[string]any)
	require.True(t, ok)
	assert.Contains(t, affinity, "nodeAffinity")
	assert.Contains(t, affinity, "additionalPodAntiAffinity")
	assert.NotContains(t, affinity, "podAntiAffinity")
}

func TestApplierEnginePreventsStorageShrink(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{Spec: everestv1alpha1.DatabaseClusterSpec{Engine: everestv1alpha1.Engine{
		Version: testEngineVersion, Replicas: 3, Storage: everestv1alpha1.Storage{Size: resource.MustParse("10Gi")},
	}}}
	provider := &Provider{
		Unstructured: &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{
			"imageName": "ghcr.io/cloudnative-pg/postgresql:16.3",
			"storage":   map[string]any{"size": "20Gi"},
		}}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: &everestv1alpha1.DatabaseEngine{}},
	}

	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Engine())
	assert.Equal(t, "20Gi", mustNested(t, provider.Object, "spec", "storage", "size"))
	require.NotNil(t, meta.FindStatusCondition(db.Status.Conditions, everestv1alpha1.ConditionTypeCannotResizeVolume))
}

func TestValidateVersionChange(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.3", testEngineVersion))
	require.ErrorContains(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.4", "16.3"), "downgrade")
	require.ErrorContains(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.4", "17.1"), "minor versions")

	// [CUSTOM CNPG] Image từ ClusterImageCatalog luôn pin digest, mà digest cũng chứa dấu ":".
	// Không cắt digest trước thì tag bị nhặt nhầm thành chuỗi hex và guard hỏng im lặng.
	digestImage := "ghcr.io/cloudnative-pg/postgresql:17.11-202608310816-standard-bookworm" +
		"@sha256:e8ffaff9d17011fb71f264d857c3b4c54cb86ed0443a4ecb0298cb96be708d4e"
	require.NoError(t, validateVersionChange(digestImage, "17.12"))
	require.ErrorContains(t, validateVersionChange(digestImage, "17.4"), "downgrade")
	require.ErrorContains(t, validateVersionChange(digestImage, "18.6"), "minor versions")
}

func mustNested(t *testing.T, object map[string]any, fields ...string) any {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}
