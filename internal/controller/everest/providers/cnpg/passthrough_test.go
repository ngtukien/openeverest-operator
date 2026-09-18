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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

// newPassthroughApplier dựng applier cho một DatabaseCluster có spec.cnpg, với Secret "app-creds"
// (username "db_user") sẵn trong fake client.
func newPassthroughApplier(t *testing.T, cnpgSpec string, mutate func(*everestv1alpha1.DatabaseCluster)) (*applier, *Provider) {
	t.Helper()
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type: everestv1alpha1.DatabaseEnginePostgresql, Version: "16.4", Replicas: 3,
				Storage:         everestv1alpha1.Storage{Size: resource.MustParse("1Gi")},
				UserSecretsName: "app-creds",
			},
			CNPG: &runtime.RawExtension{Raw: []byte(cnpgSpec)},
		},
	}
	if mutate != nil {
		mutate(db)
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Engine: everestv1alpha1.ComponentsMap{"16.4": {ImagePath: "registry.example/postgresql:16.4"}},
			},
		},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-creds", Namespace: "databases"},
		Data:       map[string][]byte{corev1.BasicAuthUsernameKey: []byte("db_user")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: engine, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.ResetDefaults())
	require.NoError(t, a.Engine())
	return a, provider
}

// [CUSTOM CNPG] Cơ chế CNPG tự reconcile phải đi thẳng xuống Cluster, và phần Everest sinh ra phải
// gộp được với phần người dùng khai cùng chỗ (initdb, managed.roles) thay vì đè nhau.
func TestPassthroughMergesCNPGNativeFields(t *testing.T) {
	t.Parallel()
	a, provider := newPassthroughApplier(t, `{
		"primaryUpdateStrategy": "supervised",
		"postgresql": {"synchronous": {"method": "any", "number": 1, "dataDurability": "required"}},
		"replicationSlots": {"highAvailability": {"enabled": true}},
		"storage": {"resizeInUseVolumes": true},
		"bootstrap": {"initdb": {
			"database": "trove",
			"dataChecksums": true,
			"import": {"type": "microservice", "databases": ["trove"], "source": {"externalCluster": "source-db"}}
		}},
		"externalClusters": [{"name": "source-db", "connectionParameters": {"host": "192.168.250.1", "dbname": "trove"}}],
		"managed": {"roles": [{"name": "reporting", "ensure": "present", "login": true}]}
	}`, nil)
	require.NoError(t, a.Passthrough())
	spec := provider.Object

	assert.Equal(t, "supervised", mustNested(t, spec, "spec", "primaryUpdateStrategy"))
	assert.Equal(t, int64(1), mustNested(t, spec, "spec", "postgresql", "synchronous", "number"))
	assert.Equal(t, true, mustNested(t, spec, "spec", "replicationSlots", "highAvailability", "enabled"))
	// storage gộp theo field: size từ spec.engine, resizeInUseVolumes từ spec.cnpg.
	assert.Equal(t, "1Gi", mustNested(t, spec, "spec", "storage", "size"))
	assert.Equal(t, true, mustNested(t, spec, "spec", "storage", "resizeInUseVolumes"))
	// initdb: database lấy từ spec.cnpg, owner/secret từ userSecretsName, import đi thẳng xuống.
	assert.Equal(t, "trove", mustNested(t, spec, "spec", "bootstrap", "initdb", "database"))
	assert.Equal(t, "db_user", mustNested(t, spec, "spec", "bootstrap", "initdb", "owner"))
	assert.Equal(t, "app-creds", mustNested(t, spec, "spec", "bootstrap", "initdb", "secret", "name"))
	assert.Equal(t, "microservice", mustNested(t, spec, "spec", "bootstrap", "initdb", "import", "type"))
	assert.Len(t, mustNested(t, spec, "spec", "externalClusters"), 1)

	// CNPG chỉ thấy Secret người dùng tạo đổi password nếu Secret mang label này.
	secret := &corev1.Secret{}
	require.NoError(t, a.C.Get(context.Background(), types.NamespacedName{Namespace: "databases", Name: "app-creds"}, secret))
	assert.Equal(t, "true", secret.Labels["cnpg.io/reload"])

	roles, ok := mustNested(t, spec, "spec", "managed", "roles").([]any)
	require.True(t, ok)
	require.Len(t, roles, 2, "role Everest sinh từ userSecretsName và role người dùng khai phải cùng tồn tại")
	owner, ok := roles[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "db_user", owner["name"])
	assert.Equal(t, map[string]any{"name": "app-creds"}, owner["passwordSecret"])
}

// [CUSTOM CNPG] storage và plugins gộp theo từng field/tên: pvcTemplate, resizeInUseVolumes đi
// cùng size/class của spec.engine; plugin người dùng khai sống chung với plugin backup Everest sinh.
func TestPassthroughMergesStorageAndPlugins(t *testing.T) {
	t.Parallel()
	a, provider := newPassthroughApplier(t, `{
		"storage": {"resizeInUseVolumes": true, "pvcTemplate": {"accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem"}},
		"walStorage": {"size": "2Gi", "storageClass": "longhorn-wal"},
		"plugins": [
			{"name": "barman-cloud.cloudnative-pg.io", "parameters": {"serverName": "orders-v2"}},
			{"name": "cnpg-i-hello-world.cloudnative-pg.io", "enabled": true}
		]
	}`, func(db *everestv1alpha1.DatabaseCluster) {
		class := "longhorn"
		db.Spec.Engine.Storage.Class = &class
	})
	// Stand-in for Backup(): the archiver entry it generates.
	require.NoError(t, unstructured.SetNestedSlice(provider.Object,
		[]any{archiverPluginConfiguration("orders-seaweedfs")}, "spec", "plugins"))
	require.NoError(t, a.Passthrough())
	spec := provider.Object

	assert.Equal(t, "1Gi", mustNested(t, spec, "spec", "storage", "size"))
	assert.Equal(t, "longhorn", mustNested(t, spec, "spec", "storage", "storageClass"))
	assert.Equal(t, true, mustNested(t, spec, "spec", "storage", "resizeInUseVolumes"))
	assert.Equal(t, []any{"ReadWriteOnce"}, mustNested(t, spec, "spec", "storage", "pvcTemplate", "accessModes"))
	assert.Equal(t, "2Gi", mustNested(t, spec, "spec", "walStorage", "size"))

	plugins, ok := mustNested(t, spec, "spec", "plugins").([]any)
	require.True(t, ok)
	require.Len(t, plugins, 2)
	barman, ok := plugins[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, barman["isWALArchiver"])
	assert.Equal(t, map[string]any{"barmanObjectName": "orders-seaweedfs", "serverName": "orders-v2"}, barman["parameters"])
}

// [CUSTOM CNPG] Hai nguồn cùng đặt một field khác giá trị thì phải lỗi và nêu đúng path — không được
// lặng lẽ để một bên thắng.
func TestPassthroughRejectsConflicts(t *testing.T) {
	t.Parallel()

	t.Run("parameter trung voi engine.config", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t, `{"postgresql": {"parameters": {"max_connections": "300"}}}`,
			func(db *everestv1alpha1.DatabaseCluster) { db.Spec.Engine.Config = "max_connections = 200" })
		require.ErrorContains(t, a.Passthrough(), "spec.cnpg.postgresql.parameters.max_connections")
	})

	t.Run("cung gia tri thi khong xung dot", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t, `{"postgresql": {"parameters": {"max_connections": "200"}}}`,
			func(db *everestv1alpha1.DatabaseCluster) { db.Spec.Engine.Config = "max_connections = 200" })
		require.NoError(t, a.Passthrough())
	})

	t.Run("field Everest so huu", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t, `{"instances": 5}`, nil)
		require.ErrorContains(t, a.Passthrough(), "set spec.engine.replicas instead")
	})

	t.Run("barmanObjectStore in-tree", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t, `{"backup": {"barmanObjectStore": {"destinationPath": "s3://x"}}}`, nil)
		require.ErrorContains(t, a.Passthrough(), "spec.cnpg.backup.barmanObjectStore is owned by Everest")
	})

	t.Run("pvcTemplate storage bi CNPG bo qua", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t, `{"storage": {"pvcTemplate": {"resources": {"requests": {"storage": "50Gi"}}}}}`, nil)
		require.ErrorContains(t, a.Passthrough(), "set spec.engine.storage.size instead")
	})

	t.Run("plugin backup tro sai ObjectStore", func(t *testing.T) {
		t.Parallel()
		a, provider := newPassthroughApplier(t,
			`{"plugins": [{"name": "barman-cloud.cloudnative-pg.io", "parameters": {"barmanObjectName": "other"}}]}`, nil)
		require.NoError(t, unstructured.SetNestedSlice(provider.Object,
			[]any{archiverPluginConfiguration("orders-seaweedfs")}, "spec", "plugins"))
		require.ErrorContains(t, a.Passthrough(),
			"spec.cnpg.plugins[name=barman-cloud.cloudnative-pg.io].parameters.barmanObjectName")
	})

	t.Run("externalCluster trung ten voi entry Everest sinh", func(t *testing.T) {
		t.Parallel()
		a, _ := newPassthroughApplier(t,
			`{"externalClusters": [{"name": "primary-replica-source", "connectionParameters": {"host": "elsewhere"}}]}`,
			func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Replica = &everestv1alpha1.ReplicaCluster{
					Enabled: true, Source: everestv1alpha1.ReplicaSource{ClusterName: "primary"},
				}
			})
		require.NoError(t, a.ReplicaCluster())
		require.ErrorContains(t, a.Passthrough(),
			"spec.cnpg.externalClusters[name=primary-replica-source].connectionParameters.host")
	})
}

// [CUSTOM CNPG] Không có spec.cnpg thì Passthrough không được đụng tới Cluster Everest sinh ra.
func TestPassthroughNoopWithoutCNPG(t *testing.T) {
	t.Parallel()
	a, provider := newPassthroughApplier(t, "null", nil)
	before := provider.DeepCopy().Object
	require.NoError(t, a.Passthrough())
	assert.Equal(t, before, provider.Object)
}
