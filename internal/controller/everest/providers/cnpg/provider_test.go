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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

func TestApplierEngine(t *testing.T) {
	t.Parallel()
	storageClass := "longhorn"
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type:     everestv1alpha1.DatabaseEnginePostgresql,
				Version:  "16.4",
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
					"16.4": {ImagePath: "registry.example/postgresql:16.4"},
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
	assert.Equal(t, "registry.example/postgresql:16.4", mustNested(t, provider.Object, "spec", "imageName"))
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
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Engine: everestv1alpha1.Engine{
				Type:     everestv1alpha1.DatabaseEnginePostgresql,
				Version:  "16.4",
				Replicas: 1,
				Storage:  everestv1alpha1.Storage{Size: resource.MustParse("1Gi")},
			},
		},
	}
	engine := &everestv1alpha1.DatabaseEngine{
		Status: everestv1alpha1.DatabaseEngineStatus{
			AvailableVersions: everestv1alpha1.Versions{
				Engine: everestv1alpha1.ComponentsMap{"16.4": {ImagePath: "registry.example/postgresql:16.4"}},
			},
		},
	}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "podmonitors.monitoring.coreos.com"},
	}}
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

func TestStatusReady(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"}}
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

func TestProxyRejectsPoolerConfiguration(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Proxy: everestv1alpha1.Proxy{Type: everestv1alpha1.ProxyTypePGBouncer},
		},
	}
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db},
	}
	err := (&applier{Provider: provider, ctx: context.Background()}).Proxy()
	require.ErrorContains(t, err, "does not use an Everest-managed proxy")
}

func TestBarmanObjectStoreS3(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", UID: types.UID("uid-1")}}
	storage := &everestv1alpha1.BackupStorage{Spec: everestv1alpha1.BackupStorageSpec{
		Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", EndpointURL: "https://s3.example",
		CredentialsSecretName: "backup-creds",
	}}
	config, err := BarmanObjectStore(storage, db)
	require.NoError(t, err)
	assert.Equal(t, "s3://backups/orders/uid-1", config["destinationPath"])
	assert.Equal(t, "https://s3.example", config["endpointURL"])
	credentials := config["s3Credentials"].(map[string]any)
	assert.Equal(t, map[string]any{"name": "backup-creds", "key": "AWS_ACCESS_KEY_ID"}, credentials["accessKeyId"])
}

func TestBackupCreatesScheduledBackup(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(ScheduledBackupGVK, &unstructured.Unstructured{})
	listGVK := ScheduledBackupGVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases", UID: types.UID("uid-1")},
		Spec: everestv1alpha1.DatabaseClusterSpec{Backup: everestv1alpha1.Backup{Schedules: []everestv1alpha1.BackupSchedule{{
			Name: "daily", Enabled: true, Schedule: "0 2 * * *", BackupStorageName: "s3",
		}}}},
	}
	storage := &everestv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "databases"},
		Spec:       everestv1alpha1.BackupStorageSpec{Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "backups", CredentialsSecretName: "creds"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, storage).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	require.NoError(t, (&applier{Provider: provider, ctx: context.Background()}).Backup())
	created := &unstructured.Unstructured{Object: map[string]any{}}
	created.SetGroupVersionKind(ScheduledBackupGVK)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "databases", Name: "orders-daily"}, created))
	assert.Equal(t, "0 0 2 * * *", mustNested(t, created.Object, "spec", "schedule"))
	assert.Equal(t, "none", mustNested(t, created.Object, "spec", "backupOwnerReference"))
	assert.Equal(t, "orders", mustNested(t, created.Object, "spec", "cluster", "name"))
	assert.Equal(t, "s3://backups/orders/uid-1", mustNested(t, provider.Object, "spec", "backup", "barmanObjectStore", "destinationPath"))
}

func TestStatusResizingVolumes(t *testing.T) {
	t.Parallel()
	// A PVC in the Resizing condition represents the online expansion window;
	// the provider should expose that transient state through Everest status.
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	db := &everestv1alpha1.DatabaseCluster{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"}}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-1", Namespace: "databases", Labels: map[string]string{"cnpg.io/cluster": "orders"}},
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
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases"},
		Status:     everestv1alpha1.DatabaseClusterStatus{Status: everestv1alpha1.AppStateResizingVolumes},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders-1", Namespace: "databases", Labels: map[string]string{"cnpg.io/cluster": "orders"},
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
		Name: "orders", Namespace: "databases", Generation: 2,
	}}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders-1", Namespace: "databases", Labels: map[string]string{"cnpg.io/cluster": "orders"},
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
	affinity := mustNested(t, provider.Object, "spec", "affinity").(map[string]any)
	assert.Contains(t, affinity, "nodeAffinity")
	assert.Contains(t, affinity, "additionalPodAntiAffinity")
	assert.NotContains(t, affinity, "podAntiAffinity")
}

func TestApplierEnginePreventsStorageShrink(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{Spec: everestv1alpha1.DatabaseClusterSpec{Engine: everestv1alpha1.Engine{
		Version: "16.4", Replicas: 3, Storage: everestv1alpha1.Storage{Size: resource.MustParse("10Gi")},
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
	require.NoError(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.3", "16.4"))
	require.ErrorContains(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.4", "16.3"), "downgrade")
	require.ErrorContains(t, validateVersionChange("ghcr.io/cloudnative-pg/postgresql:16.4", "17.1"), "minor versions")
}

func mustNested(t *testing.T, object map[string]any, fields ...string) any {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}
