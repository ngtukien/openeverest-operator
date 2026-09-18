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

package everest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	cnpgprovider "github.com/percona/everest-operator/internal/controller/everest/providers/cnpg"
)

const (
	testCNPGNamespace = "databases"
	testCNPGBackup    = "orders-manual"
	testCNPGCluster   = "orders"
)

func TestReconcileCNPGBackupCreatesClusterReference(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	registerUnstructuredGVK(scheme, cnpgprovider.BackupGVK)
	clusterGVK := schema.GroupVersionKind{
		Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGClusterKind,
	}
	registerUnstructuredGVK(scheme, clusterGVK)

	// [CUSTOM CNPG] Cluster được cấu hình qua Barman Cloud Plugin, không phải in-tree.
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"plugins": []any{map[string]any{
				"name":          consts.BarmanCloudPluginName,
				"isWALArchiver": true,
				"parameters":    map[string]any{"barmanObjectName": "orders-s3"},
			}},
		},
	}}
	cluster.SetGroupVersionKind(clusterGVK)
	cluster.SetName(testCNPGCluster)
	cluster.SetNamespace(testCNPGNamespace)

	client, requeue, err := reconcileCNPGBackupFor(scheme, cluster)
	require.NoError(t, err)
	assert.True(t, requeue)

	created := &unstructured.Unstructured{Object: map[string]any{}}
	created.SetGroupVersionKind(cnpgprovider.BackupGVK)
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{
		Name: testCNPGBackup, Namespace: testCNPGNamespace,
	}, created))
	clusterName, found, err := unstructured.NestedString(created.Object, "spec", "cluster", "name")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, testCNPGCluster, clusterName)

	// [CUSTOM CNPG] Backup phải dùng method "plugin"; "barmanObjectStore" là đường in-tree đã
	// deprecated và không chạy được với operand image flavor standard.
	method, found, err := unstructured.NestedString(created.Object, "spec", "method")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "plugin", method)
	pluginName, found, err := unstructured.NestedString(created.Object, "spec", "pluginConfiguration", "name")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, consts.BarmanCloudPluginName, pluginName)
}

// [CUSTOM CNPG] Chưa có plugin trên Cluster thì phải requeue chờ, không tạo Backup — tạo sớm sẽ
// làm Backup hỏng vĩnh viễn thay vì retry sạch.
func TestReconcileCNPGBackupWaitsForPlugin(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	registerUnstructuredGVK(scheme, cnpgprovider.BackupGVK)
	clusterGVK := schema.GroupVersionKind{
		Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGClusterKind,
	}
	registerUnstructuredGVK(scheme, clusterGVK)

	cluster := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	cluster.SetGroupVersionKind(clusterGVK)
	cluster.SetName(testCNPGCluster)
	cluster.SetNamespace(testCNPGNamespace)

	client, requeue, err := reconcileCNPGBackupFor(scheme, cluster)
	require.NoError(t, err)
	assert.True(t, requeue)

	created := &unstructured.Unstructured{Object: map[string]any{}}
	created.SetGroupVersionKind(cnpgprovider.BackupGVK)
	err = client.Get(context.Background(), types.NamespacedName{
		Name: testCNPGBackup, Namespace: testCNPGNamespace,
	}, created)
	require.Error(t, err, "Backup không được tạo khi Cluster chưa có plugin")
}

// reconcileCNPGBackupFor chạy reconcileCNPG cho một DatabaseClusterBackup trỏ vào cluster, trên
// fake client chỉ chứa cluster đó. Trả về client để test đọc lại các object được tạo.
func reconcileCNPGBackupFor(scheme *runtime.Scheme, cluster *unstructured.Unstructured) (ctrlclient.Client, bool, error) { //nolint:ireturn
	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: testCNPGBackup, Namespace: testCNPGNamespace, UID: types.UID("backup-uid"),
		},
		Spec: everestv1alpha1.DatabaseClusterBackupSpec{
			DBClusterName: testCNPGCluster, BackupStorageName: "s3",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &DatabaseClusterBackupReconciler{Client: client, Scheme: scheme}
	requeue, err := reconciler.reconcileCNPG(context.Background(), backup)
	return client, requeue, err
}

func registerUnstructuredGVK(scheme *runtime.Scheme, gvk schema.GroupVersionKind) {
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	listGVK := gvk
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
}
