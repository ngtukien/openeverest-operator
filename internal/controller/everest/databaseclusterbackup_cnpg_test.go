// everest-operator
// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	cnpgprovider "github.com/percona/everest-operator/internal/controller/everest/providers/cnpg"
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

	cluster := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"backup": map[string]any{
				"barmanObjectStore": map[string]any{"destinationPath": "s3://backups/orders"},
			},
		},
	}}
	cluster.SetGroupVersionKind(clusterGVK)
	cluster.SetName("orders")
	cluster.SetNamespace("databases")

	backup := &everestv1alpha1.DatabaseClusterBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders-manual", Namespace: "databases", UID: types.UID("backup-uid"),
		},
		Spec: everestv1alpha1.DatabaseClusterBackupSpec{
			DBClusterName: "orders", BackupStorageName: "s3",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &DatabaseClusterBackupReconciler{Client: client, Scheme: scheme}

	requeue, err := reconciler.reconcileCNPG(context.Background(), backup)
	require.NoError(t, err)
	assert.False(t, requeue)

	created := &unstructured.Unstructured{Object: map[string]any{}}
	created.SetGroupVersionKind(cnpgprovider.BackupGVK)
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{
		Name: "orders-manual", Namespace: "databases",
	}, created))
	clusterName, found, err := unstructured.NestedString(created.Object, "spec", "cluster", "name")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "orders", clusterName)
}

func registerUnstructuredGVK(scheme *runtime.Scheme, gvk schema.GroupVersionKind) {
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	listGVK := gvk
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
}
