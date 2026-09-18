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
	"strings"
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
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

func TestApplierReplicationCreatesPublicationAndSubscription(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(PublicationGVK, &unstructured.Unstructured{})
	pubListGVK := PublicationGVK
	pubListGVK.Kind += "List"
	scheme.AddKnownTypeWithName(pubListGVK, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(SubscriptionGVK, &unstructured.Unstructured{})
	subListGVK := SubscriptionGVK
	subListGVK.Kind += "List"
	scheme.AddKnownTypeWithName(subListGVK, &unstructured.UnstructuredList{})

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases", UID: types.UID("uid-1")},
		Spec: everestv1alpha1.DatabaseClusterSpec{
			Replication: &everestv1alpha1.Replication{
				Publications: []everestv1alpha1.ReplicationPublication{
					{Name: "orders_pub", DBName: "orders", Target: everestv1alpha1.ReplicationTarget{AllTables: true}},
				},
				Subscriptions: []everestv1alpha1.ReplicationSubscription{
					{
						Name: "orders_sub", DBName: "orders", PublicationName: "orders_pub",
						Source: everestv1alpha1.ReplicationSourceConnection{
							Host: "legacy.example.svc", DBName: "orders", User: "replicator",
							PasswordSecretName: "legacy-creds",
						},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.Replication())

	pub := &unstructured.Unstructured{Object: map[string]any{}}
	pub.SetGroupVersionKind(PublicationGVK)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "databases", Name: "orders-orders-pub"}, pub))
	assert.Equal(t, "orders", mustNested(t, pub.Object, "spec", "cluster", "name"))
	assert.Equal(t, "orders_pub", mustNested(t, pub.Object, "spec", "name"))
	assert.Equal(t, true, mustNested(t, pub.Object, "spec", "target", "allTables"))

	sub := &unstructured.Unstructured{Object: map[string]any{}}
	sub.SetGroupVersionKind(SubscriptionGVK)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "databases", Name: "orders-orders-sub"}, sub))
	// Tên PostgreSQL giữ gạch dưới (bắt buộc cho tên slot); chỉ tên CR đổi sang gạch ngang.
	assert.Equal(t, "orders_sub", mustNested(t, sub.Object, "spec", "name"))
	assert.Equal(t, "orders_pub", mustNested(t, sub.Object, "spec", "publicationName"))
	assert.Equal(t, "orders-sub-source", mustNested(t, sub.Object, "spec", "externalClusterName"))

	externalClusters, found, err := unstructured.NestedSlice(provider.Object, "spec", "externalClusters")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, externalClusters, 1)
	entry, ok := externalClusters[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "orders-sub-source", entry["name"])
	connParams, ok := entry["connectionParameters"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "legacy.example.svc", connParams["host"])
	assert.Equal(t, "5432", connParams["port"])
	assert.Equal(t, "prefer", connParams["sslmode"])
}

func TestApplierReplicationPrunesRemovedEntries(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(PublicationGVK, &unstructured.Unstructured{})
	pubListGVK := PublicationGVK
	pubListGVK.Kind += "List"
	scheme.AddKnownTypeWithName(pubListGVK, &unstructured.UnstructuredList{})

	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases", UID: types.UID("uid-1")},
		Spec:       everestv1alpha1.DatabaseClusterSpec{}, // no replication declared anymore
	}
	stalePub := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"cluster": map[string]any{"name": "orders"}, "dbname": "orders", "name": "old_pub"},
	}}
	stalePub.SetGroupVersionKind(PublicationGVK)
	stalePub.SetNamespace("databases")
	stalePub.SetName("orders-old_pub")
	stalePub.SetLabels(map[string]string{ReplicationOwnerLabel: "orders"})

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, stalePub).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}
	require.NoError(t, a.Replication())

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(PublicationGVK)
	require.NoError(t, c.List(context.Background(), list))
	assert.Empty(t, list.Items, "Publication removed from spec.replication must be pruned")
}

// [CUSTOM CNPG] CREATE SUBSCRIPTION tạo slot trên publisher trùng tên subscription, và tên slot chỉ
// nhận [a-z0-9_]. Tên "trove-sub" từng lọt qua apply rồi chết ở CNPG với "replication slot name
// contains invalid character".
func TestValidateSubscriptionName(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"trove_sub", "a", "sub_2026"} {
		require.NoError(t, ValidateSubscriptionName(ok), ok)
	}
	for _, bad := range []string{"trove-sub", "Trove_sub", "", "sub.1", strings.Repeat("a", 64)} {
		require.ErrorContains(t, ValidateSubscriptionName(bad), "replication slot", bad)
	}
}

const testTroveDB = "trove"

// [CUSTOM CNPG] CNPG khoá spec.name/spec.dbname của Subscription. Sửa "trove-sub" thành "trove_sub"
// cho ra CÙNG tên CR, nên Everest phải xoá CR cũ rồi tạo lại thay vì update (bị từ chối mãi).
func TestReplicationRecreatesSubscriptionOnNameChange(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	for _, gvk := range []schema.GroupVersionKind{PublicationGVK, SubscriptionGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		list := gvk
		list.Kind += "List"
		scheme.AddKnownTypeWithName(list, &unstructured.UnstructuredList{})
	}
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "databases", UID: types.UID("uid-1")},
		Spec: everestv1alpha1.DatabaseClusterSpec{Replication: &everestv1alpha1.Replication{
			Subscriptions: []everestv1alpha1.ReplicationSubscription{{
				Name: "trove_sub", DBName: testTroveDB, PublicationName: "trove_pub",
				Source: everestv1alpha1.ReplicationSourceConnection{
					Host: "192.168.250.1", DBName: testTroveDB, User: "db_user", PasswordSecretName: "src",
				},
			}},
		}},
	}
	stale := newUnstructured(SubscriptionGVK, "databases", "orders-trove-sub")
	stale.Object["spec"] = map[string]any{"name": "trove-sub", "dbname": testTroveDB}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db, stale).Build()
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}},
		ProviderOptions: providers.ProviderOptions{DB: db, C: c},
	}
	a := &applier{Provider: provider, ctx: context.Background()}

	require.ErrorIs(t, a.Replication(), errReplicationObjectRecreating)
	require.NoError(t, a.Replication())
	sub := newUnstructured(SubscriptionGVK, "", "")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "databases", Name: "orders-trove-sub"}, sub))
	assert.Equal(t, "trove_sub", mustNested(t, sub.Object, "spec", "name"))
}
