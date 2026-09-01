// everest-operator
// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

package cnpg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
	provider := &Provider{
		Unstructured:    &unstructured.Unstructured{Object: map[string]any{}},
		ProviderOptions: providers.ProviderOptions{DB: db, DBEngine: engine},
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

func mustNested(t *testing.T, object map[string]any, fields ...string) any {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}
