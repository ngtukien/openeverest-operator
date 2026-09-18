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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	apiSchema "k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

func newCNPGPassthroughDB(cnpgSpec string) *everestv1alpha1.DatabaseCluster {
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: dbcTestDbName, Namespace: dbcTestDbNamespace},
		Spec: everestv1alpha1.DatabaseClusterSpec{Engine: everestv1alpha1.Engine{
			Type: everestv1alpha1.DatabaseEnginePostgresql, Provider: everestv1alpha1.DatabaseEngineProviderCloudNativePG,
			Version: dbcTestCNPGDbVersion, UserSecretsName: dbcTestUserSecretName,
		}},
	}
	if cnpgSpec != "" {
		db.Spec.CNPG = &runtime.RawExtension{Raw: []byte(cnpgSpec)}
	}
	return db
}

// [CUSTOM CNPG] Xung đột phát hiện được mà không cần đọc cụm phải bị chặn ngay lúc apply, vì lỗi
// reconcile chỉ nằm trong log operator.
func TestValidateCNPGPassthrough(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cnpg      string
		isCNPG    bool
		mutate    func(*everestv1alpha1.DatabaseCluster)
		wantError string
	}{
		{
			name:   "CNPG-native fields are accepted",
			isCNPG: true,
			cnpg: `{"postgresql": {"synchronous": {"method": "any", "number": 1}},
				"primaryUpdateStrategy": "supervised",
				"bootstrap": {"initdb": {"database": "trove", "dataChecksums": true}},
				"storage": {"resizeInUseVolumes": true}}`,
		},
		{name: "only for CloudNativePG", cnpg: `{}`, wantError: "only supported by the CloudNativePG provider"},
		{name: "not an object", isCNPG: true, cnpg: `[1]`, wantError: "must be a JSON object"},
		{name: "owned instances", isCNPG: true, cnpg: `{"instances": 5}`, wantError: "spec.cnpg.instances: Forbidden: is owned by Everest; set spec.engine.replicas"},
		{name: "owned storage size", isCNPG: true, cnpg: `{"storage": {"size": "5Gi"}}`, wantError: "spec.cnpg.storage.size: Forbidden"},
		{name: "backup target is allowed", isCNPG: true, cnpg: `{"backup": {"target": "prefer-standby"}}`},
		{name: "in-tree barmanObjectStore", isCNPG: true, cnpg: `{"backup": {"barmanObjectStore": {}}}`, wantError: "spec.cnpg.backup.barmanObjectStore: Forbidden"},
		{
			name: "barman plugin serverName", isCNPG: true,
			cnpg:      `{"plugins": [{"name": "barman-cloud.cloudnative-pg.io", "parameters": {"serverName": "other-uid"}}]}`,
			wantError: "spec.cnpg.plugins[name=barman-cloud.cloudnative-pg.io].parameters.serverName: Forbidden",
		},
		{name: "other plugin parameters are allowed", isCNPG: true, cnpg: `{"plugins": [{"name": "cnpg-i-hello-world.cloudnative-pg.io", "parameters": {"serverName": "x"}}]}`},
		{
			name: "bootstrap with dataSource", isCNPG: true, cnpg: `{"bootstrap": {"initdb": {}}}`,
			mutate: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.DataSource = &everestv1alpha1.DataSource{DBClusterBackupName: "b"}
			},
			wantError: "cannot be combined with spec.dataSource",
		},
		{
			name: "recovery bootstrap with userSecretsName", isCNPG: true, cnpg: `{"bootstrap": {"recovery": {"source": "x"}}}`,
			wantError: "spec.cnpg.bootstrap.recovery: Forbidden",
		},
		{
			name: "initdb owner with userSecretsName", isCNPG: true, cnpg: `{"bootstrap": {"initdb": {"owner": "someone"}}}`,
			wantError: "spec.cnpg.bootstrap.initdb.owner: Forbidden: is generated from spec.engine.userSecretsName",
		},
		{
			name: "replica declared twice", isCNPG: true, cnpg: `{"replica": {"enabled": true, "source": "x"}}`,
			mutate: func(db *everestv1alpha1.DatabaseCluster) {
				db.Spec.Replica = &everestv1alpha1.ReplicaCluster{Enabled: true}
			},
			wantError: "cannot be combined with spec.replica",
		},
		{
			name: "parameter also in engine.config", isCNPG: true, cnpg: `{"postgresql": {"parameters": {"max_connections": "300"}}}`,
			mutate:    func(db *everestv1alpha1.DatabaseCluster) { db.Spec.Engine.Config = "max_connections = 200" },
			wantError: "spec.cnpg.postgresql.parameters.max_connections: Forbidden",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newCNPGPassthroughDB(tc.cnpg)
			if tc.mutate != nil {
				tc.mutate(db)
			}
			errs := validateCNPGPassthrough(db, tc.isCNPG)
			if tc.wantError == "" {
				assert.Empty(t, errs)
				return
			}
			assert.Contains(t, errs.ToAggregate().Error(), tc.wantError)
		})
	}
}

func TestDatabaseClusterValidator_CNPGPassthroughUpdateGuards(t *testing.T) {
	t.Parallel()
	clusterGVK := apiSchema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGClusterKind}
	newValidator := func(objects ...ctrlclient.Object) *DatabaseClusterValidator {
		scheme := runtime.NewScheme()
		utilruntime.Must(clientgoscheme.AddToScheme(scheme))
		utilruntime.Must(everestv1alpha1.AddToScheme(scheme))
		scheme.AddKnownTypeWithName(clusterGVK, &unstructured.Unstructured{})
		listGVK := clusterGVK
		listGVK.Kind += "List"
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
		return &DatabaseClusterValidator{
			Client: fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		}
	}
	existingCluster := &unstructured.Unstructured{}
	existingCluster.SetGroupVersionKind(clusterGVK)
	existingCluster.SetName(dbcTestDbName)
	existingCluster.SetNamespace(dbcTestDbNamespace)

	t.Run("removing cnpg.replica from a replica cluster is rejected", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB(`{"replica": {"enabled": true, "source": "zone-a"}}`)
		_, err := newValidator().ValidateUpdate(t.Context(), oldDB, newCNPGPassthroughDB(`{}`))
		require.ErrorContains(t, err, "set enabled: false explicitly to promote")
	})
	t.Run("removing typed spec.replica from a replica cluster is rejected", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB("")
		oldDB.Spec.Replica = &everestv1alpha1.ReplicaCluster{Enabled: true}
		_, err := newValidator().ValidateUpdate(t.Context(), oldDB, newCNPGPassthroughDB(""))
		require.ErrorContains(t, err, "promotes this replica cluster")
	})
	t.Run("explicit enabled false promotes", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB(`{"replica": {"enabled": true, "source": "zone-a"}}`)
		newDB := newCNPGPassthroughDB(`{"replica": {"enabled": false, "source": "zone-a"}}`)
		_, err := newValidator().ValidateUpdate(t.Context(), oldDB, newDB)
		require.NoError(t, err)
	})
	t.Run("bootstrap change on an existing Cluster is rejected", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB(`{"bootstrap": {"initdb": {"database": "trove"}}}`)
		newDB := newCNPGPassthroughDB(`{"bootstrap": {"initdb": {"database": "orders"}}}`)
		_, err := newValidator(existingCluster.DeepCopy()).ValidateUpdate(t.Context(), oldDB, newDB)
		require.ErrorContains(t, err, "reads bootstrap only when creating the cluster")
	})
	t.Run("bootstrap can still be fixed before the Cluster exists", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB(`{"bootstrap": {"initdb": {"database": "trove"}}}`)
		newDB := newCNPGPassthroughDB(`{"bootstrap": {"initdb": {"database": "orders"}}}`)
		_, err := newValidator().ValidateUpdate(t.Context(), oldDB, newDB)
		require.NoError(t, err)
	})
	t.Run("runtime fields stay mutable on an existing Cluster", func(t *testing.T) {
		t.Parallel()
		oldDB := newCNPGPassthroughDB(`{"postgresql": {"synchronous": {"method": "any", "number": 1}}}`)
		newDB := newCNPGPassthroughDB(`{"postgresql": {"synchronous": {"method": "any", "number": 2}}}`)
		_, err := newValidator(existingCluster.DeepCopy()).ValidateUpdate(t.Context(), oldDB, newDB)
		require.NoError(t, err)
	})
}

// [CUSTOM CNPG] Tên subscription là tên replication slot trên publisher: phải chặn ngay lúc apply.
func TestValidateReplicationNames(t *testing.T) {
	t.Parallel()
	db := &everestv1alpha1.DatabaseCluster{Spec: everestv1alpha1.DatabaseClusterSpec{
		Replication: &everestv1alpha1.Replication{Subscriptions: []everestv1alpha1.ReplicationSubscription{
			{Name: "trove_sub"}, {Name: "trove-sub"},
		}},
	}}
	errs := validateReplicationNames(db)
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.replication.subscriptions[1].name", errs[0].Field)
	assert.Contains(t, errs[0].Error(), "replication slot name")
}
