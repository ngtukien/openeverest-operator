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
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

func TestSetReconcileFailedCondition(t *testing.T) {
	t.Parallel()
	clusterGR := schema.GroupResource{Group: "postgresql.cnpg.io", Resource: "clusters"}
	failed := func(status everestv1alpha1.DatabaseClusterStatus) *metav1.Condition {
		return meta.FindStatusCondition(status.Conditions, everestv1alpha1.ConditionTypeReconcileFailed)
	}

	t.Run("admission denial is RejectedByAPIServer", func(t *testing.T) {
		t.Parallel()
		// Đúng dạng lỗi CreateOrUpdate trả về khi ValidatingAdmissionPolicy từ chối: bị bọc qua
		// hai lớp fmt.Errorf.
		denied := k8serrors.NewInvalid(schema.GroupKind{Group: "postgresql.cnpg.io", Kind: "Cluster"}, testCNPGCluster, nil)
		err := fmt.Errorf("failed to create or update database cluster: %w", denied)
		status := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&status, err, 7)
		condition := failed(status)
		require.NotNil(t, condition)
		assert.Equal(t, metav1.ConditionTrue, condition.Status)
		assert.Equal(t, everestv1alpha1.ReasonRejectedByAPIServer, condition.Reason)
		assert.Equal(t, int64(7), condition.ObservedGeneration)
		assert.Contains(t, condition.Message, "failed to create or update database cluster")
	})

	t.Run("passthrough conflict is ApplyFailed", func(t *testing.T) {
		t.Parallel()
		status := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&status, errors.New("spec.cnpg.postgresql.parameters.max_connections: value conflicts"), 1)
		require.NotNil(t, failed(status))
		assert.Equal(t, everestv1alpha1.ReasonApplyFailed, failed(status).Reason)
	})

	t.Run("success removes the condition", func(t *testing.T) {
		t.Parallel()
		status := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&status, errors.New("boom"), 1)
		setReconcileFailedCondition(&status, nil, 2)
		assert.Nil(t, failed(status))
	})

	t.Run("conflict keeps the previous state", func(t *testing.T) {
		t.Parallel()
		conflict := fmt.Errorf("wrapped: %w", k8serrors.NewConflict(clusterGR, testCNPGCluster, errors.New("modified")))

		status := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&status, errors.New("real failure"), 1)
		setReconcileFailedCondition(&status, conflict, 2)
		require.NotNil(t, failed(status), "một conflict thoáng qua không được xoá lỗi thật đang tồn tại")
		assert.Equal(t, "real failure", failed(status).Message)

		clean := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&clean, conflict, 1)
		assert.Nil(t, failed(clean), "conflict không phải lỗi cấu hình")
	})

	t.Run("long multi-byte message stays valid UTF-8", func(t *testing.T) {
		t.Parallel()
		status := everestv1alpha1.DatabaseClusterStatus{}
		setReconcileFailedCondition(&status, errors.New("x"+strings.Repeat("ố", maxConditionMessageLength)), 1)
		message := failed(status).Message
		assert.LessOrEqual(t, len(message), maxConditionMessageLength)
		assert.True(t, utf8.ValidString(message))
	})
}

// statusStubProvider mô phỏng hành vi chung của mọi provider thật: Status() dựng status mới từ
// status hiện có của DatabaseCluster, nên condition cũ đi theo trừ khi bị xoá tường minh.
type statusStubProvider struct {
	*metav1.ObjectMeta

	db *everestv1alpha1.DatabaseCluster
}

func (p *statusStubProvider) RunPreReconcileHook(context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

func (p *statusStubProvider) Apply(context.Context) everestv1alpha1.Applier { return nil } //nolint:ireturn

func (p *statusStubProvider) Status(context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.db.Status
	status.Status = everestv1alpha1.AppStateReady
	return status, true, nil
}

func (p *statusStubProvider) Cleanup(context.Context, *everestv1alpha1.DatabaseCluster) (bool, error) {
	return true, nil
}

func (p *statusStubProvider) DBObject() client.Object { return nil } //nolint:ireturn

// [CUSTOM CNPG] Condition phải thật sự tới API server qua status subresource, và biến mất ở lần
// reconcile thành công kế tiếp.
func TestReconcileDBStatusPersistsReconcileFailed(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	db := &everestv1alpha1.DatabaseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testCNPGCluster, Namespace: testCNPGNamespace, Generation: 3},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db).
		WithStatusSubresource(&everestv1alpha1.DatabaseCluster{}).Build()
	r := &DatabaseClusterReconciler{Client: c, Scheme: scheme}
	ctx := context.Background()
	read := func() *metav1.Condition {
		stored := &everestv1alpha1.DatabaseCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(db), stored))
		return meta.FindStatusCondition(stored.Status.Conditions, everestv1alpha1.ConditionTypeReconcileFailed)
	}

	applyErr := errors.New("failed to apply engine passthrough: spec.cnpg.instances is owned by Everest")
	_, err := r.reconcileDBStatus(ctx, db, &statusStubProvider{ObjectMeta: &db.ObjectMeta, db: db}, applyErr)
	require.NoError(t, err)
	condition := read()
	require.NotNil(t, condition)
	assert.Equal(t, everestv1alpha1.ReasonApplyFailed, condition.Reason)
	assert.Contains(t, condition.Message, "spec.cnpg.instances")

	_, err = r.reconcileDBStatus(ctx, db, &statusStubProvider{ObjectMeta: &db.ObjectMeta, db: db}, nil)
	require.NoError(t, err)
	assert.Nil(t, read())
}
