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
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// [CUSTOM CNPG] Connection pooler (PgBouncer) qua CRD "Pooler" của CloudNativePG.
//
// Pooler KHÔNG cần thiết để CNPG có HA: Service "<cluster>-rw" đã tự đổi backend sang primary mới
// sau failover. Nó giải quyết bài toán khác — quản lý SỐ LƯỢNG connection: mỗi connection của
// PostgreSQL là một process, nên hàng trăm client ngắn hạn sẽ đụng trần max_connections trước khi
// đụng trần CPU/RAM.
//
// Vì vậy pooler là tuỳ chọn, bật bằng "spec.proxy.type: pgbouncer".

// PoolerGVK là GroupVersionKind của connection pooler trong CloudNativePG.
var PoolerGVK = schema.GroupVersionKind{
	Group:   consts.CNPGAPIGroup,
	Version: "v1",
	Kind:    consts.CNPGPoolerKind,
}

// poolerName sinh tên Pooler cho một cụm. Hậu tố "-pooler-rw" phản ánh selectorType "rw": pooler
// trỏ vào primary, vì đây là đường ghi.
func poolerName(dbName string) string {
	return dbName + "-pooler-rw"
}

// poolerEnabled cho biết người dùng có yêu cầu pooler hay không.
func poolerEnabled(db *everestv1alpha1.DatabaseCluster) bool {
	return db.Spec.Proxy.Type == everestv1alpha1.ProxyTypePGBouncer
}

// reconcilePooler tạo, cập nhật hoặc xoá Pooler theo spec.proxy.
//
// Pooler thuộc sở hữu của DatabaseCluster nên Kubernetes tự dọn khi cụm bị xoá — khác hẳn với việc
// khai Pooler bằng manifest CNPG thuần, nơi người vận hành phải nhớ xoá tay.
func (a *applier) reconcilePooler(serviceTemplate map[string]any) error {
	object := newUnstructured(PoolerGVK, a.DB.Namespace, poolerName(a.DB.Name))

	if !poolerEnabled(a.DB) {
		// Tắt pooler là xoá hẳn, cùng triết lý với cách Backup() xoá ScheduledBackup khi
		// schedule.Enabled=false.
		if err := a.C.Delete(a.ctx, object); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete Pooler %q: %w", object.GetName(), err)
		}
		// PDB phải xoá TƯỜNG MINH: ownerReference của nó trỏ vào DatabaseCluster chứ không phải
		// Pooler, nên garbage collection chỉ dọn khi xoá cả cụm. Bỏ qua bước này thì tắt pooler
		// để lại một PDB mồ côi trỏ vào selector không còn pod nào.
		return a.reconcilePoolerPDB(0)
	}

	instances := int64(1)
	if a.DB.Spec.Proxy.Replicas != nil {
		instances = int64(*a.DB.Spec.Proxy.Replicas)
	}

	pgbouncer := map[string]any{
		// session ưu tiên TƯƠNG THÍCH, không phải throughput. transaction mode phá prepared
		// statement, SET ở mức session, advisory lock và cursor sống qua nhiều transaction —
		// nền tảng không thể tự ý bật thay người dùng.
		"poolMode": "session",
	}
	// Image pgbouncer đến từ componentImages của ClusterImageCatalog, do
	// DatabaseEngineReconciler đổ vào availableVersions.proxy — cùng một nguồn kiểm soát với
	// operand image, nên pooler cũng được pin digest.
	if component := a.DBEngine.Status.AvailableVersions.
		Proxy[everestv1alpha1.ProxyTypePGBouncer][a.DB.Spec.Engine.Version]; component != nil &&
		component.ImagePath != "" {
		pgbouncer["image"] = component.ImagePath
	}

	spec := map[string]any{
		fieldCluster: map[string]any{fieldName: a.DB.Name},
		"type":       "rw",
		"instances":  instances,
		"pgbouncer":  pgbouncer,
	}
	if serviceTemplate != nil {
		spec["serviceTemplate"] = serviceTemplate
	}

	resourceRequirements := a.DB.Spec.Proxy.Resources.ToResourceRequirements()
	if len(resourceRequirements.Limits) != 0 || len(resourceRequirements.Requests) != 0 {
		resources, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&resourceRequirements)
		if err != nil {
			return fmt.Errorf("convert proxy resources: %w", err)
		}
		// Tên container BẮT BUỘC là "pgbouncer" để CNPG merge được pod template.
		spec["template"] = map[string]any{
			"spec": map[string]any{
				"containers": []any{map[string]any{
					fieldName:   "pgbouncer",
					"resources": resources,
				}},
			},
		}
	}

	if _, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile Pooler %q: %w", object.GetName(), err)
	}
	return a.reconcilePoolerPDB(instances)
}

// reconcilePoolerPDB bảo đảm pooler còn ít nhất một endpoint khi node bị drain.
//
// CNPG KHÔNG tự tạo PodDisruptionBudget cho Pooler — khác với Cluster. Thiếu nó thì drain một node
// có thể hạ cả hai pod pooler cùng lúc và đứt toàn bộ đường kết nối tới database, dù cụm PostgreSQL
// vẫn khoẻ.
func (a *applier) reconcilePoolerPDB(instances int64) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolerName(a.DB.Name),
			Namespace: a.DB.Namespace,
		},
	}
	// Một replica thì PDB maxUnavailable=1 không bảo vệ được gì, mà maxUnavailable=0 lại chặn
	// drain vĩnh viễn. Không tạo PDB còn trung thực hơn.
	if instances < 2 { //nolint:mnd
		if err := a.C.Delete(a.ctx, pdb); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete Pooler PodDisruptionBudget: %w", err)
		}
		return nil
	}
	maxUnavailable := intstr.FromInt32(1)
	if _, err := controllerutil.CreateOrUpdate(a.ctx, a.C, pdb, func() error {
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{consts.CNPGPoolerNameLabel: poolerName(a.DB.Name)},
			},
		}
		return controllerutil.SetControllerReference(a.DB, pdb, a.C.Scheme())
	}); err != nil {
		return fmt.Errorf("reconcile Pooler PodDisruptionBudget: %w", err)
	}
	return nil
}
