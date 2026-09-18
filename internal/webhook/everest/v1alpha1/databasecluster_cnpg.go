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
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/providers/cnpg"
)

var (
	// [CUSTOM CNPG] .spec.cnpg and .spec.replica — CloudNativePG-only, xem PLAN.md Phase 11-12.
	dbcCNPGPath          = specPath.Child("cnpg")
	dbcCNPGBootstrapPath = dbcCNPGPath.Child("bootstrap")
	dbcReplicaPath       = specPath.Child("replica")
)

// [CUSTOM CNPG] validateCNPGPassthrough chặn tại admission những xung đột mà Passthrough() của
// provider sẽ gặp lúc reconcile. Lỗi reconcile chỉ hiện SAU khi apply (condition ReconcileFailed trong
// status), còn lỗi ở đây hiện
// ngay trong output của kubectl apply — nên mọi thứ kiểm tra được mà không cần đọc cụm đều nên
// nằm ở đây. Phần còn lại (giá trị sinh ra từ BackupStorage, PodSchedulingPolicy...) do bước
// gộp lúc reconcile bắt.
func validateCNPGPassthrough(db *everestv1alpha1.DatabaseCluster, isCNPG bool) field.ErrorList {
	if db.Spec.CNPG == nil {
		return nil
	}
	if !isCNPG {
		return field.ErrorList{field.Forbidden(dbcCNPGPath, "spec.cnpg is only supported by the CloudNativePG provider")}
	}
	user, err := db.Spec.CNPGSpec()
	if err != nil {
		return field.ErrorList{errInvalidField(dbcCNPGPath, "", err.Error())}
	}
	if user == nil {
		return nil
	}

	var allErrs field.ErrorList
	for _, owned := range cnpg.OwnedSpecPaths {
		if _, found, _ := unstructured.NestedFieldNoCopy(user, owned.Path...); found {
			allErrs = append(allErrs, field.Forbidden(
				dbcCNPGPath.Child(owned.Path[0], owned.Path[1:]...),
				fmt.Sprintf("is owned by Everest; set %s instead", owned.EverestPath),
			))
		}
	}
	allErrs = append(allErrs, validateCNPGBootstrapSource(db, user)...)
	if _, found := user["replica"]; found && db.Spec.Replica != nil {
		allErrs = append(allErrs, field.Forbidden(dbcCNPGPath.Child("replica"),
			"cannot be combined with spec.replica; declare the replica cluster in one place"))
	}
	allErrs = append(allErrs, validateCNPGParameters(db, user)...)
	return allErrs
}

// validateCNPGBootstrapSource: CNPG chỉ nhận MỘT phương thức bootstrap. dataSource và replica của
// Everest đã sinh bootstrap riêng; userSecretsName sinh initdb cùng owner/secret.
func validateCNPGBootstrapSource(db *everestv1alpha1.DatabaseCluster, user map[string]any) field.ErrorList {
	bootstrap, found := user["bootstrap"].(map[string]any)
	if !found {
		return nil
	}
	var allErrs field.ErrorList
	if db.Spec.DataSource != nil {
		allErrs = append(allErrs, field.Forbidden(dbcCNPGBootstrapPath,
			"cannot be combined with spec.dataSource, which bootstraps the cluster from a backup"))
	}
	if db.Spec.Replica != nil {
		allErrs = append(allErrs, field.Forbidden(dbcCNPGBootstrapPath,
			"cannot be combined with spec.replica, which bootstraps the cluster with pg_basebackup"))
	}
	if db.Spec.Engine.UserSecretsName == "" {
		return allErrs
	}
	for _, method := range sortedKeys(bootstrap) {
		if method != "initdb" {
			allErrs = append(allErrs, field.Forbidden(dbcCNPGBootstrapPath.Child(method),
				"spec.engine.userSecretsName bootstraps the cluster with initdb; remove userSecretsName to use another method"))
		}
	}
	for _, key := range []string{"owner", "secret"} {
		if _, found, _ := unstructured.NestedFieldNoCopy(bootstrap, "initdb", key); found {
			allErrs = append(allErrs, field.Forbidden(dbcCNPGBootstrapPath.Child("initdb", key),
				"is generated from spec.engine.userSecretsName"))
		}
	}
	return allErrs
}

func validateCNPGParameters(db *everestv1alpha1.DatabaseCluster, user map[string]any) field.ErrorList {
	parameters, found, _ := unstructured.NestedMap(user, "postgresql", "parameters")
	if !found {
		return nil
	}
	fromConfig := cnpg.ParsePostgreSQLParameters(db.Spec.Engine.Config)
	var allErrs field.ErrorList
	for _, key := range sortedKeys(parameters) {
		if _, duplicated := fromConfig[key]; duplicated {
			allErrs = append(allErrs, field.Forbidden(
				dbcCNPGPath.Child("postgresql", "parameters", key),
				"is already set by spec.engine.config; set each parameter in one place",
			))
		}
	}
	return allErrs
}

// [CUSTOM CNPG] validateCNPGPassthroughUpdate chặn hai kiểu thay đổi mà CNPG nhận nhưng hậu quả
// không phải điều người dùng nghĩ:
//
//  1. Bỏ khối replica trên một replica cluster. CNPG coi thiếu khối đó là "không phải replica" và
//     PROMOTE cụm — với cụm DR là có hai primary. Muốn promote phải nói rõ bằng enabled: false.
//  2. Sửa spec.cnpg.bootstrap sau khi Cluster đã tồn tại. CNPG chỉ đọc bootstrap lúc tạo và webhook
//     của nó không chặn, nên thay đổi được nhận rồi bị bỏ qua trong im lặng.
func (v *DatabaseClusterValidator) validateCNPGPassthroughUpdate(
	ctx context.Context,
	oldDb, newDb *everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	oldUser, oldErr := oldDb.Spec.CNPGSpec()
	newUser, newErr := newDb.Spec.CNPGSpec()
	if oldErr != nil || newErr != nil {
		return nil // Decode errors of the new object are reported by validateCNPGPassthrough.
	}

	var allErrs field.ErrorList
	if replicaEnabled(oldDb, oldUser) && !replicaDeclared(newDb, newUser) {
		allErrs = append(allErrs, field.Forbidden(dbcReplicaPath,
			"removing the replica configuration promotes this replica cluster to a writable primary; "+
				"set enabled: false explicitly to promote"))
	}

	if !equality.Semantic.DeepEqual(oldUser["bootstrap"], newUser["bootstrap"]) {
		exists, err := v.cnpgClusterExists(ctx, newDb)
		switch {
		case err != nil:
			allErrs = append(allErrs, errInvalidField(dbcCNPGBootstrapPath, "",
				fmt.Sprintf("could not check whether the CloudNativePG Cluster exists: %v", err)))
		case exists:
			allErrs = append(allErrs, field.Forbidden(dbcCNPGBootstrapPath,
				"CloudNativePG reads bootstrap only when creating the cluster; changing it now would be silently ignored"))
		}
	}
	return allErrs
}

// replicaEnabled mirrors CNPG's Cluster.IsReplica(): an explicit enabled wins, otherwise a
// distributed topology is a replica when primary names another cluster than self.
func replicaEnabled(db *everestv1alpha1.DatabaseCluster, user map[string]any) bool {
	if db.Spec.Replica != nil && db.Spec.Replica.Enabled {
		return true
	}
	replica, found := user["replica"].(map[string]any)
	if !found {
		return false
	}
	if enabled, set := replica["enabled"].(bool); set {
		return enabled
	}
	primary, _ := replica["primary"].(string)
	self, _ := replica["self"].(string)
	return primary != "" && self != "" && primary != self
}

func replicaDeclared(db *everestv1alpha1.DatabaseCluster, user map[string]any) bool {
	_, found := user["replica"]
	return db.Spec.Replica != nil || found
}

func (v *DatabaseClusterValidator) cnpgClusterExists(ctx context.Context, db *everestv1alpha1.DatabaseCluster) (bool, error) {
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGClusterKind,
	})
	err := v.Client.Get(ctx, types.NamespacedName{Namespace: db.GetNamespace(), Name: db.GetName()}, cluster)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err), meta.IsNoMatchError(err):
		return false, nil
	default:
		return false, err
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
