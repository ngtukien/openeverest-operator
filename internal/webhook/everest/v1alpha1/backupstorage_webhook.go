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
	"regexp"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers/cnpg"
)

var (
	bsObjectStorePath = specPath.Child("objectStore")

	backupStorageGroupKind = everestv1alpha1.GroupVersion.WithKind("BackupStorage").GroupKind()

	// Same pattern as the Barman Cloud Plugin ObjectStore CRD, so a typo fails at apply time
	// instead of in the operator log when Everest writes the ObjectStore.
	retentionPolicyPattern = regexp.MustCompile(`^[1-9][0-9]*[dwm]$`)
)

// SetupBackupStorageWebhookWithManager sets up the webhook with the manager.
func SetupBackupStorageWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &everestv1alpha1.BackupStorage{}).
		WithValidator(&BackupStorageValidator{}).
		Complete()
}

//nolint:lll
// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-backupstorage,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=backupstorages,verbs=create;update,versions=v1alpha1,name=vbackupstorage-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// BackupStorageValidator validates the BackupStorage resource.
//
// [CUSTOM CNPG] Hiện chỉ kiểm tra spec.objectStore: chính sách backup của CloudNativePG (xem
// PLAN.md Phase 12). Lỗi ở đây hiện ngay trong kubectl apply; không có webhook thì cùng lỗi đó chỉ
// lộ ra ở condition ReconcileFailed của từng DatabaseCluster dùng storage này.
type BackupStorageValidator struct{}

// ValidateCreate validates the creation of a BackupStorage.
func (v *BackupStorageValidator) ValidateCreate(_ context.Context, bs *everestv1alpha1.BackupStorage) (admission.Warnings, error) {
	return nil, toInvalid(bs, validateObjectStorePolicy(bs))
}

// ValidateUpdate validates the update of a BackupStorage.
func (v *BackupStorageValidator) ValidateUpdate(_ context.Context, _, bs *everestv1alpha1.BackupStorage) (admission.Warnings, error) {
	return nil, toInvalid(bs, validateObjectStorePolicy(bs))
}

// ValidateDelete validates the deletion of a BackupStorage. In-use protection is a finalizer.
func (v *BackupStorageValidator) ValidateDelete(_ context.Context, _ *everestv1alpha1.BackupStorage) (admission.Warnings, error) {
	return nil, nil
}

func toInvalid(bs *everestv1alpha1.BackupStorage, errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(backupStorageGroupKind, bs.GetName(), errs)
}

// [CUSTOM CNPG] validateObjectStorePolicy kiểm tra spec.objectStore — phần "backup THẾ NÀO" của
// ObjectStore.spec. Phần "ở đâu" (cnpg.OwnedObjectStorePaths) đã có field riêng của BackupStorage
// nên bị từ chối ở đây, cùng với tham số dòng lệnh thô cho barman-cloud.
func validateObjectStorePolicy(bs *everestv1alpha1.BackupStorage) field.ErrorList {
	store, err := bs.Spec.ObjectStoreSpec()
	if err != nil {
		return field.ErrorList{errInvalidField(bsObjectStorePath, "", err.Error())}
	}
	if store == nil {
		return nil
	}
	var allErrs field.ErrorList
	for _, owned := range cnpg.OwnedObjectStorePaths {
		if _, found, _ := unstructured.NestedFieldNoCopy(store, owned.Path...); found {
			allErrs = append(allErrs, field.Forbidden(bsObjectStorePath.Child(owned.Path[0], owned.Path[1:]...), owned.Reason))
		}
	}
	if raw, found := store["retentionPolicy"]; found {
		policy, isString := raw.(string)
		if !isString || !retentionPolicyPattern.MatchString(policy) {
			allErrs = append(allErrs, errInvalidField(bsObjectStorePath.Child("retentionPolicy"), fmt.Sprintf("%v", raw),
				"must be a recovery window such as 7d, 4w or 3m (days, weeks, months)"))
		}
	}
	return allErrs
}
