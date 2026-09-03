// everest-operator
// // everest-operator
// // Copyright (C) 2022 Percona LLC
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// // http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// // everest-operator
// // Copyright (C) 2022 Percona LLC
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// // http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

package cnpg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AlekSi/pointer"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

const (
	BackupStorageLabel = "everest.percona.com/backup-storage"
	ScheduleNameLabel  = "everest.percona.com/backup-schedule"
)

var (
	BackupGVK          = schema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGBackupKind}
	ScheduledBackupGVK = schema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGScheduledBackupKind}
)

// [CUSTOM CNPG] BarmanObjectStore: Chuyển đổi cấu hình Everest BackupStorage thành spec
// "barmanObjectStore" chuẩn của CloudNativePG:
// - destinationPath: đường dẫn s3://<bucket>/<prefix> hoặc Azure URL
// - s3Credentials / azureCredentials: ánh xạ các key từ Secret (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
// - endpointURL: hỗ trợ S3-compatible như MinIO, SeaweedFS, Ceph RGW.
func BarmanObjectStore(storage *everestv1alpha1.BackupStorage, db *everestv1alpha1.DatabaseCluster) (map[string]any, error) {
	if storage.Spec.ForcePathStyle != nil && *storage.Spec.ForcePathStyle {
		return nil, errors.New("CloudNativePG in-tree backups do not support forcePathStyle")
	}
	if storage.Spec.VerifyTLS != nil && !*storage.Spec.VerifyTLS {
		return nil, errors.New("CloudNativePG backups require TLS verification; verifyTLS=false is not supported")
	}
	prefix := strings.Trim(common.BackupStoragePrefix(db), "/")
	secret := storage.Spec.CredentialsSecretName
	result := map[string]any{}
	if storage.Spec.EndpointURL != "" {
		result["endpointURL"] = storage.Spec.EndpointURL
	}
	switch storage.Spec.Type {
	case everestv1alpha1.BackupStorageTypeS3:
		result["destinationPath"] = fmt.Sprintf("s3://%s/%s", strings.Trim(storage.Spec.Bucket, "/"), prefix)
		if storage.Spec.Region != "" {
			result["s3Credentials"] = map[string]any{
				"region":          map[string]any{"name": secret, "key": "AWS_REGION"},
				"accessKeyId":     map[string]any{"name": secret, "key": "AWS_ACCESS_KEY_ID"},
				"secretAccessKey": map[string]any{"name": secret, "key": "AWS_SECRET_ACCESS_KEY"},
			}
		} else {
			result["s3Credentials"] = map[string]any{
				"accessKeyId":     map[string]any{"name": secret, "key": "AWS_ACCESS_KEY_ID"},
				"secretAccessKey": map[string]any{"name": secret, "key": "AWS_SECRET_ACCESS_KEY"},
			}
		}
	case everestv1alpha1.BackupStorageTypeAzure:
		if storage.Spec.EndpointURL == "" {
			return nil, errors.New("Azure BackupStorage.endpointURL is required for CloudNativePG")
		}
		result["destinationPath"] = fmt.Sprintf("%s/%s/%s", strings.TrimRight(storage.Spec.EndpointURL, "/"), strings.Trim(storage.Spec.Bucket, "/"), prefix)
		result["azureCredentials"] = map[string]any{
			"storageAccount": map[string]any{"name": secret, "key": "AZURE_STORAGE_ACCOUNT_NAME"},
			"storageKey":     map[string]any{"name": secret, "key": "AZURE_STORAGE_ACCOUNT_KEY"},
		}
	default:
		return nil, fmt.Errorf("unsupported CloudNativePG backup storage type %q", storage.Spec.Type)
	}
	return result, nil
}

func getBackupStorage(ctx context.Context, c client.Client, namespace, name string) (*everestv1alpha1.BackupStorage, error) {
	if name == "" {
		return nil, errors.New("a BackupStorage name is required")
	}
	storage := &everestv1alpha1.BackupStorage{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, storage); err != nil {
		return nil, fmt.Errorf("get BackupStorage %q: %w", name, err)
	}
	return storage, nil
}

// [CUSTOM CNPG] backupStorageForDataSource: Phục vụ khôi phục (Restore / PITR):
// Tìm ra đối tượng BackupStorage và DatabaseCluster nguồn (sourceDB) tương ứng từ khai báo spec.dataSource
// để cấu hình quá trình tải dữ liệu sao lưu về cho cụm mới.
func backupStorageForDataSource(ctx context.Context, c client.Client, db *everestv1alpha1.DatabaseCluster) (*everestv1alpha1.BackupStorage, *everestv1alpha1.DatabaseCluster, error) {
	ds := pointer.Get(db.Spec.DataSource)
	if ds.DBClusterBackupName != "" {
		backup := &everestv1alpha1.DatabaseClusterBackup{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: ds.DBClusterBackupName}, backup); err != nil {
			return nil, nil, fmt.Errorf("get DatabaseClusterBackup %q: %w", ds.DBClusterBackupName, err)
		}
		sourceDB := &everestv1alpha1.DatabaseCluster{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: backup.Spec.DBClusterName}, sourceDB); err != nil {
			return nil, nil, fmt.Errorf("get source DatabaseCluster %q: %w", backup.Spec.DBClusterName, err)
		}
		storage, err := getBackupStorage(ctx, c, db.Namespace, backup.Spec.BackupStorageName)
		return storage, sourceDB, err
	}
	if ds.BackupSource != nil {
		storage, err := getBackupStorage(ctx, c, db.Namespace, ds.BackupSource.BackupStorageName)
		return storage, db, err
	}
	return nil, nil, errors.New("either dbClusterBackupName or backupSource must be specified")
}

func newUnstructured(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{}}
	object.SetGroupVersionKind(gvk)
	object.SetNamespace(namespace)
	object.SetName(name)
	return object
}
