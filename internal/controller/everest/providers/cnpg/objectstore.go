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
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/percona/everest-operator/internal/consts"
)

// [CUSTOM CNPG] Sao lưu đi qua BARMAN CLOUD PLUGIN (CNPG-I), không phải interface in-tree
// "spec.backup.barmanObjectStore".
//
// Vì sao bắt buộc phải là plugin:
//
//  1. In-tree gọi binary "barman-cloud-*" BÊN TRONG operand image, nên chỉ chạy được với flavor
//     `system` — flavor mà upstream đã khai tử. ClusterImageCatalog của nền tảng dùng `standard`,
//     nên đường in-tree hỏng ngay từ lần archive WAL đầu tiên: WAL không đi đâu cả, mất PITR
//     trong im lặng, và WAL dồn lại làm đầy PVC.
//  2. Plugin tiêm một SIDECAR mang sẵn barman-cloud vào pod instance, nên operand image dùng
//     flavor nào cũng được. Sidecar có cgroup riêng nên nó OOM cũng không giết PostgreSQL.
//  3. CNPG đã deprecated in-tree từ 1.26 và đang rút Barman khỏi lõi operator.
//
// CNPG chặn cứng việc dùng cả hai: "IsWALArchiver ... cannot be enabled if the
// .spec.backup.barmanObjectStore configuration is present".

const (
	// Khoá "name" trong các map cấu hình dạng unstructured.
	fieldName = "name"
	// Khoá "parameters" của một entry plugin.
	fieldParameters = "parameters"
	// Khoá "cluster" tham chiếu tới CNPG Cluster trong Backup và ScheduledBackup.
	fieldCluster = "cluster"
	// Khoá "key" của một SecretKeySelector dạng unstructured.
	fieldKey = "key"
	// Tên database ứng dụng mặc định. Theo quy ước "convention over configuration" của CNPG.
	defaultAppDatabase = "app"
	// Tên superuser của PostgreSQL. KHÔNG được dùng làm owner của database ứng dụng.
	superuserName = "postgres"
	// Khoá chứa region trong Secret do Everest sở hữu.
	regionSecretKey = "AWS_REGION"
	// Các khoá của Cluster.spec / ObjectStore.spec dạng unstructured.
	fieldConfiguration = "configuration"
	fieldInstances     = "instances"
	fieldStorage       = "storage"
	fieldResources     = "resources"
	// Lý do chung khi chặn *AdditionalCommandArgs trong spec.objectStore.
	reasonRawBarmanArgs = "would bypass the BackupStorage; raw barman-cloud arguments are not allowed"
)

// OwnedObjectStorePaths là phần "backup Ở ĐÂU" của ObjectStore.spec: Everest sinh từ các field có
// kiểu của BackupStorage (hoặc tự quyết), nên BackupStorage.spec.objectStore không được đặt lại. Phần "backup THẾ NÀO" — nén, mã hoá,
// số luồng của wal/data, tags, retentionPolicy, instanceSidecarConfiguration — thì được.
var OwnedObjectStorePaths = []struct {
	Path   []string
	Reason string
}{
	{Path: []string{fieldConfiguration, "destinationPath"}, Reason: "is generated from the BackupStorage bucket plus a per-cluster prefix"},
	{Path: []string{fieldConfiguration, "endpointURL"}, Reason: "is generated from the BackupStorage"},
	{Path: []string{fieldConfiguration, "endpointCA"}, Reason: "is generated from the BackupStorage"},
	{Path: []string{fieldConfiguration, "s3Credentials"}, Reason: "is generated from the BackupStorage credentials and region"},
	{Path: []string{fieldConfiguration, "azureCredentials"}, Reason: "is generated from the BackupStorage credentials"},
	{Path: []string{fieldConfiguration, "googleCredentials"}, Reason: "is generated from the BackupStorage credentials"},
	// serverName quyết định thư mục con trong bucket; restore của Everest tìm backup theo tên cụm.
	{Path: []string{fieldConfiguration, "serverName"}, Reason: "is the cluster name; Everest restores rely on it"},
	// Tham số dòng lệnh thô cho barman-cloud: `--endpoint-url` ở đây là đổi đích backup, tức đi
	// vòng qua BackupStorage.
	{Path: []string{fieldConfiguration, "wal", "archiveAdditionalCommandArgs"}, Reason: reasonRawBarmanArgs},
	{Path: []string{fieldConfiguration, "wal", "restoreAdditionalCommandArgs"}, Reason: reasonRawBarmanArgs},
	{Path: []string{fieldConfiguration, "data", "additionalCommandArgs"}, Reason: reasonRawBarmanArgs},
	{Path: []string{fieldConfiguration, "data", "restoreAdditionalCommandArgs"}, Reason: reasonRawBarmanArgs},
}

// regionSecretName sinh tên Secret chứa region cho một ObjectStore.
func regionSecretName(objectStore string) string {
	return objectStore + "-region"
}

// reconcileRegionSecret bảo đảm Secret chứa AWS_REGION tồn tại cho một ObjectStore.
//
// Barman chỉ đọc region qua secret reference, còn Everest khai region bằng chuỗi literal trong
// BackupStorage. Everest vì vậy phải tự vật chất hoá chuỗi đó thành một Secret của riêng mình —
// không sửa Secret credential của người dùng, và không bắt người dùng phải biết thêm một key.
//
// Secret thuộc sở hữu của DatabaseCluster nên Kubernetes tự dọn khi cụm bị xoá.
func (a *applier) reconcileRegionSecret(objectStore, region string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      regionSecretName(objectStore),
			Namespace: a.DB.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(a.ctx, a.C, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = map[string]string{regionSecretKey: region}
		return controllerutil.SetControllerReference(a.DB, secret, a.C.Scheme())
	})
	if err != nil {
		return fmt.Errorf("reconcile region secret for ObjectStore %q: %w", objectStore, err)
	}
	return nil
}

// ObjectStoreGVK là GroupVersionKind của đích lưu trữ backup do Barman Cloud Plugin định nghĩa.
var ObjectStoreGVK = schema.GroupVersionKind{
	Group:   consts.BarmanCloudAPIGroup,
	Version: "v1",
	Kind:    consts.BarmanCloudObjectStoreKind,
}

// objectStoreName sinh tên ObjectStore cho một cặp (cụm, BackupStorage).
//
// Mỗi cụm có ObjectStore riêng thay vì dùng chung một store cho cả namespace, vì destinationPath
// mà Everest sinh ra đã mang prefix theo từng cụm (common.BackupStoragePrefix). Giữ nguyên cách
// phân tách đó để không phá layout bucket của các bản backup đã có.
func objectStoreName(dbName, storageName string) string {
	return dbName + "-" + storageName
}

// reconcileObjectStore tạo hoặc cập nhật ObjectStore của Barman Cloud Plugin từ một BackupStorage
// của Everest.
//
// ObjectStore thuộc sở hữu của DatabaseCluster (ownerReference), nên Kubernetes tự dọn khi cụm bị
// xoá. BackupStorage vẫn là nguồn sự thật duy nhất mà người dùng khai báo; ObjectStore chỉ là bản
// dịch sang ngôn ngữ của plugin.
//
// [CUSTOM CNPG] overrides là BackupStorage.spec.objectStore — một đoạn ObjectStore.spec viết nguyên
// văn (retentionPolicy, instanceSidecarConfiguration, configuration.wal/data/tags), gộp bằng cùng
// luật với spec.cnpg. Cả "backup ở đâu" lẫn "backup thế nào" đều thuộc BackupStorage (platform
// quản); OwnedObjectStorePaths là phần "ở đâu" mà Everest tự sinh. Chỉ truyền
// cho store ARCHIVE của chính cụm này; store recovery (restore đọc từ backup của cụm NGUỒN) luôn
// nhận nil, để retention của cụm mới không bao giờ áp lên dữ liệu của cụm khác.
func (a *applier) reconcileObjectStore(
	name string,
	storageName string,
	configuration map[string]any,
	overrides map[string]any,
) error {
	for _, owned := range OwnedObjectStorePaths {
		if _, found := nestedValue(overrides, owned.Path); found {
			return fmt.Errorf("BackupStorage %q spec.objectStore.%s %s",
				storageName, strings.Join(owned.Path, "."), owned.Reason)
		}
	}
	spec := map[string]any{fieldConfiguration: configuration}
	if err := mergeSpecMap(spec, overrides, fmt.Sprintf("BackupStorage %q spec.objectStore", storageName)); err != nil {
		return err
	}

	// Plugin cài riêng, không đi kèm CNPG operator. Thiếu nó thì CreateOrUpdate bên dưới sẽ lỗi
	// "no matches for kind ObjectStore" — khó lần ra nguyên nhân. Báo thẳng ngay tại đây.
	installed, err := crdInstalled(a.ctx, a.C, consts.BarmanCloudObjectStoreCRDName)
	if err != nil {
		return fmt.Errorf("check Barman Cloud Plugin CRD: %w", err)
	}
	if !installed {
		return fmt.Errorf(
			"backups require the Barman Cloud Plugin: CRD %q is not installed on this cluster",
			consts.BarmanCloudObjectStoreCRDName,
		)
	}

	object := newUnstructured(ObjectStoreGVK, a.DB.Namespace, name)
	_, err = controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		object.SetLabels(map[string]string{BackupStorageLabel: storageName})
		// Retention của plugin là RECOVERY WINDOW theo thời gian, không phải "giữ N bản".
		// BackupStorage không khai spec.objectStore.retentionPolicy nghĩa là GIỮ VĨNH VIỄN.
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	})
	if err != nil {
		return fmt.Errorf("reconcile Barman Cloud ObjectStore %q: %w", name, err)
	}
	return nil
}

// archiverPluginConfiguration dựng entry "spec.plugins" của Cluster, trỏ tới ObjectStore đã cho.
//
// Cờ isWALArchiver bật ở đây là thứ THAY THẾ archive_command. Tối đa một plugin trong cụm được mang
// cờ này, và mặc định của trường là false — bỏ quên thì cụm vẫn chạy bình thường nhưng WAL không
// được archive đi đâu cả, tức mất PITR mà không có dấu hiệu gì.
func archiverPluginConfiguration(objectStore string) map[string]any {
	return map[string]any{
		fieldName:       consts.BarmanCloudPluginName,
		"isWALArchiver": true,
		fieldParameters: map[string]any{"barmanObjectName": objectStore},
	}
}

// recoveryPluginConfiguration dựng trường "plugin" của một entry trong spec.externalClusters, dùng
// khi bootstrap cụm mới từ backup của cụm khác.
//
// Khác với entry ở spec.plugins: KHÔNG mang isWALArchiver — cụm mới archive WAL của chính nó qua
// ObjectStore riêng, còn entry này chỉ để ĐỌC backup của cụm nguồn.
//
// Trường serverName đặt ở đây chứ không đặt trong ObjectStore: tài liệu plugin khuyến nghị để trống
// serverName phía store và ghi đè ở phía cụm, vì một store có thể phục vụ nhiều cụm và mỗi cụm tự
// phân tách bằng serverName.
func recoveryPluginConfiguration(objectStore, serverName string) map[string]any {
	return map[string]any{
		fieldName: consts.BarmanCloudPluginName,
		fieldParameters: map[string]any{
			"barmanObjectName": objectStore,
			"serverName":       serverName,
		},
	}
}
