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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
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
	// Tham số của entry Barman Cloud Plugin trong spec.plugins / externalClusters[].plugin.
	parameterBarmanObjectName = "barmanObjectName"
	parameterServerName       = "serverName"
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
	{Path: []string{fieldConfiguration, "destinationPath"}, Reason: "is generated from the BackupStorage bucket"},
	{Path: []string{fieldConfiguration, "endpointURL"}, Reason: "is generated from the BackupStorage"},
	{Path: []string{fieldConfiguration, "endpointCA"}, Reason: "is generated from the BackupStorage"},
	{Path: []string{fieldConfiguration, "s3Credentials"}, Reason: "is generated from the BackupStorage credentials and region"},
	{Path: []string{fieldConfiguration, "azureCredentials"}, Reason: "is generated from the BackupStorage credentials"},
	{Path: []string{fieldConfiguration, "googleCredentials"}, Reason: "is generated from the BackupStorage credentials"},
	// serverName quyết định thư mục con trong bucket. Store dùng chung thì mỗi cụm tự đặt serverName
	// riêng (ServerName) ở phía Cluster; đặt ở phía store là dồn mọi cụm vào một thư mục.
	{Path: []string{fieldConfiguration, "serverName"}, Reason: "is set per cluster by Everest; a store-wide value would merge every cluster into one folder"},
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
// Secret thuộc sở hữu của owner (BackupStorage với store dùng chung, DatabaseCluster với store
// recovery riêng của cụm), nên Kubernetes tự dọn cùng ObjectStore.
func reconcileRegionSecret(ctx context.Context, c client.Client, owner client.Object, objectStore, region string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      regionSecretName(objectStore),
			Namespace: owner.GetNamespace(),
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = map[string]string{regionSecretKey: region}
		return controllerutil.SetControllerReference(owner, secret, c.Scheme())
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

// ErrBarmanCloudPluginMissing báo CRD ObjectStore của Barman Cloud Plugin chưa được cài.
var ErrBarmanCloudPluginMissing = fmt.Errorf(
	"backups require the Barman Cloud Plugin: CRD %q is not installed on this cluster",
	consts.BarmanCloudObjectStoreCRDName,
)

// SharedObjectStoreName là tên ObjectStore dùng chung của một BackupStorage: trùng tên
// BackupStorage, để `kubectl get objectstore` đọc ra ngay đó là gói backup nào.
//
// [CUSTOM CNPG] Một BackupStorage ↔ một ObjectStore, theo mô hình tài liệu plugin khuyến nghị:
// store mô tả "kho", các cụm dùng chung kho và tự tách thư mục bằng serverName (ServerName).
// Store có ngay khi BackupStorage được tạo, không phải đợi cụm đầu tiên.
func SharedObjectStoreName(storageName string) string {
	return storageName
}

// ServerName là thư mục của một cụm bên trong ObjectStore dùng chung: <bucket>/<ServerName>/
// {base,wals}.
//
// Mang UID chứ không chỉ tên cụm: xoá rồi tạo lại cụm cùng tên sẽ ghi vào thư mục MỚI thay vì
// đụng WAL archive cũ (CNPG từ chối archive vào thư mục không rỗng). Đây cũng là ranh giới DUY
// NHẤT giữa các cụm trong cùng store, nên Everest luôn tự sinh và spec.cnpg không được đặt lại.
func ServerName(db *everestv1alpha1.DatabaseCluster) string {
	return db.Name + "-" + string(db.UID)
}

// legacyObjectStoreName là tên ObjectStore riêng từng cụm của thiết kế trước (<cụm>-<storage>),
// chỉ còn dùng để dọn dẹp.
func legacyObjectStoreName(dbName, storageName string) string {
	return dbName + "-" + storageName
}

// recoveryObjectStoreName là store riêng của cụm, chỉ dùng khi restore từ field
// spec.dataSource.backupSource.path — một đường dẫn tuỳ ý không nằm trong layout của store dùng chung.
func recoveryObjectStoreName(dbName string) string {
	return dbName + "-recovery"
}

// ReconcileSharedObjectStore tạo hoặc cập nhật ObjectStore dùng chung của một BackupStorage.
//
// Được gọi từ hai phía: BackupStorage controller (để store có ngay khi BackupStorage xuất hiện) và
// provider CNPG (để một cụm không bao giờ trỏ vào store chưa tồn tại). ObjectStore thuộc sở hữu
// của BackupStorage, nên xoá BackupStorage — vốn bị finalizer in-use chặn khi còn cụm dùng — là
// Kubernetes tự dọn store.
//
// Trả về ErrBarmanCloudPluginMissing nếu plugin chưa được cài, và lỗi của BarmanObjectStore nếu
// BackupStorage không dùng được cho CNPG (Azure thiếu endpoint, verifyTLS=false...).
func ReconcileSharedObjectStore(ctx context.Context, c client.Client, storage *everestv1alpha1.BackupStorage) error {
	name := SharedObjectStoreName(storage.GetName())
	config, err := BarmanObjectStore(storage, regionSecretName(name))
	if err != nil {
		return err
	}
	// "Backup thế nào" (retention, nén, sidecar) cũng thuộc BackupStorage: một chính sách cho
	// mọi cụm dùng storage này, do platform quản.
	overrides, err := storage.Spec.ObjectStoreSpec()
	if err != nil {
		return fmt.Errorf("BackupStorage %q: %w", storage.GetName(), err)
	}
	if storage.Spec.Region != "" {
		if err := ensurePluginInstalled(ctx, c); err != nil {
			return err
		}
		if err := reconcileRegionSecret(ctx, c, storage, name, storage.Spec.Region); err != nil {
			return err
		}
	}
	return applyObjectStore(ctx, c, storage, name, storage.GetName(), config, overrides)
}

// reconcileRecoveryObjectStore dựng store riêng của cụm để ĐỌC backup ở một đường dẫn tuỳ ý
// (spec.dataSource.backupSource.path). Store này thuộc sở hữu của DatabaseCluster và không nhận
// spec.objectStore của BackupStorage, để retention không bao giờ áp lên dữ liệu ngoài layout.
func (a *applier) reconcileRecoveryObjectStore(storage *everestv1alpha1.BackupStorage, destinationPath string) (string, error) {
	name := recoveryObjectStoreName(a.DB.Name)
	config, err := BarmanObjectStore(storage, regionSecretName(name))
	if err != nil {
		return "", err
	}
	config["destinationPath"] = strings.TrimRight(destinationPath, "/")
	if storage.Spec.Region != "" {
		if err := ensurePluginInstalled(a.ctx, a.C); err != nil {
			return "", err
		}
		if err := reconcileRegionSecret(a.ctx, a.C, a.DB, name, storage.Spec.Region); err != nil {
			return "", err
		}
	}
	return name, applyObjectStore(a.ctx, a.C, a.DB, name, storage.GetName(), config, nil)
}

// deleteLegacyObjectStores xoá ObjectStore riêng từng cụm (<cụm>-<storage>) và Secret region của
// nó do thiết kế trước sinh ra. Chỉ xoá object mà CHÍNH cụm này là controller owner — không bao giờ
// đụng store dùng chung hay store người dùng tự tạo trùng tên.
//
// Chỉ xoá khi Cluster ĐANG CHẠY (đọc từ API, không phải bản đang dựng trong vòng reconcile này)
// không còn trỏ vào store. Bản đang dựng chỉ được ghi xuống nếu các bước sau cũng thành công; xoá
// sớm mà một bước sau lỗi thì Cluster vẫn archive vào store đã mất — WAL archiving gãy. Vòng
// reconcile kế tiếp, sau khi Cluster đã chuyển sang store dùng chung, mới dọn.
func (a *applier) deleteLegacyObjectStores() error {
	installed, err := crdInstalled(a.ctx, a.C, consts.BarmanCloudObjectStoreCRDName)
	if err != nil || !installed {
		return err
	}
	inUse, err := a.liveClusterObjectStores()
	if err != nil {
		return err
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(ObjectStoreGVK.GroupVersion().WithKind(ObjectStoreGVK.Kind + "List"))
	if err := a.C.List(a.ctx, list, client.InNamespace(a.DB.Namespace), client.HasLabels{BackupStorageLabel}); err != nil {
		return fmt.Errorf("list ObjectStores: %w", err)
	}
	for i := range list.Items {
		store := &list.Items[i]
		storageName := store.GetLabels()[BackupStorageLabel]
		if store.GetName() != legacyObjectStoreName(a.DB.Name, storageName) || !metav1.IsControlledBy(store, a.DB) {
			continue
		}
		if _, used := inUse[store.GetName()]; used {
			continue
		}
		if err := a.C.Delete(a.ctx, store); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete legacy ObjectStore %q: %w", store.GetName(), err)
		}
		secret := &corev1.Secret{}
		key := types.NamespacedName{Namespace: a.DB.Namespace, Name: regionSecretName(store.GetName())}
		if err := a.C.Get(a.ctx, key, secret); err == nil && metav1.IsControlledBy(secret, a.DB) {
			if err := a.C.Delete(a.ctx, secret); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete legacy region Secret %q: %w", secret.GetName(), err)
			}
		} else if client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("get legacy region Secret %q: %w", key.Name, err)
		}
	}
	return nil
}

// liveClusterObjectStores trả về tên các ObjectStore mà CNPG Cluster đang chạy trỏ tới, qua
// spec.plugins (archive) và spec.externalClusters[].plugin (restore). Cluster chưa tồn tại thì rỗng.
func (a *applier) liveClusterObjectStores() (map[string]struct{}, error) {
	live := newUnstructured(clusterGVK, a.DB.Namespace, a.DB.Name)
	if err := a.C.Get(a.ctx, client.ObjectKeyFromObject(live), live); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	names := map[string]struct{}{}
	collect := func(plugin any) {
		entry, _ := plugin.(map[string]any)
		parameters, _ := entry[fieldParameters].(map[string]any)
		if name, _ := parameters[parameterBarmanObjectName].(string); name != "" {
			names[name] = struct{}{}
		}
	}
	plugins, _, _ := unstructured.NestedSlice(live.Object, "spec", "plugins")
	for _, plugin := range plugins {
		collect(plugin)
	}
	externals, _, _ := unstructured.NestedSlice(live.Object, "spec", "externalClusters")
	for _, raw := range externals {
		entry, _ := raw.(map[string]any)
		collect(entry["plugin"])
	}
	return names, nil
}

// ensurePluginInstalled báo thẳng khi thiếu Barman Cloud Plugin. Plugin cài riêng, không đi kèm
// CNPG operator; thiếu nó thì CreateOrUpdate ObjectStore lỗi "no matches for kind ObjectStore" —
// khó lần ra nguyên nhân.
func ensurePluginInstalled(ctx context.Context, c client.Client) error {
	installed, err := crdInstalled(ctx, c, consts.BarmanCloudObjectStoreCRDName)
	if err != nil {
		return fmt.Errorf("check Barman Cloud Plugin CRD: %w", err)
	}
	if !installed {
		return ErrBarmanCloudPluginMissing
	}
	return nil
}

// applyObjectStore tạo hoặc cập nhật một ObjectStore của Barman Cloud Plugin, thuộc sở hữu của
// owner.
//
// [CUSTOM CNPG] overrides là BackupStorage.spec.objectStore — một đoạn ObjectStore.spec viết nguyên
// văn (retentionPolicy, instanceSidecarConfiguration, configuration.wal/data/tags), gộp bằng cùng
// luật với spec.cnpg. Cả "backup ở đâu" lẫn "backup thế nào" đều thuộc BackupStorage (platform
// quản); OwnedObjectStorePaths là phần "ở đâu" mà Everest tự sinh.
func applyObjectStore(
	ctx context.Context,
	c client.Client,
	owner client.Object,
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
	if err := ensurePluginInstalled(ctx, c); err != nil {
		return err
	}

	object := newUnstructured(ObjectStoreGVK, owner.GetNamespace(), name)
	_, err := controllerutil.CreateOrUpdate(ctx, c, object, func() error {
		object.SetLabels(map[string]string{BackupStorageLabel: storageName})
		// Retention của plugin là RECOVERY WINDOW theo thời gian, không phải "giữ N bản".
		// BackupStorage không khai spec.objectStore.retentionPolicy nghĩa là GIỮ VĨNH VIỄN.
		object.Object["spec"] = spec
		return controllerutil.SetControllerReference(owner, object, c.Scheme())
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
//
// Tham số serverName tách thư mục của cụm này trong store dùng chung — xem ServerName.
func archiverPluginConfiguration(objectStore, serverName string) map[string]any {
	return map[string]any{
		fieldName:       consts.BarmanCloudPluginName,
		"isWALArchiver": true,
		fieldParameters: map[string]any{
			parameterBarmanObjectName: objectStore,
			parameterServerName:       serverName,
		},
	}
}

// recoveryPluginConfiguration dựng trường "plugin" của một entry trong spec.externalClusters, dùng
// khi bootstrap cụm mới từ backup của cụm khác.
//
// Khác với entry ở spec.plugins: KHÔNG mang isWALArchiver — cụm mới archive WAL của chính nó qua
// ObjectStore dùng chung dưới serverName của chính nó, còn entry này chỉ để ĐỌC backup của cụm nguồn.
//
// Trường serverName đặt ở đây chứ không đặt trong ObjectStore: tài liệu plugin khuyến nghị để trống
// serverName phía store và ghi đè ở phía cụm, vì một store có thể phục vụ nhiều cụm và mỗi cụm tự
// phân tách bằng serverName.
func recoveryPluginConfiguration(objectStore, serverName string) map[string]any {
	return map[string]any{
		fieldName: consts.BarmanCloudPluginName,
		fieldParameters: map[string]any{
			parameterBarmanObjectName: objectStore,
			parameterServerName:       serverName,
		},
	}
}
