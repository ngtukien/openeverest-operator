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
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

type applier struct {
	*Provider
	ctx       context.Context //nolint:containedctx
	pausedErr error
}

func (a *applier) ResetDefaults() error {
	if a.Object == nil {
		a.Object = map[string]any{}
	}
	if _, ok := a.Object["spec"]; !ok {
		a.Object["spec"] = map[string]any{}
	}
	return nil
}

func (a *applier) Paused(paused bool) {
	if paused {
		a.pausedErr = errors.New("CloudNativePG provider does not yet support spec.paused")
	}
}

func (a *applier) AllowUnsafeConfig() {}

func (a *applier) Metadata() error {
	a.SetLabels(map[string]string{
		"app.kubernetes.io/name":       a.DB.GetName(),
		"app.kubernetes.io/instance":   a.DB.GetName(),
		"app.kubernetes.io/managed-by": "everest-operator",
	})
	return controllerutil.SetControllerReference(a.DB, a.Unstructured, a.C.Scheme())
}

// [CUSTOM CNPG] Engine: Chuyển đổi toàn bộ cấu hình engine từ Everest sang CNPG Cluster spec:
// - instances: số lượng node PostgreSQL (engine.Replicas)
// - imageName: image của PostgreSQL (ghcr.io/cloudnative-pg/postgresql:<version>)
// - storage: dung lượng và StorageClass
// - resources: requests và limits CPU/RAM
// - bootstrap.initdb: secret chứa thông tin mật khẩu ban đầu
// - managed.roles: giữ password của user ứng dụng luôn khớp Secret sau khi bootstrap xong
// - postgresql.parameters: các tham số cấu hình custom trong engine.Config.
func (a *applier) Engine() error {
	if a.pausedErr != nil {
		return a.pausedErr
	}
	engine := &a.DB.Spec.Engine
	if engine.Version == "" {
		engine.Version = a.DBEngine.BestEngineVersion()
	}
	if engine.Version == "" {
		return errors.New("no PostgreSQL engine version is available for CloudNativePG")
	}
	if engine.Replicas < 1 {
		return errors.New("CloudNativePG requires at least one PostgreSQL instance")
	}
	currentImage, _, err := unstructured.NestedString(a.Object, "spec", "imageName")
	if err != nil {
		return fmt.Errorf("read current CloudNativePG image: %w", err)
	}
	if err := validateVersionChange(currentImage, engine.Version); err != nil {
		return err
	}
	currentSize, err := a.currentStorageSize()
	if err != nil {
		return err
	}

	resourceRequirements := engine.Resources.ToResourceRequirements()
	resources, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&resourceRequirements)
	if err != nil {
		return fmt.Errorf("convert engine resources: %w", err)
	}
	// [CUSTOM CNPG] Image LUÔN đến từ ClusterImageCatalog, không bao giờ được ghép chuỗi tại đây.
	// Ghép chuỗi sẽ cho ra một tag trần, không digest, không nằm trong danh mục đã duyệt — đúng
	// thứ mà việc kiểm soát image tập trung sinh ra để ngăn.
	var imageName string
	switch component := a.DBEngine.Status.AvailableVersions.Engine[engine.Version]; {
	case component != nil && component.ImagePath != "":
		imageName = component.ImagePath
	case currentImage != "":
		// Version đã bị rút khỏi catalog (ví dụ platform team xoay 14.24 sang 14.25) nhưng cụm
		// đang chạy nó. Giữ nguyên image đang chạy: rút một version khỏi danh mục là chặn cụm
		// MỚI dùng nó, không phải ép cụm đang sống phải rolling update ngoài ý muốn.
		imageName = currentImage
	default:
		return fmt.Errorf(
			"PostgreSQL version %q is not available in ClusterImageCatalog %q",
			engine.Version, consts.CNPGClusterImageCatalogName,
		)
	}
	spec := map[string]any{
		fieldInstances: int64(engine.Replicas),
		"imageName":    imageName,
		fieldStorage: map[string]any{
			"size": engine.Storage.Size.String(),
		},
		fieldResources: resources,
	}
	if engine.Storage.Class != nil {
		if storage, ok := spec[fieldStorage].(map[string]any); ok {
			storage["storageClass"] = *engine.Storage.Class
		}
	}
	setStorageSize := func(size resource.Quantity, storageClass *string) {
		storage, ok := spec[fieldStorage].(map[string]any)
		if !ok {
			return
		}
		storage["size"] = size.String()
		if storageClass != nil {
			storage["storageClass"] = *storageClass
		}
	}
	if err := common.ConfigureStorage(
		a.ctx,
		a.C,
		a.DB,
		currentSize,
		engine.Storage.Size,
		engine.Storage.Class,
		setStorageSize,
	); err != nil {
		return fmt.Errorf("configure CloudNativePG storage: %w", err)
	}
	if engine.UserSecretsName != "" {
		owner, err := a.initdbOwner(engine.UserSecretsName)
		if err != nil {
			return err
		}
		database, err := a.initdbDatabase()
		if err != nil {
			return err
		}
		spec["bootstrap"] = map[string]any{
			"initdb": map[string]any{
				"database": database,
				"owner":    owner,
				"secret":   map[string]any{fieldName: engine.UserSecretsName},
			},
		}
		// [CUSTOM CNPG] bootstrap.initdb.secret chỉ được đọc MỘT LẦN lúc dựng cụm. Không có khối
		// managed.roles thì đổi password trong Secret không bao giờ tới được PostgreSQL. CNPG
		// reconcile khối này liên tục (chỉ trên primary thật, nên replica cluster không bị ảnh
		// hưởng) và bắt "username" trong Secret khớp tên role — initdbOwner đã đọc đúng key đó.
		spec["managed"] = map[string]any{
			"roles": []any{map[string]any{
				fieldName:        owner,
				"ensure":         "present",
				"login":          true,
				"passwordSecret": map[string]any{fieldName: engine.UserSecretsName},
			}},
		}
	}
	if parameters := ParsePostgreSQLParameters(engine.Config); len(parameters) != 0 {
		spec["postgresql"] = map[string]any{"parameters": parameters}
	}
	if err := a.configureMonitoring(spec); err != nil {
		return err
	}
	a.Object["spec"] = spec
	return nil
}

// [CUSTOM CNPG] initdbOwner đọc tên user ứng dụng từ Secret mà người dùng khai ở
// spec.engine.userSecretsName.
//
// CNPG bắt "username" trong secret PHẢI khớp "bootstrap.initdb.owner", và owner PHẢI là một user
// KHÔNG đặc quyền. Trước đây Everest hardcode owner="postgres" — tức chính superuser — nên CNPG
// không tạo role ứng dụng nào, còn superuser thì bị chính CNPG xoá password vì
// enableSuperuserAccess mặc định là false. Kết quả: cụm lên xanh, status.hostname được công bố,
// nhưng KHÔNG client nào kết nối được và cũng không có database ứng dụng nào.
func (a *applier) initdbOwner(secretName string) (string, error) { //nolint:funcorder
	secret := &corev1.Secret{}
	if err := a.C.Get(a.ctx, types.NamespacedName{
		Namespace: a.DB.Namespace, Name: secretName,
	}, secret); err != nil {
		return "", fmt.Errorf("get user secret %q: %w", secretName, err)
	}
	owner := strings.TrimSpace(string(secret.Data[corev1.BasicAuthUsernameKey]))
	if owner == "" {
		return "", fmt.Errorf(
			"user secret %q must contain a %q key naming the application user",
			secretName, corev1.BasicAuthUsernameKey,
		)
	}
	if owner == superuserName {
		return "", fmt.Errorf(
			"user secret %q names the PostgreSQL superuser %q as the application user; "+
				"CloudNativePG requires an unprivileged owner and disables the superuser password",
			secretName, superuserName,
		)
	}
	if err := a.ensureSecretReload(secret); err != nil {
		return "", err
	}
	return owner, nil
}

// cnpgReloadLabel là label CNPG dùng để quyết định một Secret KHÔNG thuộc sở hữu của Cluster có
// được theo dõi hay không (utils.WatchedLabelName, internal/controller/cluster_predicates.go).
const cnpgReloadLabel = "cnpg.io/reload"

// [CUSTOM CNPG] ensureSecretReload gắn label cnpg.io/reload lên Secret userSecretsName.
//
// Trường managed.roles chỉ giữ password khớp Secret nếu CNPG BIẾT Secret đã đổi. CNPG chỉ watch Secret do
// chính Cluster sở hữu hoặc mang label này; Secret do người dùng tạo thì không có cả hai, nên đổi
// password xong PostgreSQL vẫn giữ password cũ cho tới khi một sự kiện khác làm Cluster reconcile
// lại — đã bắt được trên lab: 90 giây sau khi đổi Secret, password cũ vẫn đăng nhập được.
//
// Chỉ patch label, không đụng dữ liệu; Secret vẫn là của người dùng.
func (a *applier) ensureSecretReload(secret *corev1.Secret) error { //nolint:funcorder
	if _, found := secret.GetLabels()[cnpgReloadLabel]; found {
		return nil
	}
	patched := secret.DeepCopy()
	labels := patched.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[cnpgReloadLabel] = "true"
	patched.SetLabels(labels)
	if err := a.C.Patch(a.ctx, patched, client.MergeFrom(secret)); err != nil {
		return fmt.Errorf("label user secret %q with %s: %w", secret.GetName(), cnpgReloadLabel, err)
	}
	return nil
}

// [CUSTOM CNPG] initdbDatabase lấy tên database ứng dụng từ spec.cnpg.bootstrap.initdb.database
// nếu người dùng khai, ngược lại dùng mặc định. Nếu luôn ghi "app" thì Passthrough() sẽ báo xung
// đột với đúng giá trị người dùng muốn — ép họ bỏ userSecretsName chỉ để đổi tên database.
func (a *applier) initdbDatabase() (string, error) { //nolint:funcorder
	user, err := a.DB.Spec.CNPGSpec()
	if err != nil {
		return "", err
	}
	if database, ok := nestedValue(user, []string{"bootstrap", "initdb", "database"}); ok {
		if name, isString := database.(string); isString && name != "" {
			return name, nil
		}
	}
	return defaultAppDatabase, nil
}

func (a *applier) EngineFeatures() error {
	if a.DB.Spec.EngineFeatures != nil {
		return errors.New("engineFeatures are not supported by the CloudNativePG provider")
	}
	return nil
}

// [CUSTOM CNPG] Proxy: Xử lý Service Exposure cho CNPG:
//   - CNPG không dùng proxy ngoài do Everest quản lý; nó có sẵn Service native.
//   - Nếu người dùng cấu hình proxy.expose dạng LoadBalancer/NodePort, Everest sẽ khai báo
//     vào "spec.managed.services.additional" để CNPG tự sinh Service "<cluster>-rw-external"
//     trỏ trực tiếp vào Primary pod hiện tại.
//
// [CUSTOM CNPG] Proxy: quyết định đường vào của cụm.
//
//   - Không khai proxy.type  → client nối thẳng Service "<cluster>-rw" do CNPG tạo sẵn.
//   - proxy.type: pgbouncer  → Everest dựng CRD "Pooler", và ĐƯỜNG VÀO CHUYỂN SANG POOLER.
//
// proxy.expose áp lên đúng lớp đang làm đường vào: có pooler thì nó cấu hình Service của pooler,
// không có thì cấu hình Service "rw" của cụm. Nếu expose cứ trỏ vào "rw" kể cả khi bật pooler thì
// người dùng vô tình mở một đường đi vòng qua pooler — phá đúng giới hạn connection mà pooler sinh
// ra để bảo vệ.
func (a *applier) Proxy() error {
	proxy := a.DB.Spec.Proxy

	// config và storage vẫn bị chặn. proxy.config của Everest là chuỗi INI tự do; chuyển thẳng
	// xuống PgBouncer là để người dùng đặt bất kỳ tham số nào, kể cả thứ phá pool. Cần một API
	// có kiểu (poolMode, maxClientConnections...) trước khi mở. Pooler không dùng storage.
	if proxy.Config != "" || proxy.Storage != nil {
		return errors.New(
			"CloudNativePG pooler does not support spec.proxy.config or spec.proxy.storage",
		)
	}
	if proxy.Type != "" && proxy.Type != everestv1alpha1.ProxyTypePGBouncer {
		return fmt.Errorf(
			"CloudNativePG supports only %q as spec.proxy.type", everestv1alpha1.ProxyTypePGBouncer,
		)
	}

	serviceTemplate, err := a.exposeServiceTemplate()
	if err != nil {
		return err
	}

	if poolerEnabled(a.DB) {
		// Đường vào là pooler: expose áp lên Service của pooler, và cụm KHÔNG mở thêm Service
		// nào ra ngoài.
		if err := unstructured.SetNestedSlice(
			a.Object, []any{}, "spec", "managed", "services", "additional",
		); err != nil {
			return err
		}
		return a.reconcilePooler(serviceTemplate)
	}

	// Không dùng pooler: dọn Pooler cũ nếu người dùng vừa tắt, rồi expose Service "rw".
	if err := a.reconcilePooler(nil); err != nil {
		return err
	}
	if serviceTemplate == nil {
		return nil // CNPG luôn tạo sẵn Service "<cluster>-rw" dạng ClusterIP.
	}
	return unstructured.SetNestedSlice(a.Object, []any{
		map[string]any{
			"selectorType":    "rw",
			"updateStrategy":  "patch",
			"serviceTemplate": serviceTemplate,
		},
	}, "spec", "managed", "services", "additional")
}

// exposeServiceTemplate dịch spec.proxy.expose thành serviceTemplate. Trả về nil khi expose là
// ClusterIP hoặc để trống — lúc đó Service mặc định đã đủ.
func (a *applier) exposeServiceTemplate() (map[string]any, error) { //nolint:funcorder
	expose := a.DB.Spec.Proxy.Expose
	serviceType := string(expose.Type)
	if serviceType == "" || serviceType == string(corev1.ServiceTypeClusterIP) || serviceType == "internal" {
		return nil, nil //nolint:nilnil
	}
	if serviceType == "external" {
		serviceType = string(corev1.ServiceTypeLoadBalancer)
	}
	if serviceType != string(corev1.ServiceTypeLoadBalancer) && serviceType != string(corev1.ServiceTypeNodePort) {
		return nil, fmt.Errorf("unsupported CloudNativePG expose type %q", expose.Type)
	}

	metadata := map[string]any{fieldName: a.DB.GetName() + "-rw-external"}
	if poolerEnabled(a.DB) {
		// Pooler tự đặt tên Service bằng tên Pooler; ghi đè sẽ làm CNPG mất dấu.
		metadata = map[string]any{}
	}
	if expose.LoadBalancerConfigName != "" {
		config := &everestv1alpha1.LoadBalancerConfig{}
		if err := a.C.Get(a.ctx, types.NamespacedName{Name: expose.LoadBalancerConfigName}, config); err != nil {
			return nil, fmt.Errorf("get LoadBalancerConfig %q: %w", expose.LoadBalancerConfigName, err)
		}
		metadata["annotations"] = stringMapToAny(config.Spec.Annotations)
	}
	serviceSpec := map[string]any{"type": serviceType}
	if ranges := expose.IPSourceRangesStringArray(); len(ranges) != 0 {
		unstructuredRanges := make([]any, len(ranges))
		for i := range ranges {
			unstructuredRanges[i] = ranges[i]
		}
		serviceSpec["loadBalancerSourceRanges"] = unstructuredRanges
	}
	return map[string]any{"metadata": metadata, "spec": serviceSpec}, nil
}

func (a *applier) Monitoring() error {
	if a.DB.Spec.Monitoring != nil {
		return errors.New("MonitoringConfig/PMM is not yet supported by the CloudNativePG provider")
	}
	return nil
}

func (a *applier) PodSchedulingPolicy() error {
	if a.DB.Spec.PodSchedulingPolicyName == "" {
		return nil
	}
	policy := &everestv1alpha1.PodSchedulingPolicy{}
	if err := a.C.Get(a.ctx, types.NamespacedName{Name: a.DB.Spec.PodSchedulingPolicyName}, policy); err != nil {
		return fmt.Errorf("get PodSchedulingPolicy %q: %w", a.DB.Spec.PodSchedulingPolicyName, err)
	}
	if policy.Spec.EngineType != everestv1alpha1.DatabaseEnginePostgresql ||
		policy.Spec.AffinityConfig == nil || policy.Spec.AffinityConfig.PostgreSQL == nil ||
		policy.Spec.AffinityConfig.PostgreSQL.Engine == nil {
		return nil
	}
	engineAffinity := policy.Spec.AffinityConfig.PostgreSQL.Engine
	cnpgAffinity := map[string]any{}
	if engineAffinity.NodeAffinity != nil {
		nodeAffinity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(engineAffinity.NodeAffinity)
		if err != nil {
			return fmt.Errorf("convert PostgreSQL node affinity: %w", err)
		}
		cnpgAffinity["nodeAffinity"] = nodeAffinity
	}
	if engineAffinity.PodAffinity != nil {
		podAffinity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(engineAffinity.PodAffinity)
		if err != nil {
			return fmt.Errorf("convert PostgreSQL pod affinity: %w", err)
		}
		cnpgAffinity["additionalPodAffinity"] = podAffinity
	}
	if engineAffinity.PodAntiAffinity != nil {
		podAntiAffinity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(engineAffinity.PodAntiAffinity)
		if err != nil {
			return fmt.Errorf("convert PostgreSQL pod anti-affinity: %w", err)
		}
		cnpgAffinity["additionalPodAntiAffinity"] = podAntiAffinity
	}
	if len(cnpgAffinity) == 0 {
		return nil
	}
	return unstructured.SetNestedMap(a.Object, cnpgAffinity, "spec", "affinity")
}

func validateVersionChange(currentImage, desiredVersion string) error {
	desired, err := semver.NewVersion(desiredVersion)
	if err != nil {
		return fmt.Errorf("invalid CloudNativePG PostgreSQL version %q: %w", desiredVersion, err)
	}
	if currentImage == "" {
		return nil
	}

	// [CUSTOM CNPG] PHẢI cắt digest TRƯỚC khi tìm dấu ":" cuối. Image lấy từ ClusterImageCatalog
	// luôn pin bằng digest, mà chính digest cũng chứa dấu hai chấm ("@sha256:..."). Tìm dấu ":"
	// cuối trên chuỗi đầy đủ sẽ nhặt nhầm phần digest làm tag, và semver không parse nổi chuỗi hex
	// đó — guard version hỏng ngay khi bật pin digest.
	reference, _, _ := strings.Cut(currentImage, "@")
	lastSlash := strings.LastIndex(reference, "/")
	lastColon := strings.LastIndex(reference, ":")
	if lastColon <= lastSlash {
		return fmt.Errorf("cannot determine PostgreSQL version from current CloudNativePG image %q", currentImage)
	}
	currentVersion := reference[lastColon+1:]
	current, err := semver.NewVersion(currentVersion)
	if err != nil {
		return fmt.Errorf("cannot determine PostgreSQL version from current CloudNativePG image %q: %w", currentImage, err)
	}
	if desired.Major() != current.Major() {
		return fmt.Errorf(
			"CloudNativePG rolling updates only support PostgreSQL minor versions: current=%s desired=%s",
			currentVersion,
			desiredVersion,
		)
	}
	if desired.LessThan(current) {
		return fmt.Errorf("CloudNativePG PostgreSQL version downgrade is not supported: current=%s desired=%s", currentVersion, desiredVersion)
	}
	return nil
}

// [CUSTOM CNPG] Backup: Đồng bộ cấu hình sao lưu cho CNPG Cluster:
//  1. Tìm BackupStorage được chỉ định và ánh xạ vào "spec.backup.barmanObjectStore" của CNPG.
//  2. Kiểm tra ràng buộc: CNPG chỉ hỗ trợ 1 đích lưu trữ S3 duy nhất cho toàn cụm.
//  3. Với mỗi lịch trong spec.backup.schedules: tự động sinh ra hoặc xóa tài nguyên
//     ScheduledBackup ("postgresql.cnpg.io/v1") tương ứng trên K8s.
func (a *applier) Backup() error {
	storageNames := map[string]struct{}{}
	for _, schedule := range a.DB.Spec.Backup.Schedules {
		if schedule.Enabled {
			storageNames[schedule.BackupStorageName] = struct{}{}
		}
		if schedule.RetentionCopies != 0 {
			return errors.New("CloudNativePG does not support retentionCopies; the platform sets a time-based " +
				"retentionPolicy such as \"7d\" in the BackupStorage's spec.objectStore")
		}
	}
	if a.DB.Spec.Backup.PITR.Enabled {
		if a.DB.Spec.Backup.PITR.BackupStorageName == nil {
			return errors.New("backup.pitr.backupStorageName is required for CloudNativePG")
		}
		storageNames[*a.DB.Spec.Backup.PITR.BackupStorageName] = struct{}{}
	}
	backups := &everestv1alpha1.DatabaseClusterBackupList{}
	if err := a.C.List(a.ctx, backups, client.InNamespace(a.DB.Namespace)); err != nil {
		return fmt.Errorf("list DatabaseClusterBackups: %w", err)
	}
	for i := range backups.Items {
		if backups.Items[i].Spec.DBClusterName == a.DB.Name && !backups.Items[i].HasCompleted() {
			storageNames[backups.Items[i].Spec.BackupStorageName] = struct{}{}
		}
	}
	if len(storageNames) > 1 {
		return errors.New("CloudNativePG supports one object-store destination per cluster; all backups and schedules must use the same BackupStorage")
	}
	var storageName string
	for name := range storageNames {
		storageName = name
	}
	if storageName != "" {
		storage, err := getBackupStorage(a.ctx, a.C, a.DB.Namespace, storageName)
		if err != nil {
			return err
		}
		// [CUSTOM CNPG] Sao lưu đi qua Barman Cloud Plugin, không phải "spec.backup.barmanObjectStore"
		// in-tree — xem objectstore.go để biết vì sao in-tree không dùng được với operand image
		// flavor `standard` trong ClusterImageCatalog của nền tảng.
		store := objectStoreName(a.DB.Name, storageName)
		if storage.Spec.Region != "" {
			if err := a.reconcileRegionSecret(store, storage.Spec.Region); err != nil {
				return err
			}
		}
		config, err := BarmanObjectStore(storage, a.DB, regionSecretName(store))
		if err != nil {
			return err
		}
		// "Backup thế nào" (retention, nén, sidecar) cũng thuộc BackupStorage: một chính sách cho
		// mọi cụm dùng storage này, do platform quản.
		overrides, err := storage.Spec.ObjectStoreSpec()
		if err != nil {
			return fmt.Errorf("BackupStorage %q: %w", storageName, err)
		}
		if err := a.reconcileObjectStore(store, storageName, config, overrides); err != nil {
			return err
		}
		if err := unstructured.SetNestedSlice(
			a.Object, []any{archiverPluginConfiguration(store)}, "spec", "plugins",
		); err != nil {
			return err
		}
	}
	for _, schedule := range a.DB.Spec.Backup.Schedules {
		name := a.DB.Name + "-" + schedule.Name
		object := newUnstructured(ScheduledBackupGVK, a.DB.Namespace, name)
		if !schedule.Enabled {
			if err := a.C.Delete(a.ctx, object); client.IgnoreNotFound(err) != nil {
				return err
			}
			continue
		}
		cron := strings.Fields(schedule.Schedule)
		if len(cron) == 5 {
			schedule.Schedule = "0 " + schedule.Schedule
		} else if len(cron) != 6 {
			return fmt.Errorf("CloudNativePG schedule %q must contain five or six cron fields", schedule.Name)
		}
		_, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
			object.SetLabels(map[string]string{BackupStorageLabel: schedule.BackupStorageName, ScheduleNameLabel: schedule.Name})
			object.Object["spec"] = map[string]any{
				"schedule": schedule.Schedule, "backupOwnerReference": "none",
				// [CUSTOM CNPG] method "plugin" ủy thác việc chụp base backup cho Barman Cloud
				// Plugin. Cấu hình đích lưu trữ lấy từ spec.plugins của Cluster nên ở đây chỉ cần
				// nêu tên plugin.
				"method":              "plugin",
				"pluginConfiguration": map[string]any{fieldName: consts.BarmanCloudPluginName},
				fieldCluster:          map[string]any{fieldName: a.DB.Name},
			}
			return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
		})
		if err != nil {
			return fmt.Errorf("reconcile CloudNativePG ScheduledBackup %q: %w", name, err)
		}
	}
	return nil
}

// [CUSTOM CNPG] DataSource: Cấu hình phục hồi (Restore / PITR) khi khởi tạo cụm CNPG mới:
// - Lấy thông tin BackupStorage và thông tin cụm nguồn (sourceDB).
// - Cấu hình "spec.bootstrap.recovery" (chỉ định source và mốc thời gian recoveryTarget nếu dùng PITR).
// - Cấu hình "spec.externalClusters" kết nối tới Barman S3 Object Store để tải dữ liệu về.
func (a *applier) DataSource() error {
	if a.DB.Spec.DataSource == nil {
		return nil
	}
	if a.DB.Status.Status == everestv1alpha1.AppStateReady {
		if err := common.ReconcileDBRestoreFromDataSource(a.ctx, a.C, a.DB); err != nil {
			return err
		}
		if a.DB.Spec.DataSource == nil {
			return nil
		}
	}
	storage, sourceDB, err := backupStorageForDataSource(a.ctx, a.C, a.DB)
	if err != nil {
		return err
	}
	recoveryStore := objectStoreName(a.DB.Name, "recovery")
	if storage.Spec.Region != "" {
		if err := a.reconcileRegionSecret(recoveryStore, storage.Spec.Region); err != nil {
			return err
		}
	}
	config, err := BarmanObjectStore(storage, sourceDB, regionSecretName(recoveryStore))
	if err != nil {
		return err
	}
	if a.DB.Spec.DataSource.BackupSource != nil {
		config["destinationPath"] = strings.TrimRight(a.DB.Spec.DataSource.BackupSource.Path, "/")
	}
	sourceName := sourceDB.Name
	// [CUSTOM CNPG] serverName KHÔNG đặt trong ObjectStore mà đặt ở phía externalClusters — xem
	// recoveryPluginConfiguration(). Một store có thể phục vụ nhiều cụm; serverName là thứ phân
	// tách chúng, nên nó thuộc về phía người đọc chứ không phải phía cái kho.
	if err := a.reconcileObjectStore(recoveryStore, storage.GetName(), config, nil); err != nil {
		return err
	}
	recovery := map[string]any{"source": sourceName}
	if pitr := a.DB.Spec.DataSource.PITR; pitr != nil {
		target := map[string]any{}
		switch pitr.Type {
		case everestv1alpha1.PITRTypeLatest:
			// Full recovery already replays all available WAL.
		case everestv1alpha1.PITRTypeDate:
			if pitr.Date == nil {
				return errors.New("PITR date is required when type=date")
			}
			target["targetTime"] = pitr.Date.Time.UTC().Format(everestv1alpha1.DateFormat)
		default:
			return fmt.Errorf("unsupported CloudNativePG PITR type %q", pitr.Type)
		}
		if len(target) != 0 {
			recovery["recoveryTarget"] = target
		}
	}
	if err := unstructured.SetNestedMap(a.Object, map[string]any{"recovery": recovery}, "spec", "bootstrap"); err != nil {
		return err
	}
	return unstructured.SetNestedSlice(a.Object, []any{map[string]any{
		fieldName: sourceName,
		"plugin":  recoveryPluginConfiguration(recoveryStore, sourceName),
	}}, "spec", "externalClusters")
}

func (a *applier) DataImport() error {
	return errors.New("data import is not yet supported by the CloudNativePG provider")
}

func (a *applier) currentStorageSize() (resource.Quantity, error) {
	size, found, err := unstructured.NestedString(a.Object, "spec", fieldStorage, "size")
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("read current CloudNativePG storage size: %w", err)
	}
	if !found || size == "" {
		return resource.Quantity{}, nil
	}
	currentSize, err := resource.ParseQuantity(size)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("parse current CloudNativePG storage size %q: %w", size, err)
	}
	return currentSize, nil
}

func (a *applier) configureMonitoring(spec map[string]any) error {
	// [CUSTOM CNPG] Phase 8 (Observability): chỉ bật enablePodMonitor khi CRD PodMonitor của
	// Prometheus Operator đã cài trên cụm; nếu không, CNPG Cluster vẫn expose metrics ở cổng
	// "metrics" (9187) nhưng không tự sinh PodMonitor. Xem PLAN.md Phase 8.
	podMonitorInstalled, err := podMonitorCRDInstalled(a.ctx, a.C)
	if err != nil {
		return fmt.Errorf("check PodMonitor CRD: %w", err)
	}
	if podMonitorInstalled {
		spec["monitoring"] = map[string]any{"enablePodMonitor": true}
	}
	return nil
}

// ParsePostgreSQLParameters parses spec.engine.config (postgresql.conf lines) into CNPG
// postgresql.parameters. Exported so the webhook detects keys also set in spec.cnpg.
func ParsePostgreSQLParameters(config string) map[string]any {
	parameters := map[string]any{}
	for line := range strings.SplitSeq(config, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		parameters[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "'\"")
	}
	return parameters
}

func stringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
