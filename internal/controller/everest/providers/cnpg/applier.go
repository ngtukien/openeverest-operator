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
// - postgresql.parameters: các tham số cấu hình custom trong engine.Config
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
	imageName := fmt.Sprintf("ghcr.io/cloudnative-pg/postgresql:%s", engine.Version)
	if component := a.DBEngine.Status.AvailableVersions.Engine[engine.Version]; component != nil && component.ImagePath != "" {
		imageName = component.ImagePath
	}
	spec := map[string]any{
		"instances": int64(engine.Replicas),
		"imageName": imageName,
		"storage": map[string]any{
			"size": engine.Storage.Size.String(),
		},
		"resources": resources,
	}
	if engine.Storage.Class != nil {
		spec["storage"].(map[string]any)["storageClass"] = *engine.Storage.Class
	}
	setStorageSize := func(size resource.Quantity, storageClass *string) {
		storage := spec["storage"].(map[string]any)
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
		spec["bootstrap"] = map[string]any{
			"initdb": map[string]any{
				"database": "postgres",
				"owner":    "postgres",
				"secret":   map[string]any{"name": engine.UserSecretsName},
			},
		}
	}
	if parameters := parsePostgreSQLParameters(engine.Config); len(parameters) != 0 {
		spec["postgresql"] = map[string]any{"parameters": parameters}
	}
	if err := a.configureMonitoring(spec); err != nil {
		return err
	}
	a.Object["spec"] = spec
	return nil
}

func (a *applier) currentStorageSize() (resource.Quantity, error) {
	size, found, err := unstructured.NestedString(a.Object, "spec", "storage", "size")
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
func (a *applier) Proxy() error {
	proxy := a.DB.Spec.Proxy
	proxyResources := proxy.Resources.ToResourceRequirements()
	if proxy.Type != "" || proxy.Replicas != nil || proxy.Config != "" || proxy.Storage != nil ||
		len(proxyResources.Limits) != 0 || len(proxyResources.Requests) != 0 {
		return errors.New("CloudNativePG does not use an Everest-managed proxy; only spec.proxy.expose is supported")
	}
	expose := proxy.Expose
	serviceType := string(expose.Type)
	if serviceType == "" || serviceType == string(corev1.ServiceTypeClusterIP) || serviceType == "internal" {
		return nil // CNPG always creates the <cluster>-rw ClusterIP Service.
	}
	if serviceType == "external" {
		serviceType = string(corev1.ServiceTypeLoadBalancer)
	}
	if serviceType != string(corev1.ServiceTypeLoadBalancer) && serviceType != string(corev1.ServiceTypeNodePort) {
		return fmt.Errorf("unsupported CloudNativePG expose type %q", expose.Type)
	}

	metadata := map[string]any{"name": a.DB.GetName() + "-rw-external"}
	if expose.LoadBalancerConfigName != "" {
		config := &everestv1alpha1.LoadBalancerConfig{}
		if err := a.C.Get(a.ctx, types.NamespacedName{Name: expose.LoadBalancerConfigName}, config); err != nil {
			return fmt.Errorf("get LoadBalancerConfig %q: %w", expose.LoadBalancerConfigName, err)
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
	return unstructured.SetNestedSlice(a.Object, []any{
		map[string]any{
			"selectorType":    "rw",
			"updateStrategy":  "patch",
			"serviceTemplate": map[string]any{"metadata": metadata, "spec": serviceSpec},
		},
	}, "spec", "managed", "services", "additional")
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

	lastSlash := strings.LastIndex(currentImage, "/")
	lastColon := strings.LastIndex(currentImage, ":")
	if lastColon <= lastSlash {
		return fmt.Errorf("cannot determine PostgreSQL version from current CloudNativePG image %q", currentImage)
	}
	currentVersion, _, _ := strings.Cut(currentImage[lastColon+1:], "@")
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
			return errors.New("CloudNativePG does not support retentionCopies; use object-store lifecycle or a time-based retention policy")
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
		config, err := BarmanObjectStore(storage, a.DB)
		if err != nil {
			return err
		}
		if err := unstructured.SetNestedMap(a.Object, map[string]any{"barmanObjectStore": config}, "spec", "backup"); err != nil {
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
				"schedule": schedule.Schedule, "backupOwnerReference": "none", "method": "barmanObjectStore",
				"cluster": map[string]any{"name": a.DB.Name},
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
	config, err := BarmanObjectStore(storage, sourceDB)
	if err != nil {
		return err
	}
	if a.DB.Spec.DataSource.BackupSource != nil {
		config["destinationPath"] = strings.TrimRight(a.DB.Spec.DataSource.BackupSource.Path, "/")
	}
	sourceName := sourceDB.Name
	config["serverName"] = sourceName
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
	return unstructured.SetNestedSlice(a.Object, []any{map[string]any{"name": sourceName, "barmanObjectStore": config}}, "spec", "externalClusters")
}

func (a *applier) DataImport() error {
	return errors.New("data import is not yet supported by the CloudNativePG provider")
}

func parsePostgreSQLParameters(config string) map[string]any {
	parameters := map[string]any{}
	for _, line := range strings.Split(config, "\n") {
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
