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

// Package v1alpha1 ...
//
//nolint:lll
package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/AlekSi/pointer"
	"github.com/Masterminds/semver/v3"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	enginefeatureseverestv1alpha1 "github.com/percona/everest-operator/api/enginefeatures.everest/v1alpha1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
)

var (
	// .spec.engine.
	dbcEnginePath          = specPath.Child("engine")
	dbcEngineTypePath      = dbcEnginePath.Child("type")
	dbcEngineProviderPath  = dbcEnginePath.Child("provider")
	dbcEngineVersionPath   = dbcEnginePath.Child("version")
	dbcEngineCRVersionPath = dbcEnginePath.Child("crVersion")
	dbcUserSecretsNamePath = dbcEnginePath.Child("userSecretsName")

	// .spec.engine.DataSource.
	dbcDataSourcePath = specPath.Child("dataSource")
	dbcDataImportPath = dbcDataSourcePath.Child("dataImport")

	// .spec.proxy.
	dbcProxyPath          = specPath.Child("proxy")
	dbcProxyTypePath      = dbcProxyPath.Child("type")
	dbcProxyReplicasPath  = dbcProxyPath.Child("replicas")
	dbcProxyConfigPath    = dbcProxyPath.Child("config")
	dbcProxyStoragePath   = dbcProxyPath.Child("storage")
	dbcProxyResourcesPath = dbcProxyPath.Child("resources")
	dbcProxyExposePath    = dbcProxyPath.Child("expose")
	dbcProxyExposeLbcPath = dbcProxyExposePath.Child("loadBalancerConfigName")
	dbcMonitoringPath     = specPath.Child("monitoring")
	dbcPausedPath         = specPath.Child("paused")

	// .spec.engineFeatures.
	dbcEngineFeaturesPath = specPath.Child("engineFeatures")

	// .spec.engineFeatures.psmdb.
	dbcPsmdbEngineFeaturesPath    = dbcEngineFeaturesPath.Child("psmdb")
	dbcPsmdbShdcEngineFeaturePath = dbcPsmdbEngineFeaturesPath.Child("splitHorizonDnsConfigName")

	// [CUSTOM CNPG] .spec.replication — CloudNativePG-only, xem PLAN.md Phase 10.
	dbcReplicationPath = specPath.Child("replication")
)

var dbClusterGroupKind = everestv1alpha1.GroupVersion.WithKind(consts.DatabaseClusterKind).GroupKind()

// SetupDatabaseClusterWebhookWithManager sets up the webhook with the manager.
func SetupDatabaseClusterWebhookWithManager(mgr manager.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &everestv1alpha1.DatabaseCluster{}).
		WithValidator(&DatabaseClusterValidator{
			Client: mgr.GetClient(),
		}).
		WithDefaulter(&DatabaseClusterDefaulter{
			Client: mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-everest-percona-com-v1alpha1-databasecluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=everest.percona.com,resources=databaseclusters,verbs=create;update,versions=v1alpha1,name=vdatabasecluster-v1alpha1.everest.percona.com,admissionReviewVersions=v1

// DatabaseClusterValidator validates the DatabaseCluster resource.
type DatabaseClusterValidator struct {
	Client client.Client
}

// ValidateCreate validates the creation of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateCreate(ctx context.Context, db *everestv1alpha1.DatabaseCluster) (admission.Warnings, error) {
	var allErrs field.ErrorList
	var warns admission.Warnings

	logger := log.FromContext(ctx).WithName("DatabaseClusterValidator").WithValues(
		"name", db.GetName(),
		"namespace", db.GetNamespace(),
	)

	logger.Info("Validation for DatabaseCluster upon creation")

	isCNPG := db.Spec.Engine.EffectiveProvider() == everestv1alpha1.DatabaseEngineProviderCloudNativePG
	if isCNPG {
		allErrs = append(allErrs, v.validateCNPGCapabilities(ctx, db, true)...)
	}
	// Validate the engine version against the DatabaseEngine catalog.
	// [CUSTOM CNPG] Kiểm tra này trước đây bỏ qua CloudNativePG vì provider đó không có
	// DatabaseEngine nào để đối chiếu — hệ quả là gõ version không tồn tại vẫn apply được và chỉ
	// chết ở ImagePullBackOff. Nay danh sách version của CNPG đến từ ClusterImageCatalog nên
	// kiểm tra này áp dụng cho mọi provider.
	allErrs = append(allErrs, v.validateEngineVersion(ctx, db)...)

	if err := v.validateUserSecret(ctx, db); err != nil {
		allErrs = append(allErrs, err)
	}

	// If a data import source is specified, validate it.
	if di := pointer.Get(db.Spec.DataSource).DataImport; di != nil && !isCNPG {
		if errs := v.validateDataImport(ctx, db); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	}

	if errs := v.validateLoadBalancerConfig(ctx, db.Spec.Proxy.Expose.LoadBalancerConfigName); errs != nil {
		allErrs = append(allErrs, errs...)
	}

	if errs := v.validateEngineFeaturesOnCreate(ctx, db); errs != nil && !isCNPG {
		allErrs = append(allErrs, errs...)
	}

	// [CUSTOM CNPG] Replication (Publication/Subscription) is CloudNativePG-only.
	if db.Spec.Replication != nil && !isCNPG {
		allErrs = append(allErrs, field.Forbidden(dbcReplicationPath, "replication is only supported by the CloudNativePG provider"))
	}
	allErrs = append(allErrs, validateReplicationNames(db)...)
	// [CUSTOM CNPG] spec.cnpg passthrough, xem PLAN.md Phase 12.
	allErrs = append(allErrs, validateCNPGPassthrough(db, isCNPG)...)

	if warn := db.Spec.Proxy.Expose.Type.DeprecationWarning(); warn != "" {
		warns = append(warns, warn)
	}

	if len(allErrs) == 0 {
		return warns, nil
	}

	return warns, apierrors.NewInvalid(dbClusterGroupKind, db.GetName(), allErrs)
}

// ValidateUpdate validates the update of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateUpdate(ctx context.Context, oldDb, newDb *everestv1alpha1.DatabaseCluster) (admission.Warnings, error) {
	var allErrs field.ErrorList
	var warns admission.Warnings

	logger := log.FromContext(ctx).WithName("DatabaseClusterValidator").WithValues(
		"name", newDb.GetName(),
		"namespace", newDb.GetNamespace(),
	)

	logger.Info("Validation for DatabaseCluster upon update")

	// Validate the engine type immutability
	if oldDb.Spec.Engine.Type != newDb.Spec.Engine.Type {
		return nil, apierrors.NewInvalid(dbClusterGroupKind, oldDb.GetName(), field.ErrorList{
			errImmutableField(dbcEngineTypePath),
		})
	}
	if oldDb.Spec.Engine.Provider != newDb.Spec.Engine.Provider {
		return nil, apierrors.NewInvalid(dbClusterGroupKind, oldDb.GetName(), field.ErrorList{
			errImmutableField(dbcEngineProviderPath),
		})
	}

	isCNPG := newDb.Spec.Engine.EffectiveProvider() == everestv1alpha1.DatabaseEngineProviderCloudNativePG
	if isCNPG {
		allErrs = append(allErrs, v.validateCNPGCapabilities(ctx, newDb, false)...)
		allErrs = append(allErrs, validateCNPGVersionUpdate(oldDb.Spec.Engine.Version, newDb.Spec.Engine.Version)...)
	} else {
		allErrs = append(allErrs, v.validateEngineVersion(ctx, newDb)...)
	}

	// TODO: move remaining validations from Everest API
	// 1. Validate engine version change (upgrade/downgrade)
	// 2. Validate replica count change
	// 3. Validate storage size change
	// 3. Validate sharding constraints

	if errs := v.validateEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil && !isCNPG {
		allErrs = append(allErrs, errs...)
	}

	// [CUSTOM CNPG] Replication (Publication/Subscription) is CloudNativePG-only.
	if newDb.Spec.Replication != nil && !isCNPG {
		allErrs = append(allErrs, field.Forbidden(dbcReplicationPath, "replication is only supported by the CloudNativePG provider"))
	}
	allErrs = append(allErrs, validateReplicationNames(newDb)...)
	// [CUSTOM CNPG] spec.cnpg passthrough, xem PLAN.md Phase 12.
	allErrs = append(allErrs, validateCNPGPassthrough(newDb, isCNPG)...)
	if isCNPG {
		allErrs = append(allErrs, v.validateCNPGPassthroughUpdate(ctx, oldDb, newDb)...)
	}

	if warn := newDb.Spec.Proxy.Expose.Type.DeprecationWarning(); warn != "" {
		warns = append(warns, warn)
	}

	if len(allErrs) == 0 {
		return warns, nil
	}

	return warns, apierrors.NewInvalid(dbClusterGroupKind, oldDb.GetName(), allErrs)
}

// ValidateDelete validates the deletion of a DatabaseCluster.
func (v *DatabaseClusterValidator) ValidateDelete(_ context.Context, _ *everestv1alpha1.DatabaseCluster) (admission.Warnings, error) {
	return nil, nil
}

func (v *DatabaseClusterValidator) validateDataImport(
	ctx context.Context,
	db *everestv1alpha1.DatabaseCluster,
) field.ErrorList {
	var allErrs field.ErrorList

	dataImport := pointer.Get(db.Spec.DataSource).DataImport
	if dataImport == nil {
		return nil
	}

	dataimporter := &everestv1alpha1.DataImporter{}
	if err := v.Client.Get(ctx, types.NamespacedName{
		Name: dataImport.DataImporterName,
	}, dataimporter); err != nil {
		return append(allErrs, errInvalidField(dbcDataImportPath, dataImport.DataImporterName, err.Error()))
	}

	// Validate that the DataImporter supports the specified engine.
	engineType := db.Spec.Engine.Type
	if !dataimporter.Spec.SupportedEngines.Has(engineType) {
		return append(allErrs, errInvalidField(dbcDataImportPath, dataImport.DataImporterName,
			fmt.Sprintf("data importer %s does not support engine type %s", dataImport.DataImporterName, engineType)))
	}

	if requiredFields := dataimporter.Spec.DatabaseClusterConstraints.RequiredFields; len(requiredFields) > 0 {
		if errs := validateDataImportRequiredFields(requiredFields, db); errs != nil {
			return append(allErrs, errs...)
		}
	}

	// Validate the params against the schema of the DataImporter
	if err := dataimporter.Spec.Config.Validate(dataImport.Config); err != nil {
		return append(allErrs, errInvalidField(dbcDataImportPath, dataImport.DataImporterName, err.Error()))
	}

	return nil
}

func (v *DatabaseClusterValidator) validateUserSecret(ctx context.Context, db *everestv1alpha1.DatabaseCluster) *field.Error {
	if userSecretsName := db.Spec.Engine.UserSecretsName; userSecretsName != "" {
		secret := corev1.Secret{}
		if err := v.Client.Get(ctx, types.NamespacedName{
			Name:      userSecretsName,
			Namespace: db.GetNamespace(),
		}, &secret); err != nil {
			return errInvalidField(dbcUserSecretsNamePath, userSecretsName, err.Error())
		}
	}
	return nil
}

func (v *DatabaseClusterValidator) validateCNPGCapabilities(
	ctx context.Context,
	db *everestv1alpha1.DatabaseCluster,
	checkCRD bool,
) field.ErrorList {
	var allErrs field.ErrorList

	allErrs = append(allErrs, validateCNPGEngine(db.Spec.Engine)...)
	allErrs = append(allErrs, validateCNPGProxy(db.Spec.Proxy)...)
	allErrs = append(allErrs, validateCNPGUnsupportedFeatures(db)...)

	if checkCRD {
		allErrs = append(allErrs, v.validateCNPGCRD(ctx, string(db.Spec.Engine.Provider))...)
	}

	return allErrs
}

func validateCNPGEngine(engine everestv1alpha1.Engine) field.ErrorList {
	var allErrs field.ErrorList
	if engine.Type != everestv1alpha1.DatabaseEnginePostgresql {
		allErrs = append(allErrs, field.Forbidden(dbcEngineProviderPath, "cloudnative-pg is only supported for PostgreSQL"))
	}
	if engine.Version == "" {
		allErrs = append(allErrs, errRequiredField(dbcEngineVersionPath))
	} else if _, err := semver.NewVersion(engine.Version); err != nil {
		allErrs = append(allErrs, errInvalidField(dbcEngineVersionPath, engine.Version, "must be a semantic PostgreSQL version such as 16.4"))
	}
	if engine.CRVersion != nil {
		allErrs = append(allErrs, field.Forbidden(dbcEngineCRVersionPath, "CloudNativePG does not use Everest CR versions"))
	}
	return allErrs
}

// [CUSTOM CNPG] validateCNPGProxy kiểm tra phần proxy khi provider là CloudNativePG.
//
// Kiểm tra này chỉ còn chặn config và storage; proxy.type/replicas/resources nay được hỗ trợ — Everest dựng CRD
// "Pooler" của CNPG từ chúng, nên bật/tắt pooler là đổi một trường trên DatabaseCluster thay vì
// phải khai một CR riêng nằm ngoài vòng quản lý của Everest.
func validateCNPGProxy(proxy everestv1alpha1.Proxy) field.ErrorList {
	var allErrs field.ErrorList
	if proxy.Type != "" && proxy.Type != everestv1alpha1.ProxyTypePGBouncer {
		allErrs = append(allErrs, field.NotSupported(
			dbcProxyTypePath, proxy.Type,
			[]string{string(everestv1alpha1.ProxyTypePGBouncer)},
		))
	}
	// proxy.config của Everest là chuỗi INI tự do. Chuyển thẳng xuống PgBouncer nghĩa là cho phép
	// đặt bất kỳ tham số nào, kể cả thứ phá pool — cần một API có kiểu trước khi mở.
	if proxy.Config != "" {
		allErrs = append(allErrs, field.Forbidden(dbcProxyConfigPath,
			"CloudNativePG pooler does not support free-form spec.proxy.config"))
	}
	if proxy.Storage != nil {
		allErrs = append(allErrs, field.Forbidden(dbcProxyStoragePath,
			"CloudNativePG pooler does not use spec.proxy.storage"))
	}
	// Khai replicas hay resources mà không bật pooler là cấu hình vô nghĩa, dễ khiến người dùng
	// tưởng đã có pooler.
	if proxy.Type == "" {
		proxyResources := proxy.Resources.ToResourceRequirements()
		if proxy.Replicas != nil {
			allErrs = append(allErrs, field.Forbidden(dbcProxyReplicasPath,
				"set spec.proxy.type=pgbouncer to enable the pooler"))
		}
		if len(proxyResources.Limits) != 0 || len(proxyResources.Requests) != 0 {
			allErrs = append(allErrs, field.Forbidden(dbcProxyResourcesPath,
				"set spec.proxy.type=pgbouncer to enable the pooler"))
		}
	}
	return allErrs
}

func validateCNPGUnsupportedFeatures(db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList
	if db.Spec.Monitoring != nil {
		allErrs = append(allErrs, field.Forbidden(dbcMonitoringPath, "PMM monitoring is not yet supported by the CloudNativePG provider"))
	}
	if pointer.Get(db.Spec.DataSource).DataImport != nil {
		allErrs = append(allErrs, field.Forbidden(dbcDataImportPath, "data import is not yet supported by the CloudNativePG provider"))
	}
	if db.Spec.EngineFeatures != nil {
		allErrs = append(allErrs, field.Forbidden(dbcEngineFeaturesPath, "engineFeatures are not yet supported by the CloudNativePG provider"))
	}
	if db.Spec.Paused {
		allErrs = append(allErrs, field.Forbidden(dbcPausedPath, "pausing is not yet supported by the CloudNativePG provider"))
	}
	return allErrs
}

func (v *DatabaseClusterValidator) validateCNPGCRD(ctx context.Context, provider string) field.ErrorList {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	err := v.Client.Get(ctx, types.NamespacedName{Name: consts.CNPGClusterCRDName}, crd)
	switch {
	case apierrors.IsNotFound(err):
		return field.ErrorList{field.Forbidden(
			dbcEngineProviderPath,
			fmt.Sprintf("CloudNativePG CRD %q is not installed", consts.CNPGClusterCRDName),
		)}
	case err != nil:
		return field.ErrorList{errInvalidField(
			dbcEngineProviderPath,
			provider,
			fmt.Sprintf("could not verify CloudNativePG availability: %v", err),
		)}
	}
	return nil
}

func validateCNPGVersionUpdate(oldVersion, newVersion string) field.ErrorList {
	oldSemver, oldErr := semver.NewVersion(oldVersion)
	newSemver, newErr := semver.NewVersion(newVersion)
	if oldErr != nil || newErr != nil {
		return nil
	}
	if oldSemver.Major() != newSemver.Major() {
		return field.ErrorList{field.Forbidden(
			dbcEngineVersionPath,
			fmt.Sprintf("CloudNativePG rolling updates support only minor version changes (current=%s, requested=%s)", oldVersion, newVersion),
		)}
	}
	if newSemver.LessThan(oldSemver) {
		return field.ErrorList{field.Forbidden(
			dbcEngineVersionPath,
			fmt.Sprintf("CloudNativePG PostgreSQL version downgrade is not supported (current=%s, requested=%s)", oldVersion, newVersion),
		)}
	}
	return nil
}

func validateDataImportRequiredFields(requiredFields []string, db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	for _, rField := range requiredFields {
		exists, err := checkJSONKeyExists(rField, db)
		if err != nil {
			allErrs = append(allErrs, errRequiredField(field.NewPath(rField)))
		}
		if !exists {
			allErrs = append(allErrs, errRequiredField(field.NewPath(rField)))
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

// checkJSONKeyExists returns true if the specified key exists in the JSON object.
// The keyExpr is a dot-separated string representing the path to the key in the JSON object.
func checkJSONKeyExists(keyExpr string, obj any) (bool, error) {
	objB, err := json.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("failed to marshal object: %w", err)
	}
	objMap := make(map[string]any)
	if err := json.Unmarshal(objB, &objMap); err != nil {
		return false, fmt.Errorf("failed to unmarshal object: %w", err)
	}

	subKeys := slices.DeleteFunc(strings.Split(keyExpr, "."), func(s string) bool {
		return s == ""
	})
	currentSubObject := objMap
	for _, key := range subKeys {
		if value, exists := currentSubObject[key]; exists {
			switch v := value.(type) {
			case map[string]any:
				currentSubObject = v
				continue
			case int, int32, int64, float32, float64:
				return value != 0, nil
			case string:
				return value != "", nil
			}
			continue
		}
		return false, nil
	}
	return true, nil
}

func (v *DatabaseClusterValidator) validateEngineVersion(ctx context.Context, db *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// Get the DatabaseEngine.
	// [CUSTOM CNPG] Tra theo provider, không chỉ theo type: PostgreSQL có hai DatabaseEngine
	// (percona-postgresql-operator và cnpg-controller-manager) cùng spec.type.
	if engine, err := common.GetDatabaseEngineForProvider(
		ctx, v.Client,
		db.Spec.Engine.Type,
		db.Spec.Engine.EffectiveProvider(),
		db.GetNamespace(),
	); err != nil {
		allErrs = append(allErrs, errInvalidField(dbcEngineTypePath, string(db.Spec.Engine.Type), err.Error()))
	} else {
		// Check if the engine version is available
		if _, ok := engine.Status.AvailableVersions.Engine[db.Spec.Engine.Version]; !ok {
			allErrs = append(allErrs,
				field.NotSupported(dbcEngineVersionPath, db.Spec.Engine.Version, engine.Status.AvailableVersions.Engine.GetAllowedVersionsSorted()))
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterValidator) validateLoadBalancerConfig(ctx context.Context, lbcName string) field.ErrorList {
	var allErrs field.ErrorList

	if lbcName == "" {
		return nil
	}

	lbc := everestv1alpha1.LoadBalancerConfig{}
	err := v.Client.Get(ctx, client.ObjectKey{
		Name: lbcName,
	}, &lbc)
	if err != nil {
		return append(allErrs, errInvalidField(dbcProxyExposeLbcPath, lbcName, err.Error()))
	}

	return nil
}

func (v *DatabaseClusterValidator) validateEngineFeaturesOnCreate(ctx context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	switch dbObj.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		// validate PXC engine features
		if errs := v.validatePxcEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		// validate Postgresql engine features
		if errs := v.validatePostgresqlEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePSMDB:
		// validate PSMDB engine features
		if errs := v.validatePsmdbEngineFeaturesOnCreate(ctx, dbObj); errs != nil {
			return append(allErrs, errs...)
		}
	default:
		// TODO: add validator calls for the rest of DB Engines.
		return nil
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePxcEngineFeaturesOnCreate(_ context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(dbObj.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePostgresqlEngineFeaturesOnCreate(_ context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(dbObj.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePsmdbEngineFeaturesOnCreate(ctx context.Context, dbObj *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	psmdbEF := pointer.Get(pointer.Get(dbObj.Spec.EngineFeatures).PSMDB)

	// SplitHorizonDNSConfig
	if psmdbEF.SplitHorizonDNSConfigName != "" {
		// For the time being SplitHorizonDNSConfig is not supported in sharded clusters.
		if pointer.Get(dbObj.Spec.Sharding).Enabled {
			return append(allErrs, field.Forbidden(dbcPsmdbShdcEngineFeaturePath, "SplitHorizonDNSConfig and Sharding configuration is not supported"))
		}

		shdc := enginefeatureseverestv1alpha1.SplitHorizonDNSConfig{}
		err := v.Client.Get(ctx, client.ObjectKey{
			Name:      psmdbEF.SplitHorizonDNSConfigName,
			Namespace: dbObj.GetNamespace(),
		}, &shdc)
		if err != nil {
			return append(allErrs, errInvalidField(dbcPsmdbShdcEngineFeaturePath, psmdbEF.SplitHorizonDNSConfigName, err.Error()))
		}
	}

	// TODO: validate the rest of PSMDB engine features.
	return nil
}

func (v *DatabaseClusterValidator) validateEngineFeaturesOnUpdate(ctx context.Context, oldDb, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	if oldDb.Spec.EngineFeatures == nil && newDb.Spec.EngineFeatures == nil {
		// Engine features haven't been used before and are not requested in update request.
		// Nothing to do.
		return nil
	}

	switch oldDb.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		// validate PXC engine features
		if errs := v.validatePxcEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePostgresql:
		// validate Postgresql engine features
		if errs := v.validatePostgresqlEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			return append(allErrs, errs...)
		}
	case everestv1alpha1.DatabaseEnginePSMDB:
		// validate PSMDB engine features
		if errs := v.validatePsmdbEngineFeaturesOnUpdate(ctx, oldDb, newDb); errs != nil {
			allErrs = append(allErrs, errs...)
		}
	default:
		// TODO: add validators for the rest of DB Engines later.
		return nil
	}

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}

func (v *DatabaseClusterValidator) validatePxcEngineFeaturesOnUpdate(_ context.Context, _, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(newDb.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePXC)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePostgresqlEngineFeaturesOnUpdate(_ context.Context, _, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	// We do not support engine features for PXC yet.
	// Check that DB spec doesn't contain engine features related to the other db engines.
	if pointer.Get(newDb.Spec.EngineFeatures).PSMDB != nil {
		return append(allErrs, errInvalidField(dbcPsmdbShdcEngineFeaturePath, "",
			fmt.Sprintf("PSMDB engine features are not applicable to engine type=%s", everestv1alpha1.DatabaseEnginePostgresql)))
	}

	return nil
}

func (v *DatabaseClusterValidator) validatePsmdbEngineFeaturesOnUpdate(_ context.Context, oldDb, newDb *everestv1alpha1.DatabaseCluster) field.ErrorList {
	var allErrs field.ErrorList

	oldPsmdbEF := pointer.Get(pointer.Get(oldDb.Spec.EngineFeatures).PSMDB)
	newPsmdbEF := pointer.Get(pointer.Get(newDb.Spec.EngineFeatures).PSMDB)

	// SplitHorizonDNSConfig feature
	if oldPsmdbEF.SplitHorizonDNSConfigName != newPsmdbEF.SplitHorizonDNSConfigName {
		// NOTE: for the time being we do not allow to disable/enable/change SplitHorizonDNSConfig for already existing clusters.
		// This feature can be enabled during cluster creation only.
		allErrs = append(allErrs, errImmutableField(dbcPsmdbShdcEngineFeaturePath))
	}

	// TODO: validate the rest of PSMDB engine features later.

	if len(allErrs) == 0 {
		return nil
	}

	return allErrs
}
