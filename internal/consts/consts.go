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

// Package consts provides constants used across the operator.
package consts

// ClusterType represents the type of the cluster.
type ClusterType string

const (
	// Everest ...
	Everest = "everest"

	// DBClusterRestoreDBClusterNameField is the field in the DatabaseClusterRestore CR.
	DBClusterRestoreDBClusterNameField = ".spec.dbClusterName"

	// DBClusterBackupDBClusterNameField is the field in the DatabaseClusterBackup and DatabaseClusterRestore CRs.
	DBClusterBackupDBClusterNameField = ".spec.dbClusterName"
	// DBClusterBackupBackupStorageNameField is the field in the DatabaseClusterBackup CR.
	DBClusterBackupBackupStorageNameField = ".spec.backupStorageName"
	// DataSourceBackupStorageNameField is the field in the DatabaseClusterBackup and DatabaseClusterRestore CRs.
	DataSourceBackupStorageNameField = ".spec.dataSource.backupSource.backupStorageName"

	// TopologyKeyHostname is the topology key for hostname.
	TopologyKeyHostname = "kubernetes.io/hostname"

	// PXCDeploymentName is the name of the Percona XtraDB Cluster operator deployment.
	PXCDeploymentName = "percona-xtradb-cluster-operator"
	// PSMDBDeploymentName is the name of the Percona Server for MongoDB operator deployment.
	PSMDBDeploymentName = "percona-server-mongodb-operator"
	// PGDeploymentName is the name of the Percona PostgreSQL operator deployment.
	PGDeploymentName = "percona-postgresql-operator"
	// CNPGDeploymentName [CUSTOM CNPG] Tên Deployment và Namespace của CloudNativePG Operator trên cụm K8s.
	CNPGDeploymentName = "cnpg-controller-manager"
	// CNPGOperatorNamespace is the namespace used by the cluster-wide CNPG installation.
	CNPGOperatorNamespace = "cnpg-system"
	// CNPGOperatorAppName [CUSTOM CNPG] là giá trị nhãn "app.kubernetes.io/name" trên Deployment
	// của CloudNativePG. Dò theo nhãn thay vì theo tên Deployment: tên phụ thuộc cách cài (Helm
	// đặt "<release>-cloudnative-pg", manifest upstream đặt "cnpg-controller-manager").
	CNPGOperatorAppName = "cloudnative-pg"
	// CNPGClusterCRDName is the cluster-scoped CRD required by the CloudNativePG provider.
	CNPGClusterCRDName = "clusters.postgresql.cnpg.io"
	// CNPGClusterImageCatalogCRDName [CUSTOM CNPG] là CRD chứa danh mục operand image của
	// CloudNativePG. Everest chỉ đọc catalog khi CRD này tồn tại (dynamic discovery, Phase 1).
	CNPGClusterImageCatalogCRDName = "clusterimagecatalogs.postgresql.cnpg.io"
	// CNPGClusterImageCatalogKind [CUSTOM CNPG] là Kind của danh mục image cluster-scoped.
	CNPGClusterImageCatalogKind = "ClusterImageCatalog"
	// BarmanCloudAPIGroup [CUSTOM CNPG] là API group của Barman Cloud Plugin. Group này KHÔNG
	// thuộc CloudNativePG core — CRD do plugin cài vào cụm, nên phải dynamic discovery trước khi
	// dùng, giống cách kiểm tra CRD của CNPG ở Phase 1.
	BarmanCloudAPIGroup = "barmancloud.cnpg.io"
	// BarmanCloudObjectStoreKind [CUSTOM CNPG] là CRD mô tả đích lưu trữ backup của plugin.
	BarmanCloudObjectStoreKind = "ObjectStore"
	// BarmanCloudObjectStoreCRDName [CUSTOM CNPG] dùng để kiểm tra plugin đã được cài hay chưa.
	BarmanCloudObjectStoreCRDName = "objectstores.barmancloud.cnpg.io"
	// BarmanCloudPluginName [CUSTOM CNPG] là tên plugin mà CNPG dùng để tra Service có nhãn
	// "cnpg.io/pluginName". Phải khớp đúng tên plugin đã cài trong namespace cnpg-system.
	BarmanCloudPluginName = "barman-cloud.cloudnative-pg.io"
	// CNPGClusterImageCatalogName [CUSTOM CNPG] là TÊN QUY ƯỚC của ClusterImageCatalog mà Everest
	// tra để biết operand image nào được nền tảng duyệt. Catalog do platform team quản qua GitOps
	// (xem demo/k8s/argocd/01-image-catalog.yaml); Everest chỉ đọc, không bao giờ tự tạo hay sửa.
	// Đổi tên ở manifest thì phải đổi hằng số này.
	CNPGClusterImageCatalogName = "everest-postgresql"
	// PodMonitorCRDName [CUSTOM CNPG] là CRD của Prometheus Operator. Chỉ khi CRD này tồn tại
	// trên cụm K8s, CNPG provider mới bật "spec.monitoring.enablePodMonitor" cho Cluster —
	// tránh lỗi reconcile khi lab chưa cài Prometheus Operator (xem PLAN.md Phase 8).
	PodMonitorCRDName = "podmonitors.monitoring.coreos.com"

	// PXCAPIGroup is the API group for Percona XtraDB Cluster.
	PXCAPIGroup = "pxc.percona.com"
	// PSMDBAPIGroup is the API group for Percona Server for MongoDB.
	PSMDBAPIGroup = "psmdb.percona.com"
	// PGAPIGroup is the API group for Percona PostgreSQL.
	PGAPIGroup = "pgv2.percona.com"
	// CNPGAPIGroup [CUSTOM CNPG] API Group chính thức của CloudNativePG ("postgresql.cnpg.io").
	CNPGAPIGroup = "postgresql.cnpg.io"

	// PerconaXtraDBClusterKind is the kind for Percona XtraDB Cluster.
	PerconaXtraDBClusterKind = "PerconaXtraDBCluster"
	// PerconaServerMongoDBKind is the kind for Percona Server for MongoDB.
	PerconaServerMongoDBKind = "PerconaServerMongoDB"
	// PerconaPGClusterKind is the kind for Percona PostgreSQL.
	PerconaPGClusterKind = "PerconaPGCluster"
	// CNPGClusterKind [CUSTOM CNPG] là CR Kind cho CloudNativePG Cluster.
	CNPGClusterKind = "Cluster"
	// CNPGBackupKind is the Kind for CNPG Backup CRD.
	CNPGBackupKind = "Backup"
	// CNPGScheduledBackupKind is the Kind for CNPG ScheduledBackup CRD.
	CNPGScheduledBackupKind = "ScheduledBackup"
	// CNPGPublicationKind [CUSTOM CNPG] là CRD logical replication của CloudNativePG.
	CNPGPublicationKind = "Publication"
	// CNPGSubscriptionKind [CUSTOM CNPG] là CRD logical replication Subscription của CloudNativePG.
	CNPGSubscriptionKind = "Subscription"
	// CNPGPoolerKind [CUSTOM CNPG] là CRD connection pooler (PgBouncer) của CloudNativePG.
	CNPGPoolerKind = "Pooler"
	// CNPGPoolerNameLabel [CUSTOM CNPG] là nhãn CNPG gắn lên pod của Pooler, dùng cho PDB selector.
	CNPGPoolerNameLabel = "cnpg.io/poolerName"
	// PerconaXtraDBClusterRestoreKind is the kind for Percona XtraDB Cluster restore.
	PerconaXtraDBClusterRestoreKind = "PerconaXtraDBClusterRestore"
	// LoadBalancerConfigKind is the kind for load balancer configs.
	LoadBalancerConfigKind = "LoadBalancerConfig"

	// DatabaseClusterKind is the kind for DatabaseClusterKind.
	DatabaseClusterKind = "DatabaseCluster"

	// Engine Features.

	// SplitHorizonDNSConfigKind is the kind for SplitHorizonDNSConfig.
	SplitHorizonDNSConfigKind = "SplitHorizonDNSConfig"

	// ClusterTypeEKS represents the EKS cluster type.
	ClusterTypeEKS ClusterType = "eks"
	// ClusterTypeMinikube represents the Minikube cluster type.
	ClusterTypeMinikube ClusterType = "minikube"

	// LabelKubernetesManagedBy is a common label that indicates the resource is managed by a specific operator.
	LabelKubernetesManagedBy = "app.kubernetes.io/managed-by"

	// EverestSecretsPrefix is the prefix for secrets created by Everest.
	EverestSecretsPrefix = "everest-secrets-"
)
