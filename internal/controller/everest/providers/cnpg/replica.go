// everest-operator
// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

package cnpg

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// ReplicaSourceSuffix names the spec.externalClusters entry Everest generates for a replica
// cluster. It is distinct from the "<subscription>-source" entries Phase 10 generates and from
// the entry DataSource() generates for restore/PITR, so the three never collide.
const ReplicaSourceSuffix = "-replica-source"

// Defaults for a CloudNativePG replica source. "streaming_replica" is the replication role
// CloudNativePG creates on every cluster; "-replication" and "-ca" are the Secrets it generates
// to authenticate that role.
const (
	defaultReplicaUser       = "streaming_replica"
	defaultReplicaDBName     = "postgres"
	defaultReplicaPort       = 5432
	replicationSecretSuffix  = "-replication"
	caSecretSuffix           = "-ca"
	certAuthSSLMode          = "verify-full"
	passwordAuthSSLMode      = "prefer"
	defaultPasswordSecretKey = "password"
)

// [CUSTOM CNPG] ReplicaCluster: dựng cụm này thành bản sao (standby cluster) của một cụm
// PostgreSQL primary khác theo spec.replica của Everest DatabaseCluster (PLAN.md Phase 11):
//   - spec.externalClusters: một entry "<source-cluster>-replica-source" mô tả cách kết nối tới
//     primary (mặc định xác thực bằng client certificate mà CNPG sinh sẵn trên cụm nguồn).
//   - spec.bootstrap.pg_basebackup: clone dữ liệu ban đầu từ entry đó.
//   - spec.replica.enabled: giữ cụm ở chế độ chỉ đọc và replay WAL liên tục. Gạt cờ này về false
//     là thao tác promote khi Zone A gặp sự cố.
//
// Chạy sau DataSource() vì cả hai cùng ghi spec.bootstrap: bootstrap của replica cluster phải là
// cái còn lại. Hai trường này loại trừ nhau, và webhook chặn ngay từ admission.
//
// Lưu ý: engine.userSecretsName bị bỏ qua với replica cluster — cụm bản sao kế thừa toàn bộ role
// và password từ primary qua pg_basebackup, không chạy initdb.
func (a *applier) ReplicaCluster() error {
	replica := a.DB.Spec.Replica
	if replica == nil {
		return nil
	}
	if a.DB.Spec.DataSource != nil {
		return errors.New("CloudNativePG cannot bootstrap from both .spec.dataSource and .spec.replica; pick one")
	}

	source := replica.Source
	if source.ClusterName == "" {
		return errors.New("replica.source.clusterName is required")
	}
	namespace := source.Namespace
	if namespace == "" {
		namespace = a.DB.Namespace
	}
	host := source.Host
	if host == "" {
		host = fmt.Sprintf("%s-rw.%s.svc", source.ClusterName, namespace)
	}
	port := source.Port
	if port == 0 {
		port = defaultReplicaPort
	}
	dbName := source.DBName
	if dbName == "" {
		dbName = defaultReplicaDBName
	}
	user := source.User
	if user == "" {
		user = defaultReplicaUser
	}

	externalClusterName := source.ClusterName + ReplicaSourceSuffix
	entry := map[string]any{
		"name": externalClusterName,
		"connectionParameters": map[string]any{
			"host":   host,
			"port":   fmt.Sprintf("%d", port),
			"user":   user,
			"dbname": dbName,
		},
	}
	connectionParameters, ok := entry["connectionParameters"].(map[string]any)
	if !ok { // unreachable: the literal above is a map[string]any.
		return errors.New("internal error: replica connectionParameters is not a map")
	}

	sslMode := source.SSLMode
	if source.PasswordSecretName != "" {
		if sslMode == "" {
			sslMode = passwordAuthSSLMode
		}
		passwordKey := source.PasswordSecretKey
		if passwordKey == "" {
			passwordKey = defaultPasswordSecretKey
		}
		entry["password"] = map[string]any{"name": source.PasswordSecretName, "key": passwordKey}
	} else {
		if sslMode == "" {
			sslMode = certAuthSSLMode
		}
		clientCertSecret := source.ClientCertSecretName
		if clientCertSecret == "" {
			clientCertSecret = source.ClusterName + replicationSecretSuffix
		}
		caSecret := source.CASecretName
		if caSecret == "" {
			caSecret = source.ClusterName + caSecretSuffix
		}
		entry["sslKey"] = map[string]any{"name": clientCertSecret, "key": "tls.key"}
		entry["sslCert"] = map[string]any{"name": clientCertSecret, "key": "tls.crt"}
		entry["sslRootCert"] = map[string]any{"name": caSecret, "key": "ca.crt"}
	}
	connectionParameters["sslmode"] = sslMode

	if err := mergeExternalCluster(a.Object, entry); err != nil {
		return fmt.Errorf("merge replica externalCluster %q: %w", externalClusterName, err)
	}
	if err := unstructured.SetNestedMap(a.Object, map[string]any{
		"pg_basebackup": map[string]any{"source": externalClusterName},
	}, "spec", "bootstrap"); err != nil {
		return fmt.Errorf("set replica bootstrap: %w", err)
	}
	return unstructured.SetNestedMap(a.Object, map[string]any{
		"enabled": replica.Enabled,
		"source":  externalClusterName,
	}, "spec", "replica")
}

// [CUSTOM CNPG] replicaStatus đọc trạng thái bản sao từ chính CNPG Cluster (PLAN.md Phase 11).
//
// Trả về nil khi cụm không phải replica cluster, để .status.replica chỉ xuất hiện đúng lúc có ý
// nghĩa. Không có trường lag theo byte: CloudNativePG không ghi LSN vào Cluster.status, nên độ trễ
// chỉ đo được qua metric "cnpg_pg_replication_lag" (Phase 8) hoặc truy vấn pg_wal_lsn_diff trên
// designated primary — xem demo/k8s/everest-dr/README.md.
func (p *Provider) replicaStatus() *everestv1alpha1.ReplicaClusterStatus {
	sourceCluster, found, err := unstructured.NestedString(p.Object, "spec", "replica", "source")
	if err != nil || !found || sourceCluster == "" {
		return nil
	}
	enabled, _, err := unstructured.NestedBool(p.Object, "spec", "replica", "enabled")
	if err != nil {
		return nil
	}
	status := &everestv1alpha1.ReplicaClusterStatus{
		Enabled:       enabled,
		SourceCluster: sourceCluster,
	}
	status.DesignatedPrimary, _, _ = unstructured.NestedString(p.Object, "status", "currentPrimary")
	status.SourceHost = externalClusterHost(p.Object, sourceCluster)
	return status
}

// externalClusterHost returns the connection host of a spec.externalClusters entry by name.
func externalClusterHost(object map[string]any, name string) string {
	entries, _, err := unstructured.NestedSlice(object, "spec", "externalClusters")
	if err != nil {
		return ""
	}
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok || entry["name"] != name {
			continue
		}
		host, _, _ := unstructured.NestedString(entry, "connectionParameters", "host")
		return host
	}
	return ""
}
