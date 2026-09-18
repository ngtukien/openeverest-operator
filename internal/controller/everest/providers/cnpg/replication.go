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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// ReplicationOwnerLabel names the Everest DatabaseCluster that owns a Publication/Subscription,
// used to prune objects for replication entries removed from spec.replication.
const ReplicationOwnerLabel = "everest.percona.com/db-cluster-name"

var (
	// PublicationGVK is the GroupVersionKind for CloudNativePG Publication CRD.
	PublicationGVK = schema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGPublicationKind}
	// SubscriptionGVK is the GroupVersionKind for CloudNativePG Subscription CRD.
	SubscriptionGVK = schema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGSubscriptionKind}
)

// [CUSTOM CNPG] Replication: đồng bộ Publication/Subscription logical replication cho CNPG Cluster
// theo spec.replication của Everest DatabaseCluster (PLAN.md Phase 10):
//   - Publication: sinh CRD "Publication" (postgresql.cnpg.io/v1), sở hữu bởi cluster hiện tại.
//   - Subscription: tự sinh một entry "spec.externalClusters" tên "<subscription-name>-source" từ
//     Source (không đụng tới các externalClusters do DataSource() sinh cho restore/PITR), rồi tạo
//     CRD "Subscription" tương ứng trỏ vào entry đó.
//
// Publication/Subscription không còn khai báo trong spec sẽ bị xóa khỏi cluster (prune), giống cách
// Backup() xóa ScheduledBackup khi schedule.Enabled=false.
func (a *applier) Replication() error {
	replication := a.DB.Spec.Replication
	desiredPublications := map[string]struct{}{}
	desiredSubscriptions := map[string]struct{}{}

	if replication != nil {
		for _, pub := range replication.Publications {
			if err := a.reconcilePublication(pub); err != nil {
				return fmt.Errorf("reconcile Publication %q: %w", pub.Name, err)
			}
			desiredPublications[replicationObjectName(a.DB.Name, pub.Name)] = struct{}{}
		}
		for _, sub := range replication.Subscriptions {
			if err := a.reconcileSubscription(sub); err != nil {
				return fmt.Errorf("reconcile Subscription %q: %w", sub.Name, err)
			}
			desiredSubscriptions[replicationObjectName(a.DB.Name, sub.Name)] = struct{}{}
		}
	}

	if err := a.pruneReplicationObjects(PublicationGVK, desiredPublications); err != nil {
		return fmt.Errorf("prune Publications: %w", err)
	}
	if err := a.pruneReplicationObjects(SubscriptionGVK, desiredSubscriptions); err != nil {
		return fmt.Errorf("prune Subscriptions: %w", err)
	}
	return nil
}

// subscriptionNamePattern là luật tên replication slot của PostgreSQL. CREATE SUBSCRIPTION mặc định tạo
// slot trên publisher TRÙNG TÊN subscription, nên tên subscription phải hợp lệ làm tên slot — quote
// identifier chỉ cứu được tên subscription, không cứu được tên slot. Tối đa NAMEDATALEN-1 byte.
var subscriptionNamePattern = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// ValidateSubscriptionName báo lỗi khi tên subscription không dùng được làm tên replication slot.
// Webhook gọi để chặn lúc apply; reconcile gọi lại vì webhook có thể đang tắt.
func ValidateSubscriptionName(name string) error {
	if !subscriptionNamePattern.MatchString(name) {
		return fmt.Errorf("subscription name %q must contain only lowercase letters, digits and underscores "+
			"(max 63): PostgreSQL uses it as the replication slot name on the publisher", name)
	}
	return nil
}

// k8sName đổi tên PostgreSQL (thường có gạch dưới) thành dạng hợp lệ cho tên object Kubernetes.
func k8sName(pgName string) string {
	return strings.ReplaceAll(strings.ToLower(pgName), "_", "-")
}

// replicationObjectName là tên CR Publication/Subscription: "<cụm>-<tên>". Tên PostgreSQL giữ
// nguyên trong spec.name; chỉ tên object Kubernetes (DNS subdomain, không nhận gạch dưới) bị đổi.
func replicationObjectName(dbName, pgName string) string {
	return dbName + "-" + k8sName(pgName)
}

func (a *applier) reconcilePublication(pub everestv1alpha1.ReplicationPublication) error {
	if pub.Name == "" || pub.DBName == "" {
		return errors.New("publication name and dbName are required")
	}
	target := map[string]any{"allTables": true}
	if !pub.Target.AllTables && len(pub.Target.Tables) != 0 {
		objects := make([]any, 0, len(pub.Target.Tables))
		for _, t := range pub.Target.Tables {
			schemaName, table, ok := strings.Cut(t, ".")
			if !ok {
				return fmt.Errorf("table %q must be in \"schema.table\" form", t)
			}
			objects = append(objects, map[string]any{
				"table": map[string]any{"schema": schemaName, "name": table},
			})
		}
		target = map[string]any{"objects": objects}
	}

	object := newUnstructured(PublicationGVK, a.DB.Namespace, replicationObjectName(a.DB.Name, pub.Name))
	if err := a.deleteIfImmutableChanged(object, pub.Name, pub.DBName); err != nil {
		return err
	}
	_, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		object.SetLabels(map[string]string{ReplicationOwnerLabel: a.DB.Name})
		object.Object["spec"] = map[string]any{
			"cluster": map[string]any{"name": a.DB.Name},
			"dbname":  pub.DBName,
			"name":    pub.Name,
			"target":  target,
		}
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	})
	return err
}

func (a *applier) reconcileSubscription(sub everestv1alpha1.ReplicationSubscription) error {
	if sub.Name == "" || sub.DBName == "" || sub.PublicationName == "" {
		return errors.New("subscription name, dbName and publicationName are required")
	}
	if err := ValidateSubscriptionName(sub.Name); err != nil {
		return err
	}
	source := sub.Source
	if source.Host == "" || source.DBName == "" || source.User == "" || source.PasswordSecretName == "" {
		return errors.New("source.host, source.dbName, source.user and source.passwordSecretName are required")
	}
	port := source.Port
	if port == 0 {
		port = 5432
	}
	sslMode := source.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	secretKey := source.PasswordSecretKey
	if secretKey == "" {
		secretKey = corev1.BasicAuthPasswordKey
	}
	externalClusterName := k8sName(sub.Name) + "-source"
	externalCluster := map[string]any{
		"name": externalClusterName,
		"connectionParameters": map[string]any{
			"host":    source.Host,
			"port":    strconv.Itoa(int(port)),
			"user":    source.User,
			"dbname":  source.DBName,
			"sslmode": sslMode,
		},
		"password": map[string]any{"name": source.PasswordSecretName, "key": secretKey},
	}
	if err := mergeExternalCluster(a.Object, externalCluster); err != nil {
		return err
	}

	object := newUnstructured(SubscriptionGVK, a.DB.Namespace, replicationObjectName(a.DB.Name, sub.Name))
	if err := a.deleteIfImmutableChanged(object, sub.Name, sub.DBName); err != nil {
		return err
	}
	_, err := controllerutil.CreateOrUpdate(a.ctx, a.C, object, func() error {
		object.SetLabels(map[string]string{ReplicationOwnerLabel: a.DB.Name})
		object.Object["spec"] = map[string]any{
			"cluster":             map[string]any{"name": a.DB.Name},
			"dbname":              sub.DBName,
			"name":                sub.Name,
			"externalClusterName": externalClusterName,
			"publicationName":     sub.PublicationName,
		}
		return controllerutil.SetControllerReference(a.DB, object, a.C.Scheme())
	})
	return err
}

// errReplicationObjectRecreating báo CR đang được xoá để tạo lại; vòng reconcile sau sẽ tạo mới.
var errReplicationObjectRecreating = errors.New("recreating because an immutable field changed; retrying")

// deleteIfImmutableChanged xoá CR Publication/Subscription khi spec.name hoặc spec.dbname — hai field
// CNPG khoá bằng CEL "self == oldSelf" — khác giá trị mong muốn. Update thẳng sẽ bị API server từ
// chối mãi. Ví dụ: đổi "trove-sub" (tên slot không hợp lệ) thành "trove_sub" cho ra CÙNG tên CR.
//
// Xoá CR không xoá object trong PostgreSQL khi reclaim policy là retain (mặc định của CNPG).
func (a *applier) deleteIfImmutableChanged(object *unstructured.Unstructured, pgName, dbName string) error {
	existing := newUnstructured(object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	err := a.C.Get(a.ctx, client.ObjectKeyFromObject(existing), existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !existing.GetDeletionTimestamp().IsZero() {
		return fmt.Errorf("%s %q: %w", object.GetKind(), object.GetName(), errReplicationObjectRecreating)
	}
	currentName, _, _ := unstructured.NestedString(existing.Object, "spec", "name")
	currentDB, _, _ := unstructured.NestedString(existing.Object, "spec", "dbname")
	if currentName == pgName && currentDB == dbName {
		return nil
	}
	if err := a.C.Delete(a.ctx, existing); client.IgnoreNotFound(err) != nil {
		return err
	}
	return fmt.Errorf("%s %q: %w", object.GetKind(), object.GetName(), errReplicationObjectRecreating)
}

// mergeExternalCluster inserts or replaces a spec.externalClusters entry by name, without
// clobbering entries DataSource() may have already set in the same reconcile pass.
func mergeExternalCluster(object, entry map[string]any) error {
	existing, _, err := unstructured.NestedSlice(object, "spec", "externalClusters")
	if err != nil {
		return fmt.Errorf("read spec.externalClusters: %w", err)
	}
	name, _ := entry["name"].(string)
	replaced := false
	for i, raw := range existing {
		item, ok := raw.(map[string]any)
		if !ok || item["name"] != name {
			continue
		}
		existing[i] = entry
		replaced = true
		break
	}
	if !replaced {
		existing = append(existing, entry)
	}
	return unstructured.SetNestedSlice(object, existing, "spec", "externalClusters")
}

// pruneReplicationObjects deletes Publication/Subscription objects owned by this DatabaseCluster
// that are no longer declared in spec.replication.
func (a *applier) pruneReplicationObjects(gvk schema.GroupVersionKind, desired map[string]struct{}) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)
	if err := a.C.List(
		a.ctx, list,
		client.InNamespace(a.DB.Namespace),
		client.MatchingLabels{ReplicationOwnerLabel: a.DB.Name},
	); err != nil {
		return err
	}
	for i := range list.Items {
		item := &list.Items[i]
		if _, ok := desired[item.GetName()]; ok {
			continue
		}
		if err := a.C.Delete(a.ctx, item); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}
