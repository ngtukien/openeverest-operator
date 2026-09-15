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

// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

package cnpg

import (
	"errors"
	"fmt"
	"strings"

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
	PublicationGVK  = schema.GroupVersionKind{Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGPublicationKind}
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
			desiredPublications[a.DB.Name+"-"+pub.Name] = struct{}{}
		}
		for _, sub := range replication.Subscriptions {
			if err := a.reconcileSubscription(sub); err != nil {
				return fmt.Errorf("reconcile Subscription %q: %w", sub.Name, err)
			}
			desiredSubscriptions[a.DB.Name+"-"+sub.Name] = struct{}{}
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

	object := newUnstructured(PublicationGVK, a.DB.Namespace, a.DB.Name+"-"+pub.Name)
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
	passwordKey := source.PasswordSecretKey
	if passwordKey == "" {
		passwordKey = "password"
	}
	externalClusterName := sub.Name + "-source"
	externalCluster := map[string]any{
		"name": externalClusterName,
		"connectionParameters": map[string]any{
			"host":    source.Host,
			"port":    fmt.Sprintf("%d", port),
			"user":    source.User,
			"dbname":  source.DBName,
			"sslmode": sslMode,
		},
		"password": map[string]any{"name": source.PasswordSecretName, "key": passwordKey},
	}
	if err := mergeExternalCluster(a.Object, externalCluster); err != nil {
		return err
	}

	object := newUnstructured(SubscriptionGVK, a.DB.Namespace, a.DB.Name+"-"+sub.Name)
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

// mergeExternalCluster inserts or replaces a spec.externalClusters entry by name, without
// clobbering entries DataSource() may have already set in the same reconcile pass.
func mergeExternalCluster(object map[string]any, entry map[string]any) error {
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
	if err := a.C.List(a.ctx, list,
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
