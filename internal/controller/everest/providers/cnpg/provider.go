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

// Package cnpg maps Everest DatabaseCluster resources to CloudNativePG Clusters.
package cnpg

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

var clusterGVK = schema.GroupVersionKind{
	Group: consts.CNPGAPIGroup, Version: "v1", Kind: consts.CNPGClusterKind,
}

// Provider reconciles a CloudNativePG Cluster for an Everest DatabaseCluster.
// Unstructured is intentional: Everest can consume the installed CNPG v1 CRD
// without coupling its dependency graph to the CNPG operator binary.
type Provider struct {
	*unstructured.Unstructured
	providers.ProviderOptions
}

// New returns a CloudNativePG provider.
func New(ctx context.Context, opts providers.ProviderOptions) (*Provider, error) {
	cluster := &unstructured.Unstructured{Object: map[string]any{}}
	cluster.SetGroupVersionKind(clusterGVK)
	err := opts.C.Get(ctx, types.NamespacedName{
		Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace(),
	}, cluster)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	version := opts.DB.Spec.Engine.Version
	opts.DBEngine = &everestv1alpha1.DatabaseEngine{}
	if version != "" {
		opts.DBEngine.Status.AvailableVersions.Engine = everestv1alpha1.ComponentsMap{
			version: {
				ImagePath: fmt.Sprintf("ghcr.io/cloudnative-pg/postgresql:%s", version),
				Status:    everestv1alpha1.DBEngineComponentAvailable,
			},
		}
	}
	return &Provider{Unstructured: cluster, ProviderOptions: opts}, nil
}

// Apply returns the CloudNativePG applier.
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{Provider: p, ctx: ctx}
}

// RunPreReconcileHook implements the provider hook contract.
func (p *Provider) RunPreReconcileHook(context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

// DBObject returns the upstream CloudNativePG Cluster.
func (p *Provider) DBObject() client.Object {
	p.SetGroupVersionKind(clusterGVK)
	return p.Unstructured
}

// [CUSTOM CNPG] podMonitorCRDInstalled kiểm tra CRD "podmonitors.monitoring.coreos.com" của
// Prometheus Operator có tồn tại trên cụm K8s hay không (Dynamic Discovery, cùng kiểu với
// ReconcileWatchers cho CRD "clusters.postgresql.cnpg.io" — xem PLAN.md Phase 8). Nếu CRD chưa
// cài, provider bỏ qua an toàn: không bật enablePodMonitor để tránh lỗi reconcile CNPG Cluster.
func podMonitorCRDInstalled(ctx context.Context, c client.Client) (bool, error) {
	if c == nil {
		return false, nil
	}
	crd := &unstructured.Unstructured{Object: map[string]any{}}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	err := c.Get(ctx, types.NamespacedName{Name: consts.PodMonitorCRDName}, crd)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Cleanup lets Kubernetes garbage collection delete the owned CNPG Cluster.
func (p *Provider) Cleanup(ctx context.Context, db *everestv1alpha1.DatabaseCluster) (bool, error) {
	if controllerutil.ContainsFinalizer(db, consts.DBBackupCleanupFinalizer) {
		if done, err := common.DeleteBackupsForDatabase(ctx, p.C, db.GetName(), db.GetNamespace()); err != nil || !done {
			return done, err
		}
		if done, err := common.DeleteRestoresForDatabase(ctx, p.C, db.GetName(), db.GetNamespace()); err != nil || !done {
			return done, err
		}
		controllerutil.RemoveFinalizer(db, consts.DBBackupCleanupFinalizer)
		if err := p.C.Update(ctx, db); err != nil {
			return false, err
		}
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, db, p.DBObject())
}

// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

type pvcResizeStatus struct {
	resizing      bool
	failed        bool
	failureReason string
}

// getPVCResizeStatus inspects CNPG PVC conditions and capacities. PVC
// conditions can be short-lived, so comparing requested storage with the
// reported capacity prevents Everest from missing an in-progress expansion.
func getPVCResizeStatus(ctx context.Context, c client.Client, name, namespace string) (pvcResizeStatus, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{"cnpg.io/cluster": name}); err != nil {
		return pvcResizeStatus{}, fmt.Errorf("failed to list CloudNativePG PVCs: %w", err)
	}
	result := pvcResizeStatus{}
	for _, pvc := range pvcList.Items {
		requested, hasRequest := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		capacity, hasCapacity := pvc.Status.Capacity[corev1.ResourceStorage]
		if hasRequest && hasCapacity && !capacity.IsZero() && requested.Cmp(capacity) > 0 {
			result.resizing = true
		}
		for _, condition := range pvc.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case corev1.PersistentVolumeClaimResizing, corev1.PersistentVolumeClaimFileSystemResizePending:
				result.resizing = true
			case corev1.PersistentVolumeClaimControllerResizeError, corev1.PersistentVolumeClaimNodeResizeError:
				result.resizing = true
				result.failed = true
				result.failureReason = condition.Message
			}
		}
	}
	return result, nil
}

// Status maps CloudNativePG status, PVC resize progress, and resize failures
// into Everest's stable status model.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.DB.Status
	status.Port = 5432
	status.Hostname = fmt.Sprintf("%s-rw.%s.svc", p.DB.GetName(), p.DB.GetNamespace())
	status.CRVersion = "v1"

	desired, _, _ := unstructured.NestedInt64(p.Object, "spec", "instances")
	ready, _, _ := unstructured.NestedInt64(p.Object, "status", "readyInstances")
	status.Size = int32(desired) //nolint:gosec -- CNPG instance counts are bounded by its CRD.
	status.Ready = int32(ready)  //nolint:gosec -- CNPG instance counts are bounded by its CRD.

	conditions, _, err := unstructured.NestedSlice(p.Object, "status", "conditions")
	if err != nil {
		return status, false, err
	}
	status.Status = everestv1alpha1.AppStateCreating
	status.Message = "waiting for CloudNativePG Cluster to become ready"
	readyCondition := false
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok || condition["type"] != "Ready" {
			continue
		}
		status.Message, _ = condition["message"].(string)
		readyCondition = condition["status"] == "True"
		break
	}
	if readyCondition && desired > 0 && ready == desired {
		status.Status = everestv1alpha1.AppStateReady
	}

	if p.C != nil {
		resizeStatus, resizeErr := getPVCResizeStatus(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace())
		if resizeErr != nil {
			return status, false, resizeErr
		}
		meta.RemoveStatusCondition(&status.Conditions, everestv1alpha1.ConditionTypeVolumeResizeFailed)
		if resizeStatus.resizing {
			status.Status = everestv1alpha1.AppStateResizingVolumes
			if resizeStatus.failed {
				meta.SetStatusCondition(&status.Conditions, metav1.Condition{
					Type:               everestv1alpha1.ConditionTypeVolumeResizeFailed,
					Status:             metav1.ConditionTrue,
					Reason:             everestv1alpha1.ReasonVolumeResizeFailed,
					Message:            resizeStatus.failureReason,
					LastTransitionTime: metav1.Now(),
					ObservedGeneration: p.DB.GetGeneration(),
				})
			}
		}
	}

	status.Replica = p.replicaStatus()

	if rawStatus, found, nestedErr := unstructured.NestedMap(p.Object, "status"); nestedErr != nil {
		return status, false, nestedErr
	} else if found {
		data, marshalErr := json.Marshal(rawStatus)
		if marshalErr != nil {
			return status, false, marshalErr
		}
		status.Details = string(data)
	}
	return status, true, nil
}
