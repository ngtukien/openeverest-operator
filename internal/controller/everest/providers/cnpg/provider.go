// everest-operator
// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

// Package cnpg maps Everest DatabaseCluster resources to CloudNativePG Clusters.
package cnpg

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// Status maps CloudNativePG status and conditions into Everest's stable status model.
func (p *Provider) Status(context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
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
