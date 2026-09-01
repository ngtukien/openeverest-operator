// everest-operator
// Copyright (C) 2022 Percona LLC
// SPDX-License-Identifier: Apache-2.0

package cnpg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
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
	a.Object["spec"] = map[string]any{}
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
	a.Object["spec"] = spec
	return nil
}

func (a *applier) EngineFeatures() error {
	if a.DB.Spec.EngineFeatures != nil {
		return errors.New("engineFeatures are not supported by the CloudNativePG provider")
	}
	return nil
}

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
	affinity, err := runtime.DefaultUnstructuredConverter.ToUnstructured(policy.Spec.AffinityConfig.PostgreSQL.Engine)
	if err != nil {
		return fmt.Errorf("convert PostgreSQL affinity: %w", err)
	}
	return unstructured.SetNestedMap(a.Object, affinity, "spec", "affinity")
}

func (a *applier) Backup() error {
	if len(a.DB.Spec.Backup.Schedules) != 0 || a.DB.Spec.Backup.PITR.Enabled {
		return errors.New("backup schedules and PITR are not yet supported by the CloudNativePG provider")
	}
	return nil
}

func (a *applier) DataSource() error {
	if a.DB.Spec.DataSource != nil {
		return errors.New("restore bootstrap is not yet supported by the CloudNativePG provider")
	}
	return nil
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
