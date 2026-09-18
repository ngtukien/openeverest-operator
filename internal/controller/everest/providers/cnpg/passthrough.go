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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// OwnedSpecPaths are the CloudNativePG Cluster spec paths Everest always generates from
// spec.engine. The spec.cnpg field may not set them: a second source of truth for instance count, image
// or storage is exactly what makes a DBaaS drift. The value names the Everest field to use.
var OwnedSpecPaths = []struct {
	Path        []string
	EverestPath string
}{
	{Path: []string{fieldInstances}, EverestPath: "spec.engine.replicas"},
	{Path: []string{"imageName"}, EverestPath: "spec.engine.version"},
	{Path: []string{"imageCatalogRef"}, EverestPath: "spec.engine.version"},
	{Path: []string{fieldStorage, "size"}, EverestPath: "spec.engine.storage.size"},
	{Path: []string{fieldStorage, "storageClass"}, EverestPath: "spec.engine.storage.class"},
	// CNPG lets storage.size/storageClass override these and only emits an admission warning,
	// which Everest's client drops: the value would be accepted and silently ignored.
	{Path: []string{fieldStorage, "pvcTemplate", fieldResources, "requests", fieldStorage}, EverestPath: "spec.engine.storage.size"},
	{Path: []string{fieldStorage, "pvcTemplate", "storageClassName"}, EverestPath: "spec.engine.storage.class"},
	{Path: []string{fieldResources}, EverestPath: "spec.engine.resources"},
	// In-tree Barman: CNPG rejects it next to the plugin WAL archiver Everest configures, and it
	// cannot run on the `standard` operand images of the ClusterImageCatalog (see objectstore.go).
	{Path: []string{"backup", "barmanObjectStore"}, EverestPath: "spec.backup.schedules and the BackupStorage's spec.objectStore"},
}

// OwnedPluginParameters returns the spec.cnpg paths where the user sets a Barman Cloud Plugin
// parameter Everest owns. The barmanObjectName parameter picks the BackupStorage's shared
// ObjectStore and serverName is the only boundary between clusters inside it, so a user-chosen value could write
// WAL into another cluster's folder. This holds even when Everest generates no plugin entry
// (backups disabled), which is why it is checked separately from the merge.
func OwnedPluginParameters(user map[string]any) []string {
	plugins, _ := user["plugins"].([]any)
	var paths []string
	for _, raw := range plugins {
		entry, _ := raw.(map[string]any)
		if name, _ := entry[fieldName].(string); name != consts.BarmanCloudPluginName {
			continue
		}
		parameters, _ := entry[fieldParameters].(map[string]any)
		for _, key := range []string{parameterBarmanObjectName, parameterServerName} {
			if _, found := parameters[key]; found {
				paths = append(paths, fmt.Sprintf("plugins[name=%s].parameters.%s", consts.BarmanCloudPluginName, key))
			}
		}
	}
	return paths
}

// Compile-time check: the controller only runs Passthrough() through this optional interface, so a
// renamed method would otherwise skip spec.cnpg silently instead of failing the build.
var _ everestv1alpha1.PassthroughApplier = (*applier)(nil)

// [CUSTOM CNPG] Passthrough gộp spec.cnpg — một đoạn Cluster.spec viết y như manifest CNPG — vào
// Cluster mà các bước trước của pipeline vừa dựng (PLAN.md Phase 12).
//
// Chạy SAU CÙNG để thấy toàn bộ phần Everest sinh ra. Everest chỉ là trung gian: mọi cơ chế
// CNPG tự reconcile được (synchronous, managed.roles, replica, replicationSlots và các field khác) đi thẳng
// xuống mà không cần Everest dịch lại từng field.
//
// Không có "ai thắng": nếu cả hai cùng đặt một field với giá trị khác nhau thì reconcile lỗi và
// nêu đúng path. Lặng lẽ để một bên đè bên kia chính là kiểu lỗi cụm lên xanh mà cấu hình không
// được áp.
func (a *applier) Passthrough() error {
	user, err := a.DB.Spec.CNPGSpec()
	if err != nil || user == nil {
		return err
	}
	for _, owned := range OwnedSpecPaths {
		if _, found := nestedValue(user, owned.Path); found {
			return fmt.Errorf("spec.cnpg.%s is owned by Everest; set %s instead",
				strings.Join(owned.Path, "."), owned.EverestPath)
		}
	}
	if paths := OwnedPluginParameters(user); len(paths) > 0 {
		return fmt.Errorf("spec.cnpg.%s is owned by Everest; it is generated from spec.backup and the BackupStorage", paths[0])
	}
	generated, ok := a.Object["spec"].(map[string]any)
	if !ok {
		generated = map[string]any{}
	}
	if err := mergeSpecMap(generated, user, "spec.cnpg"); err != nil {
		return err
	}
	a.Object["spec"] = generated
	return nil
}

// mergeSpecMap merges src into dst in place. Maps merge recursively; lists whose entries all
// carry a "name" (externalClusters, managed.roles, plugins) merge entry by entry; any other
// pair of values must be equal.
func mergeSpecMap(dst, src map[string]any, path string) error {
	for key, srcValue := range src {
		childPath := path + "." + key
		dstValue, exists := dst[key]
		if !exists {
			dst[key] = srcValue
			continue
		}
		merged, err := mergeSpecValue(dstValue, srcValue, childPath)
		if err != nil {
			return err
		}
		dst[key] = merged
	}
	return nil
}

func mergeSpecValue(dstValue, srcValue any, path string) (any, error) {
	switch dst := dstValue.(type) {
	case map[string]any:
		if src, ok := srcValue.(map[string]any); ok {
			return dst, mergeSpecMap(dst, src, path)
		}
	case []any:
		if src, ok := srcValue.([]any); ok && namedList(dst) && namedList(src) {
			return mergeNamedList(dst, src, path)
		}
	}
	if reflect.DeepEqual(dstValue, srcValue) {
		return dstValue, nil
	}
	return nil, fmt.Errorf(
		"%s: value %s conflicts with %s generated by Everest from the DatabaseCluster spec; "+
			"remove it from spec.cnpg or change the Everest field that produces it",
		path, compactJSON(srcValue), compactJSON(dstValue),
	)
}

func mergeNamedList(dst, src []any, path string) ([]any, error) {
	for _, raw := range src {
		entry, _ := raw.(map[string]any)
		name, _ := entry[fieldName].(string)
		entryPath := fmt.Sprintf("%s[name=%s]", path, name)
		index := -1
		for i := range dst {
			if existing, _ := dst[i].(map[string]any); existing[fieldName] == name {
				index = i
				break
			}
		}
		if index < 0 {
			dst = append(dst, entry)
			continue
		}
		existing, _ := dst[index].(map[string]any)
		if err := mergeSpecMap(existing, entry, entryPath); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// namedList reports whether every entry is an object with a non-empty "name". An empty list
// qualifies, so a generated [] still accepts named user entries.
func namedList(list []any) bool {
	for _, raw := range list {
		entry, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		if name, _ := entry[fieldName].(string); name == "" {
			return false
		}
	}
	return true
}

func nestedValue(object map[string]any, path []string) (any, bool) {
	var current any = object
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		if current, ok = m[key]; !ok {
			return nil, false
		}
	}
	return current, true
}

func compactJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(data)
}
