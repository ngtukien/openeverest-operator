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

package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

// [CUSTOM CNPG] Đọc danh mục operand image của CloudNativePG (ClusterImageCatalog) và dịch nó
// thành mô hình version của Everest.
//
// Vì sao Everest tự đọc catalog thay vì để CNPG tra qua "spec.imageCatalogRef":
//
//  1. "imageCatalogRef" loại trừ lẫn nhau với "spec.imageName" theo ràng buộc của CNPG. Dùng ref
//     thì imageName rỗng, mà validateVersionChange() lại đọc version đang chạy từ chính chuỗi
//     imageName — guard chặn downgrade/major upgrade sẽ im lặng bỏ qua mọi thay đổi.
//  2. "imageCatalogRef" chỉ nhận major (int), trong khi .spec.engine.version của Everest là version
//     đầy đủ. Người dùng khai "17.4" mà cụm chạy "17.11" là API nói dối.
//
// Tự đọc catalog rồi ghi thẳng imageName giữ được cả hai: guard còn nguyên, version công bố đúng
// bằng version chạy thật, và image vẫn do platform team kiểm soát tập trung qua GitOps.

// clusterImageCatalogGVK là GroupVersionKind của danh mục image cluster-scoped của CloudNativePG.
var clusterImageCatalogGVK = schema.GroupVersionKind{
	Group:   consts.CNPGAPIGroup,
	Version: "v1",
	Kind:    consts.CNPGClusterImageCatalogKind,
}

// CNPGImageCatalogVersions đọc ClusterImageCatalog theo tên quy ước và trả về danh sách version
// khả dụng cho provider CloudNativePG.
//
// Trả về map rỗng (không phải lỗi) khi CRD chưa được cài hoặc catalog chưa tồn tại: Everest phải
// khởi động được trên cụm chưa có CloudNativePG, cùng triết lý Dynamic Discovery của Phase 1.
// Hệ quả là danh sách version rỗng, và admission sẽ từ chối mọi version — fail closed, đúng ý:
// thà không tạo được cụm còn hơn tạo cụm bằng một image không ai duyệt.
func CNPGImageCatalogVersions(
	ctx context.Context,
	c client.Client,
) (everestv1alpha1.Versions, error) {
	versions := everestv1alpha1.Versions{}

	catalog := &unstructured.Unstructured{}
	catalog.SetGroupVersionKind(clusterImageCatalogGVK)
	err := c.Get(ctx, types.NamespacedName{Name: consts.CNPGClusterImageCatalogName}, catalog)
	if err != nil {
		// CRD chưa cài (NoKindMatch) hoặc catalog chưa được apply (NotFound).
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return versions, nil
		}
		return versions, fmt.Errorf("get ClusterImageCatalog %q: %w", consts.CNPGClusterImageCatalogName, err)
	}

	images, _, err := unstructured.NestedSlice(catalog.Object, "spec", "images")
	if err != nil {
		return versions, fmt.Errorf("read spec.images of ClusterImageCatalog: %w", err)
	}

	engine := everestv1alpha1.ComponentsMap{}
	for _, raw := range images {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		image, _ := entry["image"].(string)
		if image == "" {
			continue
		}
		version, err := versionFromImage(image)
		if err != nil {
			// Một entry sai quy ước tag không được làm hỏng cả catalog: bỏ qua entry đó,
			// các version còn lại vẫn dùng được.
			continue
		}
		// Major khai trong catalog phải khớp major suy ra từ tag. Lệch nhau nghĩa là entry bị
		// đặt nhầm chỗ, và tin vào nó sẽ dựng cụm bằng sai bản PostgreSQL.
		//nolint:gosec // PostgreSQL major versions are small positive integers, bounded by the CNPG CRD.
		if major, ok := entryMajor(entry); ok && major != int64(semverMajor(version)) {
			continue
		}
		engine[version] = &everestv1alpha1.Component{
			ImagePath: image,
			ImageHash: digestFromImage(image),
			Status:    everestv1alpha1.DBEngineComponentAvailable,
		}
	}
	if len(engine) == 0 {
		return versions, nil
	}
	versions.Engine = engine

	// componentImages mang image của thành phần không phải PostgreSQL (hiện chỉ có pgbouncer).
	// Nó không gắn với một version PostgreSQL cụ thể, nên được công bố cho mọi version engine —
	// đúng hình dạng ComponentsMap mà mô hình Proxy của Everest mong đợi.
	if pgbouncer := componentImage(catalog.Object, "pgbouncer"); pgbouncer != "" {
		proxy := everestv1alpha1.ComponentsMap{}
		for version := range engine {
			proxy[version] = &everestv1alpha1.Component{
				ImagePath: pgbouncer,
				ImageHash: digestFromImage(pgbouncer),
				Status:    everestv1alpha1.DBEngineComponentAvailable,
			}
		}
		versions.Proxy = map[everestv1alpha1.ProxyType]everestv1alpha1.ComponentsMap{
			everestv1alpha1.ProxyTypePGBouncer: proxy,
		}
	}
	return versions, nil
}

// componentImage tìm image của một thành phần phụ theo "key" trong spec.componentImages.
func componentImage(object map[string]any, key string) string {
	components, _, err := unstructured.NestedSlice(object, "spec", "componentImages")
	if err != nil {
		return ""
	}
	for _, raw := range components {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := entry["key"].(string); name == key {
			image, _ := entry["image"].(string)
			return image
		}
	}
	return ""
}

// entryMajor đọc trường "major" của một entry catalog. JSON số có thể về dưới dạng int64 hoặc
// float64 tùy đường giải mã, nên phải xử lý cả hai.
func entryMajor(entry map[string]any) (int64, bool) {
	switch value := entry["major"].(type) {
	case int64:
		return value, true
	case float64:
		return int64(value), true
	}
	return 0, false
}

// versionFromImage rút version PostgreSQL ra khỏi một chuỗi image đầy đủ.
//
//	ghcr.io/cloudnative-pg/postgresql:14.24-202608310817-standard-bookworm@sha256:31221f...
//	                                  └───┘
//
// Quy ước: tag luôn bắt đầu bằng version dạng X.Y, phần sau dấu "-" đầu tiên là metadata build
// (release, flavor, distro) và bị bỏ qua. Đổi quy ước đặt tag là phá cách tra cứu này.
func versionFromImage(image string) (string, error) {
	// Bỏ digest trước, vì digest cũng chứa dấu ":".
	reference, _, _ := strings.Cut(image, "@")

	lastSlash := strings.LastIndex(reference, "/")
	lastColon := strings.LastIndex(reference, ":")
	if lastColon <= lastSlash {
		return "", fmt.Errorf("image %q has no tag", image)
	}
	tag := reference[lastColon+1:]

	version, _, _ := strings.Cut(tag, "-")
	if _, err := semver.NewVersion(version); err != nil {
		return "", fmt.Errorf("cannot determine PostgreSQL version from tag %q: %w", tag, err)
	}
	return version, nil
}

// digestFromImage trả về phần digest của chuỗi image, hoặc chuỗi rỗng nếu image chỉ pin bằng tag.
func digestFromImage(image string) string {
	_, digest, found := strings.Cut(image, "@")
	if !found {
		return ""
	}
	return digest
}

// semverMajor trả về major của một version đã được versionFromImage xác thực.
func semverMajor(version string) uint64 {
	parsed, err := semver.NewVersion(version)
	if err != nil {
		return 0
	}
	return parsed.Major()
}
