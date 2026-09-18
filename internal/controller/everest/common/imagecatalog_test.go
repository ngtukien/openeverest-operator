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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
)

const (
	testPG14Image = "ghcr.io/cloudnative-pg/postgresql:14.24-202608310817-standard-bookworm" +
		"@sha256:31221f7241b4aa568c4191d2a8e51d2bbc51b5141a2548bb9023d6e96e1b98a0"
	testPG17Image = "ghcr.io/cloudnative-pg/postgresql:17.11-202608310816-standard-bookworm" +
		"@sha256:e8ffaff9d17011fb71f264d857c3b4c54cb86ed0443a4ecb0298cb96be708d4e"
	testPgBouncerImage = "ghcr.io/cloudnative-pg/pgbouncer:1.25.1" +
		"@sha256:e6ddfe22d845e603825e235dd8334b21ecd125abea2a2172478f556b8dee2bb8"

	fieldImage = "image"
	fieldMajor = "major"
)

func catalogScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, everestv1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(clusterImageCatalogGVK, &unstructured.Unstructured{})
	listGVK := clusterImageCatalogGVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func newCatalog(images []any, componentImages []any) *unstructured.Unstructured {
	catalog := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"images": images},
	}}
	catalog.SetGroupVersionKind(clusterImageCatalogGVK)
	catalog.SetName(consts.CNPGClusterImageCatalogName)
	if componentImages != nil {
		_ = unstructured.SetNestedSlice(catalog.Object, componentImages, "spec", "componentImages")
	}
	return catalog
}

func TestCNPGImageCatalogVersions(t *testing.T) {
	t.Parallel()

	catalog := newCatalog(
		[]any{
			map[string]any{fieldMajor: int64(14), fieldImage: testPG14Image},
			map[string]any{fieldMajor: int64(17), fieldImage: testPG17Image},
		},
		[]any{
			map[string]any{"key": "pgbouncer", fieldImage: testPgBouncerImage},
		},
	)
	c := fakeclient.NewClientBuilder().
		WithScheme(catalogScheme(t)).
		WithObjects(catalog).
		Build()

	versions, err := CNPGImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)

	require.Len(t, versions.Engine, 2)
	require.Contains(t, versions.Engine, "14.24")
	require.Contains(t, versions.Engine, "17.11")

	// Image phải được giữ nguyên cả digest: đó là toàn bộ lý do dùng catalog.
	assert.Equal(t, testPG14Image, versions.Engine["14.24"].ImagePath)
	assert.Equal(
		t,
		"sha256:31221f7241b4aa568c4191d2a8e51d2bbc51b5141a2548bb9023d6e96e1b98a0",
		versions.Engine["14.24"].ImageHash,
	)
	assert.Equal(t, everestv1alpha1.DBEngineComponentAvailable, versions.Engine["17.11"].Status)

	// componentImages được công bố cho mọi version engine, đúng hình dạng ComponentsMap.
	proxy := versions.Proxy[everestv1alpha1.ProxyTypePGBouncer]
	require.Len(t, proxy, 2)
	assert.Equal(t, testPgBouncerImage, proxy["14.24"].ImagePath)
	assert.Equal(t, testPgBouncerImage, proxy["17.11"].ImagePath)
}

func TestCNPGImageCatalogVersionsMissingCatalog(t *testing.T) {
	t.Parallel()

	// Chưa apply catalog thì trả về danh sách rỗng chứ không phải lỗi: Everest vẫn phải chạy
	// được trên cụm chưa cài CloudNativePG. Danh sách rỗng khiến admission từ chối mọi version,
	// tức fail closed đúng như thiết kế.
	c := fakeclient.NewClientBuilder().WithScheme(catalogScheme(t)).Build()

	versions, err := CNPGImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)
	assert.Empty(t, versions.Engine)
	assert.Empty(t, versions.Proxy)
}

func TestCNPGImageCatalogVersionsSkipsInvalidEntries(t *testing.T) {
	t.Parallel()

	catalog := newCatalog([]any{
		// major khai trong catalog lệch với major suy ra từ tag: entry đặt nhầm chỗ, tin vào nó
		// sẽ dựng cụm bằng sai bản PostgreSQL.
		map[string]any{fieldMajor: int64(15), fieldImage: testPG14Image},
		// Image không có tag thì không suy ra được version.
		map[string]any{fieldMajor: int64(16), fieldImage: "ghcr.io/cloudnative-pg/postgresql"},
		// Entry hợp lệ vẫn phải sống sót qua các entry hỏng phía trên.
		map[string]any{fieldMajor: int64(17), fieldImage: testPG17Image},
	}, nil)
	c := fakeclient.NewClientBuilder().
		WithScheme(catalogScheme(t)).
		WithObjects(catalog).
		Build()

	versions, err := CNPGImageCatalogVersions(t.Context(), c)
	require.NoError(t, err)
	require.Len(t, versions.Engine, 1)
	assert.Contains(t, versions.Engine, "17.11")
}

func TestVersionFromImage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		image   string
		want    string
		wantErr bool
	}{
		{name: "tag kèm digest", image: testPG14Image, want: "14.24"},
		{
			name:  "tag không digest",
			image: "ghcr.io/cloudnative-pg/postgresql:17.11-202608310816-standard-bookworm",
			want:  "17.11",
		},
		{name: "tag chỉ có version", image: "ghcr.io/cloudnative-pg/postgresql:18.6", want: "18.6"},
		{name: "không có tag", image: "ghcr.io/cloudnative-pg/postgresql", wantErr: true},
		{name: "tag không phải version", image: "ghcr.io/cloudnative-pg/postgresql:latest", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := versionFromImage(tc.image)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
