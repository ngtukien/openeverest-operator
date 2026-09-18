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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// [CUSTOM CNPG] BackupStorage.spec.objectStore chỉ mang "backup thế nào"; "ở đâu" đã có field riêng.
func TestBackupStorageValidator_ObjectStorePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		policy    string
		wantError string
	}{
		{name: "no policy"},
		{name: "retention, sidecar, wal/data, tags", policy: `{"retentionPolicy": "30d",
			"instanceSidecarConfiguration": {"resources": {"requests": {"memory": "128Mi"}}},
			"configuration": {"wal": {"compression": "zstd", "maxParallel": 2, "encryption": "AES256"},
			                  "data": {"compression": "gzip", "jobs": 2}, "tags": {"tier": "gold"}}}`},
		{name: "not an object", policy: `[1]`, wantError: "spec.objectStore must be a JSON object"},
		{name: "destinationPath", policy: `{"configuration": {"destinationPath": "s3://x"}}`, wantError: "spec.objectStore.configuration.destinationPath: Forbidden"},
		{name: "endpointURL", policy: `{"configuration": {"endpointURL": "http://x"}}`, wantError: "spec.objectStore.configuration.endpointURL: Forbidden"},
		{name: "credentials", policy: `{"configuration": {"s3Credentials": {}}}`, wantError: "spec.objectStore.configuration.s3Credentials: Forbidden"},
		{name: "serverName", policy: `{"configuration": {"serverName": "other"}}`, wantError: "spec.objectStore.configuration.serverName: Forbidden"},
		{name: "raw barman args", policy: `{"configuration": {"data": {"additionalCommandArgs": ["--endpoint-url=x"]}}}`, wantError: "spec.objectStore.configuration.data.additionalCommandArgs: Forbidden"},
		{name: "retention unit", policy: `{"retentionPolicy": "7 days"}`, wantError: "spec.objectStore.retentionPolicy: Invalid value"},
		{name: "retention zero", policy: `{"retentionPolicy": "0d"}`, wantError: "must be a recovery window"},
	}
	v := &BackupStorageValidator{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bs := &everestv1alpha1.BackupStorage{
				ObjectMeta: metav1.ObjectMeta{Name: "seaweedfs", Namespace: "vdt-dbaas"},
				Spec: everestv1alpha1.BackupStorageSpec{
					Type: everestv1alpha1.BackupStorageTypeS3, Bucket: "cnpg-backup", CredentialsSecretName: "s3-credentials",
				},
			}
			if tc.policy != "" {
				bs.Spec.ObjectStore = &runtime.RawExtension{Raw: []byte(tc.policy)}
			}
			_, createErr := v.ValidateCreate(t.Context(), bs)
			_, updateErr := v.ValidateUpdate(t.Context(), bs, bs)
			if tc.wantError == "" {
				require.NoError(t, createErr)
				require.NoError(t, updateErr)
				return
			}
			require.ErrorContains(t, createErr, tc.wantError)
			require.ErrorContains(t, updateErr, tc.wantError)
		})
	}
}
