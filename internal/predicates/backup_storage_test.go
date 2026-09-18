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

package predicates

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// [CUSTOM CNPG] Đổi chính sách backup (spec.objectStore) phải kích hoạt reconcile các
// DatabaseCluster dùng storage; không thì ObjectStore của chúng giữ chính sách cũ mãi mãi.
func TestBackupStoragePredicateObjectStore(t *testing.T) {
	t.Parallel()
	p := GetBackupStoragePredicate()
	withPolicy := func(raw string) *everestv1alpha1.BackupStorage {
		bs := &everestv1alpha1.BackupStorage{Spec: everestv1alpha1.BackupStorageSpec{Bucket: "b"}}
		if raw != "" {
			bs.Spec.ObjectStore = &runtime.RawExtension{Raw: []byte(raw)}
		}
		return bs
	}
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: withPolicy(`{"retentionPolicy":"7d"}`), ObjectNew: withPolicy(`{"retentionPolicy":"30d"}`)}))
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: withPolicy(""), ObjectNew: withPolicy(`{"retentionPolicy":"7d"}`)}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: withPolicy(`{"retentionPolicy":"7d"}`), ObjectNew: withPolicy(`{"retentionPolicy":"7d"}`)}))
}
