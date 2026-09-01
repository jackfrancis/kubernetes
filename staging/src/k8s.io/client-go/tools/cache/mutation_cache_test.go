/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

func makeMutationTestPod(name string, uid types.UID, rv string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             uid,
			ResourceVersion: rv,
		},
	}
}

func TestMutationCacheOnAddOrUpdate(t *testing.T) {
	tests := map[string]struct {
		mutationRV  string
		storeUID    types.UID // UID passed to OnAddOrUpdate
		storeRV     string    // RV passed to OnAddOrUpdate
		wantCleared bool
	}{
		"equal-rv-clears": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "2",
			wantCleared: true,
		},
		"newer-store-clears": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "3",
			wantCleared: true,
		},
		"older-store-keeps": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "1",
			wantCleared: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewStore(MetaNamespaceKeyFunc)
			indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
			mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, false)

			mutated := makeMutationTestPod("pod", "uid-1", tc.mutationRV)
			mc.Mutation(mutated)

			mc.OnAddOrUpdate(makeMutationTestPod("pod", tc.storeUID, tc.storeRV))

			// Add the pod to the backing store at RV "1" (always older than the
			// mutation's RV "2"), so we can tell whether GetByKey returns the
			// mutation or the backing copy.
			require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

			got, exists, err := mc.GetByKey("pod")
			require.NoError(t, err)
			require.True(t, exists)

			gotRV := got.(*v1.Pod).ResourceVersion
			if tc.wantCleared {
				assert.Equal(t, "1", gotRV, "backing store version expected after mutation cleared")
			} else {
				assert.Equal(t, tc.mutationRV, gotRV, "mutation version expected")
			}
		})
	}
}

// TestMutationCacheOnDelete checks the behavior of OnDelete
// when invoked after Mutation.
func TestMutationCacheOnDelete(t *testing.T) {
	tests := map[string]struct {
		deleteUID  types.UID
		deleteRV   string
		wantExists bool
	}{
		"old-rv-same-UID": {
			deleteUID:  "uid-1",
			deleteRV:   "1", // Could be from a stale object in DeletedFinalStateUnknown.
			wantExists: false,
		},
		"new-rv-different-UID": {
			deleteUID:  "uid-2",
			deleteRV:   "3",
			wantExists: false,
		},
		"old-rv-different-uid": {
			deleteUID:  "uid-other",
			deleteRV:   "1",
			wantExists: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewStore(MetaNamespaceKeyFunc)
			indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
			mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, true)

			// Backing store has the pod at RV "2".
			require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "2")))

			// Mutation brings it to RV "3".
			mc.Mutation(makeMutationTestPod("pod", "uid-1", "3"))
			deletedPod := makeMutationTestPod("pod", tc.deleteUID, tc.deleteRV)
			require.NoError(t, store.Delete(deletedPod))
			mc.OnDelete(deletedPod)

			_, exists, err := mc.GetByKey("pod")
			require.NoError(t, err)
			require.Equal(t, tc.wantExists, exists)
		})
	}
}

// TestMutationCacheUpdateConcurrentDelete checks the behavior when
// Mutation is called after some informer events.
func TestMutationCacheUpdateConcurrentDelete(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Hour, true)

	// Backing store has the pod at RV "1".
	require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

	// Client starts an update leading to RV "2", which immediately gets
	// deleted by some other client. That deletion is received before the
	// client finishes its update.
	updatedPod := makeMutationTestPod("pod", "uid-1", "2")
	require.NoError(t, store.Update(updatedPod))
	require.NoError(t, store.Delete(updatedPod))

	// Informer events get delivered with a delay.
	mc.OnAddOrUpdate(updatedPod)
	mc.OnDelete(updatedPod)

	// This Mutation call is stale, which gets detected because
	// the mutation cache contains a tombstone object.
	mc.Mutation(updatedPod)

	_, exists, err := mc.GetByKey("pod")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, []any{"pod"}, mc.(*mutationCache).mutationCache.Keys())
}

// TestMutationCacheUpdateConcurrentRecreate checks the behavior when
// Mutation is called after some informer events.
func TestMutationCacheUpdateConcurrentRecreate(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Hour, true)

	// Backing store has the pod at RV "1".
	require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

	// Client starts an update leading to RV "2", which immediately gets
	// replaced by some other pod using the same name. Those changes are
	// received before the client finishes its update.
	updatedPod := makeMutationTestPod("pod", "uid-1", "2")
	require.NoError(t, store.Update(updatedPod))
	require.NoError(t, store.Delete(updatedPod))
	replacementPod := makeMutationTestPod("pod", "uid-2", "3")
	require.NoError(t, store.Add(replacementPod))

	// Informer events get delivered with a delay.
	mc.OnAddOrUpdate(updatedPod)
	mc.OnDelete(updatedPod)
	mc.OnAddOrUpdate(replacementPod)

	// This Mutation call is stale, which gets detected because there is a more
	// recent object in the store.
	mc.Mutation(updatedPod)

	got, exists, err := mc.GetByKey("pod")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, replacementPod, got)
	require.Empty(t, mc.(*mutationCache).mutationCache.Keys())
}

// TestMutationCacheOnDeleteClearsStaleMutation exercises the core bug that motivated
// the OnDelete method: a mutation stored after an update must not survive once the
// informer reports the object deleted.
//
// The scenario:
//  1. The object is added by the caller (includeAdds=true) so mutation cache
//     returns it even when the backing store is empty.
//  2. The object is deleted on the server; the backing store is now empty.
//  3. Without OnDelete the mutation would linger in the cache and ByIndex
//     would still return it, preventing the caller from recreating it.
func TestMutationCacheOnDeleteClearsStaleMutation(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	byNameIndex := "by-name"
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{
		byNameIndex: func(obj interface{}) ([]string, error) {
			return []string{obj.(*v1.Pod).Name}, nil
		},
	})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, true /* includeAdds */)

	// Simulate an update: mutation cache holds the pod at RV "2" while the
	// backing store is still empty (informer hasn't caught up yet).
	mc.Mutation(makeMutationTestPod("pod", "uid-1", "2"))

	items, err := mc.ByIndex(byNameIndex, "pod")
	require.NoError(t, err)
	assert.Len(t, items, 1, "mutation should be visible via ByIndex when backing store is empty")

	// Informer reports the delete: OnDelete must clear the stale mutation so
	// that the next ByIndex call returns nothing and lets the caller recreate
	// the object.
	mc.OnDelete(makeMutationTestPod("pod", "uid-1", "1"))

	items, err = mc.ByIndex(byNameIndex, "pod")
	require.NoError(t, err)
	assert.Empty(t, items, "OnDelete must clear the mutation; ByIndex must return nothing")
}

// racingIndexer removes objects from the wrapped Indexer on the first GetByKey
// call, simulating an informer that processes a delete event concurrently with
// a ByIndex call. This is possible because processDeltas updates the indexer
// before invoking the event handlers, so the indexer is not serialized against
// the mutation cache lock held by ByIndex.
type racingIndexer struct {
	Indexer
	deleteOnNextGet []interface{}
}

func (r *racingIndexer) GetByKey(key string) (interface{}, bool, error) {
	for _, obj := range r.deleteOnNextGet {
		if err := r.Indexer.Delete(obj); err != nil {
			return nil, false, err
		}
	}
	r.deleteOnNextGet = nil
	return r.Indexer.GetByKey(key)
}

// TestMutationCacheByIndexConcurrentDelete verifies that ByIndex returns a
// cached replacement when the indexed object with the same key disappears
// between IndexKeys and GetByKey.
func TestMutationCacheByIndexConcurrentDelete(t *testing.T) {
	byNameIndex := "by-name"
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{
		byNameIndex: func(obj interface{}) ([]string, error) {
			return []string{obj.(*v1.Pod).Name}, nil
		},
	})

	// The informer still has the old instance when ByIndex starts.
	oldPod := makeMutationTestPod("pod", "uid-1", "1")
	require.NoError(t, indexer.Add(oldPod))

	racer := &racingIndexer{Indexer: indexer}
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), NewStore(MetaNamespaceKeyFunc), racer, time.Minute, true /* includeAdds */)

	// Simulate recreating the pod under the same name.
	replacementPod := makeMutationTestPod("pod", "uid-2", "2")
	mc.Mutation(replacementPod)

	// Delete oldPod when ByIndex calls GetByKey, after IndexKeys returns its key.
	racer.deleteOnNextGet = []interface{}{oldPod}
	items, err := mc.ByIndex(byNameIndex, "pod")
	require.NoError(t, err)
	require.Equal(t, []interface{}{replacementPod}, items)
}

func TestMutationCacheByIndexMutationChangesIndex(t *testing.T) {
	const (
		indexName = "by-foo"
		fooLabel  = "foo"
	)
	tests := map[string]struct {
		indexerResourceVersion       string
		indexerLabelValue            string
		mutationCacheResourceVersion string
		mutationCacheLabelValue      string
		queriedIndexValue            string
		includeAdds                  bool
		want                         []string
	}{
		// The mutation cache contains a newer version whose indexed label
		// did not change.
		// A query for foo=a should return the newer mutation.
		"newer-mutation-same-index-without-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  false,
			want:                         []string{"2/a"},
		},
		// Same scenario as previous, prove that includeAdds has no effect.
		"newer-mutation-same-index-with-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  true,
			want:                         []string{"2/a"},
		},
		// The mutation cache contains a newer version whose indexed label
		// changed from foo=a to foo=b.
		// A query for foo=a should return nothing.
		"newer-mutation-stops-matching-without-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "b",
			queriedIndexValue:            "a",
			includeAdds:                  false,
			want:                         []string{},
		},
		// Same scenario as previous, prove that includeAdds has no effect.
		"newer-mutation-stops-matching-with-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "b",
			queriedIndexValue:            "a",
			includeAdds:                  true,
			want:                         []string{},
		},
		// The mutation cache contains a newer version whose indexed label
		// changed from foo=a to foo=b.
		// A query for foo=b should return nothing
		// because includeAdds=false.
		"newer-mutation-starts-matching-without-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "b",
			queriedIndexValue:            "b",
			includeAdds:                  false,
			want:                         []string{},
		},
		// The mutation cache contains a newer version whose indexed label
		// changed from foo=a to foo=b.
		// A query for foo=b should return the newer mutation
		// because includeAdds=true.
		"newer-mutation-starts-matching-with-include-adds": {
			indexerResourceVersion:       "1",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "2",
			mutationCacheLabelValue:      "b",
			queriedIndexValue:            "b",
			includeAdds:                  true,
			want:                         []string{"2/b"},
		},
		// Stale mutation cache scenarios

		// The mutation cache contains an older version whose indexed label
		// is the same as the indexer's.
		// A query for foo=a should return the indexer's object.
		"older-mutation-same-index-without-include-adds": {
			indexerResourceVersion:       "2",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "1",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  false,
			want:                         []string{"2/a"},
		},
		// Same scenario as previous, prove that includeAdds has no effect.
		"older-mutation-same-index-with-include-adds": {
			indexerResourceVersion:       "2",
			indexerLabelValue:            "a",
			mutationCacheResourceVersion: "1",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  true,
			want:                         []string{"2/a"},
		},
		// The older mutation matches foo=a, but the newer indexer object
		// has foo=b. A query for foo=a should return nothing.
		"older-mutation-matches-query-without-include-adds": {
			indexerResourceVersion:       "2",
			indexerLabelValue:            "b",
			mutationCacheResourceVersion: "1",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  false,
			want:                         []string{},
		},
		// Same scenario as previous, prove that includeAdds does not allow
		// the older mutation to override the newer indexer object.
		"older-mutation-matches-query-with-include-adds": {
			indexerResourceVersion:       "2",
			indexerLabelValue:            "b",
			mutationCacheResourceVersion: "1",
			mutationCacheLabelValue:      "a",
			queriedIndexValue:            "a",
			includeAdds:                  true,
			want:                         []string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Make a new indexer.
			indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{
				indexName: func(obj interface{}) ([]string, error) {
					return []string{obj.(*v1.Pod).Labels[fooLabel]}, nil
				},
			})
			// Make a pod "testpod" with the indexer resource version
			// and test-specific label value...
			indexerPod := makeMutationTestPod("testpod", "uid-1", tc.indexerResourceVersion)
			indexerPod.Labels = map[string]string{fooLabel: tc.indexerLabelValue}
			// ... and add it to the indexer.
			require.NoError(t, indexer.Add(indexerPod))

			// Keep the indexer object unchanged while recording a separate
			// version in the mutation cache.
			mc := NewIntegerResourceVersionMutationCache(klog.Background(), indexer, indexer, time.Minute, tc.includeAdds)
			mutation := makeMutationTestPod("testpod", "uid-1", tc.mutationCacheResourceVersion)
			mutation.Labels = map[string]string{fooLabel: tc.mutationCacheLabelValue}
			// Record the mutation.
			// We are not simulating an informer update to indexer...
			mc.Mutation(mutation)

			// ... so ByIndex is responsible for evaluating
			// the query using both the indexer and the mutation cache.
			items, err := mc.ByIndex(indexName, tc.queriedIndexValue)
			require.NoError(t, err)
			// Compare the return from ByIndex with the expected result.
			got := make([]string, 0, len(items))
			for _, item := range items {
				pod := item.(*v1.Pod)
				got = append(got, pod.ResourceVersion+"/"+pod.Labels[fooLabel])
			}
			require.Equal(t, tc.want, got)
		})
	}
}

type updateOnGetIndexer struct {
	Indexer
	updateOnNextGet interface{}
}

func (i *updateOnGetIndexer) GetByKey(key string) (interface{}, bool, error) {
	if i.updateOnNextGet != nil {
		if err := i.Indexer.Update(i.updateOnNextGet); err != nil {
			return nil, false, err
		}
		i.updateOnNextGet = nil
	}
	return i.Indexer.GetByKey(key)
}

func TestMutationCacheByIndexConcurrentIndexChange(t *testing.T) {
	const (
		indexName = "by-foo"
		fooLabel  = "foo"
	)
	indexer := &updateOnGetIndexer{
		Indexer: NewIndexer(MetaNamespaceKeyFunc, Indexers{
			indexName: func(obj interface{}) ([]string, error) {
				return []string{obj.(*v1.Pod).Labels[fooLabel]}, nil
			},
		}),
	}
	originalPod := makeMutationTestPod("pod", "uid-1", "1")
	originalPod.Labels = map[string]string{fooLabel: "old"}
	require.NoError(t, indexer.Add(originalPod))

	updatedPod := makeMutationTestPod("pod", "uid-1", "2")
	updatedPod.Labels = map[string]string{fooLabel: "new"}
	indexer.updateOnNextGet = updatedPod
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), indexer, indexer, time.Minute, false)

	items, err := mc.ByIndex(indexName, "old")
	require.NoError(t, err)
	require.Empty(t, items)
}
