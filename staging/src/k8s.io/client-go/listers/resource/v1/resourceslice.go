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

// Code generated by lister-gen. DO NOT EDIT.

package v1

import (
	resourcev1 "k8s.io/api/resource/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
)

// ResourceSliceLister helps list ResourceSlices.
// All objects returned here must be treated as read-only.
type ResourceSliceLister interface {
	// List lists all ResourceSlices in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*resourcev1.ResourceSlice, err error)
	// Get retrieves the ResourceSlice from the index for a given name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*resourcev1.ResourceSlice, error)
	ResourceSliceListerExpansion
}

// resourceSliceLister implements the ResourceSliceLister interface.
type resourceSliceLister struct {
	listers.ResourceIndexer[*resourcev1.ResourceSlice]
}

// NewResourceSliceLister returns a new ResourceSliceLister.
func NewResourceSliceLister(indexer cache.Indexer) ResourceSliceLister {
	return &resourceSliceLister{listers.New[*resourcev1.ResourceSlice](indexer, resourcev1.Resource("resourceslice"))}
}
