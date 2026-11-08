/*
Copyright The Alpheus Authors.

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

// Package v1alpha1 contains the StreamRun API, version v1alpha1, of the
// alpheus-dev.org API group.
//
// +kubebuilder:object:generate=true
// +groupName=alpheus-dev.org
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of the types in this package.
	GroupVersion = schema.GroupVersion{Group: "alpheus-dev.org", Version: "v1alpha1"}

	// SchemeBuilder registers the types in this package with a scheme. It
	// is apimachinery's, so the package needs nothing beyond apimachinery.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this package to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &StreamRun{}, &StreamRunList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
