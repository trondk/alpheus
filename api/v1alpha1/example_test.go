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

package v1alpha1_test

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// A run of two steps: generate writes chunks of log lines, and two
// replicas of grep keep the lines that match. A program creates it with a
// controller-runtime client, as in docs/api.md.
func ExampleStreamRun() {
	run := v1alpha1.StreamRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "StreamRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "log-report", Namespace: "flows"},
		Spec: v1alpha1.StreamRunSpec{
			Steps: []v1alpha1.Step{
				{Name: "generate", Image: "registry.example/generate@sha256:" + zeros},
				{Name: "grep", Image: "registry.example/grep@sha256:" + zeros, Replicas: ptr.To[int32](2),
					Settings: &runtime.RawExtension{Raw: []byte(`{"match":"ERROR"}`)}},
			},
			Edges: []v1alpha1.Edge{{From: "generate", To: "grep"}},
			Storage: v1alpha1.Storage{
				Endpoint:  "https://s3.example",
				Bucket:    "alpheus-data",
				SecretRef: "alpheus-s3",
			},
		},
	}
	out, err := yaml.Marshal(run)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(out))
	// Output:
	// apiVersion: alpheus-dev.org/v1alpha1
	// kind: StreamRun
	// metadata:
	//   name: log-report
	//   namespace: flows
	// spec:
	//   edges:
	//   - from: generate
	//     to: grep
	//   steps:
	//   - image: registry.example/generate@sha256:0000000000000000000000000000000000000000000000000000000000000000
	//     name: generate
	//   - image: registry.example/grep@sha256:0000000000000000000000000000000000000000000000000000000000000000
	//     name: grep
	//     replicas: 2
	//     settings:
	//       match: ERROR
	//   storage:
	//     bucket: alpheus-data
	//     endpoint: https://s3.example
	//     secretRef: alpheus-s3
}

const zeros = "0000000000000000000000000000000000000000000000000000000000000000"
