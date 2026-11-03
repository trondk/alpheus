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

package e2e

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/sdk"
)

// The test cluster's two nodes (vagrant/).
const (
	controlPlaneNode = "alpheus-talos"
	workerNode       = "alpheus-talos-worker"
)

// TestTestcase16 runs source -> pass -> sink across two nodes, with pass's
// two replicas on the worker and the others on the control plane (GO-144).
func TestTestcase16(t *testing.T) {
	b := newBench(t, "testcase16", nil)
	e := setup(t)
	e.needNodes(controlPlaneNode, workerNode)
	b.start(e, b.Chunks, sdk.DefaultPort)
	b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})

	// Every pod ran where its step asked.
	var pods corev1.PodList
	if err := e.c.List(t.Context(), &pods, client.InNamespace(e.ns), client.MatchingLabels{controller.LabelRun: "testcase16"}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"source": controlPlaneNode, "pass": workerNode, "sink": controlPlaneNode}
	seen := map[string]int{}
	for _, p := range pods.Items {
		step := p.Labels[controller.LabelStep]
		seen[step]++
		if p.Spec.NodeName != want[step] {
			t.Errorf("%s ran on %q, want %s", p.Name, p.Spec.NodeName, want[step])
		}
	}
	if seen["pass"] != 2 || seen["source"] != 1 || seen["sink"] != 1 {
		t.Errorf("pods per step %v", seen)
	}
	b.report()
}

// needNodes skips the test unless the cluster has these nodes, ready.
func (e *env) needNodes(names ...string) {
	e.t.Helper()
	for _, name := range names {
		var n corev1.Node
		if err := e.c.Get(e.t.Context(), client.ObjectKey{Name: name}, &n); err != nil {
			e.t.Skipf("no node %s: the test cluster has two nodes (vagrant/)", name)
		}
		if !nodeReady(&n) {
			e.t.Fatalf("node %s is not ready", name)
		}
	}
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
