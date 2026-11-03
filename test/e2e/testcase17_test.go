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
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase17 loses a node in the middle of a run (GO-67): the worker,
// where pass runs, is powered off and its Node deleted, as a cloud
// provider does with a dead machine. The run must succeed with every chunk
// once and exact counts, pass's new attempt on the control plane, and the
// worker is powered on again afterwards. It needs virsh and the worker's
// VM in ALPHEUS_E2E_WORKER_VM (hack/e2e.sh sets it).
func TestTestcase17(t *testing.T) {
	vm := os.Getenv("ALPHEUS_E2E_WORKER_VM")
	if vm == "" {
		t.Skip("ALPHEUS_E2E_WORKER_VM is not set: the test powers the worker's VM off and on")
	}
	b := newBench(t, "testcase17", nil)
	b.Replicas["pass"] = 1 // as in the flow file, whatever BENCH_PASS_REPLICAS says
	e := setup(t)
	e.needNodes(controlPlaneNode, workerNode)
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)

	// pass runs on the worker, which it prefers.
	var passPod string
	e.until(r, "pass running on the worker", 3*time.Minute, func() bool {
		var p corev1.Pod
		rs, err := replicaStatus(e.get(r), "pass", 0)
		if err != nil || rs.Pod == "" || e.c.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: rs.Pod}, &p) != nil {
			return false
		}
		passPod = p.Name
		return p.Spec.NodeName == workerNode && p.Status.Phase == corev1.PodRunning
	})
	at := b.Chunks / 4
	e.until(r, "sink has acked a quarter of the chunks", 5*time.Minute, func() bool { return b.acked("pass", "sink") >= at })

	// The worker goes, and with it its Node; it comes back at the end,
	// also if the test fails.
	t.Cleanup(func() { powerOn(t, e, vm) })
	virsh(t, "destroy", vm)
	t.Logf("powered off %s with %s on it", vm, passPod)
	if err := e.c.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: workerNode}}); err != nil {
		t.Fatalf("deleting node %s: %v", workerNode, err)
	}

	b.finish(e, r, b.Chunks, sdk.DefaultPort)
	b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})
	rs, err := replicaStatus(b.run, "pass", 0)
	if err != nil || rs.Restarts != 1 || rs.Attempt != 2 {
		t.Errorf("pass %+v (%v), want one restart", rs, err)
	}
	// A replica's reason is cleared when it succeeds; the events keep why
	// the first attempt failed (GO-139).
	cfg, err := ctrl.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := kube.EventsV1().Events(e.ns).List(t.Context(), metav1.ListOptions{FieldSelector: "regarding.name=testcase17"})
	if err != nil {
		t.Fatal(err)
	}
	restarted := false
	for _, ev := range evs.Items {
		if ev.Action == "Restart" {
			t.Logf("event %s %s: %s", ev.Action, ev.Reason, ev.Note)
			restarted = restarted || ev.Reason == string(v1alpha1.ReasonPodDeleted) || ev.Reason == string(v1alpha1.ReasonPodFailed)
		}
	}
	if !restarted {
		t.Error("no Restart event with the reason PodDeleted or PodFailed")
	}
	var p corev1.Pod
	if err := e.c.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: rs.Pod}, &p); err == nil && p.Spec.NodeName != controlPlaneNode {
		t.Errorf("pass's new attempt ran on %q, want %s", p.Spec.NodeName, controlPlaneNode)
	}
	b.acks("source", "pass")
	b.noOrphans()
}

func virsh(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("virsh", append([]string{"-c", "qemu:///system"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("virsh %v: %v\n%s", args, err, out)
	}
}

// powerOn starts the worker's VM again and waits until its Node is back
// and ready, so the next test has two nodes.
func powerOn(t *testing.T, e *env, vm string) {
	exec.Command("virsh", "-c", "qemu:///system", "start", vm).Run()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for ctx.Err() == nil {
		var n corev1.Node
		if e.c.Get(ctx, client.ObjectKey{Name: workerNode}, &n) == nil && nodeReady(&n) {
			t.Logf("%s is back", workerNode)
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Errorf("node %s was not ready again within 10 minutes", workerNode)
}
