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
	"os"
	"os/exec"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase18 upgrades the chart in the middle of a run (GO-66).
//
// Flow: source → pass (2 replicas) → sink, with an older commit's test
// component, since a user's components are not rebuilt when Alpheus is
// upgraded.
//
// What it tests: that a run started under an older controller and agent
// finishes under this commit's controller; that new attempts keep the agent
// the run started with (GO-24, GO-164); and that a run started after the
// upgrade gets this commit's agent.
//
// Done to the run: when sink has read a quarter of its first stream, the
// chart, CRD, controller and agent are upgraded; then a pass replica, and
// later the stateful sink, are deleted.
//
// Passes when: every pod ran the old agent before the upgrade; the run is
// Succeeded with every chunk once and exact counts; pass and sink restarted
// once, their new attempts with the old agent; status.agentImage is the old
// agent; the status never went backwards; and a run started afterwards runs
// this commit's agent.
//
// It runs only with make test-upgrade (hack/test-upgrade.sh), which installs
// the older commit first and sets:
//
//	ALPHEUS_E2E_UPGRADE             the command that upgrades to this
//	                                commit's chart and images
//	ALPHEUS_E2E_UPGRADE_BENCH_IMAGE the older commit's test component
//	ALPHEUS_E2E_UPGRADE_AGENT_IMAGE the older commit's agent
//
// and AGENT_IMAGE is this commit's agent. make e2e skips it.
func TestTestcase18(t *testing.T) {
	upgrade := os.Getenv("ALPHEUS_E2E_UPGRADE")
	if upgrade == "" {
		t.Skip("ALPHEUS_E2E_UPGRADE is not set: run hack/test-upgrade.sh (make test-upgrade)")
	}
	oldAgent := need(t, "ALPHEUS_E2E_UPGRADE_AGENT_IMAGE")
	newAgent := need(t, "AGENT_IMAGE")
	if oldAgent == newAgent {
		t.Fatal("the old and new agent are the same image: nothing is upgraded")
	}
	b := newBench(t, "testcase18", nil)
	b.Replicas["pass"] = 2 // as in the flow file
	e := setup(t)
	before := e.controllerImage()
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)
	watch := e.watchStatus(r)

	perReplica := b.Chunks / b.Replicas["pass"]
	e.until(r, "sink has acked a quarter of pass's first stream", 5*time.Minute, func() bool {
		return b.acked("pass", "sink") >= perReplica/4
	})
	for _, p := range e.pods(r, client.MatchingLabels{}) {
		if got := agentImage(p); got != oldAgent {
			t.Errorf("%s runs agent %s before the upgrade, want %s", p.Name, got, oldAgent)
		}
	}

	t.Logf("upgrading: %s", upgrade)
	cmd := exec.CommandContext(t.Context(), "bash", "-c", upgrade)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	e.waitController()
	if after := e.controllerImage(); after == before {
		t.Fatalf("the controller is still %s after the upgrade", after)
	}

	// New attempts keep the run's agent (GO-164).
	b.Kills = append(b.Kills, e.killReplica(r, "pass", 0))
	e.until(r, "sink has acked three quarters of pass's first stream", 10*time.Minute, func() bool {
		return b.acked("pass", "sink") >= perReplica*3/4
	})
	b.Kills = append(b.Kills, e.killReplica(r, "sink", 0))

	b.finish(e, r, b.Chunks, sdk.DefaultPort)
	watch.stop()
	b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})
	for _, step := range []string{"pass", "sink"} {
		rs, err := replicaStatus(b.run, step, 0)
		if err != nil {
			t.Fatal(err)
		}
		if rs.Restarts != 1 {
			t.Errorf("%s replica 0 has %d restarts, want 1: %s", step, rs.Restarts, rs.Message)
		}
		var p corev1.Pod
		if err := e.c.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: rs.Pod}, &p); err != nil {
			t.Errorf("%s's last pod: %v", step, err)
		} else if got := agentImage(p); got != oldAgent {
			t.Errorf("%s's new attempt runs agent %s, want the run's %s", step, got, oldAgent)
		}
	}
	if got := b.run.Status.AgentImage; got != oldAgent {
		t.Errorf("status.agentImage %q, want %s", got, oldAgent)
	}
	b.acks("source", "pass")
	b.consumed("pass")
	b.noOrphans()
	for _, p := range watch.problems() {
		t.Error(p)
	}

	// A run started after the upgrade gets the new agent.
	next := e.load(b.Testcase)
	next.Name += "-after"
	if err := e.c.Create(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	e.until(next, "a pod of a run started after the upgrade", 2*time.Minute, func() bool {
		return len(e.pods(next, client.MatchingLabels{})) > 0
	})
	for _, p := range e.pods(next, client.MatchingLabels{}) {
		if got := agentImage(p); got != newAgent {
			t.Errorf("%s, started after the upgrade, runs agent %s, want %s", p.Name, got, newAgent)
		}
	}
}

// controllerImage is the image of the controller's Deployment.
func (e *env) controllerImage() string {
	e.t.Helper()
	var d appsv1.Deployment
	if err := e.c.Get(e.t.Context(), client.ObjectKey{Namespace: controllerNamespace, Name: controllerName}, &d); err != nil {
		e.t.Fatal(err)
	}
	return d.Spec.Template.Spec.Containers[0].Image
}

// agentImage is the image of a step pod's agent.
func agentImage(p corev1.Pod) string {
	for _, c := range p.Spec.InitContainers {
		if c.Name == controller.AgentContainer {
			return c.Image
		}
	}
	return ""
}
