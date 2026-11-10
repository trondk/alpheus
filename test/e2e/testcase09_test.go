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
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase09 takes the storage away in the middle of a run, once for 20
// seconds and once for 3 minutes.
//
// Flow: source → pass → sink, run twice.
//
// What it tests: that the agents wait for the storage with backoff instead
// of failing (GO-136).
//
// Done to the run: the storage (Garage, or MinIO with
// ALPHEUS_E2E_STORAGE=minio) is scaled to 0 in the middle of the stream and
// back up after 20 seconds, and in the second run after 3 minutes.
//
// Passes when: each run is Succeeded with every chunk once and unchanged,
// exact counts and no restarts at all.
func TestTestcase09(t *testing.T) {
	for _, outage := range []time.Duration{20 * time.Second, 3 * time.Minute} {
		t.Run(outage.String(), func(t *testing.T) {
			b := newBench(t, "testcase09", nil)
			e := setup(t)
			r := e.create(b.Testcase)
			b.run, b.store = r, e.runStore(r)
			e.until(r, "sink has acked a quarter of its input", 5*time.Minute, func() bool {
				return b.acked("pass", "sink") >= b.Chunks/4
			})

			t.Cleanup(func() { e.scaleStorage(context.Background(), 1) })
			e.scaleStorage(t.Context(), 0)
			if r := e.get(r); r.Status.FinishedAt != nil {
				t.Fatalf("the run finished as %s before the storage was down; sink must be slower", r.Status.Phase)
			}
			t.Logf("storage down for %s", outage)
			time.Sleep(outage)
			// Without storage nothing can be committed, so the run cannot
			// have finished; it has to make it across the outage.
			if now := e.get(r); now.Status.FinishedAt != nil {
				t.Fatalf("the run finished as %s while the storage was down: %s", now.Status.Phase, now.Status.Message)
			}
			e.scaleStorage(t.Context(), 1)
			t.Log("storage back")

			b.finish(e, r, b.Chunks, sdk.DefaultPort)
			b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})
			for _, step := range []string{"source", "pass", "sink"} {
				rs, err := replicaStatus(b.run, step, 0)
				if err != nil {
					t.Fatal(err)
				}
				if rs.Restarts != 0 || rs.Attempt != 1 {
					t.Errorf("%s has %d restarts and attempt %d, want none and 1: %s", step, rs.Restarts, rs.Attempt, rs.Message)
				}
			}
			b.streams([2]string{"source", "pass"}, [2]string{"pass", "sink"})
		})
	}
}

// get reads the run as it is now.
func (e *env) get(r *v1alpha1.StreamRun) *v1alpha1.StreamRun {
	e.t.Helper()
	var now v1alpha1.StreamRun
	if err := e.c.Get(e.t.Context(), client.ObjectKeyFromObject(r), &now); err != nil {
		e.t.Fatal(err)
	}
	return &now
}

// scaleStorage scales the storage's Deployment to replicas and waits until
// it has that many ready pods and no others, so 0 means the storage is
// really gone.
func (e *env) scaleStorage(ctx context.Context, replicas int32) {
	e.t.Helper()
	st := storage(e.t)
	key := client.ObjectKey{Namespace: st.Namespace, Name: st.Deployment}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var d appsv1.Deployment
		if err := e.c.Get(ctx, key, &d); err != nil {
			return err
		}
		d.Spec.Replicas = &replicas
		return e.c.Update(ctx, &d)
	}); err != nil {
		e.t.Fatalf("scaling %s to %d: %v", st.Name, replicas, err)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		state, done := e.storageState(ctx, key, replicas)
		if done {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%s did not reach %d replicas within 3 minutes: %s", st.Name, replicas, state)
		}
		time.Sleep(time.Second)
	}
}

func (e *env) storageState(ctx context.Context, key client.ObjectKey, replicas int32) (string, bool) {
	var d appsv1.Deployment
	if err := e.c.Get(ctx, key, &d); err != nil {
		return err.Error(), false
	}
	var pods corev1.PodList
	if err := e.c.List(ctx, &pods, client.InNamespace(key.Namespace), client.MatchingLabels(d.Spec.Selector.MatchLabels)); err != nil {
		return err.Error(), false
	}
	state := fmt.Sprintf("%d pods, %d ready", len(pods.Items), d.Status.ReadyReplicas)
	return state, int32(len(pods.Items)) == replicas && d.Status.ReadyReplicas == replicas
}
