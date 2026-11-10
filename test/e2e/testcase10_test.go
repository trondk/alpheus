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
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// The controller as hack/e2e.sh installs it.
const (
	controllerNamespace = "alpheus-system"
	controllerName      = "alpheus-controller"
)

// TestTestcase10 deletes the controller's pod twice in the middle of a run.
//
// Flow: as testcase02: source → left (3) and right (2) → sink.
//
// What it tests: that a running run needs neither the controller nor its
// signal hub: the agents poll the manifests (GO-40), and the new controller
// takes over the run.
//
// Done to the run: the controller's pod, with the signal hub in it, is
// deleted when sink has read a quarter of its input, and again at three
// quarters.
//
// Passes when: the run is Succeeded with every chunk once, exact counts and
// no restarts, and its status never went backwards.
func TestTestcase10(t *testing.T) {
	b := newBench(t, "testcase10", map[string]int{"left": 3, "right": 2})
	e := setup(t)
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)

	watch := e.watchStatus(r)
	// sink reads left's replica 0 stream, a third of the chunks.
	perReplica := b.Chunks / b.Replicas["left"]
	for _, part := range []int{1, 3} {
		at := perReplica * part / 4
		e.until(r, fmt.Sprintf("sink has acked %d of left's first stream", at), 5*time.Minute, func() bool {
			return b.acked("left", "sink") >= at
		})
		b.Kills = append(b.Kills, e.killController()...)
		e.waitController()
	}

	b.finish(e, r, b.Chunks, "left", "right")
	watch.stop()
	b.counts(map[string]int{"source": b.Chunks, "left": b.Chunks, "right": b.Chunks, "sink": 2 * b.Chunks})
	for _, ss := range b.run.Status.Steps {
		for _, rs := range ss.Replicas {
			if rs.Restarts != 0 || rs.Attempt != 1 {
				t.Errorf("%s replica %d has %d restarts and attempt %d, want none and 1: %s", ss.Name, rs.Index, rs.Restarts, rs.Attempt, rs.Message)
			}
		}
	}
	b.acks("source", "left")
	b.acks("source", "right")
	b.consumed("left", "right")
	b.noOrphans()
	for _, p := range watch.problems() {
		t.Error(p)
	}
}

// killController deletes the controller's pods and returns their names.
func (e *env) killController() []string {
	e.t.Helper()
	var list corev1.PodList
	if err := e.c.List(e.t.Context(), &list, client.InNamespace(controllerNamespace),
		client.MatchingLabels{"app.kubernetes.io/name": controllerName}); err != nil {
		e.t.Fatal(err)
	}
	var killed []string
	for _, p := range list.Items {
		if p.DeletionTimestamp.IsZero() {
			if name := e.kill(p); name != "" {
				killed = append(killed, name)
			}
		}
	}
	if len(killed) == 0 {
		e.t.Fatal("no controller pod to delete")
	}
	return killed
}

// waitController waits until the controller's Deployment is ready again.
func (e *env) waitController() {
	e.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var d appsv1.Deployment
		err := e.c.Get(e.t.Context(), client.ObjectKey{Namespace: controllerNamespace, Name: controllerName}, &d)
		if err == nil && d.Status.ReadyReplicas >= 1 && d.Status.UpdatedReplicas == d.Status.Replicas && d.Status.Replicas == *d.Spec.Replicas {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("the controller was not ready again within 3 minutes: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// statusWatch reads a run's status a few times a second and notes when it
// goes backwards: a counter that falls, or a finished step or run that is
// not finished any more.
type statusWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	found  []string
}

func (e *env) watchStatus(r *v1alpha1.StreamRun) *statusWatch {
	ctx, cancel := context.WithCancel(context.Background())
	w := &statusWatch{cancel: cancel, done: make(chan struct{})}
	key := client.ObjectKeyFromObject(r)
	go func() {
		defer close(w.done)
		counts := map[string]int64{}
		finished, runFinished := map[string]bool{}, false
		for ctx.Err() == nil {
			var now v1alpha1.StreamRun
			if err := e.c.Get(ctx, key, &now); err == nil {
				for _, ss := range now.Status.Steps {
					for name, v := range ss.Counts {
						k := ss.Name + "." + name
						if v < counts[k] {
							w.note(fmt.Sprintf("%s fell from %d to %d", k, counts[k], v))
						}
						counts[k] = v
					}
					done := ss.Phase == v1alpha1.StepSucceeded || ss.Phase == v1alpha1.StepFailed
					if finished[ss.Name] && !done {
						w.note(fmt.Sprintf("step %s went from finished back to %s", ss.Name, ss.Phase))
					}
					finished[ss.Name] = finished[ss.Name] || done
				}
				if runFinished && now.Status.FinishedAt == nil {
					w.note("the run went from finished back to " + string(now.Status.Phase))
				}
				runFinished = runFinished || now.Status.FinishedAt != nil
			}
			select {
			case <-ctx.Done():
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *statusWatch) note(s string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.found) < 20 {
		w.found = append(w.found, s)
	}
}

func (w *statusWatch) stop() {
	w.cancel()
	<-w.done
}

func (w *statusWatch) problems() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.found
}
