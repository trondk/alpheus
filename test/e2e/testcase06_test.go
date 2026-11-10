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
	"math/rand/v2"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// TestTestcase06 kills random pods of a run with fan-out and fan-in until it
// is done (GO-56).
//
// Flow: as testcase02: source → left (3) and right (2) → sink.
//
// What it tests: that a run survives repeated crashes of any of its pods,
// wherever they happen.
//
// Done to the run: a random pod is killed about every three seconds,
// BENCH_CHAOS_KILLS times (default 12) or until the run finishes.
// BENCH_CHAOS_SEED picks the pods, so a failure can be run again. The kills
// stop, so the run can finish: a stateful sink killed every few seconds
// would start over for ever.
//
// Passes when: the run is Succeeded with every chunk once on each of sink's
// ports, unchanged; exact counts; the same checks of acks and streams as
// testcase02; and no chunk objects left that were never committed.
func TestTestcase06(t *testing.T) {
	b := newBench(t, "testcase06", map[string]int{"left": 3, "right": 2})
	seed := setting(t, "BENCH_CHAOS_SEED", int(time.Now().UnixNano()%1_000_000+1))
	kills := setting(t, "BENCH_CHAOS_KILLS", 12)
	t.Logf("BENCH_CHAOS_SEED=%d", seed)
	rnd := rand.New(rand.NewPCG(uint64(seed), 0))
	e := setup(t)
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)

	for len(b.Kills) < kills {
		time.Sleep(time.Duration(2000+rnd.IntN(2000)) * time.Millisecond)
		var now v1alpha1.StreamRun
		if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(r), &now); err != nil {
			t.Fatal(err)
		}
		if now.Status.FinishedAt != nil {
			break
		}
		if pods := e.pods(r, client.MatchingLabels{}); len(pods) > 0 {
			if name := e.kill(pods[rnd.IntN(len(pods))]); name != "" {
				b.Kills = append(b.Kills, name)
			}
		}
	}

	b.finish(e, r, b.Chunks, "left", "right")
	b.counts(map[string]int{"source": b.Chunks, "left": b.Chunks, "right": b.Chunks, "sink": 2 * b.Chunks})
	b.acks("source", "left")
	b.acks("source", "right")
	b.consumed("left", "right")
	b.noOrphans()
	if len(b.Kills) == 0 {
		t.Error("the run finished before any pod was killed")
	}
	b.report()
}
