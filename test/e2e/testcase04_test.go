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
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase04 kills one step's pod halfway through its work, once for
// each step.
//
// Flow: source → pass → sink, run three times.
//
// What it tests: that each kind of step goes on correctly after a restart: a
// deterministic source goes on from the next chunk after checking the chunks
// it makes again (GO-92); pass goes on from the first unprocessed chunk and
// deletes output it never committed (GO-37, GO-70); and a stateful sink
// reads all its input again (GO-95, GO-100).
//
// Done to the run: the first time source is killed when it has committed a
// quarter of its chunks; the second time pass when it has committed half its
// output; the third time sink when it has acked half its input, before EOS.
//
// Passes when: each run is Succeeded with every chunk once and unchanged at
// sink, exact counts, one restart of the killed step, and no chunk objects
// left that were never committed.
func TestTestcase04(t *testing.T) {
	for _, tc := range []struct {
		step string
		// ready says when to kill the step.
		ready func(b *bench, at int) bool
		what  string
	}{
		{"source", func(b *bench, at int) bool { return b.committed("source", "pass") >= int64(at) }, "source has committed a quarter of its chunks"},
		{"pass", func(b *bench, at int) bool { return b.committed("pass", "sink") >= int64(2*at) }, "pass has committed half its output"},
		{"sink", func(b *bench, at int) bool { return b.acked("pass", "sink") >= 2*at }, "sink has acked half its input"},
	} {
		t.Run(tc.step, func(t *testing.T) {
			b := newBench(t, "testcase04", nil)
			e := setup(t)
			r := e.create(b.Testcase)
			b.run, b.store = r, e.runStore(r)
			e.until(r, tc.what, 5*time.Minute, func() bool { return tc.ready(b, b.Chunks/4) })
			b.Kills = append(b.Kills, e.killReplica(r, tc.step, 0))

			b.finish(e, r, b.Chunks, sdk.DefaultPort)
			// Exact, although a chunk was processed twice (GO-115).
			b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})
			for _, step := range []string{"source", "pass", "sink"} {
				rs, err := replicaStatus(b.run, step, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := int32(0)
				if step == tc.step {
					want = 1
				}
				if rs.Restarts != want || rs.Attempt != want+1 {
					t.Errorf("%s has %d restarts and attempt %d, want %d and %d", step, rs.Restarts, rs.Attempt, want, want+1)
				}
			}
			b.streams([2]string{"source", "pass"}, [2]string{"pass", "sink"})
			b.report()
		})
	}
}

// committed returns how many chunks from's replica 0 has committed on its
// stream to to.
func (b *bench) committed(from, to string) int64 {
	return count(b.t.Context(), b.store, chunk.EdgeStream(string(b.run.UID), chunk.EdgeName(from, chunk.DefaultOutput, to), 0))
}

// acked returns how many chunks to's replica 0 has acked on from's
// replica 0's stream.
func (b *bench) acked(from, to string) int {
	s := chunk.EdgeStream(string(b.run.UID), chunk.EdgeName(from, chunk.DefaultOutput, to), 0)
	return ackCount(b.t.Context(), b.store, s.AckKey(to, 0))
}

// streams checks that each edge's stream from replica 0 has every chunk
// numbered 1 to b.Chunks and ends with EOS, and that the run has no chunk
// objects that were never committed.
func (b *bench) streams(edges ...[2]string) {
	t := b.t
	t.Helper()
	for _, edge := range edges {
		s := chunk.EdgeStream(string(b.run.UID), chunk.EdgeName(edge[0], chunk.DefaultOutput, edge[1]), 0)
		m, err := manifestOf(t.Context(), b.store, s)
		if err != nil {
			t.Fatal(err)
		}
		if !m.EOS || m.Count() != int64(b.Chunks) {
			t.Errorf("%s: eos %v and %d chunks, want eos and %d", s.ID, m.EOS, m.Count(), b.Chunks)
		}
	}
	b.noOrphans()
}

// noOrphans checks that the run left no chunk objects that were never
// committed.
func (b *bench) noOrphans() {
	b.t.Helper()
	o, err := orphans(b.t.Context(), b.store, string(b.run.UID))
	if err != nil {
		b.t.Fatal(err)
	}
	if len(o) > 0 {
		b.t.Errorf("chunks that were never committed: %v", o)
	}
}
