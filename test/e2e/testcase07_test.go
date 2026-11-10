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

// TestTestcase07 runs the Go and Python component templates in agent mode,
// the way users run them, and kills both.
//
// Flow: source → go (templates/component-go, 2 replicas) → python
// (templates/component-python) → sink. Both templates have an empty match,
// so every chunk passes unchanged.
//
// What it tests: that the templates work as real steps with the SDKs, and go
// on correctly after a restart (GO-37, GO-70).
//
// Done to the run: go's replica 0 is killed when it has committed a quarter
// of its chunks, and python when it has committed half.
//
// Passes when: the run is Succeeded with every chunk once and unchanged at
// sink; exact counters in both templates; one restart of each; and no chunk
// objects left that were never committed.
func TestTestcase07(t *testing.T) {
	b := newBench(t, "testcase07", map[string]int{"go": 2})
	e := setup(t)
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)

	// go's replica 0 gets half the chunks.
	e.until(r, "go has committed a quarter of its chunks", 5*time.Minute, func() bool {
		return b.committed("go", "python") >= int64(b.Chunks/8)
	})
	b.Kills = append(b.Kills, e.killReplica(r, "go", 0))
	e.until(r, "python has committed half its chunks", 5*time.Minute, func() bool {
		return b.committed("python", "sink") >= int64(b.Chunks/2)
	})
	b.Kills = append(b.Kills, e.killReplica(r, "python", 0))

	b.finish(e, r, b.Chunks, sdk.DefaultPort)
	b.counts(map[string]int{"source": b.Chunks, "sink": b.Chunks})

	// The templates count lines, not chunks. Every line passes, and a
	// restarted replica counts nothing twice (GO-115), so both steps count
	// the same lines in and out.
	goCounts, pyCounts := stepStatus(t, b.run, "go").Counts, stepStatus(t, b.run, "python").Counts
	if goCounts["lines_in"] == 0 || goCounts["lines_out"] != goCounts["lines_in"] {
		t.Errorf("go counted %v, want as many lines out as in", goCounts)
	}
	if pyCounts["lines_in"] != goCounts["lines_in"] || pyCounts["lines_out"] != goCounts["lines_out"] {
		t.Errorf("python counted %v, go %v; want the same", pyCounts, goCounts)
	}

	for step, restarts := range map[string]int32{"source": 0, "go": 1, "python": 1, "sink": 0} {
		rs, err := replicaStatus(b.run, step, 0)
		if err != nil {
			t.Fatal(err)
		}
		if rs.Restarts != restarts {
			t.Errorf("%s has %d restarts, want %d", step, rs.Restarts, restarts)
		}
	}

	// go's two streams together have every chunk, and each ended.
	var total int64
	for replica := range b.Replicas["go"] {
		s := chunk.EdgeStream(string(b.run.UID), chunk.EdgeName("go", chunk.DefaultOutput, "python"), replica)
		m, err := manifestOf(t.Context(), b.store, s)
		if err != nil {
			t.Fatal(err)
		}
		if !m.EOS {
			t.Errorf("%s has no eos", s.ID)
		}
		total += m.Count()
	}
	if total != int64(b.Chunks) {
		t.Errorf("go committed %d chunks, want %d", total, b.Chunks)
	}
	b.streams([2]string{"source", "go"}, [2]string{"python", "sink"})
	b.report()
}
