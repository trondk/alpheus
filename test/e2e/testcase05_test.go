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

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase05 runs a slow step and watches backpressure while the run
// goes.
//
// Flow: source → slow (2 replicas) → sink, where slow waits before each
// chunk.
//
// What it tests: that no consumer replica ever has more than maxUnacked
// chunks waiting that it has not acked (GO-35), and that the slowest replica
// sets the pace (GO-94).
//
// Done to the run: nothing; the test samples source's manifest and slow's
// ack files while it runs.
//
// Passes when: the run is Succeeded with every chunk once and exact counts;
// no sample shows a slow replica with more than maxUnacked chunks not acked;
// and the stream took at least chunks × delay / replicas.
func TestTestcase05(t *testing.T) {
	b := newBench(t, "testcase05", map[string]int{"slow": 2})
	delay := setting(t, "BENCH_SLOW_MS", 500)
	replicas := b.Replicas["slow"]
	e := setup(t)
	r := e.create(b.Testcase)
	b.run, b.store = r, e.runStore(r)

	ctx, stop := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	var s sampler
	wg.Go(func() { s.run(ctx, b, replicas) })
	b.finish(e, r, b.Chunks, sdk.DefaultPort)
	stop()
	wg.Wait()

	b.counts(map[string]int{"source": b.Chunks, "slow": b.Chunks, "sink": b.Chunks})
	for _, v := range s.violations {
		t.Error(v)
	}
	for rep, most := range s.most {
		// The source is fast, so each replica must have reached the
		// limit: else backpressure was never tested.
		if most != b.MaxUnacked {
			t.Errorf("slow replica %d had at most %d chunks not acked, want %d", rep, most, b.MaxUnacked)
		}
	}
	t.Logf("%d samples, at most %v chunks not acked per replica", s.samples, s.most)
	if least := float64(b.Chunks*delay) / 1000 / float64(replicas); b.StreamSeconds < least {
		t.Errorf("the stream took %.2f s, want at least %.2f s", b.StreamSeconds, least)
	}
	b.report()
}

// sampler watches the chunks source has committed to slow and how many
// each slow replica has acked.
type sampler struct {
	samples    int
	most       []int
	violations []string
}

func (s *sampler) run(ctx context.Context, b *bench, replicas int) {
	stream := chunk.EdgeStream(string(b.run.UID), chunk.EdgeName("source", chunk.DefaultOutput, "slow"), 0)
	s.most = make([]int, replicas)
	for ctx.Err() == nil {
		// The manifest first: acks read after it can only be newer, so a
		// sample can count too few chunks not acked, never too many.
		committed := count(ctx, b.store, stream)
		for r := range replicas {
			acked := ackCount(ctx, b.store, stream.AckKey("slow", r))
			assigned := 0
			for n := int64(1); n <= committed; n++ {
				if chunk.Assign(n, 0, replicas) == r {
					assigned++
				}
			}
			unacked := assigned - acked
			s.most[r] = max(s.most[r], unacked)
			if unacked > b.MaxUnacked {
				s.violations = append(s.violations, fmt.Sprintf("source had committed %d chunks and slow replica %d acked %d of its %d: %d not acked, over maxUnacked %d",
					committed, r, acked, assigned, unacked, b.MaxUnacked))
			}
		}
		s.samples++
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
}
