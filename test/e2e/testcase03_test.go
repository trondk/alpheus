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
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase03 runs a step with ten replicas, where one gets chunks but
// writes none, and keeps the result in a bucket of its own.
//
// Flow: source → filter (10 replicas) → sink. filter drops every chunk whose
// number is a multiple of its replica count, so replica 0 writes nothing.
//
// What it tests: that a replica without output still ends its stream with
// EOS (GO-80); that chunks are spread over the replicas by number (GO-76);
// and that results go to spec.storage.resultsBucket, read with a Secret of
// their own (GO-129).
//
// Done to the run: nothing.
//
// Passes when: the run is Succeeded; sink has exactly the chunks that are
// not dropped, once and unchanged; replica 0 acked the multiples of 10,
// wrote none and has last 0; status.results names alpheus-results and
// alpheus-s3-results; nothing of the result is in the main bucket; and the
// counts are exact.
func TestTestcase03(t *testing.T) {
	b := newBench(t, "testcase03", map[string]int{"filter": 10})
	replicas := b.Replicas["filter"]
	if replicas < 2 || b.Chunks < replicas {
		t.Fatalf("testcase03 needs at least 2 filter replicas and as many chunks, not %d and %d", replicas, b.Chunks)
	}
	dropped := b.Chunks / replicas
	kept := b.Chunks - dropped
	b.start(setup(t), kept, sdk.DefaultPort)
	b.counts(map[string]int{"source": b.Chunks, "filter": kept, "sink": kept})
	if got := stepStatus(t, b.run, "filter").Counts["dropped"]; got != int64(dropped) {
		t.Errorf("filter dropped %d chunks, want %d", got, dropped)
	}

	// The replicas' ack files split the chunks between them, chunk n at
	// replica n mod 10 (GO-84), so replica 0 got exactly the dropped ones.
	acked := b.acks("source", "filter")
	if len(acked[0]) != dropped {
		t.Errorf("filter replica 0 acked %v, want the %d multiples of %d", acked[0], dropped, replicas)
	}

	// Replica 0's outgoing stream ended with EOS and no chunks, every other
	// replica's with one chunk per chunk it acked, and the sink still
	// reached EOS with all of them consumed (GO-85).
	out := b.consumed("filter")["filter"]
	for r, m := range out {
		want := int64(len(acked[r]))
		if r == 0 {
			want = 0
		}
		if m.Count() != want {
			t.Errorf("filter replica %d wrote %d chunks, want %d", r, m.Count(), want)
		}
	}
	if m := out[0]; m.Last == nil || *m.Last != 0 {
		t.Errorf("filter replica 0's manifest has last %v, want 0", m.Last)
	}
	// The result is in its own bucket, and status.results names a key
	// that can read it, as GO-82 checks, but not write it (GO-129).
	res := b.run.Status.Results[0]
	if res.Bucket != "alpheus-results" || res.SecretRef != "alpheus-s3-results" {
		t.Errorf("status.results names bucket %s and Secret %s, want alpheus-results and alpheus-s3-results", res.Bucket, res.SecretRef)
	}
	if err := b.results.Put(t.Context(), res.Prefix+"written-by-a-reader", strings.NewReader("x"), 1); err == nil {
		t.Error("the results key could write")
	}
	for key, err := range b.store.List(t.Context(), "results/") {
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(key, string(b.run.UID)) {
			t.Errorf("%s is in the main bucket", key)
		}
	}
	b.report()
}
