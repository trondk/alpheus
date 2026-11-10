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
	"errors"
	"testing"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// TestTestcase02 runs fan-out from one output to two steps, and fan-in to
// two ports of one step.
//
// Flow: source → left (3 replicas) and right (2 replicas) → sink, with left
// and right on sink's two ports.
//
// What it tests: that every consumer of an output gets every chunk (GO-36),
// that chunks are spread over the replicas by number (GO-76), and that a
// step with two inputs ends only when both have ended (GO-80, GO-95).
//
// Done to the run: nothing.
//
// Passes when: the run is Succeeded; sink has every chunk once on each port,
// unchanged; left's and right's replicas acked each chunk once, at replica n
// mod replicas; every stream has EOS and was consumed to its last chunk; and
// the counts are exact.
func TestTestcase02(t *testing.T) {
	b := newBench(t, "testcase02", map[string]int{"left": 3, "right": 2})
	b.start(setup(t), b.Chunks, "left", "right")
	b.counts(map[string]int{"source": b.Chunks, "left": b.Chunks, "right": b.Chunks, "sink": 2 * b.Chunks})

	// Fan-out: each consuming step has its own ack files on source's
	// stream, and its replicas together acked every chunk once.
	b.acks("source", "left")
	b.acks("source", "right")

	// Fan-in: the sink committed its result with all five incoming
	// streams consumed.
	b.consumed("left", "right")
	b.report()
}

func acks(t *testing.T, s store.Store, key string) []int64 {
	t.Helper()
	data, err := store.ReadAll(context.Background(), s, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	a, err := chunk.ParseAcks(data)
	if err != nil {
		t.Fatal(err)
	}
	return a.Acked
}

func manifest(t *testing.T, s store.Store, key string) *chunk.Manifest {
	t.Helper()
	data, err := store.ReadAll(context.Background(), s, key)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	m, err := chunk.ParseManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
