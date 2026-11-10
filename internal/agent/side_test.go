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

package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
	"alpheus-dev.org/alpheus/sdk"
)

// The join of side inputs (GO-167): keys writes the lookup data, items the
// rows to join, and join looks each item up and fails on an unknown key,
// so a join that lacks part of its lookup data cannot pass.
const (
	keyChunks    = 3
	keysPerChunk = 4
	itemChunks   = 9
)

func keys(ctx context.Context, in sdk.Input, out sdk.Output) error {
	for c := range keyChunks {
		f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
		if err != nil {
			return err
		}
		for k := range keysPerChunk {
			fmt.Fprintf(f, "k%d v%d\n", c*keysPerChunk+k, c*keysPerChunk+k)
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func items(ctx context.Context, in sdk.Input, out sdk.Output) error {
	for n := range itemChunks {
		f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
		if err != nil {
			return err
		}
		fmt.Fprintf(f, "i%d k%d\n", n, n%(keyChunks*keysPerChunk))
		fmt.Fprintf(f, "i%d-b k%d\n", n, (n*5)%(keyChunks*keysPerChunk))
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// join reads the keys port to the end, then joins each item chunk. With
// crash > 0 it fails before the crash-th item chunk is done.
func join(crash int) sdk.Handler {
	return func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		lookup := map[string]string{}
		for c, err := range in.Port("keys").Chunks(ctx) {
			if err != nil {
				return err
			}
			f, err := c.Open()
			if err != nil {
				return err
			}
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				k, v, _ := strings.Cut(sc.Text(), " ")
				lookup[k] = v
			}
			f.Close()
			sdk.Count(ctx, "keys", keysPerChunk)
		}
		seen := 0
		for c, err := range in.Port("items").Chunks(ctx) {
			if err != nil {
				return err
			}
			if seen++; seen == crash {
				return errCrash
			}
			src, err := c.Open()
			if err != nil {
				return err
			}
			dst, err := out.Create(ctx, "joined", "txt")
			if err != nil {
				src.Close()
				return err
			}
			sc := bufio.NewScanner(src)
			for sc.Scan() {
				item, k, _ := strings.Cut(sc.Text(), " ")
				v, ok := lookup[k]
				if !ok {
					src.Close()
					return fmt.Errorf("item %s: unknown key %s, with %d keys", item, k, len(lookup))
				}
				fmt.Fprintf(dst, "%s %s\n", item, v)
			}
			src.Close()
			sdk.Count(ctx, "items", 2)
			if err := dst.Close(); err != nil {
				return err
			}
		}
		return nil
	}
}

func sideFlow(joins int, h sdk.Handler) []replica {
	base := Config{Run: testRun, Namespace: "default", UID: testUID, MaxUnacked: 2, Poll: Duration{50 * time.Millisecond}}
	k := base
	k.Step = "keys"
	k.Outputs = []OutputEdge{{Edge: "keys-join", Output: "default", To: "join", Replicas: joins, Side: true}}
	i := base
	i.Step = "items"
	i.Outputs = []OutputEdge{{Edge: "items-join", Output: "default", To: "join", Replicas: joins}}
	rs := []replica{{cfg: k, h: keys}, {cfg: i, h: items}}
	for r := range joins {
		j := base
		j.Step, j.Replica, j.Replicas = "join", r, joins
		j.Inputs = []InputEdge{
			{Edge: "items-join", From: "items", Replicas: 1, Port: "items"},
			{Edge: "keys-join", From: "keys", Replicas: 1, Port: "keys", Side: true},
		}
		rs = append(rs, replica{cfg: j, h: h})
	}
	return rs
}

// checkJoin checks that every item was joined exactly once across the
// join replicas, each replica read every key, and the counters are exact
// after any restart (GO-115).
func checkJoin(t *testing.T, st store.Store, joins int) {
	t.Helper()
	ctx := context.Background()
	var got []string
	for r := range joins {
		m := readManifest(t, st, chunk.ResultStream(testRun, testUID, "join", "joined", r))
		if !m.EOS {
			t.Errorf("join %d: no EOS", r)
		}
		if c := m.Consumed[chunk.EdgeStream(testUID, "keys-join", 0).ID]; c != keyChunks {
			t.Errorf("join %d: consumed %d of the side input, want %d", r, c, keyChunks)
		}
		for _, c := range m.Chunks {
			data, err := store.ReadAll(ctx, st, chunk.ResultStream(testRun, testUID, "join", "joined", r).ChunkKey(c.N, c.Ext))
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, strings.Fields(string(data))...)
		}
		if acked := readAcks(t, st, chunk.EdgeStream(testUID, "keys-join", 0).AckKey("join", r)); len(acked) != keyChunks {
			t.Errorf("join %d acked %v of the side input, want all %d", r, acked, keyChunks)
		}
		o := observations(t, st, "join", r)
		if o.Counts["keys"] != keyChunks*keysPerChunk {
			t.Errorf("join %d counted %d keys, want %d once", r, o.Counts["keys"], keyChunks*keysPerChunk)
		}
	}
	var joined []string
	for i := 0; i+1 < len(got); i += 2 {
		joined = append(joined, got[i])
	}
	slices.Sort(joined)
	if want := 2 * itemChunks; len(joined) != want || len(slices.Compact(slices.Clone(joined))) != want {
		t.Errorf("joined %d items (%v), want %d once each", len(joined), joined, want)
	}
}

// Every replica of the consumer gets every chunk of a side input, not
// its share of them.
func TestSideInputToEveryReplica(t *testing.T) {
	st := store.NewMemory()
	if err := run(t, st, nil, sideFlow(2, join(0))...); err != nil {
		t.Fatal(err)
	}
	checkJoin(t, st, 2)
}

// A new attempt gets the side input again and builds its lookup data
// anew, while the main input goes on from the first chunk not committed.
// With one replica it is the restart alone that is tested.
func TestSideInputAfterRestart(t *testing.T) {
	for _, joins := range []int{1, 2} {
		t.Run(fmt.Sprint(joins), func(t *testing.T) {
			st := store.NewMemory()
			rs := sideFlow(joins, join(0))
			crashed := slices.Clone(rs)
			crashed[2].h = join(3)
			if err := run(t, st, nil, crashed...); !errors.Is(err, errCrash) {
				t.Fatalf("first attempt: %v, want the crash", err)
			}
			for j := range rs {
				rs[j].cfg.Attempt = 2
			}
			if err := run(t, st, nil, rs...); err != nil {
				t.Fatal(err)
			}
			checkJoin(t, st, joins)
		})
	}
}

// A side input chunk can have no output of its own: a new attempt reads
// it again, so the output would be written twice.
func TestSideInputHasNoOutput(t *testing.T) {
	st := store.NewMemory()
	writes := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for _, err := range in.Port("keys").Chunks(ctx) {
			if err != nil {
				return err
			}
			if _, err := out.Create(ctx, "joined", "txt"); err != nil {
				return err
			}
		}
		return nil
	}
	err := run(t, st, nil, sideFlow(1, writes)...)
	if err == nil || !strings.Contains(err.Error(), "side input on port keys, which can have no output") {
		t.Fatalf("got %v", err)
	}
}
