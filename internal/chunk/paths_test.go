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

package chunk

import (
	"slices"
	"testing"
)

// TestEdgeName checks edge names: <from>-<to>, with the output only when it
// is not the default one.
func TestEdgeName(t *testing.T) {
	for _, tc := range []struct{ from, output, to, want string }{
		{"read", "", "filter", "read-filter"},
		{"read", "default", "filter", "read-filter"},
		{"filter", "rejects", "audit", "filter.rejects-audit"},
	} {
		if got := EdgeName(tc.from, tc.output, tc.to); got != tc.want {
			t.Errorf("EdgeName(%q, %q, %q) = %q, want %q", tc.from, tc.output, tc.to, got, tc.want)
		}
	}
}

// TestStreamKeys checks the object keys of a stream: its prefix, chunks,
// manifest, segments and ack files, and the prefix of a result.
func TestStreamKeys(t *testing.T) {
	s := EdgeStream("3f0c9a6e", "read-filter", 2)
	for _, tc := range []struct{ got, want string }{
		{s.ID, "read-filter/2"},
		{s.Prefix(), "runs/3f0c9a6e/read-filter/2/"},
		{s.ChunkKey(17, "csv"), "runs/3f0c9a6e/read-filter/2/part-000017.csv"},
		{s.ManifestKey(), "runs/3f0c9a6e/read-filter/2/manifest.json"},
		{s.SegmentKey(1), "runs/3f0c9a6e/read-filter/2/manifest-0001.json"},
		{s.AckKey("sum", 0), "runs/3f0c9a6e/read-filter/2/acks-sum-0.json"},
		{RunPrefix("3f0c9a6e"), "runs/3f0c9a6e/"},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}

	r := ResultStream("sales-dk", "3f0c9a6e", "sum", "result", 0)
	if r.ID != "sum.result/0" || r.Prefix() != "results/sales-dk/3f0c9a6e/sum/result/0/" {
		t.Errorf("result stream: ID %q, prefix %q", r.ID, r.Prefix())
	}
}

// Every chunk goes to exactly one replica, the same one each time.
func TestAssignPartitions(t *testing.T) {
	const replicas = 10
	seen := map[int64]int{}
	for r := range replicas {
		for n := int64(1); n <= 100; n++ {
			if Assign(n, 0, replicas) == r {
				if prev, ok := seen[n]; ok {
					t.Fatalf("chunk %d assigned to replicas %d and %d", n, prev, r)
				}
				seen[n] = r
			}
		}
	}
	if len(seen) != 100 {
		t.Fatalf("%d of 100 chunks assigned", len(seen))
	}
}

// testcase03 relies on replica 0 getting exactly the chunks filter drops:
// 10, 20 ... 100 (SPEC 12.1).
func TestAssignTestcase03(t *testing.T) {
	var got []int64
	for n := int64(1); n <= 100; n++ {
		if Assign(n, 0, 10) == 0 {
			got = append(got, n)
		}
	}
	want := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	if !slices.Equal(got, want) {
		t.Errorf("replica 0 gets %v, want %v", got, want)
	}
}

// With many producer replicas that each make a few chunks, the producer's
// number spreads the work instead of loading the first replicas (GO-76).
func TestAssignSpreadsProducers(t *testing.T) {
	const producers, consumers, chunks = 10, 10, 3
	load := make([]int, consumers)
	for p := range producers {
		for n := int64(1); n <= chunks; n++ {
			load[Assign(n, p, consumers)]++
		}
	}
	for r, l := range load {
		if l != producers*chunks/consumers {
			t.Errorf("replica %d gets %d chunks, want %d (load %v)", r, l, producers*chunks/consumers, load)
		}
	}
}

// TestAssignOneReplica checks that a step with one replica, or none given,
// gets every chunk on replica 0.
func TestAssignOneReplica(t *testing.T) {
	for _, replicas := range []int{0, 1} {
		if got := Assign(7, 3, replicas); got != 0 {
			t.Errorf("Assign(7, 3, %d) = %d, want 0", replicas, got)
		}
	}
}

// TestValidExt checks which file extensions a chunk may have: lower-case
// letters and digits, one part, at most 16 characters.
func TestValidExt(t *testing.T) {
	for ext, want := range map[string]bool{
		"csv": true, "parquet": true, "jsonl": true, "7z": true,
		"": false, "CSV": false, "tar.gz": false, "a_b": false, "abcdefghijklmnopq": false,
	} {
		if got := ValidExt(ext); got != want {
			t.Errorf("ValidExt(%q) = %v, want %v", ext, got, want)
		}
	}
}
