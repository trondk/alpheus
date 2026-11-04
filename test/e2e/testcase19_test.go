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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/cli"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

// TestTestcase19 runs testcase14's component with a bug in two replicas
// with failurePolicy continue (GO-97), as docs/dead-letters.md describes.
// pass fails on source's chunk 5, which is replica 1's, on both of its
// attempts; replica 1 then gives up, and replica 0 goes on with its own
// chunks. The run must end PartiallySucceeded with the reason
// FailedReplicas, and every chunk of source must be in the result once or
// counted in failedChunks, chunk 5 not in the result. The chunk it failed
// on is kept for a replay, and when the run has finished, every chunk the
// replica did not process is kept too, for alpheus replay --skipped.
func TestTestcase19(t *testing.T) {
	b := newBench(t, "testcase19", nil)
	if b.Chunks < 8 {
		t.Skipf("%d chunks; pass fails on chunk 5 and needs a few after it", b.Chunks)
	}
	if b.Chunks >= chunk.SegmentSize {
		t.Skipf("%d chunks; the test reads only unsealed manifests", b.Chunks)
	}
	e := setup(t)
	r := e.wait(e.create(b.Testcase), 10*time.Minute)
	st := r.Status
	t.Logf("%s: %s: %s", st.Phase, st.Reason, st.Message)
	if st.Phase != v1alpha1.RunPartiallySucceeded || st.Reason != v1alpha1.ReasonFailedReplicas {
		t.Fatalf("phase %s reason %s, want PartiallySucceeded with FailedReplicas", st.Phase, st.Reason)
	}

	pass := stepStatus(t, r, "pass")
	if len(pass.Replicas) != 2 {
		t.Fatalf("pass has %d replicas", len(pass.Replicas))
	}
	good, bad := pass.Replicas[0], pass.Replicas[1]
	if good.Phase != v1alpha1.StepSucceeded || good.FailedChunks != 0 {
		t.Errorf("replica 0 %+v, want succeeded", good)
	}
	if bad.Phase != v1alpha1.StepFailed || bad.Reason != v1alpha1.ReasonComponentFailed || bad.Restarts != 1 || bad.FailedChunks == 0 {
		t.Errorf("replica 1 %+v, want failed with ComponentFailed after one restart, with chunks not processed", bad)
	}

	// Every chunk of source is in the result once, or not processed.
	if len(st.Results) != 1 || st.Results[0].Step != "pass" {
		t.Fatalf("results %+v, want pass's output", st.Results)
	}
	res := st.Results[0]
	rst := e.storeFor(res)
	uid := string(r.UID)
	seen := map[int64]int{}
	for i := range 2 {
		m, err := manifestOf(t.Context(), rst, chunk.ResultStream(r.Name, uid, "pass", res.Output, i))
		if err != nil {
			t.Fatal(err)
		}
		if !m.EOS {
			t.Errorf("pass replica %d's result has not ended", i)
		}
		for _, c := range m.Chunks {
			if c.Src == nil {
				t.Fatalf("result chunk %d of replica %d says not where it came from", c.N, i)
			}
			if chunk.Assign(c.Src.N, c.Src.Replica, 2) != i {
				t.Errorf("source chunk %d is in replica %d's result", c.Src.N, i)
			}
			seen[c.Src.N]++
		}
	}
	for n, k := range seen {
		if k != 1 {
			t.Errorf("source chunk %d is %d times in the result", n, k)
		}
	}
	if seen[5] != 0 {
		t.Error("the chunk pass fails on is in the result")
	}
	if got := int64(len(seen)) + bad.FailedChunks; got != int64(b.Chunks) {
		t.Errorf("%d chunks in the result and %d not processed, want %d in all", len(seen), bad.FailedChunks, b.Chunks)
	}
	t.Logf("%d chunks in the result, %d not processed", len(seen), bad.FailedChunks)

	// The last attempt is kept with the chunk it failed on first.
	rs := e.runStore(r)
	data, err := store.ReadAll(t.Context(), rs, chunk.FailurePrefix(r.Name, uid, "pass", 1, 2)+"failure.json")
	var f replay.Failure
	if err == nil {
		err = json.Unmarshal(data, &f)
	}
	if err != nil || len(f.Inputs) == 0 || f.Inputs[0].N != 5 || f.Reason != v1alpha1.ReasonComponentFailed {
		t.Fatalf("failure %+v (%v)", f, err)
	}

	// Every chunk replica 1 did not process is kept, and alpheus replay
	// --skipped fetches them all, checked against their SHA-256.
	sk, _, err := replay.Read(t.Context(), rs, chunk.SkippedPrefix(r.Name, uid, "pass", 1))
	if err != nil {
		t.Fatal(err)
	}
	if !sk.Skipped || sk.NotKept != 0 || int64(len(sk.Inputs)) != bad.FailedChunks {
		t.Fatalf("skipped: %d inputs, %d not kept, want %d", len(sk.Inputs), sk.NotKept, bad.FailedChunks)
	}
	for _, in := range sk.Inputs {
		if seen[in.N] != 0 || chunk.Assign(in.N, in.ProducerReplica, 2) != 1 {
			t.Errorf("skipped chunk %d was processed, or was not replica 1's", in.N)
		}
	}
	if sk.Inputs[0].N != 5 {
		t.Errorf("the first skipped chunk is %d, want 5, the one pass failed on", sk.Inputs[0].N)
	}
	dir := t.TempDir()
	args := []string{"replay", r.Name, "pass", "-n", e.ns, "--skipped", "--with", "none", "--dir", dir}
	if v := os.Getenv("ALPHEUS_E2E_S3_ENDPOINT"); v != "" {
		args = append(args, "--endpoint", v)
	}
	if v := os.Getenv("ALPHEUS_E2E_S3_CA_FILE"); v != "" {
		args = append(args, "--ca-file", v)
	}
	var out bytes.Buffer
	env := cli.DefaultEnv()
	env.Stdout, env.Stderr = &out, &out
	if code := cli.Main(t.Context(), args, env); code != 0 {
		t.Fatalf("alpheus %s: exit %d\n%s", strings.Join(args, " "), code, out.String())
	}
	files, err := os.ReadDir(filepath.Join(dir, "in", "default"))
	if err != nil || len(files) != len(sk.Inputs) {
		t.Errorf("fetched %d skipped chunks (%v), want %d\n%s", len(files), err, len(sk.Inputs), out.String())
	}
}
