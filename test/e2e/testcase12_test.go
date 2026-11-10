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
	"io"
	"strings"
	"testing"
	"time"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// TestTestcase12 runs out of space in the middle of a run: the bucket has a
// quota of 20 MiB, far less than the run needs.
//
// Flow: source → sink.
//
// What it tests: that a full bucket fails the run instead of being waited
// for as an outage (GO-136), that a restart is not tried, since it would
// find the bucket just as full (GO-77), and that what was committed still
// holds (GO-115).
//
// Done to the run: nothing; the bucket's quota is reached during the stream.
// Garage refuses the first write over the quota, so source fails. MinIO
// counts a bucket's usage now and then, so it lets writes through until it
// has counted them, and then refuses one in whichever step writes next.
//
// Passes when: the run is Failed, with a reason that says the bucket is full
// and not that the storage is unreachable; on Garage it failed in source;
// the manifest has some but not all of the chunks; no step restarted; every
// chunk the manifests list exists and is unchanged; and source's counters
// match its manifest. The run is expected to fail: the test passes when it
// fails this way.
func TestTestcase12(t *testing.T) {
	b := newBench(t, "testcase12", nil)
	e := setup(t)
	st := storage(t)
	quota := e.storeFor(v1alpha1.ResultStatus{
		Endpoint: st.Endpoint, Bucket: "alpheus-quota", Region: st.Region, SecretRef: "alpheus-s3",
	})
	// The bucket is this test's alone; it starts and ends empty.
	empty := func(ctx context.Context) {
		if err := store.DeletePrefix(ctx, quota, ""); err != nil {
			t.Errorf("emptying alpheus-quota: %v", err)
		}
	}
	empty(t.Context())
	t.Cleanup(func() { empty(context.Background()) })

	r := e.wait(e.create(b.Testcase), 10*time.Minute)
	t.Logf("reason: %s", r.Status.Message)
	if r.Status.Phase != v1alpha1.RunFailed {
		t.Fatalf("phase %s, want Failed", r.Status.Phase)
	}
	if strings.Contains(r.Status.Message, "storage unreachable") {
		t.Errorf("a full bucket was taken for an outage: %s", r.Status.Message)
	}
	if !strings.Contains(strings.ToLower(r.Status.Message), "quota") {
		t.Errorf("the reason does not say the bucket is full: %s", r.Status.Message)
	}
	if r.Status.Reason != v1alpha1.ReasonStorageFull {
		t.Errorf("reason %q, want %s (GO-138)", r.Status.Reason, v1alpha1.ReasonStorageFull)
	}
	if !st.LateQuota && r.Status.FailedStep != "source" {
		t.Errorf("failed in step %q, want source, whose writes go over the quota", r.Status.FailedStep)
	}

	// No restarts: a new attempt would find the bucket just as full (GO-77).
	for _, ss := range r.Status.Steps {
		for _, rs := range ss.Replicas {
			if rs.Restarts != 0 {
				t.Errorf("%s replica %d restarted %d times on a full bucket", ss.Name, rs.Index, rs.Restarts)
			}
		}
	}

	stream := chunk.EdgeStream(string(r.UID), chunk.EdgeName("source", chunk.DefaultOutput, "sink"), 0)
	m, err := manifestOf(t.Context(), quota, stream)
	if err != nil {
		t.Fatal(err)
	}
	if n := m.Count(); n == 0 || n > int64(b.Chunks) || !st.LateQuota && n == int64(b.Chunks) {
		t.Fatalf("the manifest has %d of %d chunks; the quota should stop it in between", n, b.Chunks)
	}
	t.Logf("%s committed %d of %d chunks before the quota stopped the run", st.Name, m.Count(), b.Chunks)
	for _, c := range m.Chunks {
		body, err := quota.Get(t.Context(), stream.ChunkKey(c.N, c.Ext))
		if err != nil {
			t.Errorf("chunk %d is in the manifest but not in the bucket: %v", c.N, err)
			continue
		}
		if _, err := io.Copy(io.Discard, chunk.NewVerifyingReader(body, c)); err != nil {
			t.Errorf("chunk %d: %v", c.N, err)
		}
		body.Close()
	}
	if got := stepStatus(t, r, "source").Counts["chunks"]; got != m.Count() {
		t.Errorf("source counted %d chunks, and its manifest has %d", got, m.Count())
	}
}
