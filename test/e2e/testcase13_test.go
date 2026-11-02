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
	"time"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// TestTestcase13 gives sink 1 MiB of scratch and chunks of 4 MiB. Its agent
// must refuse the first chunk with an error that names the scratch folder,
// before the folder fills up and Kubernetes evicts the pod (GO-106).
func TestTestcase13(t *testing.T) {
	b := newBench(t, "testcase13", nil)
	if b.ChunkBytes <= 1<<20 {
		t.Skipf("chunks of %d bytes fit in 1 MiB of scratch", b.ChunkBytes)
	}
	e := setup(t)
	r := e.wait(e.create(b.Testcase), 10*time.Minute)
	t.Logf("reason: %s", r.Status.Message)
	if r.Status.Phase != v1alpha1.RunFailed || !strings.Contains(r.Status.Message, "does not fit in the scratch folder") {
		t.Fatalf("phase %s: %q, want Failed because a chunk does not fit in the scratch folder", r.Status.Phase, r.Status.Message)
	}
	if r.Status.Reason != v1alpha1.ReasonScratchFull || r.Status.FailedStep != "sink" {
		t.Errorf("reason %q in step %q, want %s in sink (GO-138)", r.Status.Reason, r.Status.FailedStep, v1alpha1.ReasonScratchFull)
	}
	for _, ss := range r.Status.Steps {
		for _, rs := range ss.Replicas {
			if strings.Contains(strings.ToLower(rs.Message), "evict") {
				t.Errorf("%s replica %d was evicted: %s", ss.Name, rs.Index, rs.Message)
			}
		}
	}
	if rs, err := replicaStatus(r, "sink", 0); err != nil || rs.Phase != v1alpha1.StepFailed || rs.Restarts != 1 {
		t.Errorf("sink %+v (%v), want failed after its one restart", rs, err)
	}
}
