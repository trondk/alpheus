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

	"alpheus-dev.org/alpheus/sdk"
)

// TestTestcase01 measures the system on the simplest flow: the time until
// every step runs, the latency of each chunk through two hops in object
// storage, and the throughput.
//
// Flow: source → pass → sink, with BENCH_PASS_REPLICAS replicas of pass
// (default 1).
//
// What it tests: that chunks go through the storage once and unchanged, and
// how fast (SPEC 13).
//
// Done to the run: nothing.
//
// Passes when: the run is Succeeded; sink, which checks every byte, has
// every chunk once and unchanged; and each step counted every chunk. The
// measurements go to the test report.
func TestTestcase01(t *testing.T) {
	b := newBench(t, "testcase01", map[string]int{"pass": 1})
	b.start(setup(t), b.Chunks, sdk.DefaultPort)
	b.counts(map[string]int{"source": b.Chunks, "pass": b.Chunks, "sink": b.Chunks})
	b.report()
}
