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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// The agent's metrics carry the run, step and replica, and skip what is
// not a count (GO-51, GO-116).
func TestMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg, Config{Run: "sales", Step: "filter", Replica: 1})
	m.count("rows", 5)
	m.count("rows", -1)    // invalid; the agent fails for it, the metric skips it
	m.count("Bad Unit", 1) // likewise
	m.moved("in", 100)
	m.moved("out", 40)
	m.waited(1.5)
	m.Retried()
	m.entry("error")
	m.entry("debug")
	want := `
# HELP alpheus_component_count_total What the component counted, per unit, live (GO-116).
# TYPE alpheus_component_count_total counter
alpheus_component_count_total{replica="1",run="sales",step="filter",unit="rows"} 5
# HELP alpheus_bytes_total Bytes of chunks downloaded (in) and uploaded (out).
# TYPE alpheus_bytes_total counter
alpheus_bytes_total{direction="in",replica="1",run="sales",step="filter"} 100
alpheus_bytes_total{direction="out",replica="1",run="sales",step="filter"} 40
# HELP alpheus_component_log_entries_total Errors and warnings the component reported, per level (GO-117).
# TYPE alpheus_component_log_entries_total counter
alpheus_component_log_entries_total{level="error",replica="1",run="sales",step="filter"} 1
alpheus_component_log_entries_total{level="other",replica="1",run="sales",step="filter"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"alpheus_component_count_total", "alpheus_bytes_total", "alpheus_component_log_entries_total"); err != nil {
		t.Error(err)
	}
	if got := testutil.ToFloat64(m.backpressure); got != 1.5 {
		t.Errorf("backpressure %v", got)
	}
	if got := testutil.ToFloat64(m.retries); got != 1 {
		t.Errorf("retries %v", got)
	}
	var none *Metrics // an agent without metrics records nothing
	none.count("rows", 1)
	none.moved("in", 1)
	none.Retried()
}

// TestChunkDuration runs a pipeline and checks that every agent records
// one observation of alpheus_chunk_duration_seconds for each input chunk
// its component finished, from Next to Done, and none for a step without
// input (GO-121). The component does nothing for it.
func TestChunkDuration(t *testing.T) {
	const parts, filters = 12, 3
	reg := prometheus.NewRegistry()
	rs := pipeline(parts, filters)
	for i := range rs {
		rs[i].metrics = NewMetrics(reg, rs[i].cfg)
	}
	st := store.NewMemory()
	if err := run(t, st, nil, rs...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, parts, filters)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	perStep := map[string]uint64{}
	for _, f := range families {
		if f.GetName() != "alpheus_chunk_duration_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			step := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "step" {
					step = l.GetValue()
				}
			}
			h := m.GetHistogram()
			if h.GetSampleSum() < 0 {
				t.Errorf("%s: negative time %v", step, h.GetSampleSum())
			}
			perStep[step] += h.GetSampleCount()
		}
	}
	// filter's replicas read every chunk of generate between them; sum
	// reads every chunk filter's replicas committed.
	var written uint64
	for r := range filters {
		data, err := store.ReadAll(t.Context(), st, chunk.EdgeStream(testUID, "filter-sum", r).ManifestKey())
		if err != nil {
			t.Fatal(err)
		}
		m, err := chunk.ParseManifest(data)
		if err != nil {
			t.Fatal(err)
		}
		written += uint64(m.Count())
	}
	if perStep["filter"] != parts || perStep["sum"] != written || perStep["generate"] != 0 {
		t.Errorf("observations per step %v, want filter %d, sum %d and none for generate", perStep, parts, written)
	}
}
