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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/examples/meter-etl/meter"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// etl is what testcase08 measures, written as JSON to ALPHEUS_E2E_OUT.
type etl struct {
	Testcase       string    `json:"testcase"`
	Time           time.Time `json:"time"`
	Files          int       `json:"files"`
	RowsPerFile    int       `json:"rowsPerFile"`
	SplitBytes     int       `json:"splitBytes"`
	ExportReplicas int       `json:"exportReplicas"`
	SplitReplicas  int       `json:"splitReplicas"`
	CleanReplicas  int       `json:"cleanReplicas"`
	Rows           int64     `json:"rows"`
	RowsRejected   int64     `json:"rowsRejected"`
	RawBytes       int64     `json:"rawBytes"`
	GzipBytes      int64     `json:"gzipBytes"`
	Chunks         int64     `json:"chunks"`
	StartupSeconds float64   `json:"startupSeconds"`
	TotalSeconds   float64   `json:"totalSeconds"`
	// ProcessSeconds is from when every component runs until Succeeded;
	// the throughput is measured over it.
	ProcessSeconds float64 `json:"processSeconds"`
	RawMBPerSecond float64 `json:"rawMBPerSecond"`
	RowsPerSecond  float64 `json:"rowsPerSecond"`
	// StepSeconds is when each step finished, from when every component
	// runs: the last step to finish before the others shows the bottleneck.
	StepSeconds map[string]float64 `json:"stepSeconds"`
	Kills       []string           `json:"kills"`
}

// TestTestcase08 runs the flagship ETL flow of examples/meter-etl and
// checks that it is exact: every row generated is read once, the bad ones
// are rejected and logged, the rest are in the dataset, and the totals per
// region and hour are the ones the generator says. Unless ETL_KILL=0, it
// kills clean's replica 0 when clean has read a quarter of the rows. It
// measures the run, and deletes its results at the end, since the dataset
// is large.
func TestTestcase08(t *testing.T) {
	m := etl{
		Testcase:       "testcase08",
		Time:           time.Now().UTC(),
		Files:          setting(t, "ETL_FILES", 4),
		RowsPerFile:    setting(t, "ETL_ROWS", 2_000_000),
		SplitBytes:     setting(t, "ETL_SPLIT_BYTES", 16<<20),
		ExportReplicas: setting(t, "BENCH_EXPORT_REPLICAS", 1),
		SplitReplicas:  setting(t, "BENCH_SPLIT_REPLICAS", 1),
		CleanReplicas:  setting(t, "BENCH_CLEAN_REPLICAS", 4),
		Kills:          []string{},
	}
	setting(t, "BENCH_MAX_UNACKED", 4)
	g := meter.Gen{Files: m.Files, Rows: m.RowsPerFile, Seed: 8, BadPerMille: 5, Day: "2026-10-30"}
	want, err := g.Expected()
	if err != nil {
		t.Fatal(err)
	}

	e := setup(t)
	r := e.create("testcase08")
	st := e.runStore(r)
	if os.Getenv("ETL_KILL") != "0" {
		e.until(r, "clean has read a quarter of the rows", 10*time.Minute, func() bool {
			var now v1alpha1.StreamRun
			if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(r), &now); err != nil {
				t.Fatal(err)
			}
			for _, s := range now.Status.Steps {
				if s.Name == "clean" {
					return s.Counts["rows_in"] >= want.Rows/4
				}
			}
			return false
		})
		m.Kills = append(m.Kills, e.killReplica(r, "clean", 0))
	}

	r = e.wait(r, 30*time.Minute)
	if r.Status.Phase != v1alpha1.RunSucceeded {
		t.Fatalf("phase %s: %s", r.Status.Phase, r.Status.Message)
	}
	defer func() {
		// The dataset is as large as the input; don't leave it in the
		// bucket after the namespace is gone.
		if err := store.DeletePrefix(t.Context(), st, "results/"+r.Name+"/"+string(r.UID)+"/"); err != nil {
			t.Errorf("deleting the results: %v", err)
		}
	}()

	export, split, clean := stepStatus(t, r, "export"), stepStatus(t, r, "split"), stepStatus(t, r, "clean")
	if export.Counts["rows"] != want.Rows || split.Counts["rows"] != want.Rows || clean.Counts["rows_in"] != want.Rows {
		t.Errorf("rows: export %d, split %d, clean %d; want %d", export.Counts["rows"], split.Counts["rows"], clean.Counts["rows_in"], want.Rows)
	}
	if clean.Counts["rows_rejected"] != want.Bad || clean.Counts["rows_out"] != want.Rows-want.Bad {
		t.Errorf("clean rejected %d and kept %d, want %d and %d", clean.Counts["rows_rejected"], clean.Counts["rows_out"], want.Bad, want.Rows-want.Bad)
	}
	if clean.Errors+clean.ErrorsDropped != want.Bad {
		t.Errorf("clean logged %d errors and dropped %d, want %d in all", clean.Errors, clean.ErrorsDropped, want.Bad)
	}

	// The totals, read with status.results alone (GO-82).
	var totals, dataset *v1alpha1.ResultStatus
	for i, res := range r.Status.Results {
		switch {
		case res.Step == "aggregate":
			totals = &r.Status.Results[i]
		case res.Step == "clean" && res.Output == meter.DatasetOutput:
			dataset = &r.Status.Results[i]
		}
	}
	if totals == nil || dataset == nil {
		t.Fatalf("results %+v, want aggregate's totals and clean's dataset", r.Status.Results)
	}
	got, err := meter.ParseSums(bytes.NewReader(resultChunks(t, e.storeFor(*totals), totals.Prefix)))
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want.Sums) {
		t.Errorf("the totals differ from the generator's: %d keys, want %d", len(got), len(want.Sums))
	}

	// One dataset chunk for each chunk split made, each committed once.
	var splitChunks int64
	for _, edge := range r.Status.Edges {
		switch edge.Name {
		case "split-clean":
			splitChunks = edge.Chunks
		case "export-split":
			m.GzipBytes = edge.Bytes
		}
	}
	if dataset.Chunks != splitChunks || splitChunks == 0 {
		t.Errorf("the dataset has %d chunks, want one per split chunk, %d", dataset.Chunks, splitChunks)
	}
	if rs, err := replicaStatus(r, "clean", 0); err != nil {
		t.Fatal(err)
	} else if want := int32(len(m.Kills)); rs.Restarts != want {
		t.Errorf("clean/0 has %d restarts, want %d", rs.Restarts, want)
	}
	if o, err := orphans(t.Context(), st, string(r.UID)); err != nil {
		t.Fatal(err)
	} else if len(o) > 0 {
		t.Errorf("chunks that were never committed: %v", o)
	}

	m.Rows, m.RowsRejected, m.RawBytes, m.Chunks = want.Rows, want.Bad, export.Counts["bytes"], splitChunks
	if start, ok := e.lastStart(r); ok {
		m.StartupSeconds = start.Sub(r.CreationTimestamp.Time).Seconds()
	}
	m.TotalSeconds = r.Status.FinishedAt.Sub(r.CreationTimestamp.Time).Seconds()
	m.ProcessSeconds = m.TotalSeconds - m.StartupSeconds
	if m.ProcessSeconds <= 0 {
		t.Fatalf("no time between startup (%.1f s) and the end (%.1f s)", m.StartupSeconds, m.TotalSeconds)
	}
	if start, ok := e.lastStart(r); ok {
		m.StepSeconds = e.stepEnds(r, start)
	}
	m.RawMBPerSecond = float64(m.RawBytes) / 1e6 / m.ProcessSeconds
	m.RowsPerSecond = float64(m.Rows) / m.ProcessSeconds
	m.report(t)
}

func (m etl) report(t *testing.T) {
	t.Logf("testcase08: %d files of %d rows, %s raw (%s gzipped), %d chunks of %s, replicas: export %d, split %d, clean %d",
		m.Files, m.RowsPerFile, mb(m.RawBytes), mb(m.GzipBytes), m.Chunks, mb(int64(m.SplitBytes)), m.ExportReplicas, m.SplitReplicas, m.CleanReplicas)
	t.Logf("  startup    %6.1f s    run created until every component runs", m.StartupSeconds)
	t.Logf("  total      %6.1f s    run created until Succeeded", m.TotalSeconds)
	t.Logf("  process    %6.1f s    every component runs until Succeeded", m.ProcessSeconds)
	t.Logf("  throughput %6.1f MB/s raw, %.0f rows/s, over the processing time", m.RawMBPerSecond, m.RowsPerSecond)
	t.Logf("  steps      done after %s", stepTimes(m.StepSeconds))
	t.Logf("  rejected   %d rows, all logged", m.RowsRejected)
	if len(m.Kills) > 0 {
		t.Logf("  killed     %s", strings.Join(m.Kills, ", "))
	}
	dir := os.Getenv("ALPHEUS_E2E_OUT")
	if dir == "" {
		return
	}
	data, _ := json.MarshalIndent(m, "", "  ")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, m.Testcase+"-"+m.Time.Format("20060102T150405Z")+".json")
	if err := os.WriteFile(name, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("  written to %s", name)
}

func stepTimes(s map[string]float64) string {
	var parts []string
	for _, step := range []string{"export", "split", "clean", "aggregate"} {
		if v, ok := s[step]; ok {
			parts = append(parts, fmt.Sprintf("%s %.0f s", step, v))
		}
	}
	return strings.Join(parts, ", ")
}

// stepEnds returns when each step's last component finished, in seconds
// after start.
func (e *env) stepEnds(r *v1alpha1.StreamRun, start time.Time) map[string]float64 {
	var pods corev1.PodList
	if err := e.c.List(e.t.Context(), &pods, client.InNamespace(e.ns), client.MatchingLabels{controller.LabelRun: r.Name}); err != nil {
		e.t.Logf("pods: %v", err)
		return nil
	}
	ends := map[string]float64{}
	for _, p := range pods.Items {
		for _, c := range p.Status.ContainerStatuses {
			if c.Name != controller.ComponentContainer || c.State.Terminated == nil {
				continue
			}
			step := p.Labels[controller.LabelStep]
			if d := c.State.Terminated.FinishedAt.Sub(start).Seconds(); d > ends[step] {
				ends[step] = d
			}
		}
	}
	return ends
}

func mb(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/1e6) }
