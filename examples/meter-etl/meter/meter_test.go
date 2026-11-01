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

package meter

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"alpheus-dev.org/alpheus/sdk"
)

func TestParse(t *testing.T) {
	for line, want := range map[string]error{
		"M0012345,coast,2026-10-30T13:45:00Z,1.234":  nil,
		"M0012345,2026-10-30T13:45:00Z,1.234":        ErrColumns,
		"X0012345,coast,2026-10-30T13:45:00Z,1.234":  ErrMeter,
		"M0012345,,2026-10-30T13:45:00Z,1.234":       ErrRegion,
		"M0012345,coast,2026-13-40T25:61:00Z,1.234":  ErrTimestamp,
		"M0012345,coast,2026-10-30T13:45:00Z,":       ErrMissing,
		"M0012345,coast,2026-10-30T13:45:00Z,-1.2":   ErrNegative,
		"M0012345,coast,2026-10-30T13:45:00Z,1.2345": ErrKWh,
	} {
		if _, err := Parse(line); !errors.Is(err, want) {
			t.Errorf("%q: %v, want %v", line, err, want)
		}
	}
	r, _ := Parse("M0012345,coast,2026-10-30T13:45:00Z,1.2")
	if r.Wh != 1200 || r.Time.Hour() != 13 {
		t.Errorf("reading %+v", r)
	}
}

func run(t *testing.T, h sdk.Handler, input, output, settings string, replica, replicas int) sdk.Summary {
	t.Helper()
	cfg := sdk.LocalConfig{OutputDir: output, Settings: []byte(settings), Replica: replica, Replicas: replicas}
	if input != "" {
		cfg.Inputs = map[string]string{sdk.DefaultPort: input}
	}
	if err := sdk.RunLocal(context.Background(), cfg, h); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(output, sdk.SummaryFile))
	if err != nil {
		t.Fatal(err)
	}
	var s sdk.Summary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestFlow runs the four steps in local mode, clean as two replicas, and
// checks the totals, the counters and the dataset against Expected.
func TestFlow(t *testing.T) {
	g := Gen{Files: 3, Rows: 5000, Seed: 7, BadPerMille: 20}
	settings, _ := json.Marshal(g)
	dir := t.TempDir()
	path := func(p ...string) string { return filepath.Join(append([]string{dir}, p...)...) }

	exp := run(t, Export, "", path("export"), string(settings), 0, 1)
	spl := run(t, Split, path("export", "default"), path("split"), `{"bytes": 40000}`, 0, 1)

	// Both replicas' partial totals are aggregate's input.
	if err := os.MkdirAll(path("partials"), 0o755); err != nil {
		t.Fatal(err)
	}
	var rowsIn, rowsOut, rejected, errorsLogged, datasetRows int64
	for replica := range 2 {
		out := path(fmt.Sprintf("clean-%d", replica))
		s := run(t, Clean, path("split", "default"), out, "", replica, 2)
		rowsIn += s.Counts["rows_in"]
		rowsOut += s.Counts["rows_out"]
		rejected += s.Counts["rows_rejected"]
		errorsLogged += s.Errors
		parts, _ := filepath.Glob(filepath.Join(out, "default", "part-*"))
		for _, p := range parts {
			data, _ := os.ReadFile(p)
			os.WriteFile(path("partials", fmt.Sprintf("%d-%s", replica, filepath.Base(p))), data, 0o644)
		}
		datasets, _ := filepath.Glob(filepath.Join(out, DatasetOutput, "part-*"))
		for _, p := range datasets {
			datasetRows += lines(t, p) - 1
		}
	}
	run(t, Aggregate, path("partials"), path("aggregate"), "", 0, 1)

	want, err := g.Expected()
	if err != nil {
		t.Fatal(err)
	}
	if exp.Counts["rows"] != want.Rows || spl.Counts["rows"] != want.Rows || rowsIn != want.Rows {
		t.Errorf("rows: export %d, split %d, clean %d; want %d", exp.Counts["rows"], spl.Counts["rows"], rowsIn, want.Rows)
	}
	if rejected != want.Bad || errorsLogged != want.Bad || rowsOut != want.Rows-want.Bad || datasetRows != rowsOut {
		t.Errorf("rejected %d, logged %d, out %d, dataset %d; want %d bad of %d", rejected, errorsLogged, rowsOut, datasetRows, want.Bad, want.Rows)
	}
	if want.Bad == 0 {
		t.Error("no bad rows were generated")
	}
	f, err := os.Open(path("aggregate", "default", "part-000001.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := ParseSums(f)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got, want.Sums) {
		t.Errorf("totals differ: %d keys, want %d", len(got), len(want.Sums))
	}
}

func lines(t *testing.T, gz string) int64 {
	t.Helper()
	f, err := os.Open(gz)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	var n int64
	for sc.Scan() {
		n++
	}
	return n
}
