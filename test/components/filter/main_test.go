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

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/sdk"
)

var dk = []byte(`{"field": "country", "in": ["DK"]}`)

// parts writes 10 CSV chunks like testcase03-replicas: every part has 3 DK
// rows among others, except part 10, which has none.
func parts(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for n := 1; n <= 10; n++ {
		countries := []string{"SE", "DK", "NO", "DK", "DE", "DK"}
		if n == 10 {
			countries = []string{"SE", "NO", "DE"}
		}
		var b strings.Builder
		b.WriteString("order_id,country,amount\n")
		for i, c := range countries {
			fmt.Fprintf(&b, "%d,%s,%d.50\n", n*100+i, c, i)
		}
		write(t, filepath.Join(dir, fmt.Sprintf("part-%06d.csv", n)), b.String())
	}
	return dir
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// countries returns the country of every row in the default output.
func countries(t *testing.T, dir string) []string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, sdk.DefaultOutput, "part-*.csv"))
	var got []string
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := csv.NewReader(f).ReadAll()
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range recs[1:] {
			got = append(got, rec[1])
		}
	}
	return got
}

func summary(t *testing.T, dir string) sdk.Summary {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, sdk.SummaryFile))
	if err != nil {
		t.Fatal(err)
	}
	var s sdk.Summary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func run(t *testing.T, cfg sdk.LocalConfig) error {
	t.Helper()
	return sdk.RunLocal(context.Background(), cfg, handle)
}

// TestFilterKeepsOnlyDK runs the test filter in local mode: it keeps only
// the DK rows and counts the rows in and out.
func TestFilterKeepsOnlyDK(t *testing.T) {
	out := t.TempDir()
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: parts(t)}, OutputDir: out, Settings: dk}
	if err := run(t, cfg); err != nil {
		t.Fatal(err)
	}
	got := countries(t, out)
	if len(got) != 27 {
		t.Errorf("%d rows, want 27 (3 in each of parts 1-9)", len(got))
	}
	for _, c := range got {
		if c != "DK" {
			t.Fatalf("kept a %s row", c)
		}
	}
	if s := summary(t, out); s.Counts["rows_in"] != 57 || s.Counts["rows_out"] != 27 {
		t.Errorf("counts %v, want rows_in 57, rows_out 27", s.Counts)
	}
}

// Replica 0 of 10 gets only part 10, which has no DK rows: it must finish
// without error and end its output with no chunks (testcase03-replicas,
// GO-80).
func TestFilterReplicaWithoutMatches(t *testing.T) {
	out := t.TempDir()
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: parts(t)}, OutputDir: out, Settings: dk, Replicas: 10}
	if err := run(t, cfg); err != nil {
		t.Fatal(err)
	}
	if got := countries(t, out); len(got) != 0 {
		t.Errorf("replica 0 kept %v", got)
	}
	if s := summary(t, out); s.Counts["rows_in"] != 3 || s.Counts["rows_out"] != 0 {
		t.Errorf("counts %v, want rows_in 3, rows_out 0", s.Counts)
	}
}

// TestFilterLogsBadRows checks that the test filter logs a bad row as an
// error and goes on.
func TestFilterLogsBadRows(t *testing.T) {
	in, out := t.TempDir(), t.TempDir()
	write(t, filepath.Join(in, "part-000001.csv"), "order_id,country,amount\n1,DK,1\n2,DK\n3,DK,3\n")
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out, Settings: dk}
	if err := run(t, cfg); err != nil {
		t.Fatal(err)
	}
	if got := countries(t, out); len(got) != 2 {
		t.Errorf("kept %d rows, want 2", len(got))
	}
	log, err := os.ReadFile(filepath.Join(out, sdk.ErrorLogFile))
	if err != nil {
		t.Fatal(err)
	}
	var e sdk.LogEntry
	if err := json.Unmarshal(log, &e); err != nil {
		t.Fatalf("error log %q: %v", log, err)
	}
	if e.Code != "bad_row" || e.Fields["row"] != float64(2) || e.Chunk != "default/default/1" {
		t.Errorf("log entry %+v", e)
	}
	if s := summary(t, out); s.Errors != 1 {
		t.Errorf("errors = %d, want 1", s.Errors)
	}
}

// TestFilterBadSettings checks that the test filter refuses bad settings
// with ErrUsage.
func TestFilterBadSettings(t *testing.T) {
	for name, settings := range map[string]string{
		"no field":      `{"in": ["DK"]}`,
		"no values":     `{"field": "country"}`,
		"unknown field": `{"field": "country", "in": ["DK"], "ni": 1}`,
		"no such field": `{"field": "land", "in": ["DK"]}`,
	} {
		cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: parts(t)}, OutputDir: t.TempDir(), Settings: []byte(settings)}
		if err := run(t, cfg); !errors.Is(err, sdk.ErrUsage) {
			t.Errorf("%s: %v, want ErrUsage", name, err)
		}
	}
}
