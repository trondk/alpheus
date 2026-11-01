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

package components_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const skipSettings = `{"role": "sink", "chunks": 10, "size": 1000, "seed": 3, "skip": 5}`

// TestBenchDropSkip runs pass with drop and sink with skip: pass drops
// chunks 5 and 10, and sink expects the other eight.
func TestBenchDropSkip(t *testing.T) {
	bin := build(t)
	src, err := step(bin, "", `{"role": "source", "chunks": 10, "size": 1000, "seed": 3}`, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	passed, err := step(bin, src, `{"role": "pass", "drop": 5}`, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s := summary(t, passed); s.Counts["chunks"] != 8 || s.Counts["dropped"] != 2 || s.Counts["bytes"] != 8000 {
		t.Errorf("pass counts %v", s.Counts)
	}
	result, err := step(bin, passed, skipSettings, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(result, "part-000001.csv"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n")[1:] {
		var n int
		fmt.Sscanf(line, "default,%d,", &n)
		got = append(got, fmt.Sprint(n))
	}
	if s := strings.Join(got, " "); s != "1 2 3 4 6 7 8 9" {
		t.Errorf("sink got chunks %s, want 1 2 3 4 6 7 8 9", s)
	}

	// Without drop, sink gets a chunk it should have skipped.
	if _, err := step(bin, src, skipSettings, t.TempDir()); err == nil || !strings.Contains(err.Error(), "should be skipped") {
		t.Errorf("chunk not dropped: %v", err)
	}
}
