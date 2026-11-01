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
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fixedSource = `{"role": "source", "chunks": 5, "size": 1000, "seed": 4, "fixed": true}`

// With fixed, source makes the same chunks every time, as a restarted
// source must (testcase04 and testcase06).
func TestFixedSource(t *testing.T) {
	bin := build(t)
	a, err := step(bin, "", fixedSource, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := step(bin, "", fixedSource, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := filepath.Glob(filepath.Join(a, "part-*"))
	if len(parts) != 5 {
		t.Fatalf("%d chunks", len(parts))
	}
	for _, p := range parts {
		x, _ := os.ReadFile(p)
		y, _ := os.ReadFile(filepath.Join(b, filepath.Base(p)))
		if !bytes.Equal(x, y) {
			t.Errorf("%s differs between two runs", filepath.Base(p))
		}
	}
	if _, err := step(bin, a, `{"role": "sink", "chunks": 5, "size": 1000, "seed": 4}`, t.TempDir()); err != nil {
		t.Errorf("sink: %v", err)
	}
}

// delay makes pass wait before each chunk (testcase05).
func TestDelay(t *testing.T) {
	bin := build(t)
	src, err := step(bin, "", fixedSource, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := step(bin, src, `{"role": "pass", "delay": 60}`, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Errorf("pass took %s, want at least 5 x 60 ms", d)
	}
}
