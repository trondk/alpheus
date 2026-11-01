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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/sdk"
)

const benchSettings = `{"role": "%s", "chunks": 5, "size": 100000, "seed": 7}`

// TestBenchLocal runs testcase01's steps, source, pass and sink, with
// the bench binary in local mode, one after the other.
func TestBenchLocal(t *testing.T) {
	bin := build(t)
	src, err := step(bin, "", fmt.Sprintf(benchSettings, "source"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	passed, err := step(bin, src, `{"role": "pass"}`, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	result, err := step(bin, passed, fmt.Sprintf(benchSettings, "sink"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(result, "part-000001.csv"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 6 || lines[0] != "port,n,bytes,written_ns,received_ns" {
		t.Fatalf("timings:\n%s", data)
	}
	for i, line := range lines[1:] {
		var n, size, written, received int64
		if _, err := fmt.Sscanf(line, "default,%d,%d,%d,%d", &n, &size, &written, &received); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if n != int64(i+1) || size != 100000 || received < written {
			t.Errorf("line %q", line)
		}
	}
	if s := summary(t, result); s.Counts["chunks"] != 5 || s.Counts["bytes"] != 500000 {
		t.Errorf("sink counts %v", s.Counts)
	}
}

// The sink fails on a changed byte and on a missing chunk.
func TestBenchSinkChecks(t *testing.T) {
	bin := build(t)
	src, err := step(bin, "", fmt.Sprintf(benchSettings, "source"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := filepath.Glob(filepath.Join(src, "part-*"))

	f, err := os.OpenFile(parts[2], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0}, 5000)
	f.Close()
	if _, err := step(bin, src, fmt.Sprintf(benchSettings, "sink"), t.TempDir()); err == nil || !strings.Contains(err.Error(), "content differs") {
		t.Errorf("changed byte: %v", err)
	}

	os.Remove(parts[2])
	if _, err := step(bin, src, fmt.Sprintf(benchSettings, "sink"), t.TempDir()); err == nil || !strings.Contains(err.Error(), "got 4 chunks, want 5") {
		t.Errorf("missing chunk: %v", err)
	}
}

func build(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "bench")
	if out, err := exec.Command("go", "build", "-o", bin, "./bench").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// step runs the bench binary in local mode with the chunks in input, and
// returns a folder with only the chunks of its default output, ready to be
// the next step's input.
func step(bin, input, settings, out string) (string, error) {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), sdk.EnvMode+"=local", sdk.EnvLocalOutput+"="+out, sdk.EnvSettings+"="+settings)
	if input != "" {
		cmd.Env = append(cmd.Env, sdk.EnvLocalInput+"="+input)
	}
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%v: %s", err, b)
	}
	chunks := filepath.Join(out, "chunks")
	if err := os.Mkdir(chunks, 0o755); err != nil {
		return "", err
	}
	parts, _ := filepath.Glob(filepath.Join(out, sdk.DefaultOutput, "part-*"))
	for _, p := range parts {
		if err := os.Rename(p, filepath.Join(chunks, filepath.Base(p))); err != nil {
			return "", err
		}
	}
	return chunks, nil
}

func summary(t *testing.T, chunks string) sdk.Summary {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(filepath.Dir(chunks), sdk.SummaryFile))
	if err != nil {
		t.Fatal(err)
	}
	var s sdk.Summary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return s
}
