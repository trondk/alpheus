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

package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

func validFailure() Failure {
	return Failure{Version: 1, Run: "r", Step: "pass", Inputs: []Input{{
		Port: "default", Edge: "source-default-pass", ProducerReplica: 0, N: 7,
		Bytes: 4, SHA256: strings.Repeat("ab", 32), File: "source-default-pass-0-000007.bin",
	}}}
}

func TestParse(t *testing.T) {
	data, _ := json.Marshal(validFailure())
	if _, err := Parse(data); err != nil {
		t.Fatalf("valid failure: %v", err)
	}
	for name, change := range map[string]func(*Input){
		"port that climbs":   func(in *Input) { in.Port = "../../.ssh" },
		"port with a slash":  func(in *Input) { in.Port = "a/b" },
		"file that climbs":   func(in *Input) { in.File = "../../.bashrc" },
		"file with a slash":  func(in *Input) { in.File = "x/source-default-pass-0-000007.bin" },
		"absolute file":      func(in *Input) { in.File = "/etc/passwd" },
		"dot file":           func(in *Input) { in.File = ".." },
		"no extension":       func(in *Input) { in.File = "source-default-pass-0-000007" },
		"chunk 0":            func(in *Input) { in.N = 0 },
		"short SHA-256":      func(in *Input) { in.SHA256 = "ab" },
		"negative bytes":     func(in *Input) { in.Bytes = -1 },
		"port in capitals":   func(in *Input) { in.Port = "Default" },
		"port over 20 chars": func(in *Input) { in.Port = strings.Repeat("a", 21) },
	} {
		f := validFailure()
		change(&f.Inputs[0])
		data, _ := json.Marshal(f)
		if _, err := Parse(data); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	f := validFailure()
	for range MaxInputs {
		f.Inputs = append(f.Inputs, f.Inputs[0])
	}
	data, _ = json.Marshal(f)
	if _, err := Parse(data); err == nil {
		t.Errorf("%d inputs: accepted", len(f.Inputs))
	}

	// The skipped chunks of a replica that gave up may be many more.
	f.Skipped = true
	data, _ = json.Marshal(f)
	if _, err := Parse(data); err != nil {
		t.Errorf("%d skipped inputs: %v", len(f.Inputs), err)
	}
	for len(f.Inputs) <= MaxSkipped {
		f.Inputs = append(f.Inputs, f.Inputs[0])
	}
	data, _ = json.Marshal(f)
	if _, err := Parse(data); err == nil {
		t.Errorf("%d skipped inputs: accepted", len(f.Inputs))
	}
	f = validFailure()
	f.NotKept = -1
	data, _ = json.Marshal(f)
	if _, err := Parse(data); err == nil {
		t.Error("negative notKept: accepted")
	}
}

// FuzzParse checks that a failure record from the bucket can never make
// Fetch write outside the replay folder (GO-58, GO-141).
func FuzzParse(f *testing.F) {
	data, _ := json.Marshal(validFailure())
	f.Add(data)
	f.Add([]byte(`{"inputs":[{"port":"..","file":"../x","n":1,"sha256":""}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		fl, err := Parse(data)
		if err != nil {
			return
		}
		for _, in := range fl.Inputs {
			for _, name := range []string{in.Port, in.File} {
				if !filepath.IsLocal(name) || filepath.Base(name) != name || strings.HasPrefix(name, ".") {
					t.Fatalf("accepted %q, which is not one plain name", name)
				}
			}
		}
	})
}

// Fetch writes the index that gives each fetched chunk its producer and
// number in local mode.
func TestFetchWritesIndex(t *testing.T) {
	st := store.NewMemory()
	data := []byte("data")
	sum := sha256.Sum256(data)
	f := validFailure()
	f.Inputs[0].From, f.Inputs[0].Bytes, f.Inputs[0].SHA256 = "source", int64(len(data)), hex.EncodeToString(sum[:])
	if err := st.Put(t.Context(), "p/input/"+f.Inputs[0].File, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := Fetch(t.Context(), st, "p/", &f, dir); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "in", "default", chunk.LocalIndex))
	if err != nil {
		t.Fatal(err)
	}
	var index map[string]chunk.LocalChunk
	if err := json.Unmarshal(raw, &index); err != nil {
		t.Fatal(err)
	}
	if got := index[f.Inputs[0].File]; got != (chunk.LocalChunk{Step: "source", Replica: 0, N: 7}) {
		t.Errorf("index %v", index)
	}
}
