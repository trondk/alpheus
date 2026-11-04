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
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
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
