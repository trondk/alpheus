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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const events = `{"Action":"start"}
{"Action":"run","Test":"TestA"}
{"Action":"output","Test":"TestA","Output":"=== RUN   TestA\n"}
{"Action":"pass","Test":"TestA","Elapsed":2.5}
{"Action":"run","Test":"TestB"}
{"Action":"run","Test":"TestB/20s"}
{"Action":"output","Test":"TestB/20s","Output":"    b_test.go:9: storage <down>\n"}
{"Action":"fail","Test":"TestB/20s","Elapsed":20}
{"Action":"fail","Test":"TestB","Elapsed":21}
{"Action":"run","Test":"TestC"}
{"Action":"skip","Test":"TestC","Elapsed":0}
{"Action":"run","Test":"TestD"}
not json, passed through
{"Action":"output","Output":"panic: test timed out\n"}
{"Action":"fail","Elapsed":3}
`

func TestParse(t *testing.T) {
	r, err := parse(strings.NewReader(events))
	if err != nil {
		t.Fatal(err)
	}
	if r.Result != "fail" || r.Passed != 1 || r.Failed != 2 || r.Skipped != 1 {
		t.Errorf("result %s, %d passed, %d failed, %d skipped; want fail, 1, 2 (B and the unfinished D), 1", r.Result, r.Passed, r.Failed, r.Skipped)
	}
	if r.Seconds != 23.5 {
		t.Errorf("seconds %v, want the tests' 23.5 over the package's 3", r.Seconds)
	}
	if len(r.Tests) != 4 || len(r.Tests[1].Subs) != 1 || r.Tests[1].Subs[0].Status != "fail" || r.Tests[3].Status != "run" {
		t.Errorf("tests %+v", r.Tests)
	}
	if len(r.Package) != 1 {
		t.Errorf("package output %q", r.Package)
	}
}

func TestRender(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(t.TempDir(), "events.json")
	os.WriteFile(in, []byte(events), 0o644)
	if err := render([]string{"-root", root, "-id", "one", "-events", in, "-started", "2026-11-04T08:00:00Z", "-meta", "storage=minio"}); err != nil {
		t.Fatal(err)
	}
	page, _ := os.ReadFile(filepath.Join(root, "one", "index.html"))
	for _, want := range []string{"✗ Failed", "TestB/20s", "storage &lt;down&gt;", "panic: test timed out", "minio"} {
		if !strings.Contains(string(page), want) {
			t.Errorf("report page has no %q", want)
		}
	}
	index, _ := os.ReadFile(filepath.Join(root, "index.html"))
	if !strings.Contains(string(index), `href="one/"`) {
		t.Error("the index does not list the report")
	}
	for _, id := range []string{"../x", ".hidden", "a/b"} {
		if render([]string{"-root", root, "-id", id, "-events", in}) == nil {
			t.Errorf("id %q accepted", id)
		}
	}
}

func TestDocs(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a_test.go"), []byte(`package a

// TestA runs a flow and checks it.
//
//	VAR  what it is
func TestA(t *testing.T) {}

func helper() {}
`), 0o644)
	docs, err := testDocs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := docs["TestA"]; got != "Runs a flow and checks it.\n\n\tVAR  what it is" {
		t.Errorf("doc %q", got)
	}
	ps := paragraphs(docs["TestA"])
	if len(ps) != 2 || ps[0].Code || !ps[1].Code {
		t.Errorf("paragraphs %+v", ps)
	}
}
