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

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// putResult writes a result stream with the given chunks to the harness's
// storage, ended or not, and returns the keys of the chunks.
func (h *harness) putResult(r *v1alpha1.StreamRun, step, output string, replica int, eos bool, chunks ...string) []string {
	h.t.Helper()
	ctx := context.Background()
	s := chunk.ResultStream(r.Name, string(r.UID), step, output, replica)
	m := chunk.NewManifest(r.Name, string(r.UID), step+"."+output, replica, 1)
	var keys []string
	for i, body := range chunks {
		n := int64(i + 1)
		sum := sha256.Sum256([]byte(body))
		if _, err := m.Append(chunk.Chunk{N: n, Ext: "csv", Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}); err != nil {
			h.t.Fatal(err)
		}
		key := s.ChunkKey(n, "csv")
		h.put(key, body)
		keys = append(keys, key)
	}
	if eos {
		m.End()
	}
	data, err := m.Marshal()
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.mem.Put(ctx, s.ManifestKey(), bytes.NewReader(data), int64(len(data))); err != nil {
		h.t.Fatal(err)
	}
	return keys
}

// finish sets the run's phase, as the controller does when it ends.
func (h *harness) finish(name string, phase v1alpha1.RunPhase) *v1alpha1.StreamRun {
	h.t.Helper()
	r := h.get(name)
	r.Status.Phase = phase
	if err := k8s.Status().Update(context.Background(), r); err != nil {
		h.t.Fatal(err)
	}
	return h.get(name)
}

func (h *harness) put(key, body string) {
	h.t.Helper()
	if err := h.mem.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) secret(name string, annotations map[string]string) {
	h.t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: name, Annotations: annotations}}
	if err := k8s.Create(context.Background(), s); err != nil {
		h.t.Fatal(err)
	}
}

func readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// results get, cat and ls read only the committed chunks, check them, and
// put them where SPEC 9.1 says (GO-147, GO-151, GO-153).
func TestResultsGetCatLs(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("res", "")))
	r := h.finish("res", v1alpha1.RunSucceeded)
	keys := h.putResult(r, "sum", "default", 0, true, "a,1\n", "b,2\n", "c,3\n")
	// Under the prefix, but never committed: never listed or fetched.
	h.put(strings.Replace(keys[2], "000003", "000004", 1), "uncommitted")

	out := h.mustRun("results", "ls", "res")
	if !strings.Contains(out, "s3://flows/"+keys[0]) || strings.Contains(out, "part-000004") {
		t.Errorf("results ls:\n%s", out)
	}
	var ix Index
	if err := json.Unmarshal([]byte(h.mustRun("results", "ls", "res", "sum", "-o", "json")), &ix); err != nil {
		t.Fatal(err)
	}
	if ix.Version != IndexVersion || !ix.Complete || ix.UID != string(r.UID) || len(ix.Chunks) != 3 || len(ix.Replicas) != 1 || !ix.Replicas[0].Complete ||
		ix.Chunks[1].URL != "s3://flows/"+keys[1] || ix.Chunks[1].Bytes != 4 {
		t.Errorf("results ls -o json: %+v", ix)
	}

	if got := h.mustRun("results", "cat", "res", "sum.default"); got != "a,1\nb,2\nc,3\n" {
		t.Errorf("results cat: %q", got)
	}
	if got := h.mustRun("results", "cat", "res", "sum", "--chunks", "2-3"); got != "b,2\nc,3\n" {
		t.Errorf("results cat --chunks 2-3: %q", got)
	}
	if got := h.mustRun("results", "get", "res", "-d", "-"); got != "a,1\nb,2\nc,3\n" {
		t.Errorf("results get -d -: %q", got)
	}

	dir := filepath.Join(t.TempDir(), "out", "res")
	out = h.mustRun("results", "get", "res", "-d", dir)
	if !strings.Contains(out, "Fetched 3 chunks (12 B)") {
		t.Errorf("results get: %s", out)
	}
	if got := readFile(t, filepath.Join(dir, "sum.default", "0", "part-000002.csv")); got != "b,2\n" {
		t.Errorf("chunk 2: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "sum.default", "0", "part-000004.csv")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the uncommitted chunk was fetched: %v", err)
	}
	var written Index
	if err := json.Unmarshal([]byte(readFile(t, filepath.Join(dir, IndexFile))), &written); err != nil {
		t.Fatal(err)
	}
	if written.Run != "res" || written.UID != string(r.UID) || len(written.Chunks) != 3 || written.Chunks[0].File != "sum.default/0/part-000001.csv" {
		t.Errorf("%s: %+v", IndexFile, written)
	}
	if out := h.mustRun("results", "get", "res", "-d", dir); !strings.Contains(out, "3 were already there") {
		t.Errorf("results get again: %s", out)
	}

	flat := t.TempDir()
	h.mustRun("results", "get", "res", "-d", flat, "--flat", "--force", "--chunks", "1")
	if got := readFile(t, filepath.Join(flat, "sum.default", "0-part-000001.csv")); got != "a,1\n" {
		t.Errorf("--flat: %q", got)
	}
	if _, err := os.Stat(filepath.Join(flat, "sum.default", "0-part-000002.csv")); err == nil {
		t.Error("--chunks 1 fetched chunk 2")
	}

	if code := h.run("results", "get", "res", "nope"); code != 1 || !strings.Contains(h.stderr.String(), "its results are: sum.default") {
		t.Errorf("a result the run does not have: exit %d, %s", code, h.stderr.String())
	}
}

// results get writes only into an empty folder or the same run's, and
// --force writes only the run's own files (GO-148).
func TestResultsGetFolder(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("own", "")))
	h.putResult(h.finish("own", v1alpha1.RunSucceeded), "sum", "default", 0, true, "x\n")

	dir := t.TempDir()
	other := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(other, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := h.run("results", "get", "own", "-d", dir); code != 1 || !strings.Contains(h.stderr.String(), "-d") || !strings.Contains(h.stderr.String(), "--force") {
		t.Errorf("a folder with other files: exit %d, %s", code, h.stderr.String())
	}
	h.mustRun("results", "get", "own", "-d", dir, "--force")
	if readFile(t, other) != "mine" {
		t.Error("--force changed a file that is not the run's")
	}

	// Another run's folder is never written to, not even with --force.
	h.mustRun("run", "-f", h.file("flow.yaml", flow("other", "")))
	h.putResult(h.finish("other", v1alpha1.RunSucceeded), "sum", "default", 0, true, "y\n")
	if code := h.run("results", "get", "other", "-d", dir, "--force"); code != 1 || !strings.Contains(h.stderr.String(), "results of another run, own") {
		t.Errorf("another run's folder: exit %d, %s", code, h.stderr.String())
	}
}

// A fetch that was stopped goes on from its .partial, only from the same
// version of the object; a chunk that does not match its manifest is an
// error and leaves nothing (GO-149).
func TestResultsGetResume(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("big", "")))
	r := h.finish("big", v1alpha1.RunSucceeded)
	body := strings.Repeat("0123456789", 100)
	keys := h.putResult(r, "sum", "default", 0, true, body)
	dir := t.TempDir()
	file := filepath.Join(dir, "sum.default", "0", "part-000001.csv")
	sum := sha256.Sum256([]byte(body))
	etag := hex.EncodeToString(sum[:]) // the memory store's ETag

	// A first fetch, then the chunk as if that fetch was stopped after
	// 300 bytes of this version.
	h.mustRun("results", "get", "big", "-d", dir)
	_ = os.Remove(file)
	_ = os.WriteFile(file+partialSuffix, []byte(body[:300]), 0o600)
	_ = os.WriteFile(file+etagSuffix, []byte(etag+"\n"), 0o600)
	h.mustRun("results", "get", "big", "-d", dir)
	if readFile(t, file) != body {
		t.Error("the resumed chunk is not the object")
	}
	for _, f := range []string{file + partialSuffix, file + etagSuffix} {
		if _, err := os.Stat(f); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s left: %v", f, err)
		}
	}

	// It really goes on from the .partial, without fetching its bytes
	// again: wrong bytes there make the whole chunk wrong.
	_ = os.Remove(file)
	_ = os.WriteFile(file+partialSuffix, []byte(strings.Repeat("X", 300)), 0o600)
	_ = os.WriteFile(file+etagSuffix, []byte(etag+"\n"), 0o600)
	if code := h.run("results", "get", "big", "-d", dir); code != 1 || !strings.Contains(h.stderr.String(), "has SHA-256") {
		t.Errorf("a .partial that was not continued: exit %d, %s", code, h.stderr.String())
	}

	// A .partial of another version is started over.
	_ = os.Remove(file)
	_ = os.WriteFile(file+partialSuffix, []byte("garbage of another version"), 0o600)
	_ = os.WriteFile(file+etagSuffix, []byte("another-etag\n"), 0o600)
	h.mustRun("results", "get", "big", "-d", dir)
	if readFile(t, file) != body {
		t.Error("the chunk fetched over another version's .partial is wrong")
	}

	// The object no longer matches the manifest.
	_ = os.Remove(file)
	h.put(keys[0], strings.Repeat("x", len(body)))
	if code := h.run("results", "get", "big", "-d", dir); code != 1 || !strings.Contains(h.stderr.String(), "part-000001.csv has SHA-256") {
		t.Errorf("a chunk that does not match: exit %d, %s", code, h.stderr.String())
	}
	for _, f := range []string{file, file + partialSuffix, file + etagSuffix} {
		if _, err := os.Stat(f); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s left after a chunk that did not match: %v", f, err)
		}
	}
	if code := h.run("results", "cat", "big", "sum"); code != 1 || !strings.Contains(h.stderr.String(), "has SHA-256") {
		t.Errorf("results cat of a chunk that does not match: exit %d, %s", code, h.stderr.String())
	}
}

// A replica without EOS is fetched and said to be incomplete; --complete
// makes that an error (GO-152).
func TestResultsIncomplete(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("half", "")))
	r := h.finish("half", v1alpha1.RunSucceeded)
	h.putResult(r, "sum", "default", 0, true, "done\n")
	h.putResult(r, "sum", "late", 0, false, "1\n", "2\n")

	dir := t.TempDir()
	h.mustRun("results", "get", "half", "-d", dir)
	if !strings.Contains(h.stderr.String(), "incomplete") || !strings.Contains(h.stderr.String(), "sum.late replica 0 has not ended its stream: 2 chunks") {
		t.Errorf("no warning of the incomplete result: %s", h.stderr.String())
	}
	var ix Index
	if err := json.Unmarshal([]byte(readFile(t, filepath.Join(dir, IndexFile))), &ix); err != nil {
		t.Fatal(err)
	}
	for _, rp := range ix.Replicas {
		if rp.Complete != (rp.Result == "sum.default") {
			t.Errorf("%s replica %d: complete %t", rp.Result, rp.Replica, rp.Complete)
		}
	}
	if readFile(t, filepath.Join(dir, "sum.late", "0", "part-000002.csv")) != "2\n" {
		t.Error("the committed chunks of the incomplete result were not fetched")
	}
	if code := h.run("results", "get", "half", "-d", dir, "--complete"); code != 1 {
		t.Errorf("--complete with an incomplete result: exit %d", code)
	}
	if code := h.run("results", "get", "half", "sum.default", "-d", dir, "--complete"); code != 0 {
		t.Errorf("--complete with a complete result: exit %d, %s", code, h.stderr.String())
	}
	if code := h.run("results", "get", "half", "-d", "-"); code != 2 || !strings.Contains(h.stderr.String(), "sum.default, sum.late") {
		t.Errorf("-d - with two results: exit %d, %s", code, h.stderr.String())
	}

	// A run that failed before a result had a chunk: nothing to fetch, and
	// still incomplete.
	h.mustRun("run", "-f", h.file("flow.yaml", flow("failed", "")))
	h.finish("failed", v1alpha1.RunFailed)
	if code := h.run("results", "get", "failed", "-d", t.TempDir(), "--complete"); code != 1 || !strings.Contains(h.stderr.String(), "the run is Failed") {
		t.Errorf("--complete for a failed run without results: exit %d, %s", code, h.stderr.String())
	}
	var failed Index
	if err := json.Unmarshal([]byte(h.mustRun("results", "ls", "failed", "-o", "json")), &failed); err != nil || failed.Complete {
		t.Errorf("results ls of a failed run: complete %t, %v", failed.Complete, err)
	}
}

// errors shows the committed error log, counts it per code, and saves it
// (GO-154).
func TestErrors(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("bad", "")))
	r := h.finish("bad", v1alpha1.RunSucceeded)
	prefix := chunk.ErrorLogPrefix("bad", string(r.UID), "filter", 1)
	obs := agent.Observations{Version: agent.ObservationsVersion, Run: "bad", UID: string(r.UID), Step: "filter", Replica: 1, Epoch: 1,
		Segments: []string{"log-000001.jsonl", "log-000002.jsonl"}, Errors: 2, Warnings: 1}
	data, _ := json.Marshal(obs)
	h.put(prefix+"manifest.json", string(data))
	h.put(prefix+"log-000001.jsonl",
		`{"time":"2026-11-09T10:00:00Z","level":"error","code":"bad_row","message":"row rejected","fields":{"line":7},"step":"filter","replica":1,"chunk":"default/read/1"}`+"\n"+
			`{"time":"2026-11-09T10:00:01Z","level":"warn","code":"late","message":"late row","step":"filter","replica":1,"chunk":"default/read/1"}`+"\n")
	h.put(prefix+"log-000002.jsonl",
		`{"time":"2026-11-09T10:00:02Z","level":"error","code":"bad_row","message":"row rejected","fields":{"line":9},"step":"filter","replica":1,"chunk":"default/read/2"}`+"\n")
	// Not in the manifest, so never committed.
	h.put(prefix+"log-000003.jsonl", `{"time":"2026-11-09T10:00:03Z","level":"error","code":"orphan","message":"x","step":"filter","replica":1}`+"\n")

	out := h.mustRun("errors", "bad")
	for _, want := range []string{`row rejected {"line":7}`, "default/read/2", "bad_row  error  2", "late     warn   1"} {
		if !strings.Contains(out, want) {
			t.Errorf("errors: no %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "orphan") {
		t.Errorf("errors shows a segment the manifest does not list:\n%s", out)
	}
	lines := strings.Split(strings.TrimSpace(h.mustRun("errors", "bad", "filter", "--tail", "1", "-o", "json")), "\n")
	var last agent.LogEntry
	if len(lines) != 1 || json.Unmarshal([]byte(lines[0]), &last) != nil || string(last.Fields) != `{"line":9}` {
		t.Errorf("errors --tail 1 -o json: %q", lines)
	}
	if out := h.mustRun("errors", "bad", "sum"); out != "" || !strings.Contains(h.stderr.String(), "no errors") {
		t.Errorf("errors of a step without any: %q %q", out, h.stderr.String())
	}

	dir := t.TempDir()
	h.mustRun("errors", "bad", "-d", dir)
	if got := readFile(t, filepath.Join(dir, "_errors", "filter", "1", "log-000002.jsonl")); !strings.Contains(got, `"line":9`) {
		t.Errorf("errors -d: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "_errors", "filter", "1", "log-000003.jsonl")); err == nil {
		t.Error("errors -d saved a segment the manifest does not list")
	}

	h.putResult(r, "sum", "default", 0, true, "1\n")
	got := t.TempDir()
	h.mustRun("results", "get", "bad", "-d", got, "--errors")
	if _, err := os.Stat(filepath.Join(got, "_errors", "filter", "1", "manifest.json")); err != nil {
		t.Errorf("results get --errors: %v", err)
	}
}

// The storage's address is found in the order GO-156 gives, and an error
// says which address was tried and what to set.
func TestStorageAddress(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", map[string]string{
		AnnotationExternalEndpoint: "https://s3.outside.example",
		AnnotationExternalCA:       "-----BEGIN CERTIFICATE-----\n…",
	})
	h.mustRun("run", "-f", h.file("flow.yaml", flow("addr", "")))
	h.putResult(h.finish("addr", v1alpha1.RunSucceeded), "sum", "default", 0, true, "1\n")

	last := func() store.Address { return h.opened[len(h.opened)-1] }
	h.mustRun("results", "ls", "addr")
	if a := last(); a.Endpoint != "https://s3.outside.example" || !strings.HasPrefix(string(a.CAPEM), "-----BEGIN") {
		t.Errorf("from the annotations: %+v", a)
	}
	h.mustRun("chunks", "addr", "filter-sum")
	if a := last(); a.Endpoint != "https://s3.outside.example" {
		t.Errorf("chunks from the annotations: %+v", a)
	}
	h.environ = map[string]string{"ALPHEUS_S3_ENDPOINT": "http://env.example:3900", "ALPHEUS_S3_CA_FILE": "/env/ca.crt"}
	h.mustRun("results", "ls", "addr")
	if a := last(); a.Endpoint != "http://env.example:3900" || a.CAFile != "/env/ca.crt" || a.CAPEM != nil {
		t.Errorf("from the environment: %+v", a)
	}
	h.mustRun("results", "ls", "addr", "--endpoint", "http://flag.example", "--ca-file", "/flag/ca.crt")
	if a := last(); a.Endpoint != "http://flag.example" || a.CAFile != "/flag/ca.crt" {
		t.Errorf("from the flags: %+v", a)
	}

	err := storeAddr{shown: "http://s3.example:3900", from: "spec.storage.endpoint, the address inside the cluster", secret: "alpheus-s3"}.
		explain(errors.New("dial tcp: no such host"))
	var he *hintError
	if !errors.As(err, &he) || !strings.Contains(err.Error(), "http://s3.example:3900") ||
		!strings.Contains(he.hint, "--endpoint") || !strings.Contains(he.hint, "ALPHEUS_S3_ENDPOINT") || !strings.Contains(he.hint, AnnotationExternalEndpoint) {
		t.Errorf("explain: %v (%+v)", err, he)
	}
}

// A finished run's status ends with the commands that fit it (GO-146).
func TestNext(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC))
	end := metav1.NewTime(start.Add(49 * time.Second))
	one := int32(1)
	r := &v1alpha1.StreamRun{ObjectMeta: metav1.ObjectMeta{Name: "meter-etl", Namespace: "flows"}}
	r.Status = v1alpha1.StreamRunStatus{Phase: v1alpha1.RunSucceeded, StartedAt: &start, FinishedAt: &end,
		Steps: []v1alpha1.StepStatus{
			{Name: "clean", Errors: 160070, Replicas: []v1alpha1.ReplicaStatus{{Index: 0}, {Index: 1, FailedChunks: 3}}},
			{Name: "aggregate"},
		},
		Results: []v1alpha1.ResultStatus{
			{Step: "clean", Output: "dataset", Bytes: 359 << 20},
			{Step: "aggregate", Output: "default", Bytes: 5000},
		},
	}
	text := func() string {
		var b bytes.Buffer
		if err := write(&b, "", nil, func(w io.Writer) { describe(w, r, end.Time) }); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	out := text()
	for _, want := range []string{
		"Succeeded in 49s. Next:",
		"alpheus results get meter-etl -n flows",
		"→ ./meter-etl/  (359.0 MiB)",
		"alpheus results cat meter-etl aggregate.default -n flows",
		"alpheus errors meter-etl clean -n flows",
		"(160070 errors, 0 warnings)",
		"alpheus replay meter-etl clean --replica 1 --skipped -n flows",
		"alpheus logs meter-etl -n flows",
		"kubectl -n flows delete streamrun meter-etl",
		"(results stay 7d)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "results cat meter-etl clean.dataset") || strings.Contains(out, "errors meter-etl aggregate") {
		t.Errorf("lines that do not fit the run:\n%s", out)
	}

	// Above 10 GiB the results are listed, not fetched; a failed run starts
	// with the replica that stopped it.
	r.Status.Results[0].Bytes = 20 << 30
	r.Status.Phase, r.Status.FailedStep, r.Status.FailedReplica = v1alpha1.RunFailed, "clean", &one
	r.Status.Steps[0].Logs, r.Status.Steps[0].Failures = "s3://b/results/meter-etl/u/_logs/clean/", "s3://b/results/meter-etl/u/_failures/clean/"
	r.Spec.ResultsTTL = &metav1.Duration{}
	out = text()
	failed := strings.Index(out, "alpheus logs meter-etl clean -n flows")
	kept := strings.Index(out, "alpheus logs --kept meter-etl clean -n flows")
	replay := strings.Index(out, "alpheus replay meter-etl clean --replica 1 -n flows")
	results := strings.Index(out, "alpheus results ls meter-etl -n flows")
	if failed < 0 || kept < failed || replay < kept || results < replay {
		t.Errorf("a failed run: the failed replica's lines first, then results ls:\n%s", out)
	}
	if strings.Contains(out, "results get") || !strings.Contains(out, "results stay until you delete them") {
		t.Errorf("a failed run with large results and resultsTTL 0:\n%s", out)
	}

	r.Status.Phase, r.Status.FinishedAt = v1alpha1.RunRunning, nil
	if strings.Contains(text(), "Next:") {
		t.Error("a running run has the list")
	}
}

// results get checks the free space before it fetches, and asks before
// more than 10 GiB (GO-150). The sizes come from the manifest, so the
// chunks need not exist.
func TestResultsGetSpace(t *testing.T) {
	h := newHarness(t)
	h.secret("alpheus-s3", nil)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("huge", "")))
	r := h.finish("huge", v1alpha1.RunSucceeded)
	manifest := func(size int64) {
		s := chunk.ResultStream(r.Name, string(r.UID), "sum", "default", 0)
		m := chunk.NewManifest(r.Name, string(r.UID), "sum.default", 0, 1)
		if _, err := m.Append(chunk.Chunk{N: 1, Ext: "gz", Bytes: size, SHA256: strings.Repeat("0", 64)}); err != nil {
			t.Fatal(err)
		}
		m.End()
		data, _ := m.Marshal()
		h.put(s.ManifestKey(), string(data))
	}

	dir := t.TempDir()
	manifest(1 << 60)
	if code := h.run("results", "get", "huge", "-d", dir); code != 1 ||
		!strings.Contains(h.stderr.String(), "free") || !strings.Contains(h.stderr.String(), "--replica") || !strings.Contains(h.stderr.String(), "results ls huge") {
		t.Errorf("more than the disk has: exit %d, %s", code, h.stderr.String())
	}

	free, ok := freeSpace(dir)
	if !ok || free < askAbove+(1<<30) {
		t.Skipf("%d bytes free, too few to see the question above 10 GiB", free)
	}
	manifest(askAbove + 1)
	h.stdin = "n\n"
	if code := h.run("results", "get", "huge", "-d", dir); code != 0 || !strings.Contains(h.stderr.String(), "Fetch 10.0 GiB") || !strings.Contains(h.stderr.String(), "Nothing fetched") {
		t.Errorf("above 10 GiB: exit %d, %s", code, h.stderr.String())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("fetched after the answer no: %d files", len(entries))
	}
}
