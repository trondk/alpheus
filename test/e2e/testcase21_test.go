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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/cli"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

// TestTestcase21 runs the debug example (docs/debug-example.md). Version 1
// of merge must fail in every attempt with the same panic, which shows that
// each new attempt got the side input again (GO-167): without it, an
// attempt after the first would start without customers and reject every
// order instead. The failed attempt is kept with all of the side input.
// Version 2, run with its log level at debug (GO-166), must join every
// order that passed filter and warn about each blank region.
func TestTestcase21(t *testing.T) {
	e := setup(t)
	step := func(r *v1alpha1.StreamRun, name string) v1alpha1.StepStatus {
		for _, s := range r.Status.Steps {
			if s.Name == name {
				return s
			}
		}
		t.Fatalf("no step %s", name)
		return v1alpha1.StepStatus{}
	}

	r := e.create("testcase21")
	st := e.runStore(r)
	r = e.wait(r, 15*time.Minute)
	merge := step(r, "merge")
	if r.Status.Phase != v1alpha1.RunFailed || r.Status.FailedStep != "merge" ||
		!strings.Contains(r.Status.Message, "panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:") {
		t.Fatalf("phase %s, failed step %q: %s", r.Status.Phase, r.Status.FailedStep, r.Status.Message)
	}
	if rs := merge.Replicas[0]; rs.Attempt != 3 || rs.Reason != v1alpha1.ReasonComponentFailed {
		t.Errorf("merge: attempt %d, reason %s, want 3 attempts that all failed", rs.Attempt, rs.Reason)
	}
	if merge.Errors != 0 {
		t.Errorf("merge logged %d errors: an attempt ran without the customers", merge.Errors)
	}

	data, err := store.ReadAll(t.Context(), st, chunk.FailurePrefix(r.Name, string(r.UID), "merge", 0, 3)+"failure.json")
	if err != nil {
		t.Fatal(err)
	}
	var f replay.Failure
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	side, orders := 0, 0
	for _, in := range f.Inputs {
		switch {
		case in.Side && in.Port == "customers":
			side++
		case in.Port == "orders":
			orders++
		}
	}
	if side != 1 || orders == 0 {
		t.Errorf("failure.json has %d side input chunks and %d orders chunks, want the customers and some orders", side, orders)
	}

	fixed := e.load("testcase21")
	fixed.Name = "testcase21-v2"
	for i := range fixed.Spec.Steps {
		if fixed.Spec.Steps[i].Name == "merge" {
			fixed.Spec.Steps[i].Image = need(t, "SHOP_MERGE_V2_IMAGE")
			fixed.Spec.Steps[i].LogLevel = "debug"
		}
	}
	if err := e.c.Create(t.Context(), fixed); err != nil {
		t.Fatal(err)
	}
	fixed = e.wait(fixed, 15*time.Minute)
	if fixed.Status.Phase != v1alpha1.RunSucceeded {
		t.Fatalf("with version 2: phase %s: %s", fixed.Status.Phase, fixed.Status.Message)
	}
	filtered := step(fixed, "filter").Counts["orders_out"]
	merge = step(fixed, "merge")
	if merge.Counts["orders"] != filtered || step(fixed, "sort").Counts["orders"] != filtered {
		t.Errorf("filter passed %d orders, merge joined %d and sort wrote %d", filtered, merge.Counts["orders"], step(fixed, "sort").Counts["orders"])
	}
	if merge.Warnings == 0 || merge.Warnings != merge.Counts["blank_region"] || merge.Errors != 0 {
		t.Errorf("merge: %d warnings, %d blank regions, %d errors", merge.Warnings, merge.Counts["blank_region"], merge.Errors)
	}
	t.Logf("version 2 joined %d orders, %d of them with a blank region", filtered, merge.Warnings)

	// Getting the data out with the CLI (SPEC 9.1). It finds the storage
	// from here by the annotations on the run's storage Secret, as the one
	// who runs the cluster sets them, without flags (GO-156).
	var secret corev1.Secret
	if err := e.c.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: "alpheus-s3"}, &secret); err != nil {
		t.Fatal(err)
	}
	secret.Annotations = map[string]string{cli.AnnotationExternalEndpoint: need(t, "ALPHEUS_E2E_S3_ENDPOINT")}
	if ca := os.Getenv("ALPHEUS_E2E_S3_CA_FILE"); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			t.Fatal(err)
		}
		secret.Annotations[cli.AnnotationExternalCA] = string(pem)
	}
	if err := e.c.Update(t.Context(), &secret); err != nil {
		t.Fatal(err)
	}
	alpheus := func(args ...string) (string, string, int) {
		args = append(args, "-n", e.ns)
		var stdout, stderr bytes.Buffer
		env := cli.DefaultEnv()
		env.Stdout, env.Stderr = &stdout, &stderr
		env.Getenv = func(string) string { return "" }
		code := cli.Main(t.Context(), args, env)
		return stdout.String(), stderr.String(), code
	}
	// The failed run's result is fetched and said to be incomplete
	// (GO-152).
	failedDir := t.TempDir()
	if _, stderr, code := alpheus("results", "get", r.Name, "-d", failedDir, "--complete"); code != 1 || !strings.Contains(stderr, "incomplete") {
		t.Errorf("results get --complete of the failed run: exit %d\n%s", code, stderr)
	}

	dir := t.TempDir()
	if _, stderr, code := alpheus("results", "get", fixed.Name, "-d", dir, "--complete", "--errors"); code != 0 {
		t.Fatalf("results get: exit %d\n%s", code, stderr)
	}
	var ix cli.Index
	if data, err := os.ReadFile(filepath.Join(dir, cli.IndexFile)); err != nil || json.Unmarshal(data, &ix) != nil {
		t.Fatalf("%s: %v", cli.IndexFile, err)
	}
	var want int64
	for _, res := range fixed.Status.Results {
		want += res.Chunks
	}
	if int64(len(ix.Chunks)) != want || want == 0 {
		t.Errorf("results get fetched %d chunks, status.results has %d", len(ix.Chunks), want)
	}
	for _, c := range ix.Chunks {
		if fi, err := os.Stat(filepath.Join(dir, filepath.FromSlash(c.File))); err != nil || fi.Size() != c.Bytes {
			t.Errorf("%s: %v", c.File, err)
		}
	}
	out, stderr, code := alpheus("results", "ls", fixed.Name, "-o", "json")
	var listed cli.Index
	if code != 0 || json.Unmarshal([]byte(out), &listed) != nil || len(listed.Chunks) != len(ix.Chunks) ||
		!strings.HasPrefix(listed.Chunks[0].URL, "s3://") {
		t.Errorf("results ls: exit %d, %d chunks\n%s", code, len(listed.Chunks), stderr)
	}
	first := ix.Chunks[0]
	out, stderr, code = alpheus("results", "cat", fixed.Name, first.Result, "--replica", strconv.Itoa(first.Replica), "--chunks", "1")
	if code != 0 || int64(len(out)) != first.Bytes {
		t.Errorf("results cat --chunks 1: exit %d, %d bytes, want %d\n%s", code, len(out), first.Bytes, stderr)
	}

	// results copy on the same endpoint: the storage copies the chunks,
	// and run again it finds them there (GO-155).
	prefix := "e2e-copies/" + e.ns + "/"
	t.Cleanup(func() {
		for key, err := range st.List(context.Background(), prefix) {
			if err == nil {
				_ = st.Delete(context.Background(), key)
			}
		}
	})
	to := "s3://" + fixed.Spec.Storage.Bucket + "/" + prefix
	out, stderr, code = alpheus("results", "copy", fixed.Name, "--to", to, "--to-secret", fixed.Spec.Storage.SecretRef, "--complete")
	if code != 0 || !strings.Contains(out, fmt.Sprintf("Copied %d chunks", len(ix.Chunks))) || !strings.Contains(out, "by the storage") {
		t.Errorf("results copy: exit %d\n%s%s", code, out, stderr)
	}
	for _, c := range ix.Chunks {
		if n, err := store.Size(t.Context(), st, prefix+c.File); err != nil || n != c.Bytes {
			t.Errorf("copy of %s: %d bytes, %v; want %d", c.File, n, err, c.Bytes)
		}
	}
	if _, err := store.Size(t.Context(), st, prefix+cli.IndexFile); err != nil {
		t.Errorf("the copy's %s: %v", cli.IndexFile, err)
	}
	out, stderr, code = alpheus("results", "copy", fixed.Name, "--to", to, "--to-secret", fixed.Spec.Storage.SecretRef)
	if code != 0 || !strings.Contains(out, fmt.Sprintf("%d were already there", len(ix.Chunks))) {
		t.Errorf("results copy again: exit %d\n%s%s", code, out, stderr)
	}

	// The error log has merge's warnings, one per line with -o json.
	out, stderr, code = alpheus("errors", fixed.Name, "merge", "-o", "json")
	if lines := strings.Count(out, "\n"); code != 0 || int64(lines) != merge.Warnings {
		t.Errorf("errors merge -o json: exit %d, %d entries, status has %d warnings\n%s", code, lines, merge.Warnings, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "_errors", "merge", "0", "manifest.json")); err != nil {
		t.Errorf("results get --errors: %v", err)
	}
}
