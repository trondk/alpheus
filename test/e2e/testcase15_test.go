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
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

// TestTestcase15 looks at a failed run through alpheus ui in the cluster,
// installed by the chart and reached through a port-forward, as the chart's
// notes say (GO-143).
//
// Flow: source → pass → sink, where pass fails as in testcase14.
//
// What it tests: that the page in the cluster shows the runs of every
// namespace, a failed replica's attempts, kept logs and failure, and that
// its replay button runs the attempt as a pod in the run's namespace
// (GO-141).
//
// Done to the run: nothing; the bug fails the run.
//
// Passes when: the run is Failed with ComponentFailed; the page lists it
// among the runs of every namespace; it shows the attempts and the kept log;
// and the replay runs in the cluster, fails the same way at debug level, and
// its pod is deleted afterwards.
func TestTestcase15(t *testing.T) {
	b := newBench(t, "testcase15", nil)
	if b.Chunks < 8 {
		t.Skipf("%d chunks; pass fails on chunk 5 and needs a few after it", b.Chunks)
	}
	e := setup(t)
	r := e.wait(e.create(b.Testcase), 5*time.Minute)
	if r.Status.Reason != v1alpha1.ReasonComponentFailed {
		t.Fatalf("phase %s reason %s, want ComponentFailed", r.Status.Phase, r.Status.Reason)
	}

	base := e.forwardUI()
	get := func(path string) string {
		t.Helper()
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d %s", path, resp.StatusCode, body)
		}
		return string(body)
	}
	page := get("/")
	m := regexp.MustCompile(`name="alpheus-token" content="([0-9a-f]+)"`).FindStringSubmatch(page)
	if m == nil || !strings.Contains(page, `name="alpheus-replay" content="cluster"`) {
		t.Fatalf("page has no token, or replay is not in the cluster:\n%.600s", page)
	}
	if runs := get("/api/runs"); !strings.Contains(runs, `"namespace":"`+e.ns+`","name":"testcase15"`) {
		t.Errorf("the runs of every namespace do not have this one: %.400s", runs)
	}
	run := "/api/runs/" + e.ns + "/testcase15"
	q := "?step=pass&replica=0"
	if a := get(run + "/attempts" + q); !strings.Contains(a, `{"attempt":2,"logs":true,"failure":true}`) {
		t.Errorf("attempts: %s", a)
	}
	if l := get(run + "/logs" + q + "&attempt=2"); !strings.Contains(l, "cannot handle") {
		t.Errorf("kept log: %.400s", l)
	}

	req, _ := http.NewRequest("POST", base+run+"/replay"+q+"&attempt=2", nil)
	req.Header.Set("X-Alpheus-Token", m[1])
	resp, err := (&http.Client{Timeout: 4 * time.Minute}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	t.Logf("replay:\n%s", out)
	for _, want := range []string{"Replaying in pod " + e.ns + "/", "fetched the failed attempt",
		`"level":"DEBUG","msg":"read chunk"`, "source chunk 5) is one this component cannot handle", "exited with code 1, as in the run"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("replay has no %q", want)
		}
	}
	e.eventually("the replay pod deleted", time.Minute, func() bool {
		var pods corev1.PodList
		if err := e.c.List(t.Context(), &pods, client.InNamespace(e.ns), client.MatchingLabels{controller.LabelReplay: "testcase15"}); err != nil {
			t.Fatal(err)
		}
		return len(pods.Items) == 0
	})
}

// forwardUI runs kubectl port-forward to the UI's pod, as the chart's
// notes say to, and returns the local address.
func (e *env) forwardUI() string {
	e.t.Helper()
	var pods corev1.PodList
	if err := e.c.List(e.t.Context(), &pods, client.InNamespace(controllerNamespace), client.MatchingLabels{"app.kubernetes.io/name": "alpheus-ui"}); err != nil {
		e.t.Fatal(err)
	}
	pod := ""
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp.IsZero() {
			pod = p.Name
		}
	}
	if pod == "" {
		e.t.Fatal("no running alpheus-ui pod: hack/e2e.sh installs it with ui.enabled")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", controllerNamespace, "pod/"+pod, fmt.Sprintf("%d:7171", port))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		e.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cancel(); cmd.Wait() })
	ready := make(chan bool, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "Forwarding from") {
				ready <- true
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			e.t.Fatal("kubectl port-forward to the UI ended")
		}
	case <-time.After(30 * time.Second):
		e.t.Fatal("the port-forward to the UI did not get ready")
	}
	// Through the forward, the page sees the host localhost, as a browser
	// on this machine does.
	return fmt.Sprintf("http://localhost:%d", port)
}
