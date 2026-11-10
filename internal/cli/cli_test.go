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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

var (
	k8s client.WithWatch
	nsN atomic.Int32
)

func envtestAssets() string {
	if dir := os.Getenv("KUBEBUILDER_ASSETS"); dir != "" {
		return dir
	}
	home, _ := os.UserHomeDir()
	matches, _ := filepath.Glob(filepath.Join(home, ".local/share/kubebuilder-envtest/k8s/*"))
	if len(matches) == 0 {
		return ""
	}
	return matches[len(matches)-1]
}

func TestMain(m *testing.M) {
	assets := envtestAssets()
	if assets == "" {
		fmt.Println("skipping CLI tests: no envtest binaries (run: go tool setup-envtest use)")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		BinaryAssetsDirectory: assets,
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: Scheme()})
	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Println(err)
	}
	env.Stop()
	os.Exit(code)
}

// harness runs commands against envtest, in a namespace of its own, with
// storage in memory.
type harness struct {
	t      *testing.T
	ns     string
	mem    *store.Memory
	now    time.Time
	kube   *kubefake.Clientset
	stdin  string
	stdout bytes.Buffer
	stderr bytes.Buffer
	// commands are the programs RunCommand was asked to run, and exitCode
	// what it answers.
	commands [][]string
	exitCode int
	// opened are the addresses the storage was opened at, and environ
	// the environment the commands see.
	opened  []store.Address
	environ map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ns := fmt.Sprintf("cli-%d", nsN.Add(1))
	if err := k8s.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, ns: ns, mem: store.NewMemory(), now: time.Now(), kube: kubefake.NewClientset()}
}

func (h *harness) env() *Env {
	return &Env{
		Stdin:  strings.NewReader(h.stdin),
		Stdout: &h.stdout,
		Stderr: &h.stderr,
		Connect: func(Kube) (*Clients, error) {
			return &Clients{Client: k8s, Kube: h.kube, Namespace: h.ns}, nil
		},
		OpenStore: func(_ context.Context, _ client.Reader, _ *v1alpha1.StreamRun, at store.Address) (store.Store, error) {
			h.opened = append(h.opened, at)
			return h.mem, nil
		},
		NewS3: func(cfg store.S3Config) (store.Store, error) {
			h.opened = append(h.opened, store.Address{Endpoint: cfg.Endpoint, CAFile: cfg.CAFile, CAPEM: cfg.CAPEM})
			return h.mem, nil
		},
		Now:    func() time.Time { return h.now },
		Getenv: func(k string) string { return h.environ[k] },
		RunCommand: func(_ context.Context, args []string, stdout, _ io.Writer) (int, error) {
			h.commands = append(h.commands, args)
			fmt.Fprintln(stdout, "component output")
			return h.exitCode, nil
		},
	}
}

// run runs a command line and returns its exit code; the output is in
// h.stdout and h.stderr, cleared first.
func (h *harness) run(args ...string) int {
	h.stdout.Reset()
	h.stderr.Reset()
	return Main(context.Background(), args, h.env())
}

func (h *harness) mustRun(args ...string) string {
	h.t.Helper()
	if code := h.run(args...); code != 0 {
		h.t.Fatalf("alpheus %s: exit %d\n%s%s", strings.Join(args, " "), code, h.stdout.String(), h.stderr.String())
	}
	return h.stdout.String()
}

func (h *harness) file(name, content string) string {
	p := filepath.Join(h.t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	return p
}

func (h *harness) get(name string) *v1alpha1.StreamRun {
	h.t.Helper()
	var r v1alpha1.StreamRun
	if err := k8s.Get(context.Background(), client.ObjectKey{Namespace: h.ns, Name: name}, &r); err != nil {
		h.t.Fatal(err)
	}
	return &r
}

const pin = "@sha256:2222222222222222222222222222222222222222222222222222222222222222"

func flow(name, extra string) string {
	return `apiVersion: alpheus-dev.org/v1alpha1
kind: StreamRun
metadata:
  name: ` + name + `
spec:
  steps:
    - {name: read, image: registry.example/read` + pin + `}
    - {name: filter, image: registry.example/filter` + pin + `, replicas: 2}
    - {name: sum, image: registry.example/sum` + pin + `}
  edges:
    - {from: read, to: filter}
    - {from: filter, to: sum}
  chunks: {maxBytes: 64Mi, maxUnacked: 4}
  storage: {endpoint: http://s3.example:3900, bucket: flows, secretRef: alpheus-s3}
` + extra
}

// run can give a step another image and log level, and the run another
// name, without editing the flow file (GO-161, GO-166).
func TestRunOverrides(t *testing.T) {
	h := newHarness(t)
	f := h.file("flow.yaml", flow("sales", ""))
	h.mustRun("run", "-f", f, "--name", "sales-2", "--image", "filter=registry.example/filter:v2"+pin, "--log-level", "filter=debug")
	r := h.get("sales-2")
	if s := r.Spec.Steps[1]; s.Image != "registry.example/filter:v2"+pin || s.LogLevel != "debug" {
		t.Errorf("filter: image %s, log level %q", s.Image, s.LogLevel)
	}
	if s := r.Spec.Steps[0]; s.LogLevel != "" || s.Image != "registry.example/read"+pin {
		t.Errorf("read changed: %+v", s)
	}
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--image", "nope=x"}, `no step "nope"; its steps are read, filter, sum`},
		{[]string{"--log-level", "filter"}, "want <step>=<value>"},
		{[]string{"--log-level", "filter=loud"}, `log level "loud": want debug, info, warn or error`},
	} {
		h.stderr.Reset()
		if code := h.run(append([]string{"run", "-f", f, "--name", "sales-3"}, c.args...)...); code != 2 || !strings.Contains(h.stderr.String(), c.want) {
			t.Errorf("%v: exit %d, %q", c.args, code, h.stderr.String())
		}
	}
}

func TestRunGetCancel(t *testing.T) {
	h := newHarness(t)
	f := h.file("flow.yaml", flow("sales", ""))

	out := h.mustRun("run", "-f", f)
	if !strings.Contains(out, "streamrun "+h.ns+"/sales created") || !strings.Contains(out, "alpheus watch sales") {
		t.Errorf("run: %q", out)
	}
	if code := h.run("run", "-f", f); code != 1 || !strings.Contains(h.stderr.String(), "already exists") {
		t.Errorf("run twice: exit %d, %q", code, h.stderr.String())
	}

	out = h.mustRun("get")
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "sales") {
		t.Errorf("get: %q", out)
	}
	var list v1alpha1.StreamRunList
	if err := json.Unmarshal([]byte(h.mustRun("get", "-o", "json")), &list); err != nil || len(list.Items) != 1 || list.Kind != "StreamRunList" {
		t.Errorf("get -o json: %v, %+v", err, list)
	}
	out = h.mustRun("get", "sales", "-o", "yaml")
	if !strings.Contains(out, "kind: StreamRun\n") || !strings.Contains(out, "name: sales") {
		t.Errorf("get sales -o yaml: %q", out)
	}

	// The status as the controller writes it.
	r := h.get("sales")
	r.Status = v1alpha1.StreamRunStatus{
		Phase: v1alpha1.RunRunning, StepsFinished: "1/3",
		Steps: []v1alpha1.StepStatus{
			{Name: "read", Phase: v1alpha1.StepSucceeded, Replicas: []v1alpha1.ReplicaStatus{{Index: 0, Phase: v1alpha1.StepSucceeded, Attempt: 1, Pod: "sales-read-0-1"}}},
			{Name: "filter", Phase: v1alpha1.StepRunning, Errors: 2, ErrorLog: "results/sales/x/_errors/filter/", Counts: map[string]int64{"rows": 10}, Replicas: []v1alpha1.ReplicaStatus{
				{Index: 0, Phase: v1alpha1.StepRunning, Attempt: 2, Restarts: 1, Message: "chunk 4"},
				{Index: 1, Phase: v1alpha1.StepRunning, Attempt: 1},
			}},
		},
		Edges: []v1alpha1.EdgeStatus{{Name: "read-filter", Chunks: 10, Acked: 7, Bytes: 10 << 20}},
	}
	if err := k8s.Status().Update(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	out = h.mustRun("get", "sales")
	for _, want := range []string{"Running", "read-filter", "10.0 MiB", "rows=10", "chunk 4", "Error log of filter: results/sales/x/_errors/filter/"} {
		if !strings.Contains(out, want) {
			t.Errorf("get sales: no %q in\n%s", want, out)
		}
	}
	// The lag of read-filter is 3.
	if !strings.Contains(out, "read-filter  10      10.0 MiB  7      3") {
		t.Errorf("get sales: no lag 3 in\n%s", out)
	}

	out = h.mustRun("cancel", "sales")
	if !strings.Contains(out, "cancelled") || !h.get("sales").Spec.Cancel {
		t.Errorf("cancel: %q, cancel=%t", out, h.get("sales").Spec.Cancel)
	}

	if code := h.run("get", "nothing"); code != 1 || !strings.Contains(h.stderr.String(), "alpheus get -n "+h.ns) {
		t.Errorf("get nothing: exit %d, %q", code, h.stderr.String())
	}
	if code := h.run("get", "-o", "xml"); code != 2 {
		t.Errorf("get -o xml: exit %d", code)
	}
}

func TestValidate(t *testing.T) {
	h := newHarness(t)

	out := h.mustRun("validate", "-f", h.file("ok.yaml", flow("ok", "")))
	if !strings.Contains(out, "is valid") {
		t.Errorf("validate: %q", out)
	}
	var v validation
	if err := json.Unmarshal([]byte(h.mustRun("validate", "-f", h.file("ok.yaml", flow("ok", "")), "-o", "json")), &v); err != nil || !v.Valid || !v.Server {
		t.Errorf("validate -o json: %v %+v", err, v)
	}
	// Nothing was created.
	var list v1alpha1.StreamRunList
	if err := k8s.List(context.Background(), &list, client.InNamespace(h.ns)); err != nil || len(list.Items) != 0 {
		t.Errorf("validate created %d runs (%v)", len(list.Items), err)
	}

	for _, c := range []struct {
		name, flow, want string
	}{
		{"cycle", strings.Replace(flow("c", ""), "- {from: filter, to: sum}", "- {from: filter, to: sum}\n    - {from: sum, to: read}", 1), "cycle"},
		{"no step", strings.Replace(flow("c", ""), "to: sum}", "to: nope}", 1), `no step "nope"`},
		{"unknown field", strings.Replace(flow("c", ""), "maxUnacked: 4", "maxUnacked: 4, maxUnackd: 4", 1), "unknown field"},
		{"CRD schema", strings.Replace(flow("c", ""), "maxUnacked: 4", "maxUnacked: 5000", 1), "maxUnacked"},
		{"not a run", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\n", "not a StreamRun"},
	} {
		if code := h.run("validate", "-f", h.file("bad.yaml", c.flow)); code != 1 || !strings.Contains(h.stderr.String(), c.want) {
			t.Errorf("%s: exit %d, %q", c.name, code, h.stderr.String())
		}
	}

	out = h.mustRun("validate", "--local", "-f", h.file("tag.yaml", strings.Replace(flow("t", ""), "read"+pin, "read:latest", 1)))
	if !strings.Contains(out, "warning: step read: image registry.example/read:latest is not pinned") || !strings.Contains(out, "local checks only") {
		t.Errorf("validate unpinned: %q", out)
	}
	if code := h.run("validate"); code != 2 {
		t.Errorf("validate without -f: exit %d", code)
	}
}

func TestWatch(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("w", "")))

	done := make(chan int)
	go func() { done <- h.run("watch", "w", "-o", "json") }()
	time.Sleep(500 * time.Millisecond)
	for _, phase := range []v1alpha1.RunPhase{v1alpha1.RunRunning, v1alpha1.RunSucceeded} {
		r := h.get("w")
		r.Status.Phase = phase
		if err := k8s.Status().Update(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("watch: exit %d, %s", code, h.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not end when the run succeeded")
	}
	var phases []string
	for _, line := range strings.Split(strings.TrimSpace(h.stdout.String()), "\n") {
		var r v1alpha1.StreamRun
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("watch -o json line %q: %v", line, err)
		}
		phases = append(phases, string(r.Status.Phase))
	}
	if got := strings.Join(phases, ","); got != ",Running,Succeeded" {
		t.Errorf("watch phases %q, want ,Running,Succeeded", got)
	}

	// A failed run makes watch exit with 1.
	r := h.get("w")
	r.Status.Phase, r.Status.Message = v1alpha1.RunFailed, "step sum failed"
	if err := k8s.Status().Update(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if code := h.run("watch", "w"); code != 1 || !strings.Contains(h.stderr.String(), "finished as Failed: step sum failed") {
		t.Errorf("watch failed run: exit %d, %q", code, h.stderr.String())
	}
}

func TestChunks(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("c", "")))
	r := h.get("c")
	ctx := context.Background()

	m := chunk.NewManifest("c", string(r.UID), "filter-sum", 1, 2)
	for n := int64(1); n <= 3; n++ {
		if _, err := m.Append(chunk.Chunk{N: n, Ext: "csv", Bytes: 1000, SHA256: strings.Repeat("ab", 32), Src: &chunk.Source{Edge: "read-filter", Replica: 0, N: n}}); err != nil {
			t.Fatal(err)
		}
	}
	m.End()
	data, _ := m.Marshal()
	s := chunk.EdgeStream(string(r.UID), "filter-sum", 1)
	if err := h.mem.Put(ctx, s.ManifestKey(), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}

	out := h.mustRun("chunks", "c", "filter-sum")
	for _, want := range []string{"(no manifest yet)", "part-000003.csv", "read-filter/0 #2", "abababababab…"} {
		if !strings.Contains(out, want) {
			t.Errorf("chunks: no %q in\n%s", want, out)
		}
	}
	var views []streamView
	if err := json.Unmarshal([]byte(h.mustRun("chunks", "c", "filter-sum", "-o", "json")), &views); err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 || views[0].Found || !views[1].Found || len(views[1].Chunks) != 3 || !views[1].EOS || views[1].Epoch != 2 {
		t.Errorf("chunks -o json: %+v", views)
	}
	if code := h.run("chunks", "c", "sum-read"); code != 1 || !strings.Contains(h.stderr.String(), "read-filter, filter-sum") {
		t.Errorf("chunks of a missing edge: exit %d, %q", code, h.stderr.String())
	}
}

func TestResultsGC(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := k8s.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: "s3"}}); err != nil {
		t.Fatal(err)
	}
	h.mustRun("run", "-f", h.file("flow.yaml", flow("live", "")))
	live := h.get("live")

	put := func(key, body string) {
		if err := h.mem.Put(ctx, key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatal(err)
		}
	}
	result := func(name, uid string, finished *time.Time, ttl string) {
		info, _ := json.Marshal(agent.RunInfo{Name: name, Namespace: h.ns, UID: uid, FinishedAt: finished, ResultsTTL: ttl})
		put("results/"+name+"/"+uid+"/run.json", string(info))
		put("results/"+name+"/"+uid+"/sum/default/0/part-000001.csv", "x")
	}
	old, recent := h.now.Add(-10*24*time.Hour), h.now.Add(-time.Hour)
	result("expired", "u1", &old, "168h0m0s")
	result("live", string(live.UID), &recent, "168h0m0s")
	result("orphan", "u3", &recent, "")
	result("forever", "u4", &old, "0s")
	put("results/stray/u5/sum/default/0/part-000001.csv", "x")

	gc := []string{"results", "gc", "--endpoint", "http://s3", "--bucket", "flows", "--secret", "s3"}
	out := h.mustRun(append(gc, "--dry-run")...)
	for _, want := range []string{"delete  expired", "keep    live", "keep    orphan", "keep    forever", "no run.json"} {
		if !strings.Contains(out, want) {
			t.Errorf("gc --dry-run: no %q in\n%s", want, out)
		}
	}
	exists := func(key string) bool {
		_, err := store.ReadAll(ctx, h.mem, key)
		return err == nil
	}
	if !exists("results/expired/u1/run.json") {
		t.Fatal("--dry-run deleted")
	}

	h.stdin = "n\n"
	h.mustRun(gc...)
	if !exists("results/expired/u1/run.json") || !strings.Contains(h.stderr.String(), "Nothing deleted") {
		t.Fatalf("gc answered no: %q", h.stderr.String())
	}
	h.stdin = "y\n"
	h.mustRun(gc...)
	if exists("results/expired/u1/run.json") || exists("results/expired/u1/sum/default/0/part-000001.csv") {
		t.Error("gc did not delete the expired results")
	}
	if !exists("results/orphan/u3/run.json") || !exists("results/live/"+string(live.UID)+"/run.json") {
		t.Error("gc deleted results it should keep")
	}

	var sets []resultSet
	if err := json.Unmarshal([]byte(h.mustRun(append(gc, "--orphans", "--yes", "-o", "json")...)), &sets); err != nil {
		t.Fatal(err)
	}
	if exists("results/orphan/u3/run.json") || !exists("results/forever/u4/run.json") || !exists("results/live/"+string(live.UID)+"/run.json") {
		t.Errorf("gc --orphans: %+v", sets)
	}
	if code := h.run("results", "gc"); code != 2 {
		t.Errorf("gc without flags: exit %d", code)
	}
}

func TestLogs(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("l", "")))
	ctx := context.Background()

	if code := h.run("logs", "l"); code != 1 || !strings.Contains(h.stderr.String(), "has no pods") {
		t.Errorf("logs without pods: exit %d, %q", code, h.stderr.String())
	}
	for _, p := range []struct {
		step    string
		replica int
	}{{"sum", 0}, {"filter", 1}, {"filter", 0}} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: fmt.Sprintf("l-%s-%d-1", p.step, p.replica), Labels: map[string]string{
				controller.LabelRun: "l", controller.LabelStep: p.step, controller.LabelReplica: fmt.Sprint(p.replica),
			}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "component", Image: "x"}}},
		}
		if err := k8s.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	// The fake clientset answers every log request with "fake logs".
	out := h.mustRun("logs", "l")
	if want := "[filter/0] fake logs\n[filter/1] fake logs\n[sum/0] fake logs\n"; out != want {
		t.Errorf("logs: %q, want %q", out, want)
	}
	out = h.mustRun("logs", "l", "sum", "-o", "json")
	var line logLine
	if err := json.Unmarshal([]byte(out), &line); err != nil || line.Step != "sum" || line.Container != "component" || line.Line != "fake logs" {
		t.Errorf("logs sum -o json: %q, %v", out, err)
	}
	if code := h.run("logs", "l", "nope"); code != 1 || !strings.Contains(h.stderr.String(), "read, filter, sum") {
		t.Errorf("logs of a missing step: exit %d, %q", code, h.stderr.String())
	}
	if code := h.run("logs", "l", "-c", "sidecar"); code != 2 {
		t.Errorf("logs -c sidecar: exit %d", code)
	}
}

func TestVersion(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	labels := map[string]string{"app.kubernetes.io/name": "alpheus-controller"}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: "alpheus-controller", Labels: map[string]string{
			"app.kubernetes.io/name": "alpheus-controller", "app.kubernetes.io/version": "v0.1.0",
		}},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "controller", Image: "registry.alpheus-dev.org/alpheus-dev/controller:v0.1.0"}}},
			},
		},
	}
	if err := k8s.Create(ctx, d); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, d) })
	var v versions
	if err := json.Unmarshal([]byte(h.mustRun("version", "-o", "json")), &v); err != nil {
		t.Fatal(err)
	}
	if v.CLI == "" || len(v.CRD) != 1 || v.CRD[0] != "v1alpha1 (storage)" || len(v.Controllers) != 1 || v.Controllers[0].Version != "v0.1.0" {
		t.Errorf("version: %+v", v)
	}

	// Without a cluster the CLI still shows its own version.
	e := h.env()
	e.Connect = func(Kube) (*Clients, error) { return nil, fmt.Errorf("no cluster") }
	h.stdout.Reset()
	if code := Main(ctx, []string{"version"}, e); code != 0 || !strings.Contains(h.stdout.String(), "CLI:") || !strings.Contains(h.stdout.String(), "no cluster") {
		t.Errorf("version without cluster: exit %d, %q", code, h.stdout.String())
	}
}

// The website's CLI reference has every command and its own flags, and
// leaves out the flags every command has.
func TestReference(t *testing.T) {
	var b bytes.Buffer
	if err := Reference(&b); err != nil {
		t.Fatal(err)
	}
	ref := b.String()
	for _, c := range sections() {
		if !strings.Contains(ref, "\n## alpheus "+c.name+"\n") {
			t.Errorf("no section for %s", c.name)
		}
		if shapes[c.name] == "" && c.name != "ui" {
			t.Errorf("no -o json shape for %s (GO-163)", c.name)
		}
	}
	for _, want := range []string{"| `-A` | runs in all namespaces |", "| `--orphans` |", "alpheus results gc\n", "| `--parallel int` |", "| `--tail int` |"} {
		if !strings.Contains(ref, want) {
			t.Errorf("no %q", want)
		}
	}
	if strings.Count(ref, "`--kubeconfig`") != 1 {
		t.Error("the common flags are listed more than once")
	}
}

// replay fetches a failed attempt's inputs, checked, and runs the same
// image on them in local mode at debug level (GO-141); logs --kept shows
// the attempt's logs (GO-140).
func TestReplayAndKeptLogs(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("rp", `  ttlAfterFinished: 1h`)))
	r := h.get("rp")
	uid, ctx := string(r.UID), context.Background()

	body := "a,b\n1,2\n"
	sum := sha256.Sum256([]byte(body))
	prefix := chunk.FailurePrefix("rp", uid, "filter", 1, 2)
	h.mem.Put(ctx, prefix+"input/read-filter-0-000003.csv", strings.NewReader(body), int64(len(body)))
	f := replay.Failure{Version: 1, Run: "rp", UID: uid, Step: "filter", Replica: 1, Replicas: 2, Attempt: 2,
		Reason: v1alpha1.ReasonComponentFailed, Message: "the component exited with code 1: bad row",
		Image: "registry.example/filter" + pin, Settings: json.RawMessage(`{"match":"ERROR"}`),
		Inputs: []replay.Input{{Port: "default", Edge: "read-filter", N: 3, Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), File: "read-filter-0-000003.csv"}}}
	data, _ := json.Marshal(f)
	h.mem.Put(ctx, prefix+"failure.json", bytes.NewReader(data), int64(len(data)))
	// An older attempt, which replay passes over for the last one.
	h.mem.Put(ctx, chunk.FailurePrefix("rp", uid, "filter", 1, 1)+"failure.json", strings.NewReader("{}"), 2)

	dir := filepath.Join(t.TempDir(), "replay")
	h.exitCode = 1
	out := h.mustRun("replay", "rp", "filter", "--replica", "1", "--dir", dir, "-e", "TOKEN=x")
	got, err := os.ReadFile(filepath.Join(dir, "in", "default", "read-filter-0-000003.csv"))
	if err != nil || string(got) != body {
		t.Fatalf("input %q (%v)", got, err)
	}
	if len(h.commands) != 1 {
		t.Fatalf("commands %v", h.commands)
	}
	cmd := strings.Join(h.commands[0], " ")
	for _, want := range []string{"podman run --rm --network=none --read-only --tmpfs /tmp", "TMPDIR=/tmp", ":/replay:Z", "ALPHEUS_MODE=local", "ALPHEUS_LOG_LEVEL=debug",
		"ALPHEUS_LOCAL_INPUT=/replay/in/default", `ALPHEUS_SETTINGS={"match":"ERROR"}`, "TOKEN=x", "registry.example/filter" + pin} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command has no %q: %s", want, cmd)
		}
	}
	for _, want := range []string{"attempt 2: ComponentFailed", "bad row", "1 chunks", "component output", "exited with code 1, as in the run"} {
		if !strings.Contains(out, want) {
			t.Errorf("replay: no %q in\n%s", want, out)
		}
	}

	// With -o json, stdout is only the JSON; what the component prints
	// goes to stderr (GO-163).
	var view replayView
	if err := json.Unmarshal([]byte(h.mustRun("replay", "rp", "filter", "--replica", "1", "--dir", dir, "-o", "json")), &view); err != nil {
		t.Fatalf("replay -o json: %v\n%s", err, h.stdout.String())
	}
	if view.ExitCode == nil || *view.ExitCode != 1 || view.Failure.Attempt != 2 || len(view.Command) == 0 || !strings.Contains(h.stderr.String(), "component output") {
		t.Errorf("replay -o json: %+v, stderr %q", view, h.stderr.String())
	}

	// A chunk that changed is refused.
	h.mem.Put(ctx, prefix+"input/read-filter-0-000003.csv", strings.NewReader("changed"), 7)
	if code := h.run("replay", "rp", "filter", "--replica", "1", "--dir", dir, "--with", "none"); code != 1 || !strings.Contains(h.stderr.String(), "SHA-256") {
		t.Errorf("changed chunk: exit %d, %q", code, h.stderr.String())
	}
	if code := h.run("replay", "rp", "filter", "--replica", "0", "--with", "none"); code != 1 || !strings.Contains(h.stderr.String(), "no kept failure") {
		t.Errorf("no failure: exit %d, %q", code, h.stderr.String())
	}

	// The chunks replica 1 never processed after it gave up, all of them.
	if code := h.run("replay", "rp", "filter", "--replica", "1", "--skipped", "--with", "none"); code != 1 || !strings.Contains(h.stderr.String(), "no skipped chunks kept") {
		t.Errorf("no skipped chunks: exit %d, %q", code, h.stderr.String())
	}
	skipped := chunk.SkippedPrefix("rp", uid, "filter", 1)
	f.Skipped, f.NotKept, f.Inputs = true, 1, nil
	for _, n := range []int64{3, 5} {
		file := fmt.Sprintf("read-filter-0-%06d.csv", n)
		h.mem.Put(ctx, skipped+"input/"+file, strings.NewReader(body), int64(len(body)))
		f.Inputs = append(f.Inputs, replay.Input{Port: "default", Edge: "read-filter", N: n, Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), File: file})
	}
	data, _ = json.Marshal(f)
	h.mem.Put(ctx, skipped+"failure.json", bytes.NewReader(data), int64(len(data)))
	sdir := filepath.Join(t.TempDir(), "skipped")
	out = h.mustRun("replay", "rp", "filter", "--replica", "1", "--skipped", "--dir", sdir, "--with", "none")
	for _, want := range []string{"Skipped: the 2 chunks", "and 1 more that were not kept"} {
		if !strings.Contains(out, want) {
			t.Errorf("replay --skipped: no %q in\n%s", want, out)
		}
	}
	for _, n := range []string{"000003", "000005"} {
		if _, err := os.Stat(filepath.Join(sdir, "in", "default", "read-filter-0-"+n+".csv")); err != nil {
			t.Errorf("skipped chunk %s: %v", n, err)
		}
	}
	if code := h.run("replay", "rp", "filter", "--skipped", "--attempt", "2"); code != 2 {
		t.Errorf("--skipped with --attempt: exit %d", code)
	}

	logPrefix := chunk.LogsPrefix("rp", uid, "filter", 1, 2)
	h.mem.Put(ctx, logPrefix+"component.log", strings.NewReader("line one\nline two\n"), 18)
	out = h.mustRun("logs", "rp", "filter", "--kept")
	if !strings.Contains(out, "filter replica 1 attempt 2, component") || !strings.Contains(out, "line two") {
		t.Errorf("logs --kept: %q", out)
	}
	if code := h.run("logs", "rp", "--kept", "-c", "agent"); code != 1 || !strings.Contains(h.stderr.String(), "no kept logs") {
		t.Errorf("no agent logs: exit %d, %q", code, h.stderr.String())
	}
}

// The ui answers only on localhost, serves the runs, a replica's kept logs
// and failures, and runs replay only with the page's token (GO-142).
func TestUI(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("u", "")))
	r := h.get("u")
	uid, ctx := string(r.UID), context.Background()
	h.mem.Put(ctx, chunk.LogsPrefix("u", uid, "filter", 0, 1)+"component.log", strings.NewReader("boom\n"), 5)
	f := replay.Failure{Version: 1, Run: "u", Step: "filter", Attempt: 1, Reason: v1alpha1.ReasonComponentFailed, Image: "x" + pin, Inputs: []replay.Input{}}
	data, _ := json.Marshal(f)
	h.mem.Put(ctx, chunk.FailurePrefix("u", uid, "filter", 0, 1)+"failure.json", bytes.NewReader(data), int64(len(data)))
	h.mem.Put(ctx, chunk.ErrorLogPrefix("u", uid, "filter", 0)+"log-000001.jsonl", strings.NewReader(`{"level":"error","code":"bad_row","message":"row 3"}`+"\n"), 53)

	c, _ := h.env().Connect(Kube{})
	u := &ui{e: h.env(), c: c, namespace: h.ns, token: "secret", replayMode: "podman"}
	srv := httptest.NewServer(u.handler())
	defer srv.Close()
	get := func(path string, host string) (int, string) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		if host != "" {
			req.Host = host
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get("/", ""); code != 200 || !strings.Contains(body, `content="secret"`) || !strings.Contains(body, h.ns) {
		t.Errorf("page: %d %q", code, body)
	}
	if code, _ := get("/api/runs", "attacker.example:7171"); code != http.StatusForbidden {
		t.Errorf("another host: %d, want 403", code)
	}
	if code, body := get("/api/runs", ""); code != 200 || !strings.Contains(body, `"name":"u"`) {
		t.Errorf("runs: %d %q", code, body)
	}
	if code, body := get("/api/runs/"+h.ns+"/u", ""); code != 200 || !strings.Contains(body, `"events":[]`) {
		t.Errorf("run: %d %q", code, body)
	}
	q := "?step=filter&replica=0"
	if code, body := get("/api/runs/"+h.ns+"/u/attempts"+q, ""); code != 200 || !strings.Contains(body, `{"attempt":1,"logs":true,"failure":true}`) {
		t.Errorf("attempts: %d %q", code, body)
	}
	if code, body := get("/api/runs/"+h.ns+"/u/logs"+q+"&attempt=1", ""); code != 200 || body != "boom\n" {
		t.Errorf("logs: %d %q", code, body)
	}
	if code, body := get("/api/runs/"+h.ns+"/u/errors"+q, ""); code != 200 || !strings.Contains(body, "bad_row") {
		t.Errorf("errors: %d %q", code, body)
	}
	if code, body := get("/api/runs/"+h.ns+"/u/failure"+q, ""); code != 200 || !strings.Contains(body, "ComponentFailed") {
		t.Errorf("failure: %d %q", code, body)
	}
	// The chunks the replica never processed, after it gave up (GO-97).
	sf := replay.Failure{Version: 1, Run: "u", Step: "filter", Attempt: 1, Reason: v1alpha1.ReasonComponentFailed, Image: "x" + pin, Skipped: true, NotKept: 3, Inputs: []replay.Input{}}
	data, _ = json.Marshal(sf)
	h.mem.Put(ctx, chunk.SkippedPrefix("u", uid, "filter", 0)+"failure.json", bytes.NewReader(data), int64(len(data)))
	if code, body := get("/api/runs/"+h.ns+"/u/attempts"+q, ""); code != 200 || !strings.Contains(body, `{"attempt":0,"logs":false,"failure":true,"skipped":true}`) {
		t.Errorf("attempts with skipped chunks: %d %q", code, body)
	}
	if code, body := get("/api/runs/"+h.ns+"/u/failure"+q+"&attempt=skipped", ""); code != 200 || !strings.Contains(body, `"notKept":3`) {
		t.Errorf("skipped: %d %q", code, body)
	}
	if code, _ := get("/api/runs/"+h.ns+"/nope", ""); code != 404 {
		t.Errorf("missing run: %d", code)
	}

	post := func(tok string) (int, string) {
		req, _ := http.NewRequest("POST", srv.URL+"/api/runs/"+h.ns+"/u/replay"+q+"&attempt=1", nil)
		if tok != "" {
			req.Header.Set("X-Alpheus-Token", tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := post(""); code != http.StatusForbidden || len(h.commands) != 0 {
		t.Errorf("replay without the token: %d, commands %v", code, h.commands)
	}
	if code, body := post("secret"); code != 200 || len(h.commands) != 1 || !strings.Contains(body, "component output") {
		t.Errorf("replay: %d %q, commands %v", code, body, h.commands)
	}
	req, _ := http.NewRequest("POST", srv.URL+"/api/runs/"+h.ns+"/u/replay"+q+"&attempt=skipped", nil)
	req.Header.Set("X-Alpheus-Token", "secret")
	if resp, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	} else {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(b), "--skipped") || !strings.Contains(string(b), "Skipped: the 0 chunks") {
			t.Errorf("replay of the skipped chunks: %q", b)
		}
	}
	req, _ = http.NewRequest("POST", srv.URL+"/api/runs/"+h.ns+"/u/replay"+q+"&attempt=--with=docker", nil)
	req.Header.Set("X-Alpheus-Token", "secret")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Errorf("an attempt that is a flag: %v %v", resp.StatusCode, err)
	}
	os.RemoveAll("alpheus-replay") // replay's default folder, in the test's working directory
}

// replay --with cluster runs the attempt as a pod in the run's namespace:
// the agent's image fetches the inputs, the step's image runs on them at
// debug level, the logs are printed and the pod is deleted (GO-141, GO-143).
func TestReplayInCluster(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("rc", "")))
	r := h.get("rc")
	ctx := context.Background()
	prefix := chunk.FailurePrefix("rc", string(r.UID), "filter", 0, 1)
	f := replay.Failure{Version: 1, Run: "rc", Step: "filter", Attempt: 1, Reason: v1alpha1.ReasonComponentFailed,
		Image: "registry.example/filter" + pin, Settings: json.RawMessage(`{"match":"x"}`),
		Inputs: []replay.Input{{Port: "default", N: 1, SHA256: strings.Repeat("0", 64), File: "read-filter-0-000001.csv"}}}
	data, _ := json.Marshal(f)
	h.mem.Put(ctx, prefix+"failure.json", bytes.NewReader(data), int64(len(data)))

	// No kubelet: the test plays it, failing the component with code 1.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			var pods corev1.PodList
			k8s.List(ctx, &pods, client.InNamespace(h.ns), client.MatchingLabels{controller.LabelReplay: "rc"})
			if len(pods.Items) == 1 {
				p := pods.Items[0]
				p.Status.Phase = corev1.PodFailed
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: controller.ReplayContainer,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: controller.ComponentContainer,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}
				if err := k8s.Status().Update(ctx, &p); err != nil {
					t.Error(err)
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Error("no replay pod")
	}()
	out := h.mustRun("replay", "rc", "filter", "--with", "cluster", "--agent-image", "registry.example/agent"+pin, "-e", "TOKEN=x")
	<-done
	if !strings.Contains(out, "Replaying in pod "+h.ns+"/rc-filter-0-replay-1-") || !strings.Contains(out, "exited with code 1, as in the run") {
		t.Errorf("replay: %s", out)
	}
	var pods corev1.PodList
	k8s.List(ctx, &pods, client.InNamespace(h.ns), client.MatchingLabels{controller.LabelReplay: "rc"})
	for _, p := range pods.Items {
		if p.DeletionTimestamp.IsZero() {
			t.Errorf("replay pod %s left", p.Name)
		}
	}

	// The pod it makes.
	pod, err := controller.ReplayPod(r, &f, prefix, "registry.example/agent"+pin, "x")
	if err != nil {
		t.Fatal(err)
	}
	fetch, comp := pod.Spec.InitContainers[0], pod.Spec.Containers[0]
	if fetch.Image != "registry.example/agent"+pin || strings.Join(fetch.Args, " ") != "-fetch-failure "+prefix+" -dest /replay" {
		t.Errorf("fetch container %+v", fetch)
	}
	envs := map[string]string{}
	for _, ev := range comp.Env {
		envs[ev.Name] = ev.Value
	}
	if comp.Image != f.Image || envs["ALPHEUS_MODE"] != "local" || envs["ALPHEUS_LOG_LEVEL"] != "debug" ||
		envs["ALPHEUS_LOCAL_INPUT"] != "/replay/in/default" || envs["ALPHEUS_SETTINGS"] != `{"match":"x"}` {
		t.Errorf("component container %+v", comp)
	}
	// A writable /tmp, as in the run's pods.
	if envs["TMPDIR"] != "/tmp" || !slices.ContainsFunc(comp.VolumeMounts, func(m corev1.VolumeMount) bool {
		return m.MountPath == "/tmp" && m.Name == "replay" && m.SubPath == "tmp"
	}) {
		t.Errorf("no /tmp in the replay pod: %+v", comp.VolumeMounts)
	}
	if pod.Labels[controller.LabelRun] != "" || pod.OwnerReferences[0].UID != r.UID || *pod.Spec.AutomountServiceAccountToken {
		t.Errorf("pod meta %+v, automount %v", pod.ObjectMeta, *pod.Spec.AutomountServiceAccountToken)
	}
}

// In the cluster the ui shows every namespace, answers to the host names it
// is given, and can have replay off (GO-143).
func TestUIInCluster(t *testing.T) {
	h := newHarness(t)
	h.mustRun("run", "-f", h.file("flow.yaml", flow("a", "")))
	c, _ := h.env().Connect(Kube{})
	u := &ui{e: h.env(), c: c, namespace: allNamespaces, token: "t", replayMode: "off", hosts: []string{"alpheus.example"}}
	srv := httptest.NewServer(u.handler())
	defer srv.Close()
	do := func(method, path, host string) (int, string) {
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		req.Host = host
		req.Header.Set("X-Alpheus-Token", "t")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := do("GET", "/api/runs", "alpheus.example"); code != 200 || !strings.Contains(body, `"namespace":"`+h.ns+`"`) {
		t.Errorf("runs of every namespace by the allowed host: %d %.200s", code, body)
	}
	if code, _ := do("GET", "/api/runs", "other.example"); code != http.StatusForbidden {
		t.Errorf("another host: %d", code)
	}
	if code, body := do("GET", "/", "alpheus.example:443"); code != 200 || !strings.Contains(body, `name="alpheus-replay" content="off"`) {
		t.Errorf("page: %d", code)
	}
	if code, body := do("POST", "/api/runs/"+h.ns+"/a/replay?step=filter&replica=0", "alpheus.example"); code != http.StatusForbidden || !strings.Contains(body, "off") {
		t.Errorf("replay when off: %d %q", code, body)
	}
}

// The page's help is docs/ui.md, the guide on the website, copied by make
// generate; the page renders it at #/help.
func TestUIHelpIsTheGuide(t *testing.T) {
	guide, err := os.ReadFile("../../docs/ui.md")
	if err != nil {
		t.Fatal(err)
	}
	help, err := uiFiles.ReadFile("ui/help.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(guide, help) {
		t.Error("internal/cli/ui/help.md differs from docs/ui.md: run make generate")
	}
}

// replay takes the replica that stopped the run, else one that gave up or
// failed, so the command alpheus get shows works without --replica.
func TestFailedReplica(t *testing.T) {
	one := int32(1)
	run := func(failedStep string, reps ...v1alpha1.ReplicaStatus) *v1alpha1.StreamRun {
		r := &v1alpha1.StreamRun{}
		r.Status.Steps = []v1alpha1.StepStatus{{Name: "pass", Replicas: reps}}
		if failedStep != "" {
			r.Status.FailedStep, r.Status.FailedReplica = failedStep, &one
		}
		return r
	}
	for _, c := range []struct {
		name string
		r    *v1alpha1.StreamRun
		want int
	}{
		{"stopped the run", run("pass", v1alpha1.ReplicaStatus{Index: 0}, v1alpha1.ReplicaStatus{Index: 1}), 1},
		{"gave up", run("", v1alpha1.ReplicaStatus{Index: 0}, v1alpha1.ReplicaStatus{Index: 1, Phase: v1alpha1.StepFailed, FailedChunks: 3}), 1},
		{"restarted", run("", v1alpha1.ReplicaStatus{Index: 0}, v1alpha1.ReplicaStatus{Index: 1, Reason: v1alpha1.ReasonComponentFailed}), 1},
		{"nothing failed", run("", v1alpha1.ReplicaStatus{Index: 0}, v1alpha1.ReplicaStatus{Index: 1}), 0},
		{"another step stopped it", run("sink", v1alpha1.ReplicaStatus{Index: 0}), 0},
	} {
		if got := failedReplica(c.r, "pass"); got != c.want {
			t.Errorf("%s: replica %d, want %d", c.name, got, c.want)
		}
	}
}
