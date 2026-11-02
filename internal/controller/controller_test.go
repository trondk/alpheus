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

package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/signal/builtin"
	"alpheus-dev.org/alpheus/internal/store"
)

const (
	agentImage = "registry.alpheus.test:30500/alpheus/agent@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	digest     = "@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

var (
	k8s client.Client
	mem = store.NewMemory()
	nsN atomic.Int32
	rec = &recorder{}
)

// recorder keeps the events the controller records, per run.
type recorder struct {
	mu     sync.Mutex
	events []recorded
}

type recorded struct {
	namespace, name, typ, reason, action, note string
}

func (r *recorder) Eventf(regarding runtime.Object, _ runtime.Object, typ, reason, action, note string, args ...any) {
	obj := regarding.(client.Object)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, recorded{obj.GetNamespace(), obj.GetName(), typ, reason, action, fmt.Sprintf(note, args...)})
}

// of returns the events recorded for a run.
func (r *recorder) of(namespace, name string) []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recorded
	for _, e := range r.events {
		if e.namespace == namespace && e.name == name {
			out = append(out, e)
		}
	}
	return out
}

// envtestAssets finds kube-apiserver and etcd: KUBEBUILDER_ASSETS, as
// make test sets it, or the folder setup-envtest uses by default.
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
		fmt.Println("skipping controller tests: no envtest binaries (run: go tool setup-envtest use)")
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
	code, err := runTests(m, cfg)
	if err != nil {
		fmt.Println(err)
		code = 1
	}
	env.Stop()
	os.Exit(code)
}

func runTests(m *testing.M, cfg *rest.Config) (int, error) {
	ctrl.SetLogger(logr.FromSlogHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	scheme := runtime.NewScheme()
	clientgoscheme.AddToScheme(scheme)
	v1alpha1.AddToScheme(scheme)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		return 0, err
	}
	rc := &Reconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    scheme,
		Recorder:  rec,
		// envtest has no kubelet, so a container's log is made up.
		Logs: func(_ context.Context, namespace, pod, container string, previous bool) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(fmt.Sprintf("log of %s/%s %s previous=%t\n", namespace, pod, container, previous))), nil
		},
		Options: Options{
			AgentImage: agentImage,
			HubAddress: "alpheus-signal-hub.alpheus-system.svc:7070",
			Namespace:  "alpheus-system",
			NewStore: func(_ context.Context, r *v1alpha1.StreamRun) (store.Store, error) {
				if r.Spec.Storage.Bucket == refusedBucket {
					return refusing{}, nil
				}
				return mem, nil
			},
			Resync: 200 * time.Millisecond,
		},
	}
	if err := rc.SetupWithManager(mgr); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Println("manager:", err)
		}
	}()

	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return 0, err
	}
	if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alpheus-system"}}); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// namespace gives each test a namespace of its own.
func namespace(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("test-%d", nsN.Add(1))
	if err := k8s.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
	return name
}

// testcase03 is testcase03-replicas' graph with the given filter replicas.
func testcase03(ns string, filters int32) *v1alpha1.StreamRun {
	return &v1alpha1.StreamRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sales"},
		Spec: v1alpha1.StreamRunSpec{
			Steps: []v1alpha1.Step{
				{Name: "generate", Image: "registry.alpheus.test:30500/generate" + digest},
				{Name: "filter", Image: "registry.alpheus.test:30500/filter" + digest, Replicas: ptr.To(filters),
					Settings: &runtime.RawExtension{Raw: []byte(`{"field":"country","in":["DK"]}`)}},
				{Name: "sum", Image: "registry.alpheus.test:30500/sum" + digest},
			},
			Edges: []v1alpha1.Edge{
				{From: "generate", To: "filter"},
				{From: "filter", To: "sum"},
			},
			Storage: v1alpha1.Storage{Endpoint: "http://garage:3900", Bucket: "alpheus-data", Region: "garage", SecretRef: "alpheus-s3"},
		},
	}
}

func create(t *testing.T, r *v1alpha1.StreamRun) *v1alpha1.StreamRun {
	t.Helper()
	if err := k8s.Create(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

// eventually polls cond until it returns "" or the time runs out.
func eventually(t *testing.T, what string, cond func() string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	last := ""
	for time.Now().Before(deadline) {
		if last = cond(); last == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: %s", what, last)
}

func get(t *testing.T, r *v1alpha1.StreamRun) *v1alpha1.StreamRun {
	t.Helper()
	var out v1alpha1.StreamRun
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(r), &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func waitPhase(t *testing.T, r *v1alpha1.StreamRun, phase v1alpha1.RunPhase) *v1alpha1.StreamRun {
	t.Helper()
	var got *v1alpha1.StreamRun
	eventually(t, "phase "+string(phase), func() string {
		got = get(t, r)
		if got.Status.Phase != phase {
			return fmt.Sprintf("phase %q, message %q", got.Status.Phase, got.Status.Message)
		}
		return ""
	})
	return got
}

func pods(t *testing.T, ns string) map[string]*corev1.Pod {
	t.Helper()
	var list corev1.PodList
	if err := k8s.List(t.Context(), &list, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	out := map[string]*corev1.Pod{}
	for i := range list.Items {
		out[list.Items[i].Name] = &list.Items[i]
	}
	return out
}

func waitPod(t *testing.T, ns, name string) *corev1.Pod {
	t.Helper()
	var p *corev1.Pod
	eventually(t, "pod "+name, func() string {
		if p = pods(t, ns)[name]; p == nil {
			return "missing"
		}
		return ""
	})
	return p
}

// exit sets a pod's status as if its component exited with code.
func exit(t *testing.T, p *corev1.Pod, code int32) {
	t.Helper()
	phase := corev1.PodSucceeded
	if code != 0 {
		phase = corev1.PodFailed
	}
	p.Status.Phase = phase
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  ComponentContainer,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code}},
	}}
	if err := k8s.Status().Update(t.Context(), p); err != nil {
		t.Fatal(err)
	}
}

func env(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestPodsAndSuccess(t *testing.T) {
	ns := namespace(t)
	r := create(t, testcase03(ns, 2))

	p := waitPod(t, ns, "sales-filter-1-1")
	if len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 1 {
		t.Fatalf("containers %d, init containers %d", len(p.Spec.Containers), len(p.Spec.InitContainers))
	}
	ag, comp := p.Spec.InitContainers[0], p.Spec.Containers[0]
	if ag.Image != agentImage || ag.RestartPolicy == nil || *ag.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("agent is not a native sidecar with the configured image: %+v", ag)
	}
	var cfg agent.Config
	if err := json.Unmarshal([]byte(env(ag, "ALPHEUS_AGENT_CONFIG")), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Step != "filter" || cfg.Replica != 1 || cfg.Replicas != 2 || cfg.MaxUnacked != 4 ||
		len(cfg.Inputs) != 1 || cfg.Inputs[0].Edge != "generate-filter" || cfg.Outputs[0].Edge != "filter-sum" ||
		cfg.ScratchLimit != 10<<30 || cfg.UID != string(get(t, r).UID) {
		t.Errorf("agent config %+v", cfg)
	}
	// The API server sets the default storage timeout (GO-136).
	if got := env(ag, "ALPHEUS_S3_TIMEOUT"); got != "10m0s" {
		t.Errorf("ALPHEUS_S3_TIMEOUT %q, want 10m0s", got)
	}
	if env(comp, "ALPHEUS_REPLICA") != "1" || env(comp, "ALPHEUS_SETTINGS") != `{"field":"country","in":["DK"]}` {
		t.Errorf("component env %+v", comp.Env)
	}
	for _, e := range comp.Env {
		if e.ValueFrom != nil {
			t.Errorf("the component got a secret: %+v", e)
		}
	}
	if *comp.SecurityContext.ReadOnlyRootFilesystem != true || *p.Spec.SecurityContext.RunAsNonRoot != true {
		t.Error("not restricted")
	}
	if p.Spec.RestartPolicy != corev1.RestartPolicyNever || !metav1.IsControlledBy(p, get(t, r)) {
		t.Error("restart policy or owner")
	}
	// Each attempt has its own certificate, naming it (GO-41).
	var secret corev1.Secret
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "sales-filter-1-1-signal"}, &secret); err != nil {
		t.Fatalf("signal certificate: %v", err)
	}
	if v := p.Spec.Volumes; !slices.ContainsFunc(v, func(v corev1.Volume) bool {
		return v.Secret != nil && v.Secret.SecretName == "sales-filter-1-1-signal"
	}) {
		t.Errorf("the pod does not mount its certificate: %+v", v)
	}
	block, _ := pem.Decode(secret.Data["tls.crt"])
	if block == nil {
		t.Fatal("no certificate in the signal Secret")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	want := AgentIdentity{Namespace: ns, Run: "sales", UID: string(get(t, r).UID), Step: "filter", Replica: 1, Attempt: 1}
	if id, err := ParseAgentIdentity(cert); err != nil || id != want {
		t.Errorf("certificate for %+v (%v), want %+v", id, err, want)
	}

	for _, name := range []string{"sales-generate-0-1", "sales-filter-0-1", "sales-filter-1-1", "sales-sum-0-1"} {
		exit(t, waitPod(t, ns, name), 0)
	}
	got := waitPhase(t, r, v1alpha1.RunSucceeded)
	if got.Status.StepsFinished != "3/3" || got.Status.FinishedAt == nil {
		t.Errorf("status %+v", got.Status)
	}
	if _, err := store.ReadAll(t.Context(), mem, "results/sales/"+string(got.UID)+"/summary.json"); err != nil {
		t.Errorf("summary: %v", err)
	}
}

// A pod's failure names the agent's reason when the agent failed, such as
// storage that stayed unreachable (GO-136), with a reason code a program can
// rely on (GO-138), and what the component left in its termination message
// (GO-139).
func TestFailureReason(t *testing.T) {
	component := corev1.ContainerStatus{Name: ComponentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
	}}
	componentSaid := corev1.ContainerStatus{Name: ComponentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "open input.csv: no such file\n"},
	}}
	stopped := corev1.ContainerStatus{Name: ComponentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 143},
	}}
	agentFailed := corev1.ContainerStatus{Name: AgentContainer, LastTerminationState: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "StorageUnreachable: storage unreachable for 10m0s: put x: connection refused\n"},
	}}
	agentScratch := corev1.ContainerStatus{Name: AgentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "ScratchFull: chunk of 4 bytes does not fit in the scratch folder of 1 bytes"},
	}}
	agentOld := corev1.ContainerStatus{Name: AgentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "Unknown: something"},
	}}
	agentDone := corev1.ContainerStatus{Name: AgentContainer, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "ignored"},
	}}
	for _, c := range []struct {
		name   string
		init   []corev1.ContainerStatus
		conts  []corev1.ContainerStatus
		reason v1alpha1.Reason
		want   string
	}{
		{"component only", nil, []corev1.ContainerStatus{component}, v1alpha1.ReasonComponentFailed, "the component exited with code 1"},
		{"component says why", nil, []corev1.ContainerStatus{componentSaid}, v1alpha1.ReasonComponentFailed,
			"the component exited with code 1: open input.csv: no such file"},
		{"agent finished", []corev1.ContainerStatus{agentDone}, []corev1.ContainerStatus{component}, v1alpha1.ReasonComponentFailed,
			"the component exited with code 1"},
		{"both", []corev1.ContainerStatus{agentFailed}, []corev1.ContainerStatus{component}, v1alpha1.ReasonStorageUnreachable,
			"the component exited with code 1; the agent failed: storage unreachable for 10m0s: put x: connection refused"},
		{"scratch", []corev1.ContainerStatus{agentScratch}, []corev1.ContainerStatus{stopped}, v1alpha1.ReasonScratchFull,
			"the component exited with code 143; the agent failed: chunk of 4 bytes does not fit in the scratch folder of 1 bytes"},
		{"agent only", []corev1.ContainerStatus{agentFailed}, nil, v1alpha1.ReasonStorageUnreachable,
			"the agent failed: storage unreachable for 10m0s: put x: connection refused"},
		{"agent without a known reason", []corev1.ContainerStatus{agentOld}, nil, v1alpha1.ReasonAgentFailed,
			"the agent failed: Unknown: something"},
		{"nothing", nil, nil, v1alpha1.ReasonPodFailed, "the pod failed"},
	} {
		pod := &corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: c.init, ContainerStatuses: c.conts}}
		if reason, got := failure(pod); reason != c.reason || got != c.want {
			t.Errorf("%s: %s %q, want %s %q", c.name, reason, got, c.reason, c.want)
		}
	}
}

// A failed attempt gets a new pod; when the restarts are used up the run
// stops (GO-67, GO-77).
func TestRestartsThenStop(t *testing.T) {
	ns := namespace(t)
	r := testcase03(ns, 1)
	r.Spec.Steps[1].MaxRestarts = ptr.To[int32](1)
	create(t, r)
	uid := string(get(t, r).UID)

	// generate committed two chunks, which filter never acked.
	gen := chunk.NewManifest("sales", uid, "generate-filter", 0, 1)
	for n := int64(1); n <= 2; n++ {
		body := fmt.Sprintf("rows of chunk %d\n", n)
		sum := sha256.Sum256([]byte(body))
		gen.Append(chunk.Chunk{N: n, Ext: "csv", Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])})
		mem.Put(t.Context(), chunk.EdgeStream(uid, "generate-filter", 0).ChunkKey(n, "csv"), strings.NewReader(body), int64(len(body)))
	}
	put(t, chunk.EdgeStream(uid, "generate-filter", 0).ManifestKey(), gen)

	exit(t, waitPod(t, ns, "sales-filter-0-1"), 1)
	exit(t, waitPod(t, ns, "sales-filter-0-2"), 1)
	got := waitPhase(t, r, v1alpha1.RunFailed)
	if rs := got.Status.Steps[1].Replicas[0]; rs.Restarts != 1 || rs.Attempt != 2 || !strings.Contains(got.Status.Message, "code 1") {
		t.Errorf("replica %+v, message %q", rs, got.Status.Message)
	}

	// The failure says why in codes a program can rely on (GO-138).
	st := got.Status
	if st.Reason != v1alpha1.ReasonComponentFailed || st.FailedStep != "filter" || st.FailedReplica == nil || *st.FailedReplica != 0 {
		t.Errorf("reason %q, failed step %q replica %v", st.Reason, st.FailedStep, st.FailedReplica)
	}
	if rs := st.Steps[1].Replicas[0]; rs.Reason != v1alpha1.ReasonComponentFailed {
		t.Errorf("replica reason %q", rs.Reason)
	}
	if c := meta.FindStatusCondition(st.Conditions, v1alpha1.ConditionReady); c == nil || c.Reason != string(v1alpha1.ReasonComponentFailed) {
		t.Errorf("Ready condition %+v", c)
	}

	// Each failed attempt is kept for a replay, with the chunks it had not
	// finished (GO-141).
	data, err := store.ReadAll(t.Context(), mem, chunk.FailurePrefix("sales", uid, "filter", 0, 1)+"failure.json")
	var f Failure
	if err == nil {
		err = json.Unmarshal(data, &f)
	}
	if err != nil || f.Reason != v1alpha1.ReasonComponentFailed || f.Image != r.Spec.Steps[1].Image || len(f.Inputs) != 2 || f.Inputs[0].Port != "default" {
		t.Fatalf("failure %+v (%v)", f, err)
	}
	copied, err := store.ReadAll(t.Context(), mem, chunk.FailurePrefix("sales", uid, "filter", 0, 1)+"input/"+f.Inputs[1].File)
	if err != nil || string(copied) != "rows of chunk 2\n" {
		t.Errorf("copied chunk %q (%v)", copied, err)
	}
	if want := "s3://alpheus-data/" + chunk.FailurePrefix("sales", uid, "filter", -1, 0); st.Steps[1].Failures != want {
		t.Errorf("failures %q, want %q", st.Steps[1].Failures, want)
	}

	// Both attempts' logs are kept (GO-140).
	for attempt := 1; attempt <= 2; attempt++ {
		key := chunk.LogsPrefix("sales", uid, "filter", 0, attempt) + "component.log"
		data, err := store.ReadAll(t.Context(), mem, key)
		if err != nil || !strings.Contains(string(data), fmt.Sprintf("sales-filter-0-%d component", attempt)) {
			t.Errorf("%s: %q (%v)", key, data, err)
		}
	}
	if want := "s3://alpheus-data/" + chunk.LogsPrefix("sales", uid, "filter", -1, 0); st.Steps[1].Logs != want {
		t.Errorf("logs %q, want %q", st.Steps[1].Logs, want)
	}

	// The metrics count the restart and the failed run (GO-51), once each.
	eventually(t, "metrics", func() string {
		restarts := testutil.ToFloat64(stepRestarts.WithLabelValues(ns, "filter", string(v1alpha1.ReasonComponentFailed)))
		failed := testutil.ToFloat64(runsFinished.WithLabelValues(ns, string(v1alpha1.RunFailed), string(v1alpha1.ReasonComponentFailed)))
		if restarts != 1 || failed != 1 {
			return fmt.Sprintf("restarts %v, failed runs %v", restarts, failed)
		}
		return ""
	})

	// The events tell the story (GO-139).
	var actions []string
	for _, e := range rec.of(ns, "sales") {
		actions = append(actions, e.action+"/"+e.reason)
	}
	for _, want := range []string{"Start/Started", "Restart/ComponentFailed", "GiveUp/ComponentFailed", "Finish/ComponentFailed"} {
		if !slices.Contains(actions, want) {
			t.Errorf("events %v, want %s", actions, want)
		}
	}
	eventually(t, "pods deleted", func() string {
		if n := len(pods(t, ns)); n > 0 {
			return fmt.Sprintf("%d pods left", n)
		}
		return ""
	})
}

// With failurePolicy continue a failed replica's streams are closed with a
// higher epoch, and the run ends PartiallySucceeded (GO-97).
func TestFailurePolicyContinue(t *testing.T) {
	ns := namespace(t)
	r := testcase03(ns, 2)
	r.Spec.Steps[1].MaxRestarts = ptr.To[int32](0)
	r.Spec.Steps[1].FailurePolicy = v1alpha1.FailureContinue
	create(t, r)
	uid := string(get(t, r).UID)

	// generate committed 4 chunks; replica 0 gets 2 and 4, and committed
	// chunk 2 before it failed.
	gen := chunk.NewManifest("sales", uid, "generate-filter", 0, 1)
	for n := int64(1); n <= 4; n++ {
		gen.Append(chunk.Chunk{N: n, Ext: "csv", Bytes: 1, SHA256: strings.Repeat("0", 64)})
	}
	put(t, chunk.EdgeStream(uid, "generate-filter", 0).ManifestKey(), gen)
	out := chunk.NewManifest("sales", uid, "filter-sum", 0, 1)
	out.SetConsumed("generate-filter/0", 2)
	put(t, chunk.EdgeStream(uid, "filter-sum", 0).ManifestKey(), out)

	exit(t, waitPod(t, ns, "sales-filter-0-1"), 1)

	// The controller closes the replica's ack file, so generate no longer
	// waits for it (GO-35). It lists the chunk the replica committed, with
	// an epoch above its last attempt (GO-68).
	ackKey := chunk.EdgeStream(uid, "generate-filter", 0).AckKey("filter", 0)
	eventually(t, "closed ack file", func() string {
		data, err := store.ReadAll(t.Context(), mem, ackKey)
		if err != nil {
			return err.Error()
		}
		acks, _ := chunk.ParseAcks(data)
		if !acks.Closed || acks.Epoch != 2 || !slices.Equal(acks.Acked, []int64{2}) {
			return fmt.Sprintf("%+v", acks)
		}
		return ""
	})

	// generate goes on; chunk 6 also goes to the closed replica.
	gen.Append(chunk.Chunk{N: 5, Ext: "csv", Bytes: 1, SHA256: strings.Repeat("0", 64)})
	gen.Append(chunk.Chunk{N: 6, Ext: "csv", Bytes: 1, SHA256: strings.Repeat("0", 64)})
	put(t, chunk.EdgeStream(uid, "generate-filter", 0).ManifestKey(), gen)

	for _, name := range []string{"sales-generate-0-1", "sales-filter-1-1", "sales-sum-0-1"} {
		exit(t, waitPod(t, ns, name), 0)
	}
	got := waitPhase(t, r, v1alpha1.RunPartiallySucceeded)
	if got.Status.Reason != v1alpha1.ReasonFailedReplicas {
		t.Errorf("reason %q", got.Status.Reason)
	}
	if rs := got.Status.Steps[1].Replicas[0]; rs.Phase != v1alpha1.StepFailed || rs.FailedChunks != 2 {
		t.Errorf("replica %+v, want failed with 2 failed chunks, 4 and 6", rs)
	}
	data, err := store.ReadAll(t.Context(), mem, chunk.EdgeStream(uid, "filter-sum", 0).ManifestKey())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := chunk.ParseManifest(data)
	if !m.EOS || m.Epoch != 2 {
		t.Errorf("closed manifest: eos %v, epoch %d", m.EOS, m.Epoch)
	}
}

func put(t *testing.T, key string, v interface{ Marshal() ([]byte, error) }) {
	t.Helper()
	data, _ := v.Marshal()
	if err := mem.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidRuns(t *testing.T) {
	ns := namespace(t)
	cycle := testcase03(ns, 1)
	cycle.Name = "cycle"
	cycle.Spec.Edges = append(cycle.Spec.Edges, v1alpha1.Edge{From: "sum", To: "generate"})
	create(t, cycle)
	if got := waitPhase(t, cycle, v1alpha1.RunFailed); !strings.Contains(got.Status.Message, "cycle") {
		t.Errorf("message %q", got.Status.Message)
	}

	tag := testcase03(ns, 1)
	tag.Name = "tag"
	tag.Spec.Steps[0].Image = "registry.alpheus.test:30500/generate:latest"
	create(t, tag)
	if got := waitPhase(t, tag, v1alpha1.RunFailed); !strings.Contains(got.Status.Message, "digest") {
		t.Errorf("message %q", got.Status.Message)
	}
	if n := len(pods(t, ns)); n != 0 {
		t.Errorf("%d pods for invalid runs", n)
	}
}

// spec.storage.timeout is checked by the API server and immutable like the
// rest of storage, compared as a duration, so a client that reads "10m" and
// writes it back as "10m0s" changes nothing (GO-136).
func TestStorageTimeout(t *testing.T) {
	ns := namespace(t)
	for _, bad := range []string{"-1s", "25h"} {
		r := testcase03(ns, 1)
		r.Name = "bad-timeout"
		r.Spec.Storage.Timeout = &metav1.Duration{}
		r.Spec.Storage.Timeout.Duration, _ = time.ParseDuration(bad)
		if err := k8s.Create(t.Context(), r); err == nil || !strings.Contains(err.Error(), "timeout must be between") {
			t.Errorf("timeout %s: %v, want it refused", bad, err)
		}
	}

	r := testcase03(ns, 1)
	r.Spec.Storage.Timeout = &metav1.Duration{Duration: 90 * time.Second}
	create(t, r)
	update := func(change func(*v1alpha1.StreamRun)) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur := get(t, r)
			change(cur)
			return k8s.Update(t.Context(), cur)
		})
	}
	if err := update(func(cur *v1alpha1.StreamRun) { cur.Spec.Cancel = true }); err != nil {
		t.Fatalf("writing the run back as read: %v", err)
	}
	err := update(func(cur *v1alpha1.StreamRun) { cur.Spec.Storage.Timeout.Duration = time.Minute })
	if err == nil || !strings.Contains(err.Error(), "storage is immutable") {
		t.Errorf("changing the timeout: %v, want it refused", err)
	}
}

func TestCancel(t *testing.T) {
	ns := namespace(t)
	r := create(t, testcase03(ns, 1))
	waitPod(t, ns, "sales-sum-0-1")
	// The controller writes the status meanwhile, so the update may need
	// the latest version.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := get(t, r)
		cur.Spec.Cancel = true
		return k8s.Update(t.Context(), cur)
	}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, r, v1alpha1.RunCancelled)
	eventually(t, "pods deleted", func() string {
		if n := len(pods(t, ns)); n > 0 {
			return fmt.Sprintf("%d pods left", n)
		}
		return ""
	})
}

// Deleting a run deletes runs/<uid>/ but keeps its results (GO-20, GO-78).
// Deleting a namespace deletes the storage Secret and the run together.
// The run's finalizer on the Secret keeps the keys until runs/<uid>/ is
// deleted; then both go, and the result stays (GO-20, GO-78).
func TestFinalizer(t *testing.T) {
	ns := namespace(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "alpheus-s3"}}
	if err := k8s.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	r := create(t, testcase03(ns, 1))
	eventually(t, "finalizers", func() string {
		k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret)
		if len(get(t, r).Finalizers) == 0 || !slices.Contains(secret.Finalizers, secretFinalizer(get(t, r))) {
			return fmt.Sprintf("run %v, secret %v", get(t, r).Finalizers, secret.Finalizers)
		}
		return ""
	})
	uid := string(get(t, r).UID)
	runKey, resultKey := "runs/"+uid+"/generate-filter/0/part-000001.csv", "results/sales/"+uid+"/sum/result/0/part-000001.csv"
	for _, key := range []string{runKey, resultKey} {
		mem.Put(t.Context(), key, strings.NewReader("x"), 1)
	}

	// The Secret first, as a namespace deletion may do; it stays.
	if err := k8s.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret); err != nil {
		t.Fatalf("the Secret went before the run's data: %v", err)
	}
	if err := k8s.Delete(t.Context(), get(t, r)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "run and Secret deleted", func() string {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(r), &v1alpha1.StreamRun{}); !apierrors.IsNotFound(err) {
			return fmt.Sprint("run: ", err)
		}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
			return fmt.Sprint("secret: ", err)
		}
		return ""
	})
	if _, err := store.ReadAll(t.Context(), mem, runKey); err == nil {
		t.Error("runs/<uid>/ was not deleted")
	}
	if _, err := store.ReadAll(t.Context(), mem, resultKey); err != nil {
		t.Errorf("the result was deleted: %v", err)
	}
}

// refusedBucket is a bucket whose storage refuses the run's keys.
const refusedBucket = "refused"

// refusing is storage that turns every request away, as S3 does with a
// key it does not know.
type refusing struct{}

var errRefused = minio.ErrorResponse{StatusCode: 403, Code: "InvalidAccessKeyId", Message: "The access key ID you provided does not exist"}

func (refusing) Put(context.Context, string, io.Reader, int64) error { return errRefused }
func (refusing) Get(context.Context, string) (io.ReadCloser, error)  { return nil, errRefused }
func (refusing) Delete(context.Context, string) error                { return errRefused }
func (refusing) List(context.Context, string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) { yield("", errRefused) }
}

// Keys the storage refuses fail the run at once, with no restarts, and the
// run can still be deleted, leaving its data (GO-137).
func TestRefusedKeys(t *testing.T) {
	ns := namespace(t)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "alpheus-s3"}}
	if err := k8s.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	r := testcase03(ns, 1)
	r.Spec.Storage.Bucket = refusedBucket
	create(t, r)
	got := waitPhase(t, r, v1alpha1.RunFailed)
	if got.Status.Reason != v1alpha1.ReasonStorageRefused || got.Status.FailedStep != "" {
		t.Errorf("reason %q, failed step %q", got.Status.Reason, got.Status.FailedStep)
	}
	if !strings.Contains(got.Status.Message, "the storage refused the keys in Secret alpheus-s3") ||
		!strings.Contains(got.Status.Message, "InvalidAccessKeyId") && !strings.Contains(got.Status.Message, "access key") {
		t.Errorf("message %q", got.Status.Message)
	}
	for _, ss := range got.Status.Steps {
		for _, rs := range ss.Replicas {
			if rs.Restarts != 0 {
				t.Errorf("%s replica %d restarted %d times", ss.Name, rs.Index, rs.Restarts)
			}
		}
	}
	eventually(t, "pods deleted", func() string {
		if n := len(pods(t, ns)); n > 0 {
			return fmt.Sprintf("%d pods left", n)
		}
		return ""
	})
	if err := k8s.Delete(t.Context(), get(t, r)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "run and the Secret's finalizer gone", func() string {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(r), &v1alpha1.StreamRun{}); !apierrors.IsNotFound(err) {
			return fmt.Sprint("run: ", err)
		}
		k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret)
		if len(secret.Finalizers) > 0 {
			return fmt.Sprint("secret finalizers: ", secret.Finalizers)
		}
		return ""
	})
}

// A run whose storage Secret never existed is still deleted, leaving its
// data, instead of hanging.
func TestFinalizerWithoutSecret(t *testing.T) {
	ns := namespace(t)
	r := create(t, testcase03(ns, 1))
	eventually(t, "finalizer", func() string {
		if len(get(t, r).Finalizers) == 0 {
			return "none"
		}
		return ""
	})
	key := "runs/" + string(get(t, r).UID) + "/generate-filter/0/part-000001.csv"
	mem.Put(t.Context(), key, strings.NewReader("x"), 1)
	if err := k8s.Delete(t.Context(), get(t, r)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "run deleted", func() string {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(r), &v1alpha1.StreamRun{}); !apierrors.IsNotFound(err) {
			return fmt.Sprint(err)
		}
		return ""
	})
	if _, err := store.ReadAll(t.Context(), mem, key); err != nil {
		t.Errorf("data deleted without keys: %v", err)
	}
}

// Status shows edges, results and counters from storage (GO-79, GO-115).
func TestStatusFromStorage(t *testing.T) {
	ns := namespace(t)
	r := create(t, testcase03(ns, 1))
	uid := string(get(t, r).UID)

	gen := chunk.NewManifest("sales", uid, "generate-filter", 0, 1)
	gen.Append(chunk.Chunk{N: 1, Ext: "csv", Bytes: 100, SHA256: strings.Repeat("0", 64)})
	gen.End()
	// generate stopped after its last commit, before its summary: the
	// committed totals count (GO-115).
	gen.Observed = &chunk.Observed{Seq: 2, Counts: map[string]int64{"lines": 9}}
	put(t, chunk.EdgeStream(uid, "generate-filter", 0).ManifestKey(), gen)
	acks := chunk.NewAcks()
	acks.Ack(1)
	put(t, chunk.EdgeStream(uid, "generate-filter", 0).AckKey("filter", 0), acks)
	res := chunk.NewManifest("sales", uid, "sum.result", 0, 1)
	res.Append(chunk.Chunk{N: 1, Ext: "txt", Bytes: 7, SHA256: strings.Repeat("0", 64), Counts: map[string]int64{"products": 3}})
	// sum's summary is newer than its result manifest, which is older.
	res.Observed = &chunk.Observed{Seq: 1, Counts: map[string]int64{"lines": 1}}
	sumObs, _ := json.Marshal(agent.Observations{Version: 1, Seq: 4, Counts: map[string]int64{"lines": 18}})
	mem.Put(t.Context(), chunk.ErrorLogPrefix("sales", uid, "sum", 0)+"manifest.json", bytes.NewReader(sumObs), int64(len(sumObs)))
	put(t, chunk.ResultStream("sales", uid, "sum", "result", 0).ManifestKey(), res)
	obs, _ := json.Marshal(agent.Observations{Version: 1, Counts: map[string]int64{"rows_in": 57}, Errors: 2})
	mem.Put(t.Context(), chunk.ErrorLogPrefix("sales", uid, "filter", 0)+"manifest.json", bytes.NewReader(obs), int64(len(obs)))

	eventually(t, "status from storage", func() string {
		st := get(t, r).Status
		if len(st.Edges) != 2 || st.Edges[0].Chunks != 1 || st.Edges[0].Bytes != 100 || st.Edges[0].Acked != 1 || !st.Edges[0].EOS {
			return fmt.Sprintf("edges %+v", st.Edges)
		}
		if len(st.Results) != 1 || st.Results[0].Output != "result" || st.Results[0].Counts["products"] != 3 ||
			st.Results[0].SecretRef != "alpheus-s3" || st.Results[0].Prefix != "results/sales/"+uid+"/sum/result/" {
			return fmt.Sprintf("results %+v", st.Results)
		}
		if f := st.Steps[1]; f.Counts["rows_in"] != 57 || f.Errors != 2 || f.ErrorLog == "" {
			return fmt.Sprintf("filter %+v", f)
		}
		if g := st.Steps[0]; g.Counts["lines"] != 9 {
			return fmt.Sprintf("generate %+v", g)
		}
		if s := st.Steps[2]; s.Counts["lines"] != 18 {
			return fmt.Sprintf("sum %+v", s)
		}
		return ""
	})
}

// The hub accepts an agent with a certificate for the current attempt of a
// replica of the run it uses, and turns away a client without a
// certificate, an older attempt, another run and a certificate that names
// no agent (GO-41).
func TestHubMTLS(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go (&Hub{Client: k8s, Addr: addr, DNSNames: []string{"localhost"}, Namespace: "alpheus-system"}).Start(ctx)

	ns := namespace(t)
	r := testcase03(ns, 1)
	if err := k8s.Create(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	eventually(t, "replica status", func() string {
		if rs, err := replicaOf(get(t, r), "filter", 0); err != nil || rs.Attempt != 1 {
			return fmt.Sprintf("%+v %v", rs, err)
		}
		return ""
	})

	ca, err := LoadCA(t.Context(), k8s, "alpheus-system")
	if err != nil {
		t.Fatal(err)
	}
	agentCert := func(id AgentIdentity) []tls.Certificate {
		certPEM, keyPEM, err := ca.IssueAgent(id)
		if err != nil {
			t.Fatal(err)
		}
		cert, _ := tls.X509KeyPair(certPEM, keyPEM)
		return []tls.Certificate{cert}
	}
	dial := func(certs []tls.Certificate) *builtin.Client {
		creds := credentials.NewTLS(&tls.Config{Certificates: certs, RootCAs: ca.Pool(), ServerName: "localhost"})
		c, err := builtin.Dial(addr, creds, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	current := AgentIdentity{Namespace: ns, Run: r.Name, UID: string(get(t, r).UID), Step: "filter", Replica: 0, Attempt: 1}

	run := signal.RunKey(r.Name, current.UID)
	good := dial(agentCert(current))
	ch, _ := good.Subscribe(t.Context(), run, "a-b")
	sig := signal.Signal{Kind: signal.Ready, Run: run, Edge: "a-b", N: 1}
	eventually(t, "signal over mTLS", func() string {
		good.Publish(t.Context(), sig)
		select {
		case <-ch:
			return ""
		case <-time.After(50 * time.Millisecond):
			return "no signal"
		}
	})

	older := current
	older.Attempt = 0
	otherRun := current
	otherRun.Run = "other"
	plainPEM, plainKey, err := ca.Issue("test/run/uid")
	if err != nil {
		t.Fatal(err)
	}
	plainCert, _ := tls.X509KeyPair(plainPEM, plainKey)
	for name, certs := range map[string][]tls.Certificate{
		"no certificate":         nil,
		"an older attempt":       agentCert(older),
		"another run":            agentCert(otherRun),
		"a certificate for none": {plainCert},
	} {
		bad := dial(certs)
		badCh, _ := bad.Subscribe(t.Context(), run, "a-b")
		for range 10 {
			good.Publish(t.Context(), sig)
			select {
			case <-badCh:
				t.Fatalf("a client with %s got a signal", name)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}

func replicaOf(r *v1alpha1.StreamRun, step string, replica int32) (v1alpha1.ReplicaStatus, error) {
	for _, s := range r.Status.Steps {
		if s.Name != step {
			continue
		}
		for _, rs := range s.Replicas {
			if rs.Index == replica {
				return rs, nil
			}
		}
	}
	return v1alpha1.ReplicaStatus{}, fmt.Errorf("no replica %s/%d", step, replica)
}

// The first LoadCA in a namespace creates the CA and returns it; the next
// one reads the same CA back.
func TestLoadCACreates(t *testing.T) {
	ns := namespace(t)
	first, err := LoadCA(t.Context(), k8s, ns)
	if err != nil {
		t.Fatalf("first LoadCA: %v", err)
	}
	again, err := LoadCA(t.Context(), k8s, ns)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.CertPEM(), again.CertPEM()) {
		t.Error("the second LoadCA returned another CA")
	}
}

// Adding the finalizer must not write the spec back as Go encodes it:
// resultsTTL stays 168h, not 168h0m0s.
func TestSpecNotRewritten(t *testing.T) {
	ns := namespace(t)
	u := &unstructured.Unstructured{}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(testcase03(ns, 1))
	if err != nil {
		t.Fatal(err)
	}
	u.SetUnstructuredContent(obj)
	u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("StreamRun"))
	unstructured.SetNestedField(u.Object, "168h", "spec", "resultsTTL")
	if err := k8s.Create(t.Context(), u); err != nil {
		t.Fatal(err)
	}
	eventually(t, "finalizer", func() string {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(u), u); err != nil {
			return err.Error()
		}
		if len(u.GetFinalizers()) == 0 {
			return "none"
		}
		return ""
	})
	if ttl, _, _ := unstructured.NestedString(u.Object, "spec", "resultsTTL"); ttl != "168h" {
		t.Errorf("resultsTTL = %q, want 168h", ttl)
	}
}

// With a results bucket, the agents write results there, and
// status.results names that bucket and the Secret that can only read it,
// never the agents' key (GO-129).
func TestResultsBucket(t *testing.T) {
	ns := namespace(t)
	r := testcase03(ns, 1)
	r.Spec.Storage.ResultsBucket = "alpheus-results"
	r.Spec.Storage.ResultsSecretRef = "results-reader"
	create(t, r)
	uid := string(get(t, r).UID)

	p := waitPod(t, ns, "sales-sum-0-1")
	if got := env(p.Spec.InitContainers[0], "ALPHEUS_S3_RESULTS_BUCKET"); got != "alpheus-results" {
		t.Errorf("agent results bucket %q", got)
	}
	res := chunk.NewManifest("sales", uid, "sum.result", 0, 1)
	res.Append(chunk.Chunk{N: 1, Ext: "txt", Bytes: 7, SHA256: strings.Repeat("0", 64)})
	put(t, chunk.ResultStream("sales", uid, "sum", "result", 0).ManifestKey(), res)
	eventually(t, "results status", func() string {
		st := get(t, r).Status
		if len(st.Results) != 1 || st.Results[0].Bucket != "alpheus-results" || st.Results[0].SecretRef != "results-reader" {
			return fmt.Sprintf("%+v", st.Results)
		}
		return ""
	})
}
