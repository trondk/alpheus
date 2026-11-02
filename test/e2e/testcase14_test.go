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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/cli"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// TestTestcase14 runs a component with a bug and checks everything a
// person or a tool needs to find it: the reason as a code and in the
// component's words (GO-138, GO-139), the events (GO-139), the logs of
// both attempts (GO-140), the agent's live metrics (GO-51), and a replay
// of the last attempt on its own, on the chunks it had open, which fails
// the same way and logs at debug level (GO-141).
func TestTestcase14(t *testing.T) {
	b := newBench(t, "testcase14", nil)
	if b.Chunks < 8 {
		t.Skipf("%d chunks; pass fails on chunk 5 and needs a few after it", b.Chunks)
	}
	e := setup(t)
	cfg, err := ctrl.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := e.create(b.Testcase)

	// While pass works, sink's agent serves its live metrics.
	scraped := ""
	deadline := time.Now().Add(2 * time.Minute)
	for scraped == "" && time.Now().Before(deadline) {
		cur := e.get(r)
		if rs, err := replicaStatus(cur, "sink", 0); err == nil && rs.Pod != "" {
			body, err := kube.CoreV1().RESTClient().Get().Namespace(e.ns).Resource("pods").
				Name(rs.Pod + ":9090").SubResource("proxy").Suffix("metrics").DoRaw(t.Context())
			if err == nil && bytes.Contains(body, []byte("alpheus_chunks_total{")) {
				scraped = string(body)
			}
		}
		if controller.Terminal(cur.Status.Phase) {
			break
		}
		time.Sleep(time.Second)
	}
	if !strings.Contains(scraped, `step="sink"`) || !strings.Contains(scraped, `direction="in"`) {
		t.Errorf("sink's agent metrics: %.500s", scraped)
	}

	r = e.wait(r, 5*time.Minute)
	st := r.Status
	t.Logf("reason: %s: %s", st.Reason, st.Message)
	if st.Phase != v1alpha1.RunFailed || st.Reason != v1alpha1.ReasonComponentFailed {
		t.Fatalf("phase %s reason %s, want Failed with ComponentFailed", st.Phase, st.Reason)
	}
	if st.FailedStep != "pass" || st.FailedReplica == nil || *st.FailedReplica != 0 {
		t.Errorf("failed step %q replica %v, want pass 0", st.FailedStep, st.FailedReplica)
	}
	// The component's own words, from its termination message.
	if !strings.Contains(st.Message, "source chunk 5) is one this component cannot handle") {
		t.Errorf("the message does not say what the component said: %s", st.Message)
	}
	if c := findCondition(st.Conditions, v1alpha1.ConditionReady); c == nil || c.Reason != string(v1alpha1.ReasonComponentFailed) {
		t.Errorf("Ready condition %+v", c)
	}
	pass := stepStatus(t, r, "pass")
	if pass.Logs == "" || pass.Failures == "" {
		t.Errorf("pass keeps no logs (%q) or failures (%q)", pass.Logs, pass.Failures)
	}

	// The events tell the story.
	evs, err := kube.EventsV1().Events(e.ns).List(t.Context(), metav1.ListOptions{FieldSelector: "regarding.name=testcase14"})
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for _, ev := range evs.Items {
		seen = append(seen, ev.Action+"/"+ev.Reason)
	}
	for _, want := range []string{"Start/Started", "Restart/ComponentFailed", "GiveUp/ComponentFailed", "Finish/ComponentFailed"} {
		if !contains(seen, want) {
			t.Errorf("events %v, want %s", seen, want)
		}
	}

	// Both attempts' logs are kept in the bucket.
	st3 := e.runStore(r)
	uid := string(r.UID)
	for attempt := 1; attempt <= 2; attempt++ {
		key := chunk.LogsPrefix("testcase14", uid, "pass", 0, attempt) + "component.log"
		data, err := store.ReadAll(t.Context(), st3, key)
		if err != nil || !bytes.Contains(data, []byte("cannot handle")) {
			t.Errorf("%s: %v\n%.300s", key, err, data)
		}
	}

	// The last attempt is kept with the chunks it had open, the first of
	// which is the one it failed on.
	data, err := store.ReadAll(t.Context(), st3, chunk.FailurePrefix("testcase14", uid, "pass", 0, 2)+"failure.json")
	var f controller.Failure
	if err == nil {
		err = json.Unmarshal(data, &f)
	}
	if err != nil || len(f.Inputs) == 0 || f.Inputs[0].N != 5 || f.Image != r.Spec.Steps[1].Image {
		t.Fatalf("failure %+v (%v)", f, err)
	}

	// alpheus replay fetches them, checked; the bench component then runs
	// on them in local mode at debug level, here without a container.
	dir := t.TempDir()
	args := []string{"replay", "testcase14", "pass", "-n", e.ns, "--with", "none", "--dir", dir}
	if v := os.Getenv("ALPHEUS_E2E_S3_ENDPOINT"); v != "" {
		args = append(args, "--endpoint", v)
	}
	if v := os.Getenv("ALPHEUS_E2E_S3_CA_FILE"); v != "" {
		args = append(args, "--ca-file", v)
	}
	var out bytes.Buffer
	env := cli.DefaultEnv()
	env.Stdout, env.Stderr = &out, &out
	if code := cli.Main(t.Context(), args, env); code != 0 {
		t.Fatalf("alpheus %s: exit %d\n%s", strings.Join(args, " "), code, out.String())
	}
	t.Logf("replay:\n%s", out.String())
	bin := filepath.Join(t.TempDir(), "bench")
	if b, err := exec.Command("go", "build", "-o", bin, "../components/bench").CombinedOutput(); err != nil {
		t.Fatalf("build bench: %v\n%s", err, b)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = append(os.Environ(), "ALPHEUS_MODE=local", "ALPHEUS_LOG_LEVEL=debug",
		"ALPHEUS_LOCAL_INPUT="+filepath.Join(dir, "in", "default"), "ALPHEUS_LOCAL_OUTPUT="+filepath.Join(dir, "out"),
		"ALPHEUS_SETTINGS="+string(f.Settings))
	got, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("replayed component: %v, want exit code 1\n%s", err, got)
	}
	for _, want := range []string{`"level":"DEBUG","msg":"read chunk"`, "source chunk 5) is one this component cannot handle"} {
		if !bytes.Contains(got, []byte(want)) {
			t.Errorf("replayed log has no %q:\n%s", want, got)
		}
	}
}

func findCondition(cs []metav1.Condition, typ string) *metav1.Condition {
	for i := range cs {
		if cs[i].Type == typ {
			return &cs[i]
		}
	}
	return nil
}

func contains(list []string, s string) bool { return slices.Contains(list, s) }
