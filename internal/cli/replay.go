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
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/replay"
	"alpheus-dev.org/alpheus/internal/store"
)

// replayView is what replay prints with -o.
type replayView struct {
	Failure  replay.Failure `json:"failure"`
	Dir      string         `json:"dir,omitempty"`
	Pod      string         `json:"pod,omitempty"`
	Command  []string       `json:"command,omitempty"`
	ExitCode *int           `json:"exitCode,omitempty"`
	// FailedImage is the image of the failed attempt, when the replay ran
	// another version of the component with --image (GO-161).
	FailedImage string `json:"failedImage,omitempty"`
}

// stringList is a flag that can be given more than once.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func replayCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "replay", "<run> <step>", &o)
	replica := fs.Int("replica", -1, "the replica; default: the one that stopped the run, else one that gave up or failed, else 0")
	attempt := fs.Int("attempt", 0, "the failed attempt; default: the last one kept")
	skipped := fs.Bool("skipped", false, "run every input chunk the replica never processed after it gave up with failurePolicy continue, instead of one attempt's")
	dir := fs.String("dir", "", "where the inputs go; default: ./alpheus-replay/RUN-STEP-REPLICA-ATTEMPT")
	with := fs.String("with", "podman", "run the component with podman or docker here, as a pod in the cluster (cluster), or not at all (none: only fetch the inputs)")
	image := fs.String("image", "", "run this image instead: the same digest from a mirror, or a new version to try a fix on the same inputs")
	agentImage := fs.String("agent-image", "", "with --with cluster: the agent's image, which fetches the inputs; default: the controller's")
	keep := fs.Bool("keep", false, "with --with cluster: keep the replay pod")
	var env stringList
	fs.Var(&env, "e", "KEY=VALUE for the component, such as what the step's envFrom gives; can be repeated")
	var s3 s3Flags
	s3.add(fs)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usagef("replay takes a run and a step")
	}
	if !slices.Contains([]string{"podman", "docker", "cluster", "none"}, *with) {
		return usagef("--with %s: want podman, docker, cluster or none", *with)
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
		return notFound(err, ns, pos[0])
	}
	step := pos[1]
	if !slices.ContainsFunc(r.Spec.Steps, func(s v1alpha1.Step) bool { return s.Name == step }) {
		return fmt.Errorf("run %s has no step %q", r.Name, step)
	}
	st, at, err := e.openRun(ctx, c.Client, &r, s3)
	if err != nil {
		return err
	}

	if *skipped && *attempt != 0 {
		return usagef("--skipped and --attempt: the skipped chunks are the replica's, not an attempt's")
	}
	if *replica < 0 {
		*replica = failedReplica(&r, step)
	}
	var prefix string
	if *skipped {
		prefix = chunk.SkippedPrefix(r.Name, string(r.UID), step, *replica)
	} else if prefix, err = replay.Find(ctx, st, r.Name, string(r.UID), step, *replica, *attempt); err != nil {
		return at.explain(err)
	}
	f, data, err := replay.Read(ctx, st, prefix)
	if *skipped && errors.Is(err, store.ErrNotFound) {
		return hint(fmt.Errorf("step %s replica %d of run %s has no skipped chunks kept", step, *replica, r.Name),
			"they are kept when the run has finished, for a replica that gave up with failurePolicy continue")
	}
	if err != nil {
		return at.explain(err)
	}
	failed := f.Image
	if *image != "" {
		f.Image = *image
	}
	// Another digest is another version of the component, such as a fix
	// to try on the inputs that made it fail; the same digest from another
	// registry is the same version.
	digest := func(ref string) string { _, d, _ := strings.Cut(ref, "@"); return d }
	newVersion := *image != "" && digest(*image) != digest(failed)
	view := replayView{Failure: *f}
	if newVersion {
		view.FailedImage = failed
	}
	if o.output == "" {
		fmt.Fprintf(e.Stdout, "Step %s replica %d, attempt %d: %s\n", f.Step, f.Replica, f.Attempt, f.Reason)
		fmt.Fprintf(e.Stdout, "  %s\n", f.Message)
		if f.Skipped {
			// The side inputs are kept too, but they are lookup data, not
			// chunks the replica skipped (GO-167).
			skipped := 0
			for _, in := range f.Inputs {
				if !in.Side {
					skipped++
				}
			}
			fmt.Fprintf(e.Stdout, "Skipped: the %d chunks the replica never processed, after it gave up\n", skipped)
			if f.NotKept > 0 {
				fmt.Fprintf(e.Stdout, "  and %d more that were not kept\n", f.NotKept)
			}
		}
		fmt.Fprintf(e.Stdout, "Image:  %s\n", f.Image)
		if newVersion {
			fmt.Fprintf(e.Stdout, "        another version than the one that failed: %s\n", failed)
		}
		fmt.Fprintf(e.Stdout, "Inputs: %d chunks, checked against their SHA-256\n", len(f.Inputs))
		fmt.Fprintf(e.Stdout, "Logs of the attempt: %s\n", f.Logs)
	}

	var code int
	if *with == "cluster" {
		if *agentImage == "" {
			if *agentImage, err = controllerAgentImage(ctx, c.Client); err != nil {
				return hint(err, "pass the agent's image with --agent-image")
			}
		}
		name, code2, err := replayInCluster(ctx, e, c, &r, f, prefix, *agentImage, env, *keep, o.output == "")
		view.Pod = name
		if err != nil {
			return err
		}
		code = code2
	} else {
		if *dir == "" {
			*dir = filepath.Join("alpheus-replay", fmt.Sprintf("%s-%s-%d-%d", f.Run, f.Step, f.Replica, f.Attempt))
			if f.Skipped {
				*dir = filepath.Join("alpheus-replay", fmt.Sprintf("%s-%s-%d-skipped", f.Run, f.Step, f.Replica))
			}
		}
		view.Dir = *dir
		if err := replay.Fetch(ctx, st, prefix, f, *dir); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*dir, "failure.json"), data, 0o644); err != nil {
			return err
		}
		if o.output == "" {
			fmt.Fprintf(e.Stdout, "Fetched to %s\n", filepath.Join(*dir, "in"))
		}
		if *with == "none" {
			if o.output != "" {
				return write(e.Stdout, o.output, view, nil)
			}
			fmt.Fprintf(e.Stdout, "\nRun it yourself in local mode with:\n  %s\n", strings.Join(replay.LocalEnv(f, filepath.Join(*dir, "in"), filepath.Join(*dir, "out")), " \\\n    "))
			return nil
		}
		abs, err := filepath.Abs(*dir)
		if err != nil {
			return err
		}
		// The root file system is read-only and /tmp is the only other
		// place to write, as in the pod.
		cmd := []string{*with, "run", "--rm", "--network=none", "--read-only", "--tmpfs", controller.TmpPath}
		if *with == "podman" {
			// The component's user, 65532 as in the pod, is you outside the
			// container, so you can read and delete what it writes.
			cmd = append(cmd, "--userns=keep-id:uid=65532,gid=65532")
		}
		cmd = append(cmd, "-v", abs+":/replay:Z", "-e", "TMPDIR="+controller.TmpPath, "-e", "HOME="+controller.TmpPath)
		for _, kv := range replay.LocalEnv(f, "/replay/in", "/replay/out") {
			cmd = append(cmd, "-e", kv)
		}
		for _, kv := range env {
			cmd = append(cmd, "-e", kv)
		}
		cmd = append(cmd, f.Image)
		view.Command = cmd
		if o.output == "" {
			fmt.Fprintf(e.Stdout, "\n$ %s\n\n", strings.Join(cmd, " "))
		}
		// With -o, stdout has only the JSON or YAML, so what the component
		// prints goes to stderr (GO-163).
		stdout := e.Stdout
		if o.output != "" {
			stdout = e.Stderr
		}
		if code, err = e.RunCommand(ctx, cmd, stdout, e.Stderr); err != nil {
			return hint(fmt.Errorf("%s: %w", *with, err), "install "+*with+", run it in the cluster with --with cluster, or pass --with none and run the component yourself")
		}
	}
	view.ExitCode = &code
	if o.output != "" {
		return write(e.Stdout, o.output, view, nil)
	}
	switch {
	case code == 125 && *with != "cluster":
		// podman and docker exit with 125 when they cannot run the
		// container at all, such as when the image cannot be pulled.
		fmt.Fprintf(e.Stdout, "\n%s could not run the component (exit code 125); see its error above.\n", *with)
		fmt.Fprintln(e.Stdout, "If the image's registry is only reachable in the cluster, pull the same digest from an address you reach and pass it with --image.")
		return nil
	case code == 0 && newVersion:
		fmt.Fprintf(e.Stdout, "\nThis version of the component succeeded on the inputs the failed attempt had; the attempt failed with: %s\n", f.Message)
		if view.Dir != "" {
			fmt.Fprintf(e.Stdout, "Its output is in %s, and what it counted and logged in _summary.json and _errors.jsonl there.\n", filepath.Join(view.Dir, "out"))
		}
		fmt.Fprintf(e.Stdout, "To run the flow with it: alpheus run -f <flow file> --name <a new name> --image %s=%s\n", f.Step, f.Image)
		if *with != "cluster" {
			fmt.Fprintln(e.Stdout, "(with the address of the registry that the cluster pulls from)")
		}
	case code == 0:
		fmt.Fprintf(e.Stdout, "\nThe component succeeded on these inputs; in the run it failed with: %s\n", f.Message)
		fmt.Fprintln(e.Stdout, "The failure may depend on chunks before these, on the environment or on resources.")
	default:
		where := ""
		if view.Dir != "" {
			where = " Its output is in " + filepath.Join(view.Dir, "out") + "."
		}
		if newVersion {
			fmt.Fprintf(e.Stdout, "\nThis version of the component also failed, with exit code %d.%s\n", code, where)
		} else {
			fmt.Fprintf(e.Stdout, "\nThe component exited with code %d, as in the run.%s\n", code, where)
		}
	}
	return nil
}

// controllerAgentImage is the agent's image the controller in
// alpheus-system starts step pods with.
func controllerAgentImage(ctx context.Context, c client.Reader) (string, error) {
	var d appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: "alpheus-system", Name: "alpheus-controller"}, &d); err != nil {
		return "", fmt.Errorf("find the agent's image in alpheus-system/alpheus-controller: %w", err)
	}
	for _, ct := range d.Spec.Template.Spec.Containers {
		for _, a := range ct.Args {
			if img, ok := strings.CutPrefix(a, "-agent-image="); ok {
				return img, nil
			}
		}
	}
	return "", errors.New("alpheus-system/alpheus-controller has no -agent-image")
}

// replayInCluster runs the replay as a pod in the run's namespace
// (GO-141), prints the fetch's and the component's logs as they come, and
// returns the component's exit code. The pod is deleted afterwards unless
// keep is set.
func replayInCluster(ctx context.Context, e *Env, c *Clients, r *v1alpha1.StreamRun, f *replay.Failure, prefix, agentImage string, env []string, keep, text bool) (string, int, error) {
	suffix := make([]byte, 3)
	rand.Read(suffix)
	name := fmt.Sprintf("%s-%s-%d-replay-%d-%s", r.Name, f.Step, f.Replica, f.Attempt, hex.EncodeToString(suffix))
	if len(name) > 63 {
		name = name[len(name)-63:]
		name = strings.TrimLeft(name, "-.")
	}
	pod, err := controller.ReplayPod(r, f, prefix, agentImage, name)
	if err != nil {
		return "", 0, err
	}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: k, Value: v})
	}
	if err := c.Client.Create(ctx, pod); err != nil {
		return "", 0, hint(apiError(err), "a replay in the cluster creates a pod in the run's namespace")
	}
	if !keep {
		defer func() {
			if err := c.Client.Delete(context.WithoutCancel(ctx), pod); err != nil && !isNotFound(err) {
				fmt.Fprintf(e.Stderr, "the replay pod %s/%s was not deleted: %v\n", pod.Namespace, pod.Name, err)
			}
		}()
	}
	if text {
		fmt.Fprintf(e.Stdout, "\nReplaying in pod %s/%s, at debug level:\n\n", r.Namespace, name)
	}
	// With -o, stdout has only the JSON or YAML, so the logs go to stderr
	// (GO-163).
	logs := e.Stdout
	if !text {
		logs = e.Stderr
	}
	for _, container := range []string{controller.ReplayContainer, controller.ComponentContainer} {
		if err := followLog(ctx, c, r.Namespace, name, container, logs); err != nil {
			return name, 0, err
		}
	}
	// The pod's status may lag the end of its log.
	for {
		var cur corev1.Pod
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(pod), &cur); err != nil {
			return name, 0, apiError(err)
		}
		for _, cs := range cur.Status.InitContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
				return name, 0, fmt.Errorf("fetching the inputs failed: %s", strings.TrimSpace(t.Message))
			}
		}
		for _, cs := range cur.Status.ContainerStatuses {
			if t := cs.State.Terminated; cs.Name == controller.ComponentContainer && t != nil {
				return name, int(t.ExitCode), nil
			}
		}
		select {
		case <-ctx.Done():
			return name, 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// followLog prints a container's log until it ends, waiting for the
// container to start. A container that never starts, such as the
// component after a failed fetch, has no log.
func followLog(ctx context.Context, c *Clients, ns, pod, container string, w io.Writer) error {
	for {
		var cur corev1.Pod
		if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pod}, &cur); err != nil {
			return apiError(err)
		}
		started, done := containerState(&cur, container)
		if started {
			body, err := c.Kube.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: container, Follow: true}).Stream(ctx)
			if err != nil {
				return fmt.Errorf("log of %s: %w", container, err)
			}
			_, err = io.Copy(w, body)
			body.Close()
			return err
		}
		if done || cur.Status.Phase == corev1.PodFailed || cur.Status.Phase == corev1.PodSucceeded {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// containerState says whether a container has started (it runs or ran),
// or will never start because the pod is past it.
func containerState(p *corev1.Pod, name string) (started, never bool) {
	for _, cs := range append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
		if cs.Name == name {
			return cs.State.Running != nil || cs.State.Terminated != nil, false
		}
	}
	return false, false
}

// runCommand runs a command with its output to stdout and stderr, and
// returns its exit code.
func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), nil
	}
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// failedReplica is the replica of step that replay takes by default: the
// one that stopped the run, else the first that gave up with failurePolicy
// continue, else the first with a failed attempt, else 0.
func failedReplica(r *v1alpha1.StreamRun, step string) int {
	if r.Status.FailedStep == step && r.Status.FailedReplica != nil {
		return int(*r.Status.FailedReplica)
	}
	for _, s := range r.Status.Steps {
		if s.Name != step {
			continue
		}
		for _, rs := range s.Replicas {
			if rs.FailedChunks > 0 || rs.Phase == v1alpha1.StepFailed {
				return int(rs.Index)
			}
		}
		for _, rs := range s.Replicas {
			if rs.Reason != "" {
				return int(rs.Index)
			}
		}
	}
	return 0
}
