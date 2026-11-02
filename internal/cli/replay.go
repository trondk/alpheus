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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// replayView is what replay prints with -o.
type replayView struct {
	Failure  controller.Failure `json:"failure"`
	Dir      string             `json:"dir"`
	Command  []string           `json:"command,omitempty"`
	ExitCode *int               `json:"exitCode,omitempty"`
}

// stringList is a flag that can be given more than once.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func replayCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "replay", "<run> <step>", &o)
	replica := fs.Int("replica", -1, "the replica; default: the one that stopped the run, else 0")
	attempt := fs.Int("attempt", 0, "the failed attempt; default: the last one kept")
	dir := fs.String("dir", "", "where the inputs go; default: ./alpheus-replay/RUN-STEP-REPLICA-ATTEMPT")
	with := fs.String("with", "podman", "run the component with podman or docker, or none to only fetch the inputs")
	image := fs.String("image", "", "pull the component from here instead, such as a mirror's address for the same digest")
	var env stringList
	fs.Var(&env, "e", "KEY=VALUE for the component, such as what the step's envFrom gives; can be repeated")
	caFile := fs.String("ca-file", "", "CA bundle for the storage endpoint")
	endpoint := fs.String("endpoint", "", "the S3 endpoint as seen from here, when spec.storage.endpoint is only reachable in the cluster")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usagef("replay takes a run and a step")
	}
	if *with != "podman" && *with != "docker" && *with != "none" {
		return usagef("--with %s: want podman, docker or none", *with)
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
	sr := r.DeepCopy()
	if *endpoint != "" {
		sr.Spec.Storage.Endpoint = *endpoint
	}
	st, err := e.OpenStore(ctx, c.Client, sr, *caFile)
	if err != nil {
		return hint(err, "replay reads the failure with the keys in spec.storage.secretRef, so you need access to that Secret")
	}
	reachable := "if " + sr.Spec.Storage.Endpoint + " is only reachable inside the cluster, pass the address you reach it by with --endpoint"

	if *replica < 0 {
		*replica = 0
		if r.Status.FailedStep == step && r.Status.FailedReplica != nil {
			*replica = int(*r.Status.FailedReplica)
		}
	}
	prefix, err := findFailure(ctx, st, &r, step, *replica, *attempt)
	if err != nil {
		return hint(err, reachable)
	}
	data, err := store.ReadAll(ctx, st, prefix+"failure.json")
	if err != nil {
		return hint(err, reachable)
	}
	var f controller.Failure
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("%sfailure.json: %w", prefix, err)
	}

	if *dir == "" {
		*dir = filepath.Join("alpheus-replay", fmt.Sprintf("%s-%s-%d-%d", f.Run, f.Step, f.Replica, f.Attempt))
	}
	if err := fetchInputs(ctx, st, prefix, &f, *dir); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "failure.json"), data, 0o644); err != nil {
		return err
	}
	view := replayView{Failure: f, Dir: *dir}
	if o.output == "" {
		fmt.Fprintf(e.Stdout, "Step %s replica %d, attempt %d: %s\n", f.Step, f.Replica, f.Attempt, f.Reason)
		fmt.Fprintf(e.Stdout, "  %s\n", f.Message)
		fmt.Fprintf(e.Stdout, "Image:  %s\n", f.Image)
		fmt.Fprintf(e.Stdout, "Inputs: %d chunks in %s, checked against their SHA-256\n", len(f.Inputs), filepath.Join(*dir, "in"))
		fmt.Fprintf(e.Stdout, "Logs of the attempt: %s\n", f.Logs)
	}
	if *with == "none" {
		if o.output != "" {
			return write(e.Stdout, o.output, view, nil)
		}
		fmt.Fprintf(e.Stdout, "\nRun it yourself in local mode with:\n  %s\n", strings.Join(localEnv(&f, filepath.Join(*dir, "in"), filepath.Join(*dir, "out")), " \\\n    "))
		return nil
	}

	img := f.Image
	if *image != "" {
		img = *image
	}
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	cmd := []string{*with, "run", "--rm", "--network=none", "-v", abs + ":/replay:Z"}
	for _, kv := range localEnv(&f, "/replay/in", "/replay/out") {
		cmd = append(cmd, "-e", kv)
	}
	for _, kv := range env {
		cmd = append(cmd, "-e", kv)
	}
	cmd = append(cmd, img)
	view.Command = cmd
	if o.output == "" {
		fmt.Fprintf(e.Stdout, "\n$ %s\n\n", strings.Join(cmd, " "))
	}
	code, err := e.RunCommand(ctx, cmd, e.Stdout, e.Stderr)
	if err != nil {
		return hint(fmt.Errorf("%s: %w", *with, err), "install "+*with+", or pass --with none and run the component yourself")
	}
	view.ExitCode = &code
	if o.output != "" {
		return write(e.Stdout, o.output, view, nil)
	}
	switch code {
	case 0:
		fmt.Fprintf(e.Stdout, "\nThe component succeeded on these inputs; in the run it failed with: %s\n", f.Message)
		fmt.Fprintln(e.Stdout, "The failure may depend on chunks before these, on the environment or on resources.")
	default:
		fmt.Fprintf(e.Stdout, "\nThe component exited with code %d, as in the run. Its output is in %s.\n", code, filepath.Join(*dir, "out"))
	}
	return nil
}

// localEnv is the environment that runs the component in local mode on
// the failure's inputs, at debug level (GO-141). The inputs are only this
// replica's, so it runs as replica 0 of 1.
func localEnv(f *controller.Failure, in, out string) []string {
	settings := string(f.Settings)
	if settings == "" {
		settings = "{}"
	}
	env := []string{"ALPHEUS_MODE=local", "ALPHEUS_LOCAL_OUTPUT=" + out, "ALPHEUS_LOG_LEVEL=debug",
		"ALPHEUS_REPLICA=0", "ALPHEUS_REPLICAS=1", "ALPHEUS_SETTINGS=" + settings}
	var ports []string
	for _, p := range inputPorts(f) {
		ports = append(ports, p+"="+path.Join(in, p))
	}
	switch {
	case len(ports) == 1 && strings.HasPrefix(ports[0], "default="):
		env = append(env, "ALPHEUS_LOCAL_INPUT="+path.Join(in, "default"))
	case len(ports) > 0:
		env = append(env, "ALPHEUS_LOCAL_INPUTS="+strings.Join(ports, ","))
	}
	return env
}

func inputPorts(f *controller.Failure) []string {
	var ports []string
	for _, in := range f.Inputs {
		if !slices.Contains(ports, in.Port) {
			ports = append(ports, in.Port)
		}
	}
	sort.Strings(ports)
	return ports
}

// findFailure is the prefix of a kept failure: the given attempt, or the
// last one.
func findFailure(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, step string, replica, attempt int) (string, error) {
	if attempt > 0 {
		return chunk.FailurePrefix(r.Name, string(r.UID), step, replica, attempt), nil
	}
	base := chunk.FailurePrefix(r.Name, string(r.UID), step, replica, 1)
	base = base[:strings.LastIndex(strings.TrimSuffix(base, "/"), "/")+1]
	last := 0
	for key, err := range st.List(ctx, base) {
		if err != nil {
			return "", err
		}
		rest, ok := strings.CutPrefix(key, base+"attempt-")
		if !ok {
			continue
		}
		n, _, _ := strings.Cut(rest, "/")
		if a, err := strconv.Atoi(n); err == nil && a > last {
			last = a
		}
	}
	if last == 0 {
		return "", fmt.Errorf("step %s replica %d of run %s has no kept failure", step, replica, r.Name)
	}
	return chunk.FailurePrefix(r.Name, string(r.UID), step, replica, last), nil
}

// fetchInputs downloads the failure's input chunks into dir/in/<port>/,
// checked against their size and SHA-256, and makes dir/out/ for the
// output, writable by the component's user.
func fetchInputs(ctx context.Context, st store.Store, prefix string, f *controller.Failure, dir string) error {
	out := filepath.Join(dir, "out")
	if err := os.RemoveAll(out); err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o777); err != nil {
		return err
	}
	if err := os.Chmod(out, 0o777); err != nil { // the image's user is not this one
		return err
	}
	for _, in := range f.Inputs {
		pdir := filepath.Join(dir, "in", in.Port)
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			return err
		}
		if err := fetchChunk(ctx, st, prefix+"input/"+in.File, filepath.Join(pdir, in.File), in); err != nil {
			return err
		}
	}
	return nil
}

func fetchChunk(ctx context.Context, st store.Store, key, file string, in controller.FailureInput) error {
	r, err := st.Get(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(file)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), r)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err == nil && (n != in.Bytes || hex.EncodeToString(h.Sum(nil)) != in.SHA256) {
		err = errors.New("its size or SHA-256 differs from the manifest's")
	}
	if err != nil {
		os.Remove(file)
		return fmt.Errorf("input %s: %w", in.File, err)
	}
	return nil
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
