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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// logLine is one line of a container's log, as logs prints it with -o.
type logLine struct {
	Pod       string `json:"pod"`
	Step      string `json:"step"`
	Replica   int    `json:"replica"`
	Container string `json:"container"`
	Line      string `json:"line"`
}

func logsCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "logs", "<run> [step]", &o)
	container := fs.String("c", controller.ComponentContainer, "the container: component or agent")
	followLogs := fs.Bool("follow", false, "keep printing new lines until the pods end")
	fs.BoolVar(followLogs, "f", false, "short for --follow")
	tail := fs.Int64("tail", -1, "only the last lines of each container; -1 means all")
	kept := fs.Bool("kept", false, "show the logs kept from failed attempts in the bucket, also after the pods are gone (GO-140)")
	caFile := fs.String("ca-file", "", "with --kept: CA bundle for the storage endpoint")
	endpoint := fs.String("endpoint", "", "with --kept: the S3 endpoint as seen from here, when spec.storage.endpoint is only reachable in the cluster")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return usagef("logs takes a run and optionally a step")
	}
	if *container != controller.ComponentContainer && *container != controller.AgentContainer {
		return usagef("-c %s: want %s or %s", *container, controller.ComponentContainer, controller.AgentContainer)
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
		return notFound(err, ns, pos[0])
	}
	if *kept {
		step := ""
		if len(pos) == 2 {
			step = pos[1]
		}
		return keptLogs(ctx, e, c, &r, step, *container, o.output, *caFile, *endpoint)
	}
	labels := client.MatchingLabels{controller.LabelRun: r.Name}
	var order []string
	for _, s := range r.Spec.Steps {
		order = append(order, s.Name)
	}
	if len(pos) == 2 {
		if !slices.Contains(order, pos[1]) {
			return hint(fmt.Errorf("run %s has no step %q", r.Name, pos[1]), "its steps are: "+strings.Join(order, ", "))
		}
		labels[controller.LabelStep] = pos[1]
	}
	var pods corev1.PodList
	if err := c.Client.List(ctx, &pods, client.InNamespace(ns), labels); err != nil {
		return apiError(err)
	}
	if len(pods.Items) == 0 {
		h := "pods are deleted when a run is cancelled or stopped, and after ttlAfterFinished; the logs of failed attempts are kept: alpheus logs --kept " + r.Name
		for _, s := range r.Status.Steps {
			if s.ErrorLog != "" && (len(pos) == 1 || s.Name == pos[1]) {
				h += "; the error log of " + s.Name + " is in " + s.ErrorLog
			}
		}
		return hint(fmt.Errorf("run %s has no pods", r.Name), h)
	}
	// Steps in the order of the spec, then replicas.
	slices.SortFunc(pods.Items, func(a, b corev1.Pod) int {
		if d := slices.Index(order, a.Labels[controller.LabelStep]) - slices.Index(order, b.Labels[controller.LabelStep]); d != 0 {
			return d
		}
		ra, _ := strconv.Atoi(a.Labels[controller.LabelReplica])
		rb, _ := strconv.Atoi(b.Labels[controller.LabelReplica])
		return ra - rb
	})

	var mu sync.Mutex
	emit := func(l logLine) {
		mu.Lock()
		defer mu.Unlock()
		switch o.output {
		case "json":
			data, _ := json.Marshal(l)
			fmt.Fprintf(e.Stdout, "%s\n", data)
		case "yaml":
			data, _ := yaml.Marshal(l)
			fmt.Fprintf(e.Stdout, "---\n%s", data)
		default:
			if len(pods.Items) > 1 {
				fmt.Fprintf(e.Stdout, "[%s/%d] %s\n", l.Step, l.Replica, l.Line)
			} else {
				fmt.Fprintln(e.Stdout, l.Line)
			}
		}
	}
	stream := func(p *corev1.Pod) error {
		opts := &corev1.PodLogOptions{Container: *container, Follow: *followLogs}
		if *tail >= 0 {
			opts.TailLines = tail
		}
		rc, err := c.Kube.CoreV1().Pods(ns).GetLogs(p.Name, opts).Stream(ctx)
		if err != nil {
			return fmt.Errorf("pod %s: %w", p.Name, err)
		}
		defer rc.Close()
		replica, _ := strconv.Atoi(p.Labels[controller.LabelReplica])
		sc := bufio.NewScanner(rc)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			emit(logLine{Pod: p.Name, Step: p.Labels[controller.LabelStep], Replica: replica, Container: *container, Line: sc.Text()})
		}
		return sc.Err()
	}

	// Without --follow the pods are printed one after the other; with it,
	// all at once, as their lines come.
	if !*followLogs {
		var errs []error
		for i := range pods.Items {
			if err := stream(&pods.Items[i]); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	var wg sync.WaitGroup
	errs := make([]error, len(pods.Items))
	for i := range pods.Items {
		wg.Go(func() { errs[i] = stream(&pods.Items[i]) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// keptLogs prints the logs kept from failed attempts (GO-140), from
// results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/<container>.log.
func keptLogs(ctx context.Context, e *Env, c *Clients, r *v1alpha1.StreamRun, step, container, output, caFile, endpoint string) error {
	sr := r.DeepCopy()
	if endpoint != "" {
		sr.Spec.Storage.Endpoint = endpoint
	}
	st, err := e.OpenStore(ctx, c.Client, sr, caFile)
	if err != nil {
		return hint(err, "the kept logs are read with the keys in spec.storage.secretRef, so you need access to that Secret")
	}
	found := 0
	for _, s := range r.Spec.Steps {
		if step != "" && s.Name != step {
			continue
		}
		prefix := chunk.LogsPrefix(r.Name, string(r.UID), s.Name, -1, 0)
		for key, err := range st.List(ctx, prefix) {
			if err != nil {
				return hint(err, "if "+sr.Spec.Storage.Endpoint+" is only reachable inside the cluster, pass the address you reach it by with --endpoint")
			}
			// <replica>/attempt-<n>/<container>.log
			parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
			if len(parts) != 3 || parts[2] != container+".log" {
				continue
			}
			replica, _ := strconv.Atoi(parts[0])
			attempt := strings.TrimPrefix(parts[1], "attempt-")
			data, err := store.ReadAll(ctx, st, key)
			if err != nil {
				return err
			}
			found++
			if output == "" {
				fmt.Fprintf(e.Stdout, "==> %s replica %d attempt %s, %s <==\n", s.Name, replica, attempt, container)
			}
			sc := bufio.NewScanner(bytes.NewReader(data))
			sc.Buffer(make([]byte, 64*1024), 1024*1024)
			for sc.Scan() {
				l := logLine{Pod: "attempt-" + attempt, Step: s.Name, Replica: replica, Container: container, Line: sc.Text()}
				switch output {
				case "json":
					b, _ := json.Marshal(l)
					fmt.Fprintf(e.Stdout, "%s\n", b)
				case "yaml":
					b, _ := yaml.Marshal(l)
					fmt.Fprintf(e.Stdout, "---\n%s", b)
				default:
					fmt.Fprintln(e.Stdout, l.Line)
				}
			}
		}
	}
	if found == 0 {
		return fmt.Errorf("run %s has no kept logs of the %s container: they are kept when an attempt fails", r.Name, container)
	}
	return nil
}
