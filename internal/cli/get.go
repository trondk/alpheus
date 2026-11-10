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
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

func getCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "get", "[run]", &o)
	all := fs.Bool("A", false, "runs in all namespaces")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) > 1 {
		return usagef("get takes at most one run")
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	if len(pos) == 1 {
		var r v1alpha1.StreamRun
		if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
			return notFound(err, ns, pos[0])
		}
		return write(e.Stdout, o.output, typed(&r), func(w io.Writer) { describe(w, &r, e.Now()) })
	}

	var list v1alpha1.StreamRunList
	var opts []client.ListOption
	if !*all {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := c.Client.List(ctx, &list, opts...); err != nil {
		return apiError(err)
	}
	list.APIVersion, list.Kind = v1alpha1.GroupVersion.String(), "StreamRunList"
	for i := range list.Items {
		typed(&list.Items[i])
	}
	return write(e.Stdout, o.output, &list, func(w io.Writer) {
		if len(list.Items) == 0 {
			if *all {
				fmt.Fprintln(w, "No runs.")
			} else {
				fmt.Fprintf(w, "No runs in namespace %s.\n", ns)
			}
			return
		}
		if *all {
			fmt.Fprint(w, "NAMESPACE\t")
		}
		fmt.Fprintln(w, "NAME\tPHASE\tSTEPS\tAGE\tMESSAGE")
		for _, r := range list.Items {
			if *all {
				fmt.Fprintf(w, "%s\t", r.Namespace)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", r.Name, dash(string(r.Status.Phase)), dash(r.Status.StepsFinished),
				age(e.Now().Sub(r.CreationTimestamp.Time)), r.Status.Message)
		}
	})
}

// describe prints one run's status: steps, replicas, edges and results,
// and where to fetch the results and error logs (GO-79, GO-119).
func describe(w io.Writer, r *v1alpha1.StreamRun, now time.Time) {
	st := &r.Status
	fmt.Fprintf(w, "Name:\t%s\n", r.Name)
	fmt.Fprintf(w, "Namespace:\t%s\n", r.Namespace)
	fmt.Fprintf(w, "UID:\t%s\n", r.UID)
	phase := dash(string(st.Phase))
	if st.Reason != "" {
		phase += ": " + string(st.Reason)
	}
	if st.Message != "" {
		phase += " (" + st.Message + ")"
	}
	fmt.Fprintf(w, "Phase:\t%s\n", phase)
	if st.FailedStep != "" && st.FailedReplica != nil {
		fmt.Fprintf(w, "Failed:\tstep %s replica %d\n", st.FailedStep, *st.FailedReplica)
	}
	fmt.Fprintf(w, "Steps finished:\t%s\n", dash(st.StepsFinished))
	fmt.Fprintf(w, "Started:\t%s\n", when(st.StartedAt, now))
	fmt.Fprintf(w, "Finished:\t%s\n", when(st.FinishedAt, now))
	if st.AgentImage != "" {
		fmt.Fprintf(w, "Agent:\t%s\n", st.AgentImage)
	}

	if len(st.Steps) > 0 {
		fmt.Fprintln(w, "\nSTEP\tPHASE\tREPLICAS\tRESTARTS\tERRORS\tWARNINGS\tCOUNTS")
		for _, s := range st.Steps {
			done, restarts := 0, int32(0)
			for _, rep := range s.Replicas {
				if rep.Phase == v1alpha1.StepSucceeded {
					done++
				}
				restarts += rep.Restarts
			}
			fmt.Fprintf(w, "%s\t%s\t%d/%d\t%d\t%d\t%d\t%s\n", s.Name, dash(string(s.Phase)), done, len(s.Replicas),
				restarts, s.Errors, s.Warnings, counts(s.Counts))
		}
		fmt.Fprintln(w, "\nSTEP\tREPLICA\tPHASE\tATTEMPT\tPOD\tREASON\tMESSAGE")
		for _, s := range st.Steps {
			for _, rep := range s.Replicas {
				msg := rep.Message
				if rep.FailedChunks > 0 {
					msg = fmt.Sprintf("%s (%d chunks not processed)", msg, rep.FailedChunks)
				}
				fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%s\t%s\t%s\n", s.Name, rep.Index, dash(string(rep.Phase)), rep.Attempt, dash(rep.Pod), dash(string(rep.Reason)), msg)
			}
		}
	}
	if len(st.Edges) > 0 {
		fmt.Fprintln(w, "\nEDGE\tCHUNKS\tBYTES\tACKED\tLAG\tEOS")
		for _, e := range st.Edges {
			fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%t\n", e.Name, e.Chunks, bytesString(e.Bytes), e.Acked, e.Chunks-e.Acked, e.EOS)
		}
	}
	if len(st.Results) > 0 {
		fmt.Fprintln(w, "\nRESULT\tCHUNKS\tBYTES\tCOUNTS")
		for _, res := range st.Results {
			fmt.Fprintf(w, "%s.%s\t%d\t%s\t%s\n", res.Step, res.Output, res.Chunks, bytesString(res.Bytes), counts(res.Counts))
		}
		fmt.Fprintln(w, "\nFetch the results with the keys in their Secret:")
		for _, res := range st.Results {
			fmt.Fprintf(w, "  %s.%s\t%s/%s/%s\tregion %s\tsecret %s\n", res.Step, res.Output, res.Endpoint, res.Bucket, res.Prefix, dash(res.Region), res.SecretRef)
		}
	}
	if controller.Terminal(st.Phase) {
		next(w, r)
		return
	}
	for _, s := range st.Steps {
		if s.Errors+s.Warnings+s.ErrorsDropped > 0 {
			fmt.Fprintf(w, "\nError log of %s: %s (%d errors, %d warnings, %d dropped)\n", s.Name, s.ErrorLog, s.Errors, s.Warnings, s.ErrorsDropped)
		}
		if s.Logs != "" {
			fmt.Fprintf(w, "\nLogs of %s's failed attempts: %s\n  alpheus logs --kept %s %s\n", s.Name, s.Logs, r.Name, s.Name)
		}
		if s.Failures != "" {
			fmt.Fprintf(w, "Replay %s's last failed attempt on its inputs: alpheus replay %s %s\n", s.Name, r.Name, s.Name)
		}
	}
}

// catBelow is the size of a result below which the list after a run
// suggests results cat (GO-146).
const catBelow = 1 << 20

// next prints, for a finished run, the commands that get its results,
// error log and logs out and clean up, with its name and -n filled in, so
// they can be copied (GO-146). It has only the lines that fit the run.
func next(w io.Writer, r *v1alpha1.StreamRun) {
	st := &r.Status
	ns := " -n " + r.Namespace
	took := ""
	if st.StartedAt != nil && st.FinishedAt != nil {
		took = " in " + st.FinishedAt.Sub(st.StartedAt.Time).Round(time.Second).String()
	}
	fmt.Fprintf(w, "\n%s%s. Next:\n\n", st.Phase, took)
	// Every line has three cells, so the notes line up.
	line := func(label, cmd, note string) { fmt.Fprintf(w, "  %s\t%s\t%s\n", label, cmd, note) }

	// The replica that stopped the run first, then the rest.
	if st.Phase == v1alpha1.RunFailed && st.FailedStep != "" {
		rep := ""
		if st.FailedReplica != nil {
			rep = fmt.Sprintf(" --replica %d", *st.FailedReplica)
		}
		line("Failed", "alpheus logs "+r.Name+" "+st.FailedStep+ns, "(its pods, while they are there)")
		for _, s := range st.Steps {
			if s.Name == st.FailedStep && s.Logs != "" {
				line("", "alpheus logs --kept "+r.Name+" "+st.FailedStep+ns, "(the logs of its failed attempts)")
			}
			if s.Name == st.FailedStep && s.Failures != "" {
				line("", "alpheus replay "+r.Name+" "+st.FailedStep+rep+ns, "(run the failed attempt again on its inputs)")
			}
		}
	}

	if len(st.Results) > 0 {
		var total int64
		for _, res := range st.Results {
			total += res.Bytes
		}
		label := "Results"
		if total > askAbove {
			line(label, "alpheus results ls "+r.Name+ns, fmt.Sprintf("(%s: the chunks as S3 paths, to read where they are)", bytesString(total)))
			line("", "alpheus results copy "+r.Name+ns+" --to s3://<bucket>/<prefix> --to-secret <secret>", "(keep them elsewhere)")
			label = ""
		} else {
			line(label, "alpheus results get "+r.Name+ns, fmt.Sprintf("→ ./%s/  (%s)", r.Name, bytesString(total)))
			line("", "add -d <dir> to put them somewhere else", "")
			label = ""
		}
		for _, res := range st.Results {
			if res.Bytes < catBelow {
				line(label, "alpheus results cat "+r.Name+" "+res.Step+"."+res.Output+ns, fmt.Sprintf("(%s, to stdout)", bytesString(res.Bytes)))
			}
		}
	}
	label := "Error log"
	for _, s := range st.Steps {
		if s.Errors+s.Warnings+s.ErrorsDropped == 0 {
			continue
		}
		note := fmt.Sprintf("(%d errors, %d warnings", s.Errors, s.Warnings)
		if s.ErrorsDropped > 0 {
			note += fmt.Sprintf(", %d dropped", s.ErrorsDropped)
		}
		line(label, "alpheus errors "+r.Name+" "+s.Name+ns, note+")")
		label = ""
	}
	label = "Skipped"
	for _, s := range st.Steps {
		for _, rep := range s.Replicas {
			if rep.FailedChunks > 0 {
				line(label, fmt.Sprintf("alpheus replay %s %s --replica %d --skipped%s", r.Name, s.Name, rep.Index, ns),
					fmt.Sprintf("(%d chunks it never processed)", rep.FailedChunks))
				label = ""
			}
		}
	}
	label = "Logs"
	if st.Phase != v1alpha1.RunFailed {
		line(label, "alpheus logs "+r.Name+ns, "")
		label = ""
	}
	for _, s := range st.Steps {
		if s.Logs != "" && !(st.Phase == v1alpha1.RunFailed && s.Name == st.FailedStep) {
			line(label, "alpheus logs --kept "+r.Name+" "+s.Name+ns, "(the logs of its failed attempts)")
			label = ""
		}
	}

	stay := "results stay " + age(defaultResultsTTL)
	if ttl := r.Spec.ResultsTTL; ttl != nil {
		stay = "results stay " + age(ttl.Duration)
		if ttl.Duration == 0 {
			stay = "results stay until you delete them"
		}
	}
	line("Clean up", "kubectl -n "+r.Namespace+" delete streamrun "+r.Name, "("+stay+")")
}

func when(t *metav1.Time, now time.Time) string {
	if t == nil {
		return "-"
	}
	return fmt.Sprintf("%s (%s ago)", t.UTC().Format(time.RFC3339), age(now.Sub(t.Time)))
}

func watchCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "watch", "<run>", &o)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usagef("watch takes one run")
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: ns, Name: pos[0]}

	// Each change is printed whole: as text separated by a line with the
	// time, as one JSON object per line, or as YAML documents.
	var last *v1alpha1.StreamRunStatus
	show := func(r *v1alpha1.StreamRun) error {
		if last != nil && reflect.DeepEqual(*last, r.Status) {
			return nil
		}
		s := r.Status
		last = &s
		switch o.output {
		case "json":
			data, err := json.Marshal(typed(r))
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(e.Stdout, "%s\n", data)
			return err
		case "yaml":
			data, err := yaml.Marshal(typed(r))
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(e.Stdout, "---\n%s", data)
			return err
		}
		return write(e.Stdout, "", nil, func(w io.Writer) {
			fmt.Fprintf(w, "--- %s\n", e.Now().Format(time.TimeOnly))
			describe(w, r, e.Now())
			fmt.Fprintln(w)
		})
	}

	for {
		var r v1alpha1.StreamRun
		if err := c.Client.Get(ctx, key, &r); err != nil {
			return notFound(err, ns, pos[0])
		}
		if err := show(&r); err != nil {
			return err
		}
		if controller.Terminal(r.Status.Phase) {
			return finished(&r)
		}
		w, err := c.Client.Watch(ctx, &v1alpha1.StreamRunList{}, client.InNamespace(ns),
			client.MatchingFields{"metadata.name": pos[0]},
			&client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: r.ResourceVersion}})
		if err != nil {
			return apiError(err)
		}
		done, err := follow(ctx, w, r.UID, show)
		w.Stop()
		if err != nil || done != nil {
			if err == nil {
				err = finished(done)
			}
			return err
		}
		// The watch ended, as watches do after a while: start again from
		// the current state.
	}
}

// follow shows each change until the run finishes, which it returns, or
// until the watch ends, which returns nil and no error.
func follow(ctx context.Context, w watch.Interface, uid types.UID, show func(*v1alpha1.StreamRun) error) (*v1alpha1.StreamRun, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ev, ok := <-w.ResultChan():
			if !ok {
				return nil, nil
			}
			r, isRun := ev.Object.(*v1alpha1.StreamRun)
			switch {
			case ev.Type == watch.Error:
				return nil, nil
			case !isRun || r.UID != uid:
			case ev.Type == watch.Deleted:
				return nil, fmt.Errorf("streamrun %s was deleted", r.Name)
			default:
				if err := show(r); err != nil {
					return nil, err
				}
				if controller.Terminal(r.Status.Phase) {
					return r, nil
				}
			}
		}
	}
}

// finished makes watch exit with 0 only when the run succeeded, so scripts
// can wait for a run with it.
func finished(r *v1alpha1.StreamRun) error {
	if r.Status.Phase == v1alpha1.RunSucceeded {
		return nil
	}
	msg := fmt.Sprintf("streamrun %s finished as %s", r.Name, r.Status.Phase)
	if r.Status.Message != "" {
		msg += ": " + r.Status.Message
	}
	return hint(fmt.Errorf("%s", msg), "see the logs with: alpheus logs "+r.Name+" -n "+r.Namespace)
}

func cancelCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "cancel", "<run>", &o)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usagef("cancel takes one run")
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
		return notFound(err, ns, pos[0])
	}
	text := fmt.Sprintf("streamrun %s/%s cancelled\n", ns, r.Name)
	switch {
	case controller.Terminal(r.Status.Phase):
		text = fmt.Sprintf("streamrun %s/%s has already finished as %s\n", ns, r.Name, r.Status.Phase)
	case !r.Spec.Cancel:
		patch := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"cancel":true}}`))
		if err := c.Client.Patch(ctx, &r, patch); err != nil {
			return apiError(err)
		}
	}
	return write(e.Stdout, o.output, typed(&r), func(w io.Writer) { fmt.Fprint(w, text) })
}
