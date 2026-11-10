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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/store"
)

// defaultResultsTTL is the CRD's default for spec.resultsTTL (GO-78), for
// a run.json without one.
const defaultResultsTTL = 7 * 24 * time.Hour

// resultSet is one run's results in the bucket, as results gc prints it.
type resultSet struct {
	Prefix     string     `json:"prefix"`
	Run        string     `json:"run"`
	Namespace  string     `json:"namespace,omitempty"`
	UID        string     `json:"uid"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	// Reason says why the results are deleted or kept.
	Reason string `json:"reason"`
	// Action is delete, deleted or keep.
	Action string `json:"action"`
}

func resultsCmd(ctx context.Context, e *Env, args []string) error {
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "get":
		return resultsGet(ctx, e, args[1:])
	case "cat":
		return resultsCat(ctx, e, args[1:])
	case "ls":
		return resultsLs(ctx, e, args[1:])
	case "gc":
		return resultsGC(ctx, e, args[1:])
	}
	return usagef("results takes a subcommand: get, cat, ls or gc")
}

func resultsGC(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "results gc", "", &o)
	endpoint := fs.String("endpoint", "", "the S3 endpoint, as in spec.storage.endpoint")
	bucket := fs.String("bucket", "", "the bucket with the results: spec.storage.resultsBucket, or bucket")
	region := fs.String("region", "", "the bucket's region, as in spec.storage.region")
	secret := fs.String("secret", "", "the Secret in -n with keys that can delete in the bucket (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
	caFile := fs.String("ca-file", "", "CA bundle for the endpoint")
	dryRun := fs.Bool("dry-run", false, "only show what would be deleted")
	yes := fs.Bool("yes", false, "delete without asking")
	orphans := fs.Bool("orphans", false, "also delete results whose StreamRun is gone, before their TTL runs out")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) > 0 {
		return usagef("results gc takes no arguments")
	}
	if *endpoint == "" || *bucket == "" || *secret == "" {
		return usagef("--endpoint, --bucket and --secret are required")
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var s corev1.Secret
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: *secret}, &s); err != nil {
		if isNotFound(err) {
			return hint(fmt.Errorf("secret %s not found in namespace %s", *secret, ns), "pass the namespace of the Secret with -n")
		}
		return apiError(err)
	}
	st, err := e.NewS3(store.S3Config{
		Endpoint:  *endpoint,
		Bucket:    *bucket,
		Region:    *region,
		AccessKey: string(s.Data["AWS_ACCESS_KEY_ID"]),
		SecretKey: string(s.Data["AWS_SECRET_ACCESS_KEY"]),
		CAFile:    *caFile,
	})
	if err != nil {
		return err
	}

	sets, err := findResults(ctx, e, c.Client, st, *orphans)
	if err != nil {
		return err
	}
	var doomed []*resultSet
	for i := range sets {
		if sets[i].Action == "delete" {
			doomed = append(doomed, &sets[i])
		}
	}
	text := func(w io.Writer) {
		if len(sets) == 0 {
			fmt.Fprintln(w, "No results in the bucket.")
			return
		}
		fmt.Fprintln(w, "ACTION\tRUN\tNAMESPACE\tFINISHED\tREASON\tPREFIX")
		for _, r := range sets {
			finished := "-"
			if r.FinishedAt != nil {
				finished = age(e.Now().Sub(*r.FinishedAt)) + " ago"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", r.Action, r.Run, dash(r.Namespace), finished, r.Reason, r.Prefix)
		}
	}
	if *dryRun || len(doomed) == 0 {
		return write(e.Stdout, o.output, sets, text)
	}
	if !*yes {
		if err := write(e.Stderr, "", nil, text); err != nil {
			return err
		}
		fmt.Fprintf(e.Stderr, "\nDelete the results of %d runs? This cannot be undone. [y/N] ", len(doomed))
		answer, _ := bufio.NewReader(e.Stdin).ReadString('\n')
		if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
			fmt.Fprintln(e.Stderr, "Nothing deleted.")
			return nil
		}
	}
	for _, r := range doomed {
		if err := store.DeletePrefix(ctx, st, r.Prefix); err != nil {
			return hint(fmt.Errorf("delete %s: %w", r.Prefix, err), "the keys in --secret must be allowed to delete in the bucket")
		}
		r.Action = "deleted"
	}
	return write(e.Stdout, o.output, sets, text)
}

// findResults lists the runs under results/ and decides for each whether
// it goes (GO-99): when its resultsTTL has run out since it finished, or,
// with orphans, when its StreamRun is gone. Results with resultsTTL 0 and
// results without run.json are kept: they never expire.
func findResults(ctx context.Context, e *Env, c client.Reader, st store.Store, orphans bool) ([]resultSet, error) {
	var prefixes []string
	for key, err := range st.List(ctx, store.ResultsPrefix) {
		if err != nil {
			return nil, err
		}
		parts := strings.SplitN(key, "/", 4)
		if len(parts) < 4 {
			continue
		}
		p := strings.Join(parts[:3], "/") + "/"
		if len(prefixes) == 0 || prefixes[len(prefixes)-1] != p {
			prefixes = append(prefixes, p)
		}
	}
	prefixes = slices.Compact(slices.Sorted(slices.Values(prefixes)))

	var sets []resultSet
	for _, p := range prefixes {
		parts := strings.Split(p, "/")
		set := resultSet{Prefix: p, Run: parts[1], UID: parts[2], Action: "keep"}
		data, err := store.ReadAll(ctx, st, p+"run.json")
		if errors.Is(err, store.ErrNotFound) {
			set.Reason = "no run.json, so no TTL"
			sets = append(sets, set)
			continue
		}
		if err != nil {
			return nil, err
		}
		var info agent.RunInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("%srun.json: %w", p, err)
		}
		set.Namespace, set.FinishedAt = info.Namespace, info.FinishedAt
		ttl := defaultResultsTTL
		if info.ResultsTTL != "" {
			if ttl, err = time.ParseDuration(info.ResultsTTL); err != nil {
				return nil, fmt.Errorf("%srun.json: resultsTTL: %w", p, err)
			}
		}

		var r v1alpha1.StreamRun
		err = c.Get(ctx, client.ObjectKey{Namespace: info.Namespace, Name: info.Name}, &r)
		if err != nil && !isNotFound(err) {
			return nil, apiError(err)
		}
		gone := err != nil || string(r.UID) != info.UID

		switch {
		case info.FinishedAt != nil && ttl > 0 && e.Now().After(info.FinishedAt.Add(ttl)):
			set.Action = "delete"
			set.Reason = fmt.Sprintf("resultsTTL %s ran out %s ago", ttl, age(e.Now().Sub(info.FinishedAt.Add(ttl))))
		case ttl == 0:
			set.Reason = "resultsTTL 0: kept"
		case gone && orphans:
			set.Action, set.Reason = "delete", "StreamRun is gone"
		case gone:
			set.Reason = "StreamRun is gone; kept until its TTL runs out (--orphans deletes it)"
		case info.FinishedAt == nil:
			set.Reason = "run not finished"
		default:
			set.Reason = fmt.Sprintf("expires in %s", age(info.FinishedAt.Add(ttl).Sub(e.Now())))
		}
		sets = append(sets, set)
	}
	return sets, nil
}
