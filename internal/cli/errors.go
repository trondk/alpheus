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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// errorFile is one file of a replica's error log: its key in the bucket
// and where it goes in a folder, _errors/<step>/<replica>/<name>.
type errorFile struct {
	key, file string
	// step and replica it belongs to; segment says it holds entries.
	step    string
	replica int
	segment bool
}

// errorLogFiles finds the error log of every step of the run, or of one:
// each replica's manifest.json and the segments it lists, oldest first
// (GO-118). Segments the manifest does not list were never committed.
func errorLogFiles(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, step string) ([]errorFile, error) {
	var files []errorFile
	for i := range r.Spec.Steps {
		s := &r.Spec.Steps[i]
		if step != "" && s.Name != step {
			continue
		}
		for rep := range controller.Replicas(s) {
			prefix := chunk.ErrorLogPrefix(r.Name, string(r.UID), s.Name, rep)
			data, err := store.ReadAll(ctx, st, prefix+"manifest.json")
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			var obs agent.Observations
			if err := json.Unmarshal(data, &obs); err != nil {
				return nil, fmt.Errorf("%smanifest.json: %w", prefix, err)
			}
			if obs.Version != agent.ObservationsVersion {
				return nil, fmt.Errorf("%smanifest.json has format version %d, which this alpheus does not know", prefix, obs.Version)
			}
			local := "_errors/" + s.Name + "/" + strconv.Itoa(rep) + "/"
			files = append(files, errorFile{key: prefix + "manifest.json", file: local + "manifest.json", step: s.Name, replica: rep})
			for _, seg := range obs.Segments {
				files = append(files, errorFile{key: prefix + seg, file: local + seg, step: s.Name, replica: rep, segment: true})
			}
		}
	}
	return files, nil
}

func errorsCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "errors", "<run> [step]", &o)
	var s3 s3Flags
	s3.add(fs)
	tail := fs.Int("tail", -1, "only the last entries; -1 means all")
	dir := fs.String("d", "", "save the error log to <dir>/_errors/<step>/<replica>/ instead of showing it")
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return usagef("errors takes a run and optionally a step")
	}
	r, st, at, _, err := openRunResults(ctx, e, &o, pos[0], s3)
	if err != nil {
		return err
	}
	step := ""
	if len(pos) == 2 {
		step = pos[1]
		if !slices.ContainsFunc(r.Spec.Steps, func(s v1alpha1.Step) bool { return s.Name == step }) {
			var names []string
			for _, s := range r.Spec.Steps {
				names = append(names, s.Name)
			}
			return hint(fmt.Errorf("run %s has no step %q", r.Name, step), "its steps are: "+strings.Join(names, ", "))
		}
	}
	files, err := errorLogFiles(ctx, st, r, step)
	if err != nil {
		return at.explain(err)
	}
	if len(files) == 0 {
		what := "run " + r.Name
		if step != "" {
			what = "step " + step
		}
		fmt.Fprintf(e.Stderr, "The components of %s have reported no errors or warnings.\n", what)
		return nil
	}

	if *dir != "" {
		for _, f := range files {
			if err := fetchWhole(ctx, st, f.key, filepath.Join(*dir, filepath.FromSlash(f.file))); err != nil {
				return at.explain(err)
			}
		}
		fmt.Fprintf(e.Stdout, "Saved the error log to %s\n", filepath.Join(*dir, "_errors"))
		return nil
	}

	var entries []agent.LogEntry
	for _, f := range files {
		if !f.segment {
			continue
		}
		data, err := store.ReadAll(ctx, st, f.key)
		if err != nil {
			return at.explain(err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			var le agent.LogEntry
			if err := json.Unmarshal(sc.Bytes(), &le); err != nil {
				return fmt.Errorf("%s: %w", f.key, err)
			}
			entries = append(entries, le)
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("%s: %w", f.key, err)
		}
	}
	if *tail >= 0 && len(entries) > *tail {
		entries = entries[len(entries)-*tail:]
	}

	switch o.output {
	case "json":
		for _, le := range entries {
			data, err := json.Marshal(le)
			if err != nil {
				return err
			}
			fmt.Fprintf(e.Stdout, "%s\n", data)
		}
		return nil
	case "yaml":
		for _, le := range entries {
			data, err := yaml.Marshal(le)
			if err != nil {
				return err
			}
			fmt.Fprintf(e.Stdout, "---\n%s", data)
		}
		return nil
	}
	return write(e.Stdout, "", nil, func(w io.Writer) {
		fmt.Fprintln(w, "TIME\tSTEP\tREPLICA\tLEVEL\tCODE\tCHUNK\tMESSAGE")
		for _, le := range entries {
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n", le.Time.Format("2006-01-02T15:04:05.000Z07:00"),
				le.Step, le.Replica, le.Level, le.Code, dash(le.Chunk), oneLine(le.Message, le.Fields))
		}
		fmt.Fprintln(w)
		fmt.Fprintln(w, "CODE\tLEVEL\tCOUNT")
		type key struct{ code, level string }
		counts := map[key]int{}
		for _, le := range entries {
			counts[key{le.Code, le.Level}]++
		}
		keys := make([]key, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b key) int {
			return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a.code, b.code), cmp.Compare(a.level, b.level))
		})
		for _, k := range keys {
			fmt.Fprintf(w, "%s\t%s\t%d\n", k.code, k.level, counts[k])
		}
	})
}

// oneLine is an entry's message with its fields, on one line.
func oneLine(msg string, fields json.RawMessage) string {
	msg = strings.ReplaceAll(msg, "\n", " ")
	if len(fields) > 0 && string(fields) != "null" {
		var buf bytes.Buffer
		if json.Compact(&buf, fields) == nil {
			msg += " " + buf.String()
		}
	}
	return msg
}
