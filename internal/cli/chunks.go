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
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// streamView is one producer replica's manifest, with its sealed segments
// read in, as chunks prints it with -o.
type streamView struct {
	Stream   string           `json:"stream"`
	Prefix   string           `json:"prefix"`
	Replica  int              `json:"replica"`
	Found    bool             `json:"found"`
	Epoch    int64            `json:"epoch,omitempty"`
	EOS      bool             `json:"eos"`
	Last     *int64           `json:"last,omitempty"`
	Observed *chunk.Observed  `json:"observed,omitempty"`
	Consumed map[string]int64 `json:"consumed,omitempty"`
	Chunks   []chunk.Chunk    `json:"chunks"`
}

func chunksCmd(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "chunks", "<run> <edge>", &o)
	var s3 s3Flags
	s3.add(fs)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usagef("chunks takes a run and an edge, or a result as <step>.<output>")
	}
	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
		return notFound(err, ns, pos[0])
	}
	streams, err := streamsOf(&r, pos[1])
	if err != nil {
		return err
	}
	st, at, err := e.openRun(ctx, c.Client, &r, s3)
	if err != nil {
		return err
	}
	var views []streamView
	for i, s := range streams {
		v, err := readStream(ctx, st, s, i)
		if err != nil {
			return at.explain(err)
		}
		views = append(views, v)
	}
	return write(e.Stdout, o.output, views, func(w io.Writer) {
		fmt.Fprintln(w, "REPLICA\tEPOCH\tCHUNKS\tBYTES\tEOS\tPREFIX")
		for _, v := range views {
			if !v.Found {
				fmt.Fprintf(w, "%d\t-\t0\t0 B\tfalse\t%s (no manifest yet)\n", v.Replica, v.Prefix)
				continue
			}
			var n int64
			for _, c := range v.Chunks {
				n += c.Bytes
			}
			fmt.Fprintf(w, "%d\t%d\t%d\t%s\t%t\t%s\n", v.Replica, v.Epoch, len(v.Chunks), bytesString(n), v.EOS, v.Prefix)
		}
		fmt.Fprintln(w, "\nREPLICA\tN\tFILE\tBYTES\tSHA256\tFROM\tCOUNTS")
		for _, v := range views {
			for _, c := range v.Chunks {
				from := "-"
				if c.Src != nil {
					from = fmt.Sprintf("%s/%d #%d", c.Src.Edge, c.Src.Replica, c.Src.N)
				}
				file := path.Base(chunk.Stream{}.ChunkKey(c.N, c.Ext))
				fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s…\t%s\t%s\n", v.Replica, c.N, file, bytesString(c.Bytes), c.SHA256[:min(12, len(c.SHA256))], from, counts(c.Counts))
			}
		}
	})
}

// streamsOf finds the streams of an edge, or of a result named
// <step>.<output> or just <step> for its default output.
func streamsOf(r *v1alpha1.StreamRun, name string) ([]chunk.Stream, error) {
	steps := map[string]*v1alpha1.Step{}
	for i := range r.Spec.Steps {
		steps[r.Spec.Steps[i].Name] = &r.Spec.Steps[i]
	}
	uid := string(r.UID)
	var names []string
	for _, e := range r.Spec.Edges {
		edge := controller.EdgeName(e)
		names = append(names, edge)
		if edge != name || steps[e.From] == nil {
			continue
		}
		var out []chunk.Stream
		for i := range controller.Replicas(steps[e.From]) {
			out = append(out, chunk.EdgeStream(uid, edge, i))
		}
		return out, nil
	}
	for _, res := range r.Status.Results {
		result := res.Step + "." + res.Output
		names = append(names, result)
		if (name != result && !(res.Output == "default" && name == res.Step)) || steps[res.Step] == nil {
			continue
		}
		var out []chunk.Stream
		for i := range controller.Replicas(steps[res.Step]) {
			out = append(out, chunk.ResultStream(r.Name, uid, res.Step, res.Output, i))
		}
		return out, nil
	}
	return nil, hint(fmt.Errorf("run %s has no edge or result %q", r.Name, name), "its edges and results are: "+strings.Join(names, ", "))
}

func readStream(ctx context.Context, st store.Store, s chunk.Stream, replica int) (streamView, error) {
	v := streamView{Stream: s.ID, Prefix: s.Prefix(), Replica: replica, Chunks: []chunk.Chunk{}}
	data, err := store.ReadAll(ctx, st, s.ManifestKey())
	if errors.Is(err, store.ErrNotFound) {
		return v, nil
	}
	if err != nil {
		return v, fmt.Errorf("%s: %w", s.ManifestKey(), err)
	}
	m, err := chunk.ParseManifest(data)
	if err != nil {
		return v, fmt.Errorf("%s: %w", s.ManifestKey(), err)
	}
	v.Found, v.Epoch, v.EOS, v.Last, v.Observed, v.Consumed = true, m.Epoch, m.EOS, m.Last, m.Observed, m.Consumed
	for i := range m.Segments {
		data, err := store.ReadAll(ctx, st, s.SegmentKey(i))
		if err != nil {
			return v, fmt.Errorf("%s: %w", s.SegmentKey(i), err)
		}
		seg, err := chunk.ParseSegment(data, i)
		if err != nil {
			return v, fmt.Errorf("%s: %w", s.SegmentKey(i), err)
		}
		v.Chunks = append(v.Chunks, seg.Chunks...)
	}
	v.Chunks = append(v.Chunks, m.Chunks...)
	return v, nil
}
