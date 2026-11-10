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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// refreshStorage fills in the status that comes from storage: edges,
// results, and each step's committed counters and log (GO-79, GO-115,
// GO-119). It is best effort; a storage error is logged and shown as the
// run's message, and the next resync tries again. It returns the error, so
// a storage that refuses the run's keys can fail the run.
func (rc *Reconciler) refreshStorage(ctx context.Context, r *v1alpha1.StreamRun, g *graph) error {
	st, err := rc.store(ctx, r)
	if err == nil {
		err = rc.readStorage(ctx, st, r, g)
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "read status from storage")
		if !terminal(r.Status.Phase) {
			r.Status.Message = "cannot read storage: " + err.Error()
		}
	} else if strings.HasPrefix(r.Status.Message, "cannot read storage: ") {
		r.Status.Message = ""
	}
	return err
}

func (rc *Reconciler) readStorage(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, g *graph) error {
	uid := string(r.UID)
	var edges []v1alpha1.EdgeStatus
	for _, from := range g.order {
		for _, e := range g.out[from] {
			es := v1alpha1.EdgeStatus{Name: edgeName(e), EOS: true}
			for p := range replicas(g.steps[e.From]) {
				stream := chunk.EdgeStream(uid, es.Name, p)
				m, err := readManifest(ctx, st, stream)
				if err != nil {
					return err
				}
				if m == nil {
					es.EOS = false
					continue
				}
				t, err := rc.streamTotals(ctx, st, stream, m)
				if err != nil {
					return err
				}
				es.Chunks += m.Count()
				es.Bytes += t.bytes
				es.EOS = es.EOS && m.EOS
				// Every consumer replica reads all of a side input, so a
				// chunk is acked when they all have (GO-167).
				least := int64(-1)
				for c := range replicas(g.steps[e.To]) {
					n, err := ackCount(ctx, st, stream.AckKey(e.To, c))
					if err != nil {
						return err
					}
					if !e.Side {
						es.Acked += n
					} else if least < 0 || n < least {
						least = n
					}
				}
				if least > 0 {
					es.Acked += least
				}
			}
			edges = append(edges, es)
		}
	}
	r.Status.Edges = edges
	if !terminal(r.Status.Phase) {
		ns, name := r.Namespace, r.Name
		afterWrite(ctx, func() {
			for _, e := range edges {
				edgeLag.WithLabelValues(ns, name, e.Name).Set(float64(e.Chunks - e.Acked))
			}
		})
	}

	var results []v1alpha1.ResultStatus
	for i := range r.Status.Steps {
		ss := &r.Status.Steps[i]
		s := g.steps[ss.Name]
		ss.Counts, ss.Errors, ss.Warnings, ss.ErrorsDropped = nil, 0, 0, 0
		for rep := range replicas(s) {
			o, err := readObservations(ctx, st, r, ss.Name, rep)
			if err != nil {
				return err
			}
			committed, err := committedObserved(ctx, st, r, g, s, rep)
			if err != nil {
				return err
			}
			if committed != nil && (o == nil || committed.Seq > o.Seq) {
				// The replica stopped after a commit and before its
				// summary, and has not run again.
				o = &agent.Observations{Counts: committed.Counts, Errors: committed.Errors,
					Warnings: committed.Warnings, ErrorsDropped: committed.ErrorsDropped}
			}
			if o == nil {
				continue
			}
			for u, n := range o.Counts {
				if ss.Counts == nil {
					ss.Counts = map[string]int64{}
				}
				ss.Counts[u] += n
			}
			ss.Errors += o.Errors
			ss.Warnings += o.Warnings
			ss.ErrorsDropped += o.ErrorsDropped
		}
		if s.FailurePolicy == v1alpha1.FailureContinue {
			for j := range ss.Replicas {
				rs := &ss.Replicas[j]
				if rs.Phase != v1alpha1.StepFailed {
					continue
				}
				n, err := failedChunks(ctx, st, r, g, s, int(rs.Index))
				if err != nil {
					return err
				}
				rs.FailedChunks = n
			}
		}
		if ss.Errors+ss.Warnings+ss.ErrorsDropped > 0 {
			ss.ErrorLog = fmt.Sprintf("s3://%s/results/%s/%s/_errors/%s/", ResultsBucket(r), r.Name, uid, ss.Name)
		}

		found, err := rc.results(ctx, st, r, s)
		if err != nil {
			return err
		}
		results = append(results, found...)
	}
	r.Status.Results = results
	return nil
}

// ResultsBucket is the bucket with everything under results/<run>/<uid>/:
// spec.storage.resultsBucket, or bucket (GO-129).
func ResultsBucket(r *v1alpha1.StreamRun) string {
	if b := r.Spec.Storage.ResultsBucket; b != "" {
		return b
	}
	return r.Spec.Storage.Bucket
}

// results lists a step's results: outputs no edge leaves from, found by
// their manifests (SPEC 5.2, GO-79).
func (rc *Reconciler) results(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, s *v1alpha1.Step) ([]v1alpha1.ResultStatus, error) {
	prefix := chunk.ResultsPrefix(r.Name, string(r.UID), s.Name)
	byOutput := map[string]*v1alpha1.ResultStatus{}
	var order []string
	for key, err := range st.List(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
		if len(parts) != 3 || parts[2] != "manifest.json" {
			continue
		}
		output := parts[0]
		replica, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		stream := chunk.ResultStream(r.Name, string(r.UID), s.Name, output, replica)
		m, err := readManifest(ctx, st, stream)
		if err != nil || m == nil {
			return nil, err
		}
		t, err := rc.streamTotals(ctx, st, stream, m)
		if err != nil {
			return nil, err
		}
		res := byOutput[output]
		if res == nil {
			res = &v1alpha1.ResultStatus{
				Step:      s.Name,
				Output:    output,
				Endpoint:  r.Spec.Storage.Endpoint,
				Bucket:    r.Spec.Storage.Bucket,
				Region:    r.Spec.Storage.Region,
				Prefix:    prefix + output + "/",
				SecretRef: r.Spec.Storage.SecretRef,
			}
			// A results bucket with its own read key: readers never get
			// the agents' key (GO-129).
			if b := r.Spec.Storage.ResultsBucket; b != "" {
				res.Bucket = b
				if ref := r.Spec.Storage.ResultsSecretRef; ref != "" {
					res.SecretRef = ref
				}
			}
			byOutput[output] = res
			order = append(order, output)
		}
		res.Chunks += m.Count()
		res.Bytes += t.bytes
		for u, n := range t.counts {
			if res.Counts == nil {
				res.Counts = map[string]int64{}
			}
			res.Counts[u] += n
		}
	}
	var out []v1alpha1.ResultStatus
	for _, o := range order {
		out = append(out, *byOutput[o])
	}
	return out, nil
}

func readManifest(ctx context.Context, st store.Store, s chunk.Stream) (*chunk.Manifest, error) {
	data, err := store.ReadAll(ctx, st, s.ManifestKey())
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return chunk.ParseManifest(data)
}

func ackCount(ctx context.Context, st store.Store, key string) (int64, error) {
	data, err := store.ReadAll(ctx, st, key)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	a, err := chunk.ParseAcks(data)
	if err != nil {
		return 0, err
	}
	return int64(len(a.Acked)), nil
}

// committedObserved returns a replica's newest committed totals: the copy
// with the highest seq in its outgoing manifests, its results and its ack
// files (GO-115).
func committedObserved(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, rep int) (*chunk.Observed, error) {
	uid := string(r.UID)
	var copies []*chunk.Observed
	for _, e := range g.out[s.Name] {
		m, err := readManifest(ctx, st, chunk.EdgeStream(uid, edgeName(e), rep))
		if err != nil {
			return nil, err
		}
		if m != nil {
			copies = append(copies, m.Observed)
		}
	}
	prefix := chunk.ResultsPrefix(r.Name, uid, s.Name)
	suffix := "/" + strconv.Itoa(rep) + "/manifest.json"
	for key, err := range st.List(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		data, err := store.ReadAll(ctx, st, key)
		if err != nil {
			return nil, err
		}
		m, err := chunk.ParseManifest(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		copies = append(copies, m.Observed)
	}
	for _, e := range g.in[s.Name] {
		for p := range replicas(g.steps[e.From]) {
			key := chunk.EdgeStream(uid, edgeName(e), p).AckKey(s.Name, rep)
			data, err := store.ReadAll(ctx, st, key)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			a, err := chunk.ParseAcks(data)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			copies = append(copies, a.Observed)
		}
	}
	return chunk.Newest(copies...), nil
}

func readObservations(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, step string, replica int) (*agent.Observations, error) {
	key := chunk.ErrorLogPrefix(r.Name, string(r.UID), step, replica) + "manifest.json"
	data, err := store.ReadAll(ctx, st, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var o agent.Observations
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return &o, nil
}

// closeStreams ends the outgoing streams of a replica that has failed for
// good, so its consumers reach EOS (GO-97). The pod is confirmed gone; the
// manifests get an epoch above the replica's last attempt, so an agent of
// that attempt that is somehow still running is fenced (GO-68). It returns
// how many input chunks assigned to the replica it never committed.
func (rc *Reconciler) closeStreams(ctx context.Context, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, replica int, epoch int64) (int64, error) {
	st, err := rc.store(ctx, r)
	if err != nil {
		return 0, err
	}
	uid := string(r.UID)
	var streams []chunk.Stream
	for _, e := range g.out[s.Name] {
		streams = append(streams, chunk.EdgeStream(uid, edgeName(e), replica))
	}
	prefix := chunk.ResultsPrefix(r.Name, uid, s.Name)
	for key, err := range st.List(ctx, prefix) {
		if err != nil {
			return 0, err
		}
		parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
		if len(parts) == 3 && parts[1] == strconv.Itoa(replica) && parts[2] == "manifest.json" {
			streams = append(streams, chunk.ResultStream(r.Name, uid, s.Name, parts[0], replica))
		}
	}

	consumed := map[string]int64{}
	first := true
	for _, stream := range streams {
		m, err := readManifest(ctx, st, stream)
		if err != nil {
			return 0, err
		}
		if m == nil {
			m = chunk.NewManifest(r.Name, uid, strings.SplitN(stream.ID, "/", 2)[0], replica, epoch)
		}
		for id := range consumed {
			if m.Consumed[id] < consumed[id] {
				consumed[id] = m.Consumed[id]
			}
		}
		if first {
			for id, n := range m.Consumed {
				consumed[id] = n
			}
			first = false
		}
		m.Epoch = max(m.Epoch, epoch)
		m.End()
		data, err := m.Marshal()
		if err != nil {
			return 0, err
		}
		if err := st.Put(ctx, stream.ManifestKey(), bytes.NewReader(data), int64(len(data))); err != nil {
			return 0, err
		}
	}

	// Close the replica's ack files, so producers stop waiting for it
	// (GO-35): they list the chunks it committed, and an epoch above its
	// last attempt fences an agent of it that still runs (GO-68).
	for _, e := range g.in[s.Name] {
		for p := range replicas(g.steps[e.From]) {
			in := chunk.EdgeStream(uid, edgeName(e), p)
			old, err := readAcks(ctx, st, in.AckKey(s.Name, replica))
			if err != nil {
				return 0, err
			}
			done := consumed[in.ID]
			if len(streams) == 0 && old != nil {
				// Without outgoing streams the ack file is the commit point
				// (GO-72).
				done = old.Last()
			}
			acks := chunk.NewAcks()
			for n := int64(1); n <= done; n++ {
				if assigned(e, n, p, replicas(s), replica) {
					if err := acks.Ack(n); err != nil {
						return 0, err
					}
				}
			}
			acks.Epoch, acks.Closed = epoch, true
			if old != nil {
				acks.Observed = old.Observed
			}
			data, err := acks.Marshal()
			if err != nil {
				return 0, err
			}
			if err := st.Put(ctx, in.AckKey(s.Name, replica), bytes.NewReader(data), int64(len(data))); err != nil {
				return 0, err
			}
		}
	}
	return failedChunks(ctx, st, r, g, s, replica)
}

// failedChunks counts the input chunks assigned to a closed replica that
// it never committed (GO-97). Producers go on after the replica is closed,
// so the count grows until they have ended.
func failedChunks(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, replica int) (int64, error) {
	var failed int64
	err := eachSkipped(ctx, st, r, g, s, replica, func(skippedChunk) bool {
		failed++
		return true
	})
	return failed, err
}

// skippedChunk is an input chunk a closed replica never committed.
type skippedChunk struct {
	edge     v1alpha1.Edge
	producer int
	stream   chunk.Stream
	chunk    chunk.Chunk
}

// eachSkipped calls yield for each input chunk assigned to a closed
// replica that it never committed, in the order of the step's inputs, of
// their producer replicas, and of the chunks, until yield returns false.
func eachSkipped(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, replica int, yield func(skippedChunk) bool) error {
	uid := string(r.UID)
	for _, e := range g.in[s.Name] {
		if e.Side {
			// A side input is lookup data, not work left undone; a replay of
			// the skipped chunks gets all of it (GO-167).
			continue
		}
		for p := range replicas(g.steps[e.From]) {
			in := chunk.EdgeStream(uid, edgeName(e), p)
			m, err := readManifest(ctx, st, in)
			if err != nil {
				return err
			}
			acks, err := readAcks(ctx, st, in.AckKey(s.Name, replica))
			if err != nil {
				return err
			}
			if m == nil || acks == nil || !acks.Closed {
				continue
			}
			chunks := m.Chunks
			if len(m.Segments) > 0 {
				if chunks, err = allChunks(ctx, st, in, m); err != nil {
					return err
				}
			}
			for _, c := range chunks {
				if chunk.Assign(c.N, p, replicas(s)) == replica && !acks.Has(c.N) {
					if !yield(skippedChunk{edge: e, producer: p, stream: in, chunk: c}) {
						return nil
					}
				}
			}
		}
	}
	return nil
}

func readAcks(ctx context.Context, st store.Store, key string) (*chunk.Acks, error) {
	data, err := store.ReadAll(ctx, st, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a, err := chunk.ParseAcks(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return a, nil
}

// Summary is results/<run>/<uid>/summary.json, written when a run is
// finished (GO-123).
type Summary struct {
	Name       string                `json:"name"`
	Namespace  string                `json:"namespace"`
	UID        string                `json:"uid"`
	Phase      v1alpha1.RunPhase     `json:"phase"`
	Message    string                `json:"message,omitempty"`
	StartedAt  *time.Time            `json:"startedAt,omitempty"`
	FinishedAt *time.Time            `json:"finishedAt,omitempty"`
	Steps      []v1alpha1.StepStatus `json:"steps"`
}

func (rc *Reconciler) writeSummary(ctx context.Context, r *v1alpha1.StreamRun) error {
	st, err := rc.store(ctx, r)
	if err != nil {
		return err
	}
	sum := Summary{Name: r.Name, Namespace: r.Namespace, UID: string(r.UID), Phase: r.Status.Phase, Message: r.Status.Message, Steps: r.Status.Steps}
	if t := r.Status.StartedAt; t != nil {
		sum.StartedAt = &t.Time
	}
	if t := r.Status.FinishedAt; t != nil {
		sum.FinishedAt = &t.Time
	}
	base := "results/" + r.Name + "/" + string(r.UID) + "/"
	if err := putJSON(ctx, st, base+"summary.json", sum); err != nil {
		return err
	}

	// run.json gets finishedAt, so results can be cleaned up after the
	// StreamRun is gone (GO-98, GO-99).
	key := chunk.RunInfoKey(r.Name, string(r.UID))
	var info agent.RunInfo
	data, err := store.ReadAll(ctx, st, key)
	switch {
	case errors.Is(err, store.ErrNotFound):
		info = agent.RunInfo{Name: r.Name, Namespace: r.Namespace, UID: string(r.UID)}
		if r.Spec.ResultsTTL != nil {
			info.ResultsTTL = r.Spec.ResultsTTL.Duration.String()
		}
	case err != nil:
		return err
	default:
		if err := json.Unmarshal(data, &info); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	info.FinishedAt = sum.FinishedAt
	return putJSON(ctx, st, key, info)
}

func putJSON(ctx context.Context, st store.Store, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return st.Put(ctx, key, bytes.NewReader(data), int64(len(data)))
}

// allChunks is every chunk of a stream: those in its sealed segments,
// then those in the manifest (GO-32).
func allChunks(ctx context.Context, st store.Store, s chunk.Stream, m *chunk.Manifest) ([]chunk.Chunk, error) {
	var out []chunk.Chunk
	for i := range m.Segments {
		data, err := store.ReadAll(ctx, st, s.SegmentKey(i))
		if err != nil {
			return nil, err
		}
		seg, err := chunk.ParseSegment(data, i)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.SegmentKey(i), err)
		}
		out = append(out, seg.Chunks...)
	}
	return append(out, m.Chunks...), nil
}
