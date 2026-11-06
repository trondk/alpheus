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

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/store"
)

// target is a stream this replica writes: on an outgoing edge, or for a
// result. Its manifest is guarded by commitMu.
type target struct {
	output   string
	edge     string // empty for a result
	to       string
	replicas int
	side     bool // the edge is a side input of its consumer (GO-167)
	stream   chunk.Stream
	m        *chunk.Manifest

	// sources has, per incoming stream, the highest input chunk with
	// output in this stream. Only a restarted stateful step uses it, to
	// discard output that is already committed (GO-95).
	sources map[string]int64
	// skip is how many chunks an earlier attempt of a source committed;
	// the first skip chunks the source closes are compared and discarded
	// (GO-92). closed counts them.
	skip, closed int64

	// Guarded by Agent.mu, for backpressure.
	committed int64
	acks      map[int]ackState
}

type ackState struct {
	last  int64
	read  time.Time
	stale bool // an ack signal came after the read
}

// outFile is an output chunk from Create until it is committed.
type outFile struct {
	id     int64
	output string
	ext    string
	input  int64 // 0 for none
	path   string
	closed bool
	seq    int64 // order of Close
	size   int64
	sha256 string
}

var partPattern = regexp.MustCompile(`/part-(\d{6,})\.[a-z0-9]{1,16}$`)

// loadTarget reads the target's manifest from an earlier attempt, or
// starts a new one, and deletes chunk objects the manifest does not list
// (SPEC 7.3 step 4).
func (a *Agent) loadTarget(ctx context.Context, t *target) error {
	data, err := store.ReadAll(ctx, a.store, t.stream.ManifestKey())
	switch {
	case errors.Is(err, store.ErrNotFound):
		name := t.edge
		if name == "" {
			name = a.cfg.Step + "." + t.output
		}
		t.m = chunk.NewManifest(a.cfg.Run, a.cfg.UID, name, a.cfg.Replica, a.cfg.Attempt)
	case err != nil:
		return fmt.Errorf("manifest %s: %w", t.stream.ID, err)
	default:
		if t.m, err = chunk.ParseManifest(data); err != nil {
			return fmt.Errorf("manifest %s: %w", t.stream.ID, err)
		}
		switch err := t.m.CheckEpoch(a.cfg.Attempt); {
		case errors.Is(err, chunk.ErrNewerEpoch):
			return fmt.Errorf("fencing: manifest %s: %w", t.stream.ID, err)
		case err != nil:
			// Claim the stream, so an agent of the older attempt that is
			// somehow still running stops at its next write (GO-68).
			t.m.Epoch = a.cfg.Attempt
			if err := a.putJSON(ctx, t.stream.ManifestKey(), t.m); err != nil {
				return err
			}
		}
	}
	t.committed = t.m.Count()
	if a.source() {
		t.skip = t.m.Count()
	}
	for key, err := range a.store.List(ctx, t.stream.Prefix()) {
		if err != nil {
			return fmt.Errorf("list %s: %w", t.stream.Prefix(), err)
		}
		match := partPattern.FindStringSubmatch(key)
		if match == nil {
			continue
		}
		if n, _ := strconv.ParseInt(match[1], 10, 64); n > t.m.Count() {
			if err := a.store.Delete(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadResults finds the results an earlier attempt started.
func (a *Agent) loadResults(ctx context.Context) error {
	prefix := chunk.ResultsPrefix(a.cfg.Run, a.cfg.UID, a.cfg.Step)
	suffix := "/" + strconv.Itoa(a.cfg.Replica) + "/manifest.json"
	for key, err := range a.store.List(ctx, prefix) {
		if err != nil {
			return err
		}
		output, ok := strings.CutSuffix(strings.TrimPrefix(key, prefix), suffix)
		if !ok || strings.Contains(output, "/") || a.hasEdges(output) {
			continue
		}
		t := a.resultTarget(output)
		if err := a.loadTarget(ctx, t); err != nil {
			return err
		}
		a.results[output] = t
		a.targets = append(a.targets, t)
	}
	return nil
}

func (a *Agent) hasEdges(output string) bool {
	return slices.ContainsFunc(a.cfg.Outputs, func(o OutputEdge) bool { return o.Output == output })
}

func (a *Agent) resultTarget(output string) *target {
	return &target{
		output: output,
		stream: chunk.ResultStream(a.cfg.Run, a.cfg.UID, a.cfg.Step, output, a.cfg.Replica),
	}
}

// loadSources reads which input chunks already have output in the target.
func (t *target) loadSources(ctx context.Context, st store.Store) error {
	t.sources = map[string]int64{}
	add := func(chunks []chunk.Chunk) {
		for _, c := range chunks {
			if c.Src == nil {
				continue
			}
			id := c.Src.Edge + "/" + strconv.Itoa(c.Src.Replica)
			t.sources[id] = max(t.sources[id], c.Src.N)
		}
	}
	for i := range t.m.Segments {
		data, err := store.ReadAll(ctx, st, t.stream.SegmentKey(i))
		if err != nil {
			return err
		}
		seg, err := chunk.ParseSegment(data, i)
		if err != nil {
			return err
		}
		add(seg.Chunks)
	}
	add(t.m.Chunks)
	return nil
}

// targetsFor returns the streams an output's chunks go to: its edges, or
// a result when no edge leaves from it. A result's manifest starts with
// consumed set to what is already committed elsewhere, and run.json is
// written before its first chunk (GO-98). Call with commitMu held.
func (a *Agent) targetsFor(ctx context.Context, output string) ([]*target, error) {
	var ts []*target
	for _, t := range a.targets {
		if t.output == output {
			ts = append(ts, t)
		}
	}
	if len(ts) > 0 {
		return ts, nil
	}
	if err := a.writeRunInfo(ctx); err != nil {
		return nil, err
	}
	t := a.resultTarget(output)
	if err := a.loadTarget(ctx, t); err != nil {
		return nil, err
	}
	a.mu.Lock()
	stateful := a.stateful
	for _, s := range a.streams {
		if !stateful && !s.in.Side && s.processed > 0 {
			t.m.SetConsumed(s.stream.ID, s.processed)
		}
	}
	a.mu.Unlock()
	a.results[output] = t
	a.targets = append(a.targets, t)
	return []*target{t}, nil
}

// RunInfo is results/<run>/<uid>/run.json (GO-98). The controller sets
// FinishedAt when the run is finished.
type RunInfo struct {
	Name       string     `json:"name"`
	Namespace  string     `json:"namespace"`
	UID        string     `json:"uid"`
	FinishedAt *time.Time `json:"finishedAt"`
	ResultsTTL string     `json:"resultsTTL,omitempty"`
}

func (a *Agent) writeRunInfo(ctx context.Context) error {
	key := chunk.RunInfoKey(a.cfg.Run, a.cfg.UID)
	if _, err := store.ReadAll(ctx, a.store, key); err == nil {
		return nil
	}
	data, err := json.Marshal(RunInfo{Name: a.cfg.Run, Namespace: a.cfg.Namespace, UID: a.cfg.UID, ResultsTTL: a.cfg.ResultsTTL})
	if err != nil {
		return err
	}
	return a.store.Put(ctx, key, bytes.NewReader(data), int64(len(data)))
}

// commit is the commit point (SPEC 7.4): it uploads closed output files to
// their streams, writes each changed manifest with the new chunks and, for
// an input chunk, consumed; then the counts and log entries. src is the
// input chunk the files came from, nil for a source or EOS. With final the
// streams also end (GO-80, GO-95). Call with commitMu held.
func (a *Agent) commit(ctx context.Context, src *inChunk, files []*outFile, r *report, final bool) error {
	a.mu.Lock()
	stateful := a.stateful
	a.mu.Unlock()
	slices.SortFunc(files, func(x, y *outFile) int { return int(x.seq - y.seq) })
	// A side input chunk is committed only with the end of the streams, as
	// every attempt reads it again (GO-167). So it can have no output of
	// its own, which a new attempt would write twice, and its counts and
	// log entries are held until then, as a stateful step's are.
	side := src != nil && src.s.in.Side
	if side && len(files) > 0 {
		return fmt.Errorf("side input chunk %s/%d #%d has %d output files", src.s.in.Edge, src.s.p, src.meta.N, len(files))
	}

	changed := map[*target]bool{}
	// committed says an earlier attempt already committed this commit in
	// at least one outgoing manifest, and with it the totals including its
	// report: the agent continues from the newest totals, so adding the
	// report again would count it twice (GO-115). A step with several
	// outgoing manifests writes them one at a time, and can stop between.
	committed := false
	var srcRef *chunk.Source
	if src != nil {
		srcRef = &chunk.Source{Edge: src.s.in.Edge, Replica: src.s.p, N: src.meta.N}
	}
	for _, t := range a.targets {
		if src != nil && !stateful && !side && t.m.IsConsumed(src.s.stream.ID, src.meta.N) || final && t.m.EOS {
			committed = true
		}
	}
	for i, f := range files {
		ts, err := a.targetsFor(ctx, f.output)
		if err != nil {
			return err
		}
		for _, t := range ts {
			c := chunk.Chunk{Ext: f.ext, Bytes: f.size, SHA256: f.sha256, Src: srcRef}
			if a.source() && i == len(files)-1 && r != nil && len(r.counts) > 0 {
				c.Counts = r.counts
			}
			keep, err := a.keep(ctx, t, src, c)
			if err != nil {
				return err
			}
			if !keep {
				// A source's chunk an earlier attempt committed (GO-92).
				committed = committed || a.source()
				continue
			}
			c.N = t.m.NextN()
			if err := a.upload(ctx, t.stream.ChunkKey(c.N, c.Ext), f); err != nil {
				return err
			}
			seg, err := t.m.Append(c)
			if err != nil {
				return fmt.Errorf("manifest %s: %w", t.stream.ID, err)
			}
			if seg != nil {
				if err := a.putJSON(ctx, t.stream.SegmentKey(len(t.m.Segments)-1), seg); err != nil {
					return err
				}
			}
			changed[t] = true
		}
	}

	for _, t := range a.targets {
		if src != nil && !stateful && !side && !t.m.IsConsumed(src.s.stream.ID, src.meta.N) && !t.m.EOS {
			t.m.SetConsumed(src.s.stream.ID, src.meta.N)
			changed[t] = true
		}
		if final && !t.m.EOS {
			a.mu.Lock()
			for _, s := range a.streams {
				if stateful || s.in.Side {
					t.m.SetConsumed(s.stream.ID, s.last)
				}
			}
			a.mu.Unlock()
			t.m.End()
			changed[t] = true
		}
	}

	chunkName := ""
	if src != nil {
		chunkName = fmt.Sprintf("%s/%s/%d", src.s.in.Port, src.s.in.From, src.meta.N)
	}
	switch {
	case committed:
		r = nil
		if final {
			a.mu.Lock()
			a.obs.held = &report{counts: map[string]int64{}}
			a.mu.Unlock()
		}
	case (stateful || side) && !final:
		// Committed with the OnEOS output or the end of the streams, since
		// a restart before that reads every chunk again (GO-95, GO-167).
		a.mu.Lock()
		a.obs.held.merge(r, chunkName)
		a.mu.Unlock()
		r = nil
	case final:
		a.mu.Lock()
		held := a.obs.held
		a.obs.held = &report{counts: map[string]int64{}}
		a.mu.Unlock()
		held.merge(r, "")
		r = held
	}
	// The totals go in the same write as consumed (GO-115, GO-118): in
	// every outgoing manifest, so the newest copy is committed with the
	// chunk. A step without outgoing manifests commits them in its ack
	// files instead (GO-72).
	observedChanged, err := a.prepareObservations(ctx, r, chunkName)
	if err != nil {
		return err
	}
	a.mu.Lock()
	observed := a.obs.observed()
	a.mu.Unlock()
	for _, t := range a.targets {
		if observedChanged {
			changed[t] = true
		}
		if !changed[t] {
			continue
		}
		t.m.Observed = observed
		if err := a.writeManifest(ctx, t); err != nil {
			return err
		}
		a.mu.Lock()
		t.committed = t.m.Count()
		a.mu.Unlock()
		if t.edge == "" {
			continue
		}
		kind := signal.Ready
		if t.m.EOS {
			kind = signal.EOS
		}
		a.publish(ctx, signal.Signal{Kind: kind, Edge: t.edge, Replica: a.cfg.Replica, N: t.m.Count()})
	}
	if len(a.targets) == 0 && final && observedChanged {
		return a.writeAcks(ctx)
	}
	return nil
}

// keep decides whether a chunk goes into a target, or is discarded because
// an earlier attempt already committed it (GO-70, GO-92, GO-95).
func (a *Agent) keep(ctx context.Context, t *target, src *inChunk, c chunk.Chunk) (bool, error) {
	if a.source() && src == nil {
		t.closed++
		if t.closed <= t.skip {
			return false, a.compare(ctx, t, c)
		}
		if t.m.EOS {
			return false, fmt.Errorf("%w: chunk %d of %s, after the earlier attempt's last", ErrNotDeterministic, t.closed, t.stream.ID)
		}
		return true, nil
	}
	if t.m.EOS {
		return false, nil
	}
	if src != nil {
		id := src.s.stream.ID
		return !t.m.IsConsumed(id, src.meta.N) && t.sources[id] < src.meta.N, nil
	}
	return true, nil
}

// compare checks a restarted source's chunk against the one the earlier
// attempt committed with the same number (GO-92).
func (a *Agent) compare(ctx context.Context, t *target, c chunk.Chunk) error {
	old, err := t.chunkMeta(ctx, a.store, t.closed)
	if err != nil {
		return err
	}
	if old.SHA256 != c.SHA256 || old.Bytes != c.Bytes {
		return fmt.Errorf("%w: chunk %d of %s differs from the earlier attempt's", ErrNotDeterministic, t.closed, t.stream.ID)
	}
	return nil
}

func (t *target) chunkMeta(ctx context.Context, st store.Store, n int64) (chunk.Chunk, error) {
	i := int((n - 1) / chunk.SegmentSize)
	if i >= len(t.m.Segments) {
		return t.m.Chunks[n-1-int64(len(t.m.Segments))*chunk.SegmentSize], nil
	}
	data, err := store.ReadAll(ctx, st, t.stream.SegmentKey(i))
	if err != nil {
		return chunk.Chunk{}, err
	}
	seg, err := chunk.ParseSegment(data, i)
	if err != nil {
		return chunk.Chunk{}, err
	}
	return seg.Chunks[(n-1)%chunk.SegmentSize], nil
}

func (a *Agent) upload(ctx context.Context, key string, f *outFile) error {
	r, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := a.store.Put(ctx, key, r, f.size); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	a.metrics.moved("out", f.size)
	return nil
}

// writeManifest writes a target's manifest, after checking that no newer
// attempt has taken the stream over (GO-68).
func (a *Agent) writeManifest(ctx context.Context, t *target) error {
	data, err := store.ReadAll(ctx, a.store, t.stream.ManifestKey())
	if err == nil {
		cur, err := chunk.ParseManifest(data)
		if err != nil {
			return fmt.Errorf("manifest %s: %w", t.stream.ID, err)
		}
		if err := cur.CheckEpoch(a.cfg.Attempt); errors.Is(err, chunk.ErrNewerEpoch) {
			return fmt.Errorf("fencing: manifest %s: %w", t.stream.ID, err)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("manifest %s: %w", t.stream.ID, err)
	}
	return a.putJSON(ctx, t.stream.ManifestKey(), t.m)
}

func (a *Agent) putJSON(ctx context.Context, key string, v interface{ Marshal() ([]byte, error) }) error {
	data, err := v.Marshal()
	if err != nil {
		return err
	}
	if err := a.store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("write %s: %w", key, err)
	}
	return nil
}

// closeFile hashes a written output file and checks its size (GO-105).
func (a *Agent) closeFile(f *outFile) error {
	r, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	defer r.Close()
	hw := chunk.NewHashingWriter(io.Discard)
	if _, err := io.Copy(hw, r); err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	if a.cfg.MaxBytes > 0 && hw.Size() > a.cfg.MaxBytes {
		return fmt.Errorf("output %s: chunk is %d bytes, more than maxBytes %d", f.output, hw.Size(), a.cfg.MaxBytes)
	}
	a.mu.Lock()
	a.closeSeq++
	f.closed, f.seq, f.size, f.sha256 = true, a.closeSeq, hw.Size(), hw.Sum()
	a.scratch += f.size
	a.notify()
	a.mu.Unlock()
	return nil
}

// removeFiles deletes committed or discarded files from the scratch folder
// (GO-34). Call with mu held.
func (a *Agent) removeFiles(files []*outFile) {
	for _, f := range files {
		os.Remove(f.path)
		delete(a.files, f.id)
		a.scratch -= f.size
	}
}

// waitForConsumers blocks while the next chunk of an output would go to a
// consumer replica that already has maxUnacked unacked chunks from this
// stream (GO-35). The chunk's number counts the output's files not yet
// committed, but only committed chunks count as unacked: the consumer
// cannot see the others, let alone ack them, so counting them would
// deadlock a step that makes maxUnacked or more chunks for one consumer
// replica from one input chunk.
func (a *Agent) waitForConsumers(ctx context.Context, output string) error {
	if a.cfg.MaxUnacked == 0 {
		return nil
	}
	var ts []*target
	for _, t := range a.edges {
		// A side input is small and read whole, so its consumer does not
		// hold it back (GO-167).
		if t.output == output && !t.side {
			ts = append(ts, t)
		}
	}
	var since time.Time // when the output started to wait
	for {
		blocked := false
		for _, t := range ts {
			a.mu.Lock()
			pending := int64(0)
			for _, f := range a.files {
				if f.output == output {
					pending++
				}
			}
			committed := t.committed
			next := committed + pending + 1
			a.mu.Unlock()
			c := chunk.Assign(next, a.cfg.Replica, t.replicas)
			last, err := a.ackedBy(ctx, t, c)
			if err != nil {
				return err
			}
			if last == closedReplica {
				continue
			}
			unacked := 0
			for n := last + 1; n <= committed; n++ {
				if chunk.Assign(n, a.cfg.Replica, t.replicas) == c {
					unacked++
				}
			}
			if unacked >= a.cfg.MaxUnacked {
				blocked = true
				break
			}
		}
		if !blocked {
			if !since.IsZero() {
				a.metrics.waited(time.Since(since).Seconds())
			}
			return nil
		}
		if since.IsZero() {
			since = time.Now()
		}
		a.mu.Lock()
		ch := a.changed
		err := a.err
		a.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-time.After(a.cfg.Poll.Duration):
		}
	}
}

// closedReplica is what ackedBy returns for a consumer replica that
// failed for good and was closed: it never acks, and nothing waits for it
// (GO-97).
const closedReplica = math.MaxInt64

// ackedBy is the last chunk of a target's stream that consumer replica c
// has acked, read again when the cached value is old or a signal said it
// changed.
func (a *Agent) ackedBy(ctx context.Context, t *target, c int) (int64, error) {
	a.mu.Lock()
	st, ok := t.acks[c]
	a.mu.Unlock()
	if ok && !st.stale && time.Since(st.read) < a.cfg.Poll.Duration {
		return st.last, nil
	}
	data, err := store.ReadAll(ctx, a.store, t.stream.AckKey(t.to, c))
	last := int64(0)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return 0, err
	default:
		acks, err := chunk.ParseAcks(data)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", path.Base(t.stream.AckKey(t.to, c)), err)
		}
		last = acks.Last()
		if acks.Closed {
			last = closedReplica
		}
	}
	a.mu.Lock()
	if t.acks == nil {
		t.acks = map[int]ackState{}
	}
	t.acks[c] = ackState{last: last, read: time.Now()}
	a.mu.Unlock()
	return last, nil
}

// ackSignal marks a consumer replica's acks as changed and wakes producers
// waiting for it.
func (a *Agent) ackSignal(sig signal.Signal) {
	if sig.Replica != a.cfg.Replica {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range a.edges {
		if t.edge == sig.Edge && t.to == sig.ConsumerStep {
			if st, ok := t.acks[sig.ConsumerReplica]; ok {
				st.stale = true
				t.acks[sig.ConsumerReplica] = st
			}
		}
	}
	a.notify()
}

// publish sends a signal about this run, if there is a driver.
func (a *Agent) publish(ctx context.Context, sig signal.Signal) {
	if a.sig == nil {
		return
	}
	sig.Run = signal.RunKey(a.cfg.Run, a.cfg.UID)
	sig.Epoch = a.cfg.Attempt
	a.sig.Publish(ctx, sig)
}

// subscribe listens for signals on the step's incoming and outgoing edges.
func (a *Agent) subscribe(ctx context.Context) {
	if a.sig == nil {
		return
	}
	run := signal.RunKey(a.cfg.Run, a.cfg.UID)
	listen := func(edge string, handle func(signal.Signal)) {
		ch, err := a.sig.Subscribe(ctx, run, edge)
		if err != nil {
			a.log.Warn("signal subscription failed; polling", "edge", edge, "error", err.Error())
			return
		}
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sig := <-ch:
					handle(sig)
				}
			}
		}()
	}
	for _, in := range a.cfg.Inputs {
		listen(in.Edge, func(sig signal.Signal) {
			if sig.Kind == signal.Ready || sig.Kind == signal.EOS {
				a.wakeStream(sig)
			}
		})
	}
	for _, out := range a.cfg.Outputs {
		listen(out.Edge, func(sig signal.Signal) {
			if sig.Kind == signal.Ack {
				a.ackSignal(sig)
			}
		})
	}
}
