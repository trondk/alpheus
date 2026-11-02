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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/store"
)

// inStream is one incoming stream: the chunks producer replica p writes on
// an incoming edge. This replica reads only the chunks assigned to it
// (GO-76).
type inStream struct {
	in     InputEdge
	p      int
	stream chunk.Stream
	ackKey string
	acks   *chunk.Acks // written only during a commit
	assign func(n int64) bool
	wake   chan struct{} // a signal for the stream arrived

	// Guarded by Agent.mu.
	processed int64 // the last chunk done
	fetched   int64 // the last chunk fetched or skipped
	ready     []*inChunk
	open      []*inChunk // delivered, in order
	eos       bool
	last      int64 // with eos

	// Used only by the stream's fetcher.
	m       *chunk.Manifest
	segment *chunk.Segment
	segIdx  int
}

// inChunk is an input chunk in the scratch folder.
type inChunk struct {
	id   int64
	s    *inStream
	meta chunk.Chunk
	path string
	seq  int64 // order of arrival, for inputOrder arrival
}

func newInStream(cfg Config, in InputEdge, p int) *inStream {
	stream := chunk.EdgeStream(cfg.UID, in.Edge, p)
	replica, replicas := cfg.Replica, cfg.Replicas
	return &inStream{
		in:     in,
		p:      p,
		stream: stream,
		ackKey: stream.AckKey(cfg.Step, replica),
		assign: func(n int64) bool { return chunk.Assign(n, p, replicas) == replica },
		wake:   make(chan struct{}, 1),
		segIdx: -1,
	}
}

// nextAssigned is the first chunk after n assigned to this replica.
func (s *inStream) nextAssigned(n int64) int64 {
	for n++; !s.assign(n); n++ {
	}
	return n
}

// drained reports whether the stream has nothing more to deliver. Call
// with Agent.mu held.
func (s *inStream) drained() bool {
	return s.eos && len(s.ready) == 0 && s.nextAssigned(s.fetched) > s.last
}

// done reports whether every chunk assigned from the stream is done. Call
// with Agent.mu held.
func (s *inStream) done() bool { return s.drained() && len(s.open) == 0 }

func (a *Agent) loadAcks(ctx context.Context, s *inStream) error {
	data, err := store.ReadAll(ctx, a.store, s.ackKey)
	switch {
	case errors.Is(err, store.ErrNotFound):
		s.acks = chunk.NewAcks()
		return nil
	case err != nil:
		return fmt.Errorf("ack file %s: %w", s.ackKey, err)
	}
	if s.acks, err = chunk.ParseAcks(data); err != nil {
		return err
	}
	if s.acks.Epoch > a.cfg.Attempt || s.acks.Closed {
		return fmt.Errorf("fencing: ack file %s: %w", s.ackKey, chunk.ErrNewerEpoch)
	}
	return nil
}

// ackThrough acks every assigned chunk up to n and writes the ack file if
// it changed. Call with commitMu held.
func (a *Agent) ackThrough(ctx context.Context, s *inStream, n int64) error {
	before := s.acks.Last()
	for k := s.nextAssigned(before); k <= n; k = s.nextAssigned(k) {
		if err := s.acks.Ack(k); err != nil {
			return err
		}
	}
	if s.acks.Last() == before {
		return nil
	}
	return a.writeAck(ctx, s)
}

// writeAck writes a stream's ack file with the replica's committed totals,
// which it commits for a step without outgoing manifests (GO-72), and
// signals the newest ack.
func (a *Agent) writeAck(ctx context.Context, s *inStream) error {
	data, err := store.ReadAll(ctx, a.store, s.ackKey)
	if err == nil {
		cur, err := chunk.ParseAcks(data)
		if err != nil {
			return fmt.Errorf("ack file %s: %w", s.ackKey, err)
		}
		if cur.Epoch > a.cfg.Attempt || cur.Closed {
			return fmt.Errorf("fencing: ack file %s: %w", s.ackKey, chunk.ErrNewerEpoch)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("ack file %s: %w", s.ackKey, err)
	}
	a.mu.Lock()
	s.acks.Observed = a.obs.observed()
	a.mu.Unlock()
	s.acks.Epoch = a.cfg.Attempt
	data, err = s.acks.Marshal()
	if err != nil {
		return err
	}
	if err := a.store.Put(ctx, s.ackKey, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("ack file %s: %w", s.ackKey, err)
	}
	a.publish(ctx, signal.Signal{
		Kind:            signal.Ack,
		Edge:            s.in.Edge,
		Replica:         s.p,
		N:               s.acks.Last(),
		ConsumerStep:    a.cfg.Step,
		ConsumerReplica: a.cfg.Replica,
	})
	return nil
}

// writeAcks writes every input stream's ack file again, to commit the
// totals of a step without outgoing manifests when it finishes.
func (a *Agent) writeAcks(ctx context.Context) error {
	for _, s := range a.streams {
		if err := a.writeAck(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// fetch downloads the stream's assigned chunks to the scratch folder, up
// to prefetch ahead (GO-111), until the stream has ended.
func (a *Agent) fetch(ctx context.Context, s *inStream) {
	if err := a.fetchLoop(ctx, s); err != nil && ctx.Err() == nil {
		a.fail(fmt.Errorf("input %s: %w", s.stream.ID, err))
	}
}

func (a *Agent) fetchLoop(ctx context.Context, s *inStream) error {
	poll := time.NewTicker(a.cfg.Poll.Duration)
	defer poll.Stop()
	for {
		var next int64
		err := a.waitFor(ctx, func() (bool, error) {
			next = s.nextAssigned(s.fetched)
			return len(s.ready) < a.cfg.Prefetch, nil
		})
		if err != nil {
			return err
		}

		if s.m == nil || next > s.m.Count() {
			if err := s.readManifest(ctx, a.store); err != nil {
				return err
			}
		}
		if s.m != nil && s.m.EOS && next > *s.m.Last {
			a.mu.Lock()
			s.eos, s.last = true, *s.m.Last
			a.notify()
			a.mu.Unlock()
			return nil
		}
		if s.m == nil || next > s.m.Count() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.wake:
			case <-poll.C:
			}
			continue
		}

		meta, err := s.chunkMeta(ctx, a.store, next)
		if err != nil {
			return err
		}
		if err := a.waitForScratch(ctx, meta.Bytes); err != nil {
			return err
		}
		path, err := a.download(ctx, s, meta)
		if err != nil {
			return err
		}
		a.mu.Lock()
		a.nextID++
		s.ready = append(s.ready, &inChunk{id: a.nextID, s: s, meta: meta, path: path, seq: a.nextID})
		s.fetched = next
		a.scratch += meta.Bytes
		a.notify()
		a.mu.Unlock()
	}
}

// waitForScratch waits until a chunk of size bytes fits in the scratch
// folder (GO-106). A chunk that cannot fit even in an empty folder fails.
func (a *Agent) waitForScratch(ctx context.Context, size int64) error {
	limit := a.cfg.ScratchLimit
	if limit == 0 {
		return nil
	}
	if size > limit*9/10 {
		return fmt.Errorf("chunk of %d bytes %w of %d bytes", size, ErrScratchFull, limit)
	}
	return a.waitFor(ctx, func() (bool, error) { return a.scratch+size <= limit*9/10, nil })
}

func (a *Agent) download(ctx context.Context, s *inStream, meta chunk.Chunk) (string, error) {
	r, err := a.store.Get(ctx, s.stream.ChunkKey(meta.N, meta.Ext))
	if err != nil {
		return "", err
	}
	defer r.Close()
	path := filepath.Join(a.inDir(), fmt.Sprintf("%s-%d-%06d.%s", s.in.Edge, s.p, meta.N, meta.Ext))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(f, chunk.NewVerifyingReader(r, meta))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return "", err
	}
	a.metrics.moved("in", meta.Bytes)
	return path, nil
}

// readManifest reads the stream's manifest. A stream whose producer has
// not committed anything yet has none.
func (s *inStream) readManifest(ctx context.Context, st store.Store) error {
	data, err := store.ReadAll(ctx, st, s.stream.ManifestKey())
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	m, err := chunk.ParseManifest(data)
	if err != nil {
		return err
	}
	s.m = m
	return nil
}

// chunkMeta returns the manifest entry of chunk n, from a sealed segment
// if it is in one.
func (s *inStream) chunkMeta(ctx context.Context, st store.Store, n int64) (chunk.Chunk, error) {
	i := int((n - 1) / chunk.SegmentSize)
	if i >= len(s.m.Segments) {
		return s.m.Chunks[n-1-int64(len(s.m.Segments))*chunk.SegmentSize], nil
	}
	if s.segIdx != i {
		data, err := store.ReadAll(ctx, st, s.stream.SegmentKey(i))
		if err != nil {
			return chunk.Chunk{}, err
		}
		seg, err := chunk.ParseSegment(data, i)
		if err != nil {
			return chunk.Chunk{}, err
		}
		s.segment, s.segIdx = seg, i
	}
	return s.segment.Chunks[(n-1)%chunk.SegmentSize], nil
}

// pick takes the next chunk to deliver from the streams of a port, or of
// every port when port is empty, and opens it. It returns nil and false
// while the component must wait, and eos when nothing is left. Call with
// mu held.
func (a *Agent) pick(port string) (c *inChunk, eos bool) {
	var streams []*inStream
	for _, s := range a.streams {
		if port == "" || s.in.Port == port {
			streams = append(streams, s)
		}
	}
	if len(a.inputs) >= a.cfg.MaxOpen {
		return nil, false
	}
	var from *inStream
	if a.cfg.InputOrder == OrderDeterministic {
		// One chunk from each stream in turn, in config order (sorted by
		// port, step and replica), skipping streams that have ended.
		pos := a.rr[port]
		for i := range streams {
			s := streams[(pos+i)%len(streams)]
			if len(s.ready) > 0 {
				from = s
				a.rr[port] = (pos + i + 1) % len(streams)
				break
			}
			if !s.drained() {
				return nil, false
			}
		}
	} else {
		for _, s := range streams {
			if len(s.ready) > 0 && (from == nil || s.ready[0].seq < from.ready[0].seq) {
				from = s
			}
		}
	}
	if from == nil {
		for _, s := range streams {
			if !s.drained() {
				return nil, false
			}
		}
		return nil, true
	}
	c = from.ready[0]
	from.ready = from.ready[1:]
	from.open = append(from.open, c)
	a.inputs[c.id] = c
	a.notify()
	return c, false
}

// wakeStream wakes the fetcher of the stream a signal is about.
func (a *Agent) wakeStream(sig signal.Signal) {
	for _, s := range a.streams {
		if s.in.Edge == sig.Edge && s.p == sig.Replica {
			select {
			case s.wake <- struct{}{}:
			default:
			}
		}
	}
}
