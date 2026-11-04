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
	"maps"
	"regexp"
	"strconv"
	"time"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/pb/componentpb"
	"alpheus-dev.org/alpheus/internal/store"
)

// Limits of the error log (GO-120).
const (
	maxLogEntryBytes = 4096
	maxLogEntries    = chunk.MaxLogEntries
)

// ObservationsVersion is the format version of the error log's manifest.
const ObservationsVersion = 1

// Observations is a summary of a replica's committed counters and error
// log, written to manifest.json under chunk.ErrorLogPrefix. Segments are
// JSON Lines files of log entries, oldest first. The totals are committed
// as chunk.Observed in the outgoing manifests or ack files; this summary is
// written after them.
type Observations struct {
	Version int    `json:"version"`
	Run     string `json:"run"`
	UID     string `json:"uid"`
	Step    string `json:"step"`
	Replica int    `json:"replica"`
	Epoch   int64  `json:"epoch"`
	// Seq is the seq of the committed totals this summary shows.
	Seq           int64            `json:"seq"`
	Segments      []string         `json:"segments"`
	Counts        map[string]int64 `json:"counts"`
	Errors        int64            `json:"errors"`
	Warnings      int64            `json:"warnings"`
	ErrorsDropped int64            `json:"errorsDropped"`
}

// logEntry is one line of the error log (GO-117). It has the same fields as
// the SDK's local mode writes.
type logEntry struct {
	Time    time.Time       `json:"time"`
	Level   string          `json:"level"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Fields  json.RawMessage `json:"fields,omitempty"`
	Step    string          `json:"step"`
	Replica int             `json:"replica"`
	Chunk   string          `json:"chunk,omitempty"`
}

// report is what the component reported for one input chunk, or for no
// input chunk (a source, or EOS), and not yet committed.
type report struct {
	counts  map[string]int64
	entries []*componentpb.LogEntry
	// chunks names the input chunk of each entry, for a report held over
	// several chunks; nil means the chunk the report is committed with.
	chunks []string
}

// merge adds r, reported for the input chunk chunkName, to h.
func (h *report) merge(r *report, chunkName string) {
	if r == nil {
		return
	}
	for u, n := range r.counts {
		h.counts[u] += n
	}
	for _, e := range r.entries {
		h.entries = append(h.entries, e)
		h.chunks = append(h.chunks, chunkName)
	}
}

func (r *report) empty() bool { return r == nil || (len(r.counts) == 0 && len(r.entries) == 0) }

// observer holds reports until they are committed. Guarded by Agent.mu.
type observer struct {
	units   map[string]bool
	pending map[int64]*report
	seq     int64 // last Observe message received
	err     error // the first invalid report; fails the next commit
	state   Observations
	// dirty says the committed totals are newer than the summary.
	dirty bool
	// held is what a stateful step reported for chunks it is done with.
	// It is committed with the OnEOS output, since a restart before that
	// reads every chunk again (GO-95, GO-115, GO-118).
	held *report
}

func newObserver(cfg Config) *observer {
	return &observer{
		units:   map[string]bool{},
		pending: map[int64]*report{},
		held:    &report{counts: map[string]int64{}},
		state: Observations{
			Version:  ObservationsVersion,
			Run:      cfg.Run,
			UID:      cfg.UID,
			Step:     cfg.Step,
			Replica:  cfg.Replica,
			Epoch:    cfg.Attempt,
			Segments: []string{},
			Counts:   map[string]int64{},
		},
	}
}

func (o *observer) add(msg *componentpb.ObserveRequest) {
	if msg.Seq > o.seq {
		o.seq = msg.Seq
	}
	r := o.pending[msg.Input]
	if r == nil {
		r = &report{counts: map[string]int64{}}
		o.pending[msg.Input] = r
	}
	switch m := msg.Report.(type) {
	case *componentpb.ObserveRequest_Count:
		unit, n := m.Count.Unit, m.Count.N
		switch {
		case !chunk.ValidUnit(unit):
			o.fail(fmt.Errorf("counter unit %q: want lower-case letters, digits and _, at most 32", unit))
		case n < 0:
			o.fail(fmt.Errorf("counter %q: negative count %d", unit, n))
		case !o.units[unit] && len(o.units) >= chunk.MaxUnits:
			o.fail(fmt.Errorf("counter %q: a step can have at most %d units", unit, chunk.MaxUnits))
		default:
			o.units[unit] = true
			r.counts[unit] += n
		}
	case *componentpb.ObserveRequest_Log:
		r.entries = append(r.entries, m.Log)
	}
}

func (o *observer) fail(err error) {
	if o.err == nil {
		o.err = err
	}
}

// take removes and returns what was reported for an input chunk.
func (o *observer) take(input int64) (*report, error) {
	if o.err != nil {
		return nil, o.err
	}
	r := o.pending[input]
	delete(o.pending, input)
	return r, nil
}

// restore continues from the committed state of an earlier attempt.
func (o *observer) restore(s Observations) {
	o.state = s
	for u := range s.Counts {
		o.units[u] = true
	}
}

// prepareObservations adds a report to the replica's totals before the
// commit that carries them (GO-115, GO-118): it writes the report's log
// entries as a new segment, and returns whether the totals changed. The
// caller then commits the totals, with observed(), in the same write as
// consumed, and writes the summary with writeObservations. chunkName names
// the input chunk in the entries, empty for none.
func (a *Agent) prepareObservations(ctx context.Context, r *report, chunkName string) (bool, error) {
	if r.empty() {
		return false, nil
	}
	a.mu.Lock()
	state := a.obs.state
	state.Counts = maps.Clone(state.Counts)
	state.Segments = append([]string(nil), state.Segments...)
	a.mu.Unlock()

	for u, n := range r.counts {
		state.Counts[u] += n
	}
	var buf bytes.Buffer
	for i, e := range r.entries {
		name := chunkName
		if r.chunks != nil {
			name = r.chunks[i]
		}
		line, err := json.Marshal(logEntry{
			Time:    time.Unix(0, e.Time).UTC(),
			Level:   e.Level,
			Code:    e.Code,
			Message: e.Message,
			Fields:  validFields(e.Fields),
			Step:    a.cfg.Step,
			Replica: a.cfg.Replica,
			Chunk:   name,
		})
		if err != nil || len(line) > maxLogEntryBytes || state.Errors+state.Warnings >= maxLogEntries {
			state.ErrorsDropped++
			continue
		}
		if e.Level == "error" {
			state.Errors++
		} else {
			state.Warnings++
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	if buf.Len() > 0 {
		name := segmentName(len(state.Segments) + 1)
		key := chunk.ErrorLogPrefix(a.cfg.Run, a.cfg.UID, a.cfg.Step, a.cfg.Replica) + name
		if err := a.store.Put(ctx, key, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			return false, fmt.Errorf("error log: %w", err)
		}
		state.Segments = append(state.Segments, name)
	}
	state.Seq++
	a.mu.Lock()
	a.obs.state, a.obs.dirty = state, true
	a.mu.Unlock()
	return true, nil
}

func segmentName(i int) string { return fmt.Sprintf("log-%06d.jsonl", i) }

// observed returns the totals to commit. Guarded by Agent.mu.
func (o *observer) observed() *chunk.Observed {
	s := o.state
	return &chunk.Observed{
		Seq:           s.Seq,
		Counts:        maps.Clone(s.Counts),
		Errors:        s.Errors,
		Warnings:      s.Warnings,
		ErrorsDropped: s.ErrorsDropped,
		Segments:      len(s.Segments),
	}
}

// writeObservations writes the summary of the committed totals under
// chunk.ErrorLogPrefix, for the controller and for people. It is only a
// copy: after a crash before it, the next attempt writes it again from the
// committed totals.
func (a *Agent) writeObservations(ctx context.Context) error {
	a.mu.Lock()
	if !a.obs.dirty {
		a.mu.Unlock()
		return nil
	}
	data, err := json.Marshal(a.obs.state)
	a.obs.dirty = false
	a.mu.Unlock()
	if err != nil {
		return err
	}
	key := chunk.ErrorLogPrefix(a.cfg.Run, a.cfg.UID, a.cfg.Step, a.cfg.Replica) + "manifest.json"
	if err := a.store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("error log: %w", err)
	}
	return nil
}

var logPattern = regexp.MustCompile(`/log-(\d+)\.jsonl$`)

// loadObservations continues from the totals an earlier attempt committed:
// the copy with the highest seq in the outgoing manifests and ack files. It
// deletes log segments written after them, and writes the summary again
// when it is behind.
func (a *Agent) loadObservations(ctx context.Context) error {
	prefix := chunk.ErrorLogPrefix(a.cfg.Run, a.cfg.UID, a.cfg.Step, a.cfg.Replica)
	var summary *Observations
	data, err := store.ReadAll(ctx, a.store, prefix+"manifest.json")
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return fmt.Errorf("error log: %w", err)
	default:
		var s Observations
		if err := json.Unmarshal(data, &s); err != nil {
			return fmt.Errorf("error log manifest: %w", err)
		}
		if s.Version != ObservationsVersion {
			return fmt.Errorf("error log manifest: %w: version %d", chunk.ErrVersion, s.Version)
		}
		if s.Epoch > a.cfg.Attempt {
			return fmt.Errorf("error log manifest: %w", chunk.ErrNewerEpoch)
		}
		summary = &s
	}

	var copies []*chunk.Observed
	for _, t := range a.targets {
		copies = append(copies, t.m.Observed)
	}
	for _, s := range a.streams {
		copies = append(copies, s.acks.Observed)
	}
	committed := chunk.Newest(copies...)

	a.mu.Lock()
	state := a.obs.state
	a.mu.Unlock()
	if committed != nil {
		state.Seq = committed.Seq
		state.Counts = maps.Clone(committed.Counts)
		if state.Counts == nil {
			state.Counts = map[string]int64{}
		}
		state.Errors, state.Warnings, state.ErrorsDropped = committed.Errors, committed.Warnings, committed.ErrorsDropped
		state.Segments = []string{}
		for i := range committed.Segments {
			state.Segments = append(state.Segments, segmentName(i+1))
		}
	}
	for key, err := range a.store.List(ctx, prefix) {
		if err != nil {
			return fmt.Errorf("error log: %w", err)
		}
		if m := logPattern.FindStringSubmatch(key); m != nil {
			if i, _ := strconv.Atoi(m[1]); i > len(state.Segments) {
				if err := a.store.Delete(ctx, key); err != nil {
					return fmt.Errorf("error log: %w", err)
				}
			}
		}
	}
	a.mu.Lock()
	a.obs.restore(state)
	a.obs.dirty = !(summary == nil && committed == nil || summary != nil && summary.Seq == state.Seq)
	a.mu.Unlock()
	return a.writeObservations(ctx)
}

// validFields keeps fields only when they are a JSON object.
func validFields(data []byte) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return nil
	}
	return data
}
