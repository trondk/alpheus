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

// Package agent is the sidecar next to each component (SPEC 7). It serves
// the component API on a Unix socket, fetches input chunks from storage to
// the scratch folder, uploads output chunks and commits them in manifests,
// writes acks, and sends and receives signals. It is the only part that
// talks to storage and the signal driver.
package agent

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/pb/componentpb"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/store"
)

// ErrNotDeterministic means a restarted source closed a chunk that differs
// from the one the earlier attempt committed (GO-92).
var ErrNotDeterministic = errors.New("the source is not deterministic")

// ErrNoComponent means the component did not connect in time (GO-29).
var ErrNoComponent = errors.New("the component did not connect")

// ErrScratchFull is returned when a chunk cannot fit in the scratch folder,
// even an empty one (GO-106).
var ErrScratchFull = errors.New("does not fit in the scratch folder")

// Reason is the code the agent fails with for err, which it puts in front
// of its termination message for the controller (GO-138).
func Reason(err error) v1alpha1.Reason {
	switch {
	case errors.Is(err, ErrScratchFull):
		return v1alpha1.ReasonScratchFull
	case errors.Is(err, store.ErrUnreachable):
		return v1alpha1.ReasonStorageUnreachable
	case store.QuotaExceeded(err):
		return v1alpha1.ReasonStorageFull
	case store.Refused(err):
		return v1alpha1.ReasonStorageRefused
	case store.Answered(err):
		return v1alpha1.ReasonStorageError
	}
	return v1alpha1.ReasonAgentFailed
}

// gracePeriod is how long Run waits for the component's calls to end after
// Finish, before it closes the socket.
const gracePeriod = 5 * time.Second

// Agent serves one replica of a step.
type Agent struct {
	cfg   Config
	store store.Store
	sig   signal.Driver // nil means poll only
	log   *slog.Logger

	// commitMu serializes commits, which write the manifests in targets.
	// It is taken before mu, never after.
	commitMu sync.Mutex
	targets  []*target          // edge targets first, then results
	edges    []*target          // the edge targets; fixed after recover
	results  map[string]*target // result targets by output

	// mu guards everything below.
	mu       sync.Mutex
	changed  chan struct{} // closed and replaced on every change
	streams  []*inStream
	fetching bool // fetchers started, at the first Next or Finish
	stateful bool
	asked    bool // the component has called Next, so stateful is known
	// ended is true when an earlier attempt committed and ended all of
	// this replica's output (GO-95); set by resume.
	ended    bool
	nextID   int64
	inputs   map[int64]*inChunk // delivered and not done
	files    map[int64]*outFile // created and not committed
	closeSeq int64
	rr       map[string]int // deterministic order's next stream, per port
	scratch  int64          // bytes of known files in the scratch folder
	obs      *observer
	metrics  *Metrics
	contact  bool // the component has called the agent
	finished bool
	err      error // fatal; stops the agent
}

// New returns an agent. sig may be nil, in which case the agent only
// polls.
func New(cfg Config, st store.Store, sig signal.Driver, log *slog.Logger) (*Agent, error) {
	if err := cfg.complete(); err != nil {
		return nil, err
	}
	a := &Agent{
		cfg:     cfg,
		store:   st,
		sig:     sig,
		log:     log.With("run", cfg.Run, "step", cfg.Step, "replica", cfg.Replica),
		results: map[string]*target{},
		changed: make(chan struct{}),
		inputs:  map[int64]*inChunk{},
		files:   map[int64]*outFile{},
		rr:      map[string]int{},
		obs:     newObserver(cfg),
	}
	for _, in := range cfg.Inputs {
		for p := range in.Replicas {
			a.streams = append(a.streams, newInStream(cfg, in, p))
		}
	}
	// Sorted by port, step and replica, the order inputOrder deterministic
	// takes them in (GO-112).
	slices.SortStableFunc(a.streams, func(x, y *inStream) int {
		return cmp.Or(strings.Compare(x.in.Port, y.in.Port), strings.Compare(x.in.From, y.in.From), x.p-y.p)
	})
	return a, nil
}

// SetMetrics makes the agent record its live metrics in m (GO-51). Call it
// before Run.
func (a *Agent) SetMetrics(m *Metrics) { a.metrics = m }

// source reports whether the step has no incoming edges (GO-91).
func (a *Agent) source() bool { return len(a.cfg.Inputs) == 0 }

// Run recovers from an earlier attempt, serves the component until it
// calls Finish, and returns nil once every output has ended. It returns an
// error when the component fails to connect, the agent hits an error it
// cannot recover from, or ctx ends first.
func (a *Agent) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, dir := range []string{a.inDir(), a.outDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("scratch folder: %w", err)
		}
	}
	if err := a.recover(ctx); err != nil {
		return err
	}
	a.subscribe(ctx)

	os.Remove(a.cfg.Socket)
	lis, err := net.Listen("unix", a.cfg.Socket)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	srv := grpc.NewServer()
	componentpb.RegisterAgentServer(srv, &server{a: a, ctx: ctx})
	go srv.Serve(lis)
	defer srv.Stop()
	a.log.Info("agent ready", "socket", a.cfg.Socket, "attempt", a.cfg.Attempt)

	start := time.NewTimer(a.cfg.StartTimeout.Duration)
	defer start.Stop()
	for {
		a.mu.Lock()
		finished, err, contact, ch := a.finished, a.err, a.contact, a.changed
		a.mu.Unlock()
		switch {
		case err != nil:
			return err
		case finished:
			stopGracefully(srv)
			a.log.Info("finished")
			return nil
		}
		select {
		case <-ctx.Done():
			a.fail(ctx.Err())
			return ctx.Err()
		case <-start.C:
			if !contact {
				err := fmt.Errorf("%w within %s", ErrNoComponent, a.cfg.StartTimeout.Duration)
				a.fail(err)
				return err
			}
		case <-ch:
		}
	}
}

func stopGracefully(srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracePeriod):
		srv.Stop()
	}
}

// notify wakes everything waiting for a change. Call with mu held.
func (a *Agent) notify() {
	close(a.changed)
	a.changed = make(chan struct{})
}

// fail stops the agent with a fatal error.
func (a *Agent) fail(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil && !a.finished {
		a.err = err
		if errors.Is(err, context.Canceled) {
			a.log.Info("agent stopping") // SIGTERM, not a failure
		} else {
			a.log.Error("agent failed", "error", err.Error())
		}
	}
	a.notify()
}

// waitFor calls cond with mu held until it returns true or an error, and
// waits for a change in between.
func (a *Agent) waitFor(ctx context.Context, cond func() (bool, error)) error {
	for {
		a.mu.Lock()
		if a.err != nil {
			err := a.err
			a.mu.Unlock()
			return err
		}
		ok, err := cond()
		ch := a.changed
		a.mu.Unlock()
		if ok || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

func (a *Agent) inDir() string  { return filepath.Join(a.cfg.Scratch, "in") }
func (a *Agent) outDir() string { return filepath.Join(a.cfg.Scratch, "out") }

// recover loads what earlier attempts committed: the outgoing manifests,
// the ack files and the error log (SPEC 7.4).
func (a *Agent) recover(ctx context.Context) error {
	for _, out := range a.cfg.Outputs {
		t := &target{
			output:   out.Output,
			edge:     out.Edge,
			to:       out.To,
			replicas: out.Replicas,
			side:     out.Side,
			stream:   chunk.EdgeStream(a.cfg.UID, out.Edge, a.cfg.Replica),
		}
		if err := a.loadTarget(ctx, t); err != nil {
			return err
		}
		a.targets = append(a.targets, t)
	}
	a.edges = slices.Clone(a.targets)
	if err := a.loadResults(ctx); err != nil {
		return err
	}
	for _, s := range a.streams {
		if err := a.loadAcks(ctx, s); err != nil {
			return err
		}
	}
	return a.loadObservations(ctx)
}

// startFetching decides where each input stream continues from, now that
// the component has said whether it is stateful, and starts fetching.
// Call with mu held.
func (a *Agent) startFetching(ctx context.Context) {
	if a.fetching {
		return
	}
	a.fetching = true
	go func() {
		if err := a.resume(ctx); err != nil {
			a.fail(err)
			return
		}
		for _, s := range a.streams {
			go a.fetch(ctx, s)
		}
	}()
}

// resume sets each input stream's last processed chunk (GO-37) and brings
// its ack file up to date.
func (a *Agent) resume(ctx context.Context) error {
	a.commitMu.Lock()
	defer a.commitMu.Unlock()
	a.mu.Lock()
	stateful := a.stateful
	a.mu.Unlock()

	allEnded := len(a.targets) > 0
	for _, t := range a.targets {
		allEnded = allEnded && t.m.EOS
	}
	a.mu.Lock()
	a.ended = allEnded
	a.mu.Unlock()
	if stateful && !allEnded {
		// The state is lost with the old attempt, so every chunk is read
		// again; output already committed for a chunk is discarded (GO-95).
		for _, t := range a.targets {
			if err := t.loadSources(ctx, a.store); err != nil {
				return err
			}
		}
		return nil
	}
	for _, s := range a.streams {
		if s.in.Side && !allEnded {
			// A side input is read again by every attempt, which builds
			// its lookup data from it (GO-167).
			continue
		}
		processed := s.acks.Last()
		if len(a.targets) > 0 {
			processed = -1
			for _, t := range a.targets {
				if c := t.m.Consumed[s.stream.ID]; processed < 0 || c < processed {
					processed = c
				}
			}
		}
		if err := a.ackThrough(ctx, s, processed); err != nil {
			return err
		}
		a.mu.Lock()
		s.processed, s.fetched = processed, processed
		a.notify()
		a.mu.Unlock()
	}
	return nil
}
