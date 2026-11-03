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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/pb/componentpb"
)

// server is the component API on the socket (SPEC 7.2).
type server struct {
	componentpb.UnimplementedAgentServer
	a *Agent
	// ctx ends when the agent stops; calls end with it.
	ctx context.Context
}

func usage(format string, args ...any) error {
	return status.Errorf(codes.FailedPrecondition, format, args...)
}

// rpcErr turns an agent error into a status the SDK understands.
func (s *server) rpcErr(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || s.ctx.Err() != nil {
		return status.Error(codes.Aborted, "the agent is stopping")
	}
	return status.Error(codes.Internal, err.Error())
}

// call marks contact with the component and merges the call's context with
// the agent's.
func (s *server) call(ctx context.Context) (context.Context, context.CancelFunc) {
	a := s.a
	a.mu.Lock()
	if !a.contact {
		a.contact = true
		a.notify()
	}
	a.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

func (s *server) Next(ctx context.Context, req *componentpb.NextRequest) (*componentpb.NextResponse, error) {
	ctx, cancel := s.call(ctx)
	defer cancel()
	a := s.a

	a.mu.Lock()
	if req.Port != "" && !a.hasPort(req.Port) {
		a.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "no input port %q", req.Port)
	}
	if a.asked && req.Stateful != a.stateful {
		a.mu.Unlock()
		return nil, usage("OnEOS must be registered before the first chunk is read")
	}
	if !a.asked {
		a.asked, a.stateful = true, req.Stateful
	}
	a.startFetching(s.ctx)
	a.mu.Unlock()

	var c *inChunk
	var eos bool
	err := a.waitFor(ctx, func() (bool, error) {
		c, eos = a.pick(req.Port)
		return c != nil || eos, nil
	})
	if err != nil {
		return nil, s.rpcErr(err)
	}
	if eos {
		a.mu.Lock()
		ended := a.ended
		a.mu.Unlock()
		return &componentpb.NextResponse{Eos: true, Ended: ended}, nil
	}
	return &componentpb.NextResponse{Chunk: &componentpb.InputChunk{
		Id:      c.id,
		Port:    c.s.in.Port,
		Step:    c.s.in.From,
		Replica: int32(c.s.p),
		N:       c.meta.N,
		Path:    c.path,
		Ext:     c.meta.Ext,
		Bytes:   c.meta.Bytes,
	}}, nil
}

func (a *Agent) hasPort(port string) bool {
	for _, s := range a.streams {
		if s.in.Port == port {
			return true
		}
	}
	return false
}

func (s *server) Create(ctx context.Context, req *componentpb.CreateRequest) (*componentpb.CreateResponse, error) {
	ctx, cancel := s.call(ctx)
	defer cancel()
	a := s.a

	output := req.Output
	if output == "" {
		output = chunk.DefaultOutput
	}
	if !validName(output) {
		return nil, status.Errorf(codes.InvalidArgument, "output %q: want a name like a step's", output)
	}
	if !chunk.ValidExt(req.Ext) {
		return nil, status.Errorf(codes.InvalidArgument, "extension %q: want lower-case letters and digits, at most 16", req.Ext)
	}
	a.mu.Lock()
	switch {
	case req.Input != 0 && a.inputs[req.Input] == nil:
		a.mu.Unlock()
		return nil, usage("input chunk %d is not open", req.Input)
	case req.Input == 0 && !a.source() && !a.allDrained():
		a.mu.Unlock()
		return nil, usage("output must belong to an input chunk until all input is read")
	case a.cfg.ScratchLimit > 0 && a.scratch >= a.cfg.ScratchLimit*9/10:
		a.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted, "the scratch folder is nearly full: %d of %d bytes", a.scratch, a.cfg.ScratchLimit)
	}
	a.mu.Unlock()

	if err := a.waitForConsumers(ctx, output); err != nil {
		return nil, s.rpcErr(err)
	}

	a.mu.Lock()
	a.nextID++
	f := &outFile{id: a.nextID, output: output, ext: req.Ext, input: req.Input}
	f.path = filepath.Join(a.outDir(), fmt.Sprintf("%d.%s", f.id, f.ext))
	a.files[f.id] = f
	a.mu.Unlock()
	if err := os.WriteFile(f.path, nil, 0o644); err != nil {
		return nil, s.rpcErr(err)
	}
	return &componentpb.CreateResponse{Id: f.id, Path: f.path}, nil
}

// allDrained reports whether every input stream has delivered all of its
// chunks. Call with mu held.
func (a *Agent) allDrained() bool {
	for _, s := range a.streams {
		if !s.drained() {
			return false
		}
	}
	return a.fetching
}

func (s *server) Close(ctx context.Context, req *componentpb.CloseRequest) (*componentpb.CloseResponse, error) {
	ctx, cancel := s.call(ctx)
	defer cancel()
	a := s.a

	a.mu.Lock()
	f := a.files[req.Id]
	a.mu.Unlock()
	if f == nil || f.closed {
		return nil, usage("output chunk %d is not open", req.Id)
	}
	if err := a.closeFile(f); err != nil {
		return nil, status.Error(codes.OutOfRange, err.Error())
	}
	if f.input != 0 || !a.source() {
		return &componentpb.CloseResponse{Bytes: f.size}, nil
	}

	// A source's chunk is committed when it is closed (GO-91), with the
	// counts reported since the last one.
	if err := a.waitObserved(ctx, req.ObserveSeq); err != nil {
		return nil, s.rpcErr(err)
	}
	a.commitMu.Lock()
	defer a.commitMu.Unlock()
	a.mu.Lock()
	r, err := a.obs.take(0)
	a.mu.Unlock()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := a.commit(ctx, nil, []*outFile{f}, r, false); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	if err := a.writeObservations(ctx); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	a.mu.Lock()
	a.removeFiles([]*outFile{f})
	a.mu.Unlock()
	return &componentpb.CloseResponse{Bytes: f.size}, nil
}

func (a *Agent) waitObserved(ctx context.Context, seq int64) error {
	return a.waitFor(ctx, func() (bool, error) { return a.obs.seq >= seq, nil })
}

func (s *server) Done(ctx context.Context, req *componentpb.DoneRequest) (*componentpb.DoneResponse, error) {
	ctx, cancel := s.call(ctx)
	defer cancel()
	a := s.a

	a.mu.Lock()
	c := a.inputs[req.Id]
	switch {
	case c == nil:
		a.mu.Unlock()
		return nil, usage("input chunk %d is not open", req.Id)
	case c.s.open[0] != c:
		a.mu.Unlock()
		return nil, usage("input chunk %d of %s is done before chunk %d (GO-71)", c.meta.N, c.s.stream.ID, c.s.open[0].meta.N)
	}
	a.mu.Unlock()
	if err := a.waitObserved(ctx, req.ObserveSeq); err != nil {
		return nil, s.rpcErr(err)
	}

	a.commitMu.Lock()
	defer a.commitMu.Unlock()
	files, err := a.filesOf(req.Id)
	if err != nil {
		return nil, status.Error(codes.OutOfRange, err.Error())
	}
	a.mu.Lock()
	r, err := a.obs.take(req.Id)
	a.mu.Unlock()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := a.commit(ctx, c, files, r, false); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	if err := a.ackThrough(ctx, c.s, c.meta.N); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	if err := a.writeObservations(ctx); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}

	a.mu.Lock()
	a.removeFiles(files)
	c.s.open = c.s.open[1:]
	c.s.processed = c.meta.N
	delete(a.inputs, c.id)
	os.Remove(c.path)
	a.scratch -= c.meta.Bytes
	a.notify()
	a.mu.Unlock()
	return &componentpb.DoneResponse{}, nil
}

// filesOf closes the files of an input chunk the component left open
// (GO-86) and returns all of its files.
func (a *Agent) filesOf(input int64) ([]*outFile, error) {
	a.mu.Lock()
	var files []*outFile
	for _, f := range a.files {
		if f.input == input {
			files = append(files, f)
		}
	}
	a.mu.Unlock()
	for _, f := range files {
		if !f.closed {
			if err := a.closeFile(f); err != nil {
				return nil, err
			}
		}
	}
	return files, nil
}

func (s *server) Observe(stream componentpb.Agent_ObserveServer) error {
	_, cancel := s.call(stream.Context())
	defer cancel()
	a := s.a
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&componentpb.ObserveResponse{})
		}
		if err != nil {
			return err
		}
		switch m := msg.Report.(type) {
		case *componentpb.ObserveRequest_Progress:
			a.log.Info("progress", "text", m.Progress)
		case *componentpb.ObserveRequest_Count:
			a.metrics.count(m.Count.GetUnit(), m.Count.GetN())
		case *componentpb.ObserveRequest_Log:
			a.metrics.entry(m.Log.GetLevel())
		}
		a.mu.Lock()
		a.obs.add(msg)
		a.notify()
		a.mu.Unlock()
	}
}

func (s *server) Finish(ctx context.Context, req *componentpb.FinishRequest) (*componentpb.FinishResponse, error) {
	ctx, cancel := s.call(ctx)
	defer cancel()
	a := s.a

	a.mu.Lock()
	if !a.source() {
		a.startFetching(s.ctx)
	}
	switch {
	case a.finished:
		a.mu.Unlock()
		return nil, usage("Finish was called before")
	case len(a.inputs) > 0:
		a.mu.Unlock()
		return nil, usage("the component finished with %d input chunks not done", len(a.inputs))
	case !a.source() && !a.allDrained():
		a.mu.Unlock()
		return nil, usage("the component finished before reading all of its input")
	}
	a.mu.Unlock()
	if err := a.waitObserved(ctx, req.ObserveSeq); err != nil {
		return nil, s.rpcErr(err)
	}

	a.commitMu.Lock()
	defer a.commitMu.Unlock()
	files, err := a.filesOf(0)
	if err != nil {
		return nil, status.Error(codes.OutOfRange, err.Error())
	}
	a.mu.Lock()
	r, err := a.obs.take(0)
	a.mu.Unlock()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := a.commit(ctx, nil, files, r, true); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	if err := a.writeObservations(ctx); err != nil {
		a.fail(err)
		return nil, s.rpcErr(err)
	}
	a.mu.Lock()
	a.removeFiles(files)
	a.finished = true
	a.notify()
	a.mu.Unlock()
	return &componentpb.FinishResponse{}, nil
}
