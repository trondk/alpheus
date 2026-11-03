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

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"alpheus-dev.org/alpheus/internal/pb/componentpb"
)

// AgentConfig configures agent mode, where the component talks to the
// agent next to it in the pod (GO-46). Run fills it in from the
// environment the controller sets.
type AgentConfig struct {
	// Socket is the agent's Unix socket.
	Socket string
	// Settings are the step's settings as JSON.
	Settings []byte
	// Replica and Replicas say which replica this is (GO-75); Replicas 0
	// means 1.
	Replica, Replicas int
}

// RunAgent runs the component against the agent at cfg.Socket and returns
// its error. Run calls it when ALPHEUS_SOCKET is set.
func RunAgent(ctx context.Context, cfg AgentConfig, h Handler) error {
	if cfg.Socket == "" {
		return fmt.Errorf("%w: agent mode needs a socket", ErrUsage)
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	// Calls wait for the agent, which may start listening after the
	// component starts; the agent fails the step if the component never
	// gets through (GO-29).
	conn, err := grpc.NewClient("unix://"+cfg.Socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	defer conn.Close()
	c := &agentConn{cfg: cfg, api: componentpb.NewAgentClient(conn)}
	if err := c.rep.start(ctx, c.api); err != nil {
		return rpcErr("connect to the agent", err)
	}
	ctx = withObserver(ctx, &c.rep)

	err = h(ctx, c, c)
	if err == nil {
		err = c.finish(ctx)
	}
	if rerr := c.rep.close(); err == nil {
		err = rerr
	}
	return err
}

// agentConn is the component's side of the component API (SPEC 7.2).
type agentConn struct {
	cfg     AgentConfig
	api     componentpb.AgentClient
	rep     agentReporter
	current int64   // the input chunk being handled; 0 for none
	open    []*File // output files created and not closed
	eosFns  []func(context.Context, Output) error
	broken  bool // a loop over chunks was left before a chunk was done
	// ended is set when the agent says an earlier attempt already ended
	// all output, so OnEOS must not run on an empty state (GO-95).
	ended bool
}

func (c *agentConn) Replica() int  { return c.cfg.Replica }
func (c *agentConn) Replicas() int { return c.cfg.Replicas }

func (c *agentConn) Settings(v any) error { return decodeSettings(c.cfg.Settings, v) }

func (c *agentConn) OnEOS(fn func(context.Context, Output) error) { c.eosFns = append(c.eosFns, fn) }

func (c *agentConn) Port(name string) Port { return agentPort{c: c, name: name} }

type agentPort struct {
	c    *agentConn
	name string
}

func (p agentPort) Chunks(ctx context.Context) iter.Seq2[Chunk, error] {
	return p.c.chunks(ctx, p.name)
}

func (c *agentConn) Chunks(ctx context.Context) iter.Seq2[Chunk, error] { return c.chunks(ctx, "") }

// chunks asks the agent for one chunk at a time and tells it the chunk is
// done when the loop asks for the next one or ends.
func (c *agentConn) chunks(ctx context.Context, port string) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		for {
			resp, err := c.api.Next(ctx, &componentpb.NextRequest{Port: port, Stateful: len(c.eosFns) > 0})
			if err != nil {
				yield(Chunk{}, rpcErr("next chunk", err))
				return
			}
			if resp.Eos {
				c.ended = c.ended || resp.Ended
				return
			}
			in := resp.Chunk
			c.current = in.Id
			c.rep.setInput(in.Id)
			ch := Chunk{
				Port:    in.Port,
				Step:    in.Step,
				Replica: int(in.Replica),
				N:       in.N,
				Path:    in.Path,
				Ext:     in.Ext,
				Bytes:   in.Bytes,
			}
			if !yield(ch, nil) {
				c.broken = true
				return
			}
			if err := c.done(ctx); err != nil {
				yield(Chunk{}, err)
				return
			}
		}
	}
}

// done closes the current chunk's open output files and tells the agent the
// chunk is done, which commits its output, counts and log entries (GO-86,
// GO-90).
func (c *agentConn) done(ctx context.Context) error {
	if err := c.closeOpen(); err != nil {
		return err
	}
	if err := c.rep.failed(); err != nil {
		return err
	}
	id := c.current
	c.current = 0
	c.rep.setInput(0)
	if _, err := c.api.Done(ctx, &componentpb.DoneRequest{Id: id, ObserveSeq: c.rep.lastSeq()}); err != nil {
		return rpcErr("done", err)
	}
	return nil
}

func (c *agentConn) closeOpen() error {
	for len(c.open) > 0 {
		if err := c.open[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func (c *agentConn) Create(ctx context.Context, output, ext string) (*File, error) {
	if output == "" {
		output = DefaultOutput
	}
	resp, err := c.api.Create(ctx, &componentpb.CreateRequest{Output: output, Ext: ext, Input: c.current})
	if err != nil {
		return nil, rpcErr("output "+output, err)
	}
	f, err := os.OpenFile(resp.Path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", output, err)
	}
	file := &File{f: f, path: resp.Path, output: output, ext: ext, id: resp.Id}
	file.onClose = func(f *File) error {
		c.dropOpen(f)
		resp, err := c.api.Close(ctx, &componentpb.CloseRequest{Id: f.id, ObserveSeq: c.rep.lastSeq()})
		if err != nil {
			return rpcErr("output "+f.output, err)
		}
		f.size = resp.Bytes
		return nil
	}
	c.open = append(c.open, file)
	return file, nil
}

func (c *agentConn) dropOpen(f *File) {
	for i, o := range c.open {
		if o == f {
			c.open = append(c.open[:i], c.open[i+1:]...)
			return
		}
	}
}

// finish runs after the handler returned nil: OnEOS runs, then the agent
// checks that all input was read and ends every output.
func (c *agentConn) finish(ctx context.Context) error {
	if c.broken {
		return errors.New("component returned in the middle of an input chunk")
	}
	for _, fn := range c.eosFns {
		if c.ended {
			break
		}
		if err := fn(ctx, c); err != nil {
			return fmt.Errorf("on EOS: %w", err)
		}
	}
	if err := c.closeOpen(); err != nil {
		return err
	}
	if err := c.rep.failed(); err != nil {
		return err
	}
	if _, err := c.api.Finish(ctx, &componentpb.FinishRequest{ObserveSeq: c.rep.lastSeq()}); err != nil {
		return rpcErr("finish", err)
	}
	return nil
}

// rpcErr turns an error from the agent into one the component and Run
// understand: misuse of the API is ErrUsage.
func rpcErr(what string, err error) error {
	s, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%s: %w", what, err)
	}
	switch s.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition:
		return fmt.Errorf("%w: %s: %s", ErrUsage, what, s.Message())
	case codes.Canceled:
		return fmt.Errorf("%s: %w", what, context.Canceled)
	}
	return fmt.Errorf("%s: %s", what, s.Message())
}

// agentReporter sends what the component reports to the agent on the
// Observe stream. Reports are queued, so they never block the component
// (GO-124); Done and Finish name the last one, and the agent waits for it.
type agentReporter struct {
	mu     sync.Mutex
	seq    int64
	input  int64
	queue  []*componentpb.ObserveRequest
	wake   chan struct{}
	done   chan struct{}
	units  map[string]bool
	err    error // the first invalid report
	sendEr error
	stream componentpb.Agent_ObserveClient
	lastPr time.Time
}

// progressInterval limits progress reports (GO-122).
const progressInterval = time.Second

func (r *agentReporter) start(ctx context.Context, api componentpb.AgentClient) error {
	stream, err := api.Observe(ctx)
	if err != nil {
		return err
	}
	r.stream = stream
	r.units = map[string]bool{}
	r.wake = make(chan struct{}, 1)
	r.done = make(chan struct{})
	go r.send()
	return nil
}

func (r *agentReporter) send() {
	defer close(r.done)
	for {
		r.mu.Lock()
		queue := r.queue
		r.queue = nil
		r.mu.Unlock()
		for _, msg := range queue {
			if msg == nil {
				return
			}
			if err := r.stream.Send(msg); err != nil {
				r.mu.Lock()
				r.sendEr = err
				r.mu.Unlock()
				return
			}
		}
		if len(queue) == 0 {
			<-r.wake
		}
	}
}

func (r *agentReporter) push(msg *componentpb.ObserveRequest) {
	r.mu.Lock()
	r.seq++
	msg.Seq, msg.Input = r.seq, r.input
	r.queue = append(r.queue, msg)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *agentReporter) setInput(id int64) {
	r.mu.Lock()
	r.input = id
	r.mu.Unlock()
}

func (r *agentReporter) lastSeq() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}

// failed returns the first invalid report, or why the stream broke.
func (r *agentReporter) failed() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if r.sendEr != nil {
		return rpcErr("observe", r.sendEr)
	}
	return nil
}

func (r *agentReporter) count(unit string, n int64) {
	r.mu.Lock()
	err := checkCount(r.units, unit, n)
	if err != nil && r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
	if err == nil {
		r.push(&componentpb.ObserveRequest{Report: &componentpb.ObserveRequest_Count{Count: &componentpb.Count{Unit: unit, N: n}}})
	}
}

func (r *agentReporter) log(level, code, message string, fields []LogField) {
	e := &componentpb.LogEntry{Level: level, Code: code, Message: message, Time: time.Now().UnixNano()}
	if len(fields) > 0 {
		m := make(map[string]any, len(fields))
		for _, f := range fields {
			m[f.Key] = f.Value
		}
		e.Fields, _ = json.Marshal(m)
	}
	r.push(&componentpb.ObserveRequest{Report: &componentpb.ObserveRequest_Log{Log: e}})
}

func (r *agentReporter) progress(text string) {
	r.mu.Lock()
	if time.Since(r.lastPr) < progressInterval {
		r.mu.Unlock()
		return
	}
	r.lastPr = time.Now()
	r.mu.Unlock()
	r.push(&componentpb.ObserveRequest{Report: &componentpb.ObserveRequest_Progress{Progress: text}})
}

// close sends what is queued and ends the stream.
func (r *agentReporter) close() error {
	if r.stream == nil {
		return nil
	}
	r.mu.Lock()
	r.queue = append(r.queue, nil)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
	<-r.done
	if _, err := r.stream.CloseAndRecv(); err != nil && r.failed() == nil {
		return rpcErr("observe", err)
	}
	return r.failed()
}
