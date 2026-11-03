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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/signal/builtin"
	"alpheus-dev.org/alpheus/internal/store"
	"alpheus-dev.org/alpheus/sdk"
)

const (
	testRun = "sales"
	testUID = "3f0c9a6e"
)

// generate is a source: parts chunks of "<country> <amount>" lines. Every
// part has DK lines except every tenth.
func generate(parts int) sdk.Handler {
	return func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for n := 1; n <= parts; n++ {
			f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
			if err != nil {
				return err
			}
			for i, country := range []string{"SE", "DK", "NO", "DK"} {
				if country == "DK" && n%10 == 0 {
					country = "DE"
				}
				fmt.Fprintf(f, "%s %d\n", country, n*10+i)
			}
			sdk.Count(ctx, "lines", 4)
			if err := f.Close(); err != nil {
				return err
			}
		}
		return nil
	}
}

// filter keeps the DK lines; a chunk without any gives no output.
func filter(ctx context.Context, in sdk.Input, out sdk.Output) error {
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		data, err := os.ReadFile(c.Path)
		if err != nil {
			return err
		}
		var kept []string
		for line := range strings.Lines(string(data)) {
			if strings.HasPrefix(line, "DK ") {
				kept = append(kept, line)
			} else if strings.HasPrefix(line, "DE ") {
				sdk.Warn(ctx, "no_dk", "line is not DK", sdk.Field("chunk", c.N))
			}
		}
		sdk.Count(ctx, "lines_in", int64(strings.Count(string(data), "\n")))
		sdk.Count(ctx, "lines_out", int64(len(kept)))
		if len(kept) == 0 {
			continue
		}
		f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
		if err != nil {
			return err
		}
		io.WriteString(f, strings.Join(kept, ""))
		// Left open: the SDK closes it when the chunk is done (GO-86).
	}
	return nil
}

// sum adds the amounts and writes the total to the result at EOS (GO-48).
// It counts the lines it reads.
func sum(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var total int64
	in.OnEOS(func(ctx context.Context, out sdk.Output) error {
		f, err := out.Create(ctx, "result", "txt")
		if err != nil {
			return err
		}
		fmt.Fprintln(f, total)
		return f.Close()
	})
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		r, err := c.Open()
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			_, amount, _ := strings.Cut(sc.Text(), " ")
			n, _ := strconv.ParseInt(amount, 10, 64)
			total += n
			sdk.Count(ctx, "lines", 1)
		}
		r.Close()
	}
	return nil
}

// expected is the sum of the DK amounts generate makes.
func expected(parts int) int64 {
	var total int64
	for n := 1; n <= parts; n++ {
		if n%10 != 0 {
			total += int64(n*10+1) + int64(n*10+3)
		}
	}
	return total
}

type replica struct {
	cfg Config
	h   sdk.Handler
}

// pipeline is generate -> filter (filters replicas) -> sum.
func pipeline(parts, filters int) []replica {
	base := Config{Run: testRun, Namespace: "default", UID: testUID, MaxUnacked: 2, Poll: Duration{50 * time.Millisecond}}
	var rs []replica

	gen := base
	gen.Step = "generate"
	gen.Outputs = []OutputEdge{{Edge: "generate-filter", Output: "default", To: "filter", Replicas: filters}}
	rs = append(rs, replica{gen, generate(parts)})

	for r := range filters {
		f := base
		f.Step, f.Replica, f.Replicas = "filter", r, filters
		f.Inputs = []InputEdge{{Edge: "generate-filter", From: "generate", Replicas: 1}}
		f.Outputs = []OutputEdge{{Edge: "filter-sum", Output: "default", To: "sum", Replicas: 1}}
		rs = append(rs, replica{f, filter})
	}

	s := base
	s.Step = "sum"
	s.Inputs = []InputEdge{{Edge: "filter-sum", From: "filter", Replicas: filters}}
	rs = append(rs, replica{s, sum})
	return rs
}

// run runs each replica's agent and component until all have finished.
func run(t *testing.T, st store.Store, driver func() signal.Driver, rs ...replica) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	log := slog.New(slog.DiscardHandler)
	var wg sync.WaitGroup
	errs := make([]error, 2*len(rs))
	for i, r := range rs {
		dir, err := os.MkdirTemp("", "alpheus-agent-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		r.cfg.Socket = filepath.Join(dir, "agent.sock")
		r.cfg.Scratch = filepath.Join(dir, "scratch")
		var sig signal.Driver
		if driver != nil {
			sig = driver()
		}
		a, err := New(r.cfg, st, sig, log)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			if errs[2*i] = a.Run(ctx); errs[2*i] != nil {
				cancel()
			}
		}()
		go func() {
			defer wg.Done()
			cfg := sdk.AgentConfig{Socket: r.cfg.Socket, Replica: r.cfg.Replica, Replicas: r.cfg.Replicas}
			if errs[2*i+1] = sdk.RunAgent(ctx, cfg, r.h); errs[2*i+1] != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	var all []error
	for i, err := range errs {
		if err != nil {
			r := rs[i/2]
			all = append(all, fmt.Errorf("%s/%d: %w", r.cfg.Step, r.cfg.Replica, err))
		}
	}
	return errors.Join(all...)
}

func hub(t *testing.T) func() signal.Driver {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	builtin.NewHub(nil).Register(s)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return func() signal.Driver {
		c, err := builtin.Dial(lis.Addr().String(), insecure.NewCredentials(), slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
}

func readManifest(t *testing.T, st store.Store, s chunk.Stream) *chunk.Manifest {
	t.Helper()
	data, err := store.ReadAll(context.Background(), st, s.ManifestKey())
	if err != nil {
		t.Fatalf("%s: %v", s.ID, err)
	}
	m, err := chunk.ParseManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readAcks(t *testing.T, st store.Store, key string) []int64 {
	t.Helper()
	data, err := store.ReadAll(context.Background(), st, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	a, err := chunk.ParseAcks(data)
	if err != nil {
		t.Fatal(err)
	}
	return a.Acked
}

func observations(t *testing.T, st store.Store, step string, replica int) Observations {
	t.Helper()
	data, err := store.ReadAll(context.Background(), st, chunk.ErrorLogPrefix(testRun, testUID, step, replica)+"manifest.json")
	if errors.Is(err, store.ErrNotFound) {
		// Nothing was ever counted.
		return Observations{}
	}
	if err != nil {
		t.Fatal(err)
	}
	var o Observations
	if err := json.Unmarshal(data, &o); err != nil {
		t.Fatal(err)
	}
	return o
}

// checkPipeline checks the outcome as testcase03 does (GO-81, GO-84,
// GO-85), with exact counters (GO-115).
func checkPipeline(t *testing.T, st store.Store, parts, filters int) {
	t.Helper()
	ctx := context.Background()

	result := chunk.ResultStream(testRun, testUID, "sum", "result", 0)
	m := readManifest(t, st, result)
	if !m.EOS || len(m.Chunks) != 1 {
		t.Fatalf("result manifest %+v", m)
	}
	data, err := store.ReadAll(ctx, st, result.ChunkKey(1, "txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(data)), fmt.Sprint(expected(parts)); got != want {
		t.Errorf("sum %s, want %s", got, want)
	}
	if _, err := store.ReadAll(ctx, st, chunk.RunInfoKey(testRun, testUID)); err != nil {
		t.Errorf("run.json: %v", err)
	}

	var all []int64
	edge := chunk.EdgeStream(testUID, "generate-filter", 0)
	for r := range filters {
		acked := readAcks(t, st, edge.AckKey("filter", r))
		for _, n := range acked {
			if chunk.Assign(n, 0, filters) != r {
				t.Errorf("replica %d acked chunk %d", r, n)
			}
		}
		all = append(all, acked...)

		out := readManifest(t, st, chunk.EdgeStream(testUID, "filter-sum", r))
		if !out.EOS {
			t.Errorf("filter %d did not end its output", r)
		}
		if r == 0 && filters == 10 && len(out.Chunks) != 0 {
			t.Errorf("filter 0 has %d chunks, want none", len(out.Chunks))
		}
	}
	slices.Sort(all)
	want := make([]int64, parts)
	for i := range want {
		want[i] = int64(i + 1)
	}
	if !slices.Equal(all, want) {
		t.Errorf("acked chunks %v, want 1-%d once each", all, parts)
	}

	gen := readManifest(t, st, edge)
	if !gen.EOS || gen.Count() != int64(parts) || gen.Chunks[0].Counts["lines"] != 4 {
		t.Errorf("generate manifest: eos %v, %d chunks, counts %v", gen.EOS, gen.Count(), gen.Chunks[0].Counts)
	}
	var in, out, warnings int64
	for r := range filters {
		o := observations(t, st, "filter", r)
		in += o.Counts["lines_in"]
		out += o.Counts["lines_out"]
		warnings += o.Warnings
	}
	wantIn, wantWarnings := int64(4*parts), int64(2*(parts/10))
	if in != wantIn || warnings != wantWarnings {
		t.Errorf("filter counted %d lines in, %d warnings, want %d and %d", in, warnings, wantIn, wantWarnings)
	}
	// A restarted source and the stateful sum see chunks again, which
	// must not be counted again.
	for step, want := range map[string]int64{"generate": int64(4 * parts), "sum": int64(2 * (parts - parts/10))} {
		got := observations(t, st, step, 0).Counts["lines"]
		if got != want {
			t.Errorf("%s counted %d lines, want %d", step, got, want)
		}
	}
}

func TestPipelineWithHub(t *testing.T) {
	st := store.NewMemory()
	if err := run(t, st, hub(t), pipeline(30, 10)...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, 30, 10)
}

// generate's output goes to two steps (fan-out), and sum reads from both
// (fan-in): every chunk reaches each consuming step once, through its own
// ack files (GO-36), and sum gets EOS only when both inputs have ended.
// fanOut is generate -> left (3 replicas) and right (2 replicas) -> sum,
// with right on its own port.
func fanOut(parts int) []replica {
	base := Config{Run: testRun, Namespace: "default", UID: testUID, MaxUnacked: 2, Poll: Duration{50 * time.Millisecond}}
	gen := base
	gen.Step = "generate"
	gen.Outputs = []OutputEdge{
		{Edge: "generate-left", Output: "default", To: "left", Replicas: 3},
		{Edge: "generate-right", Output: "default", To: "right", Replicas: 2},
	}
	rs := []replica{{gen, generate(parts)}}
	for _, side := range sides {
		for r := range side.replicas {
			f := base
			f.Step, f.Replica, f.Replicas = side.name, r, side.replicas
			f.Inputs = []InputEdge{{Edge: "generate-" + side.name, From: "generate", Replicas: 1}}
			f.Outputs = []OutputEdge{{Edge: side.name + "-sum", Output: "default", To: "sum", Replicas: 1}}
			rs = append(rs, replica{f, filter})
		}
	}
	s := base
	s.Step = "sum"
	s.Inputs = []InputEdge{
		{Edge: "left-sum", From: "left", Replicas: 3},
		{Edge: "right-sum", From: "right", Replicas: 2, Port: "right"},
	}
	return append(rs, replica{s, sum})
}

var sides = []struct {
	name     string
	replicas int
}{{"left", 3}, {"right", 2}}

// checkFanOut checks that sum got every chunk once from each side, that
// each side's replicas acked every chunk once, and that the counters are
// exact (GO-115).
func checkFanOut(t *testing.T, st store.Store, parts int) {
	t.Helper()
	result := chunk.ResultStream(testRun, testUID, "sum", "result", 0)
	if m := readManifest(t, st, result); !m.EOS || len(m.Chunks) != 1 {
		t.Fatalf("result manifest %+v", m)
	}
	data, err := store.ReadAll(context.Background(), st, result.ChunkKey(1, "txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(data)), fmt.Sprint(2*expected(parts)); got != want {
		t.Errorf("sum %s, want %s: both sides once", got, want)
	}

	want := make([]int64, parts)
	for i := range want {
		want[i] = int64(i + 1)
	}
	dk := int64(2 * (parts - parts/10))
	for _, side := range sides {
		edge := chunk.EdgeStream(testUID, "generate-"+side.name, 0)
		var all []int64
		var in int64
		for r := range side.replicas {
			all = append(all, readAcks(t, st, edge.AckKey(side.name, r))...)
			in += observations(t, st, side.name, r).Counts["lines_in"]
		}
		slices.Sort(all)
		if !slices.Equal(all, want) {
			t.Errorf("%s acked %v, want 1-%d once each", side.name, all, parts)
		}
		if in != int64(4*parts) {
			t.Errorf("%s counted %d lines in, want %d", side.name, in, 4*parts)
		}
	}
	for step, want := range map[string]int64{"generate": int64(4 * parts), "sum": 2 * dk} {
		if got := observations(t, st, step, 0).Counts["lines"]; got != want {
			t.Errorf("%s counted %d lines, want %d", step, got, want)
		}
	}
}

func TestFanOutFanIn(t *testing.T) {
	const parts = 12
	st := store.NewMemory()
	if err := run(t, st, hub(t), fanOut(parts)...); err != nil {
		t.Fatal(err)
	}
	checkFanOut(t, st, parts)
}

// Without signals the agents find everything by polling (GO-40).
func TestPipelinePollingOnly(t *testing.T) {
	st := store.NewMemory()
	if err := run(t, st, nil, pipeline(12, 3)...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, 12, 3)
}

// A restarted source must close the same chunks again; they are compared
// and discarded (GO-92).
func TestSourceRestart(t *testing.T) {
	st := store.NewMemory()
	rs := pipeline(5, 1)
	gen := rs[0]
	gen.cfg.MaxUnacked = 0
	if err := run(t, st, nil, gen); err != nil {
		t.Fatal(err)
	}
	before := readManifest(t, st, chunk.EdgeStream(testUID, "generate-filter", 0))

	gen.cfg.Attempt = 2
	if err := run(t, st, nil, gen); err != nil {
		t.Fatal(err)
	}
	after := readManifest(t, st, chunk.EdgeStream(testUID, "generate-filter", 0))
	if after.Count() != before.Count() || after.Epoch != 2 {
		t.Errorf("after restart: %d chunks, epoch %d", after.Count(), after.Epoch)
	}

	different := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
		if err != nil {
			return err
		}
		io.WriteString(f, "something else")
		return f.Close()
	}
	for name, h := range map[string]sdk.Handler{
		"different chunk":      different,
		"more chunks than EOS": generate(6),
	} {
		gen.cfg.Attempt++
		gen.h = h
		if err := run(t, st, nil, gen); err == nil || !strings.Contains(err.Error(), ErrNotDeterministic.Error()) {
			t.Errorf("%s: %v, want %v", name, err, ErrNotDeterministic)
		}
	}
}

// crashAfter runs h but fails while handling the n-th input chunk, before
// it is done, as a crash would.
func crashAfter(n int, h sdk.Handler) sdk.Handler {
	return func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		return h(ctx, crashingInput{Input: in, left: &n}, out)
	}
}

type crashingInput struct {
	sdk.Input
	left *int
}

var errCrash = errors.New("crash")

func (c crashingInput) Chunks(ctx context.Context) iter.Seq2[sdk.Chunk, error] {
	return func(yield func(sdk.Chunk, error) bool) {
		for ch, err := range c.Input.Chunks(ctx) {
			if err == nil {
				if *c.left--; *c.left == 0 {
					yield(sdk.Chunk{}, errCrash)
					return
				}
			}
			if !yield(ch, err) {
				return
			}
		}
	}
}

// restart runs the pipeline with the replica at index i crashing, then
// runs every replica again as a new attempt. The result must be as if
// nothing happened (GO-37, GO-70, GO-74).
func restart(t *testing.T, parts, filters, i, after int) {
	t.Helper()
	st := store.NewMemory()
	rs := pipeline(parts, filters)
	crashed := slices.Clone(rs)
	crashed[i].h = crashAfter(after, rs[i].h)
	if err := run(t, st, nil, crashed...); !errors.Is(err, errCrash) {
		t.Fatalf("first attempt: %v, want the crash", err)
	}
	for j := range rs {
		rs[j].cfg.Attempt = 2
	}
	if err := run(t, st, nil, rs...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, parts, filters)
}

func TestRestartFilter(t *testing.T) { restart(t, 12, 3, 2, 3) }

// sum keeps its state in memory only, so after a crash every chunk is read
// again and the sum is still right (GO-95, GO-100).
func TestRestartStatefulSum(t *testing.T) { restart(t, 12, 3, 4, 5) }

// A stateful step restarted after it committed and ended its output, as
// when its pod dies between that write and the end of the pod, has nothing
// left to do: its OnEOS must not run on an empty state, which would make a
// careful component fail on every restart (GO-95; found by testcase06).
func TestRestartAfterStatefulEnded(t *testing.T) {
	st := store.NewMemory()
	rs := pipeline(12, 3)
	if err := run(t, st, nil, rs...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, 12, 3)

	s := rs[len(rs)-1]
	s.cfg.Attempt = 2
	ranEOS := false
	s.h = func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		read := 0
		in.OnEOS(func(ctx context.Context, out sdk.Output) error {
			ranEOS = true
			if read == 0 {
				return errors.New("on EOS with no input: the state is gone")
			}
			return nil
		})
		for _, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			read++
		}
		return nil
	}
	if err := run(t, st, nil, s); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	if ranEOS {
		t.Error("OnEOS ran although the output had ended")
	}
	checkPipeline(t, st, 12, 3)
}

// An agent stops when a newer attempt owns its stream (GO-68).
func TestFencing(t *testing.T) {
	st := store.NewMemory()
	gen := pipeline(2, 1)[0]
	gen.cfg.MaxUnacked = 0
	gen.cfg.Attempt = 5
	if err := run(t, st, nil, gen); err != nil {
		t.Fatal(err)
	}
	gen.cfg.Attempt = 4
	if err := run(t, st, nil, gen); err == nil || !strings.Contains(err.Error(), "fencing") {
		t.Errorf("%v, want a fencing error", err)
	}
}

// Backpressure: with nobody consuming, a producer stops after maxUnacked
// chunks per consumer replica (GO-35).
func TestBackpressure(t *testing.T) {
	st := store.NewMemory()
	gen := pipeline(20, 2)[0]
	// Long enough to commit four chunks also on a busy machine; it then
	// waits for acks that never come.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	dir, _ := os.MkdirTemp("", "alpheus-agent-")
	defer os.RemoveAll(dir)
	gen.cfg.Socket, gen.cfg.Scratch = filepath.Join(dir, "s"), filepath.Join(dir, "scratch")
	a, err := New(gen.cfg, st, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	go a.Run(ctx)
	sdk.RunAgent(ctx, sdk.AgentConfig{Socket: gen.cfg.Socket}, gen.h)

	m := readManifest(t, st, chunk.EdgeStream(testUID, "generate-filter", 0))
	if m.Count() != 4 || m.EOS {
		t.Errorf("%d chunks committed, want 4 (2 replicas x maxUnacked 2)", m.Count())
	}
}

// A step may make more chunks from one input chunk than maxUnacked: its
// output is not committed until the input chunk is done, so the consumer
// cannot ack it before, and backpressure must not wait for that (GO-35).
// Here filter writes each DK line five times, each in its own chunk: ten
// chunks per input chunk, with maxUnacked 2.
func TestManyChunksPerInput(t *testing.T) {
	st := store.NewMemory()
	rs := pipeline(6, 1)
	rs[1].h = func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for c, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			data, err := os.ReadFile(c.Path)
			if err != nil {
				return err
			}
			for line := range strings.Lines(string(data)) {
				if !strings.HasPrefix(line, "DK ") {
					continue
				}
				for range 5 {
					f, err := out.Create(ctx, sdk.DefaultOutput, "txt")
					if err != nil {
						return err
					}
					io.WriteString(f, line)
					if err := f.Close(); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	if err := run(t, st, nil, rs...); err != nil {
		t.Fatal(err)
	}
	result := chunk.ResultStream(testRun, testUID, "sum", "result", 0)
	data, err := store.ReadAll(t.Context(), st, result.ChunkKey(1, "txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(data)), strconv.FormatInt(5*expected(6), 10); got != want {
		t.Errorf("sum %s, want %s", got, want)
	}
}

// A consumer replica whose ack file the controller closed, after it failed
// for good with failurePolicy continue, no longer holds the producer back
// (GO-35, GO-97). With maxUnacked 2 and nobody consuming, replica 0 stops
// generate at chunk 5, which it could not before, and replica 1 is closed.
func TestBackpressureClosedReplica(t *testing.T) {
	st := store.NewMemory()
	edge := chunk.EdgeStream(testUID, "generate-filter", 0)
	closed := chunk.NewAcks()
	closed.Epoch, closed.Closed = 2, true
	data, _ := closed.Marshal()
	st.Put(t.Context(), edge.AckKey("filter", 1), bytes.NewReader(data), int64(len(data)))

	gen := pipeline(20, 2)[0]
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	dir, _ := os.MkdirTemp("", "alpheus-agent-")
	defer os.RemoveAll(dir)
	gen.cfg.Socket, gen.cfg.Scratch = filepath.Join(dir, "s"), filepath.Join(dir, "scratch")
	a, err := New(gen.cfg, st, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- a.Run(ctx) }()
	sdk.RunAgent(ctx, sdk.AgentConfig{Socket: gen.cfg.Socket}, gen.h)
	if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("agent: %v, want it still waiting", err)
	}

	// Chunks 2 and 4 wait for replica 0; chunk 6 would be its third.
	if m := readManifest(t, st, edge); m.Count() != 5 || m.EOS {
		t.Errorf("%d chunks committed, want 5", m.Count())
	}
}

// An agent whose ack file has a newer epoch, or was closed by the
// controller, stops instead of writing it (GO-68, GO-97).
func TestAckFencing(t *testing.T) {
	st := store.NewMemory()
	rs := pipeline(2, 1)
	if err := run(t, st, nil, rs[0]); err != nil {
		t.Fatal(err)
	}
	closed := chunk.NewAcks()
	closed.Epoch, closed.Closed = 2, true
	data, _ := closed.Marshal()
	st.Put(t.Context(), chunk.EdgeStream(testUID, "generate-filter", 0).AckKey("filter", 0), bytes.NewReader(data), int64(len(data)))
	if err := run(t, st, nil, rs[1]); err == nil || !strings.Contains(err.Error(), "fencing") {
		t.Errorf("%v, want a fencing error", err)
	}
}
