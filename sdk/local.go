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
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"alpheus-dev.org/alpheus/internal/chunk"
)

// Files local mode writes next to the outputs. Output names start with a
// letter, so these never collide with an output's folder.
const (
	SummaryFile  = "_summary.json"
	ErrorLogFile = "_errors.jsonl"
)

// LocalConfig configures local mode (GO-47, GO-87).
type LocalConfig struct {
	// Inputs maps each port to a folder of input chunks. Every file in a
	// folder is a chunk, except manifest.json and its segments, so another
	// step's output folder can be an input; they are read in name order,
	// and the n-th file is chunk n. No inputs means the component is a
	// source.
	Inputs map[string]string

	// OutputDir gets one folder per output, with the output's chunks as
	// part-000001.<ext> and so on and a manifest.json in the format of
	// spec 7.3, and the files _summary.json (counters, errors, warnings)
	// and _errors.jsonl (the error log).
	OutputDir string

	// Settings are the step's settings as JSON.
	Settings []byte

	// Replica and Replicas make the component read only the chunks this
	// replica would get, as chunk n goes to replica n mod Replicas
	// (GO-76). Replicas 0 means 1.
	Replica, Replicas int

	// MaxBytes is the largest output chunk, as spec.chunks.maxBytes; 0
	// means no limit (GO-105).
	MaxBytes int64

	// Step names the step in error log entries; empty means "local".
	Step string

	// Log gets progress reports; nil means they are dropped.
	Log *slog.Logger
}

// RunLocal runs the component in local mode and returns its error. Tests
// of a component call it directly.
func RunLocal(ctx context.Context, cfg LocalConfig, h Handler) error {
	if cfg.OutputDir == "" {
		return fmt.Errorf("%w: local mode needs an output folder", ErrUsage)
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	if cfg.Replicas < 1 || cfg.Replica < 0 || cfg.Replica >= cfg.Replicas {
		return fmt.Errorf("%w: replica %d of %d", ErrUsage, cfg.Replica, cfg.Replicas)
	}
	if cfg.Step == "" {
		cfg.Step = "local"
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("output folder: %w", err)
	}
	l, err := newLocal(cfg)
	if err != nil {
		return err
	}
	err = h(withObserver(ctx, l.obs), l, l)
	if err == nil {
		err = l.finish(withObserver(ctx, l.obs))
	}
	if serr := l.writeSummary(); err == nil {
		err = serr
	}
	return err
}

type local struct {
	cfg    LocalConfig
	obs    *observer
	ports  []string
	queues map[string][]Chunk
	pos    map[string]int
	next   int // port to try first in Chunks, for round robin

	current *Chunk  // the input chunk being handled
	open    []*File // output files created and not closed
	closed  []*File // closed, waiting for the input chunk to be done
	outs    map[string]*localOutput
	eosFns  []func(context.Context, Output) error
	broken  bool // a loop over chunks was left before a chunk was done
}

type localOutput struct {
	dir      string
	manifest *chunk.Manifest
	stream   chunk.Stream
}

func newLocal(cfg LocalConfig) (*local, error) {
	l := &local{
		cfg:    cfg,
		queues: map[string][]Chunk{},
		pos:    map[string]int{},
		outs:   map[string]*localOutput{},
	}
	l.obs = newObserver(cfg.Step, cfg.Replica, func(text string) {
		if cfg.Log != nil {
			cfg.Log.Info("progress", "text", text)
		}
	})
	for port, dir := range cfg.Inputs {
		chunks, err := readInputs(port, dir, cfg.Replica, cfg.Replicas)
		if err != nil {
			return nil, err
		}
		l.ports = append(l.ports, port)
		l.queues[port] = chunks
	}
	sort.Strings(l.ports)
	return l, nil
}

// manifestFile matches the manifest and its sealed segments, which local
// mode writes next to the chunks, so an output folder can be the next
// step's input folder.
var manifestFile = regexp.MustCompile(`^manifest(-\d+)?\.json$`)

// readInputs lists the chunks of one port that this replica gets.
func readInputs(port, dir string, replica, replicas int) ([]Chunk, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: input %s: %w", ErrUsage, port, err)
	}
	var chunks []Chunk
	n := int64(0)
	for _, e := range entries { // os.ReadDir sorts by name
		if !e.Type().IsRegular() || manifestFile.MatchString(e.Name()) {
			continue
		}
		n++
		if chunk.Assign(n, 0, replicas) != replica {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("input %s: %w", port, err)
		}
		chunks = append(chunks, Chunk{
			Port:  port,
			Step:  port,
			N:     n,
			Path:  filepath.Join(dir, e.Name()),
			Ext:   strings.ToLower(strings.TrimPrefix(filepath.Ext(e.Name()), ".")),
			Bytes: info.Size(),
		})
	}
	return chunks, nil
}

func (l *local) Replica() int  { return l.cfg.Replica }
func (l *local) Replicas() int { return l.cfg.Replicas }

func (l *local) Settings(v any) error { return decodeSettings(l.cfg.Settings, v) }

func (l *local) OnEOS(fn func(context.Context, Output) error) { l.eosFns = append(l.eosFns, fn) }

func (l *local) Port(name string) Port { return localPort{l: l, name: name} }

type localPort struct {
	l    *local
	name string
}

func (p localPort) Chunks(ctx context.Context) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		if _, ok := p.l.queues[p.name]; !ok {
			yield(Chunk{}, fmt.Errorf("%w: no input port %q", ErrUsage, p.name))
			return
		}
		for p.l.pos[p.name] < len(p.l.queues[p.name]) {
			if !p.l.deliver(ctx, p.name, yield) {
				return
			}
		}
	}
}

// Chunks takes one chunk from each port in turn, ports in name order, so
// local runs are deterministic (as inputOrder: deterministic, GO-112).
func (l *local) Chunks(ctx context.Context) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		for {
			port, ok := l.nextPort()
			if !ok {
				return
			}
			if !l.deliver(ctx, port, yield) {
				return
			}
		}
	}
}

func (l *local) nextPort() (string, bool) {
	for i := range l.ports {
		port := l.ports[(l.next+i)%len(l.ports)]
		if l.pos[port] < len(l.queues[port]) {
			l.next = (l.next + i + 1) % len(l.ports)
			return port, true
		}
	}
	return "", false
}

// deliver yields the next chunk of a port and marks it done when the loop
// asks for more or ends; false means stop.
func (l *local) deliver(ctx context.Context, port string, yield func(Chunk, error) bool) bool {
	if err := ctx.Err(); err != nil {
		yield(Chunk{}, err)
		return false
	}
	c := l.queues[port][l.pos[port]]
	l.current = &c
	l.obs.begin(fmt.Sprintf("%s/%s/%d", c.Port, c.Step, c.N))
	if !yield(c, nil) {
		l.broken = true
		return false
	}
	l.pos[port]++
	if err := l.done(); err != nil {
		yield(Chunk{}, err)
		return false
	}
	return true
}

// done finishes the current input chunk: it closes its open output files
// and commits its output, counts and log entries together (GO-86, GO-90).
func (l *local) done() error {
	for len(l.open) > 0 {
		if err := l.open[0].Close(); err != nil {
			return err
		}
	}
	src := l.current
	l.current = nil
	return l.commit(src)
}

// commit puts closed output files in their manifests and writes the log
// entries reported since the last commit. src is the input chunk they came
// from, nil for a source or for OnEOS.
func (l *local) commit(src *Chunk) error {
	counts, lines, err := l.obs.commit()
	if err != nil {
		return err
	}
	closed := l.closed
	l.closed = nil
	for i, f := range closed {
		c := chunk.Chunk{Ext: f.ext, Bytes: f.size, SHA256: f.sha256}
		// Counts go with the chunk they were reported for. For a step with
		// input they also count chunks that gave no output, so local mode
		// keeps them in _summary.json only.
		if src == nil && i == len(closed)-1 {
			c.Counts = counts
		}
		if src != nil {
			c.Src = &chunk.Source{Edge: src.Port, Replica: src.Replica, N: src.N}
		}
		if err := l.put(f, c); err != nil {
			return err
		}
	}
	if src != nil {
		stream := src.Port + "/" + fmt.Sprint(src.Replica)
		for _, o := range l.outs {
			o.manifest.SetConsumed(stream, src.N)
		}
	}
	if err := l.writeManifests(); err != nil {
		return err
	}
	return l.appendLog(lines)
}

// put renames a closed file to its chunk name and adds it to the manifest.
func (l *local) put(f *File, c chunk.Chunk) error {
	o := l.outs[f.output]
	c.N = o.manifest.NextN()
	if err := os.Rename(f.path, filepath.Join(o.dir, filepath.Base(o.stream.ChunkKey(c.N, c.Ext)))); err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	if _, err := o.manifest.Append(c); err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	return nil
}

func (l *local) Create(ctx context.Context, output, ext string) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if output == "" {
		output = DefaultOutput
	}
	if !chunk.ValidExt(ext) {
		return nil, fmt.Errorf("%w: extension %q: want lower-case letters and digits, at most 16", ErrUsage, ext)
	}
	o, ok := l.outs[output]
	if !ok {
		dir := filepath.Join(l.cfg.OutputDir, output)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("output %s: %w", output, err)
		}
		o = &localOutput{
			dir:      dir,
			manifest: chunk.NewManifest("local", "local", output, l.cfg.Replica, 1),
			stream:   chunk.EdgeStream("local", output, l.cfg.Replica),
		}
		l.outs[output] = o
	}
	// A temporary name until the chunk is committed and gets its number.
	f, err := os.CreateTemp(o.dir, ".tmp-*."+ext)
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", output, err)
	}
	// Output is there to be read, also by another user than the
	// component's, such as after alpheus replay in a container.
	if err := f.Chmod(0o644); err != nil {
		f.Close()
		return nil, fmt.Errorf("output %s: %w", output, err)
	}
	file := &File{f: f, path: f.Name(), output: output, ext: ext, onClose: l.closedFile}
	l.open = append(l.open, file)
	return file, nil
}

// closedFile is called when the component closes an output file. Its size
// and SHA-256 are taken from the file as it is now, so they also cover what
// other tools wrote to Path. A source's chunk is committed at once (GO-91);
// otherwise it waits for Done (GO-90).
func (l *local) closedFile(f *File) error {
	r, err := os.Open(f.path)
	if err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	defer r.Close()
	hw := chunk.NewHashingWriter(io.Discard)
	if _, err := io.Copy(hw, r); err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	f.size, f.sha256 = hw.Size(), hw.Sum()
	if l.cfg.MaxBytes > 0 && f.size > l.cfg.MaxBytes {
		return fmt.Errorf("output %s: chunk is %d bytes, more than maxBytes %d", f.output, f.size, l.cfg.MaxBytes)
	}
	l.open = slices.DeleteFunc(l.open, func(o *File) bool { return o == f })
	l.closed = append(l.closed, f)
	if l.current == nil && len(l.cfg.Inputs) == 0 {
		return l.commit(nil)
	}
	return nil
}

// finish runs after the handler returned nil: all input must have been
// read, then OnEOS runs and every output ends.
func (l *local) finish(ctx context.Context) error {
	if l.broken {
		return errors.New("component returned in the middle of an input chunk")
	}
	for _, port := range l.ports {
		if l.pos[port] < len(l.queues[port]) {
			return fmt.Errorf("component returned before reading all of input %s", port)
		}
	}
	for _, fn := range l.eosFns {
		if err := fn(ctx, l); err != nil {
			return fmt.Errorf("on EOS: %w", err)
		}
	}
	for len(l.open) > 0 {
		if err := l.open[0].Close(); err != nil {
			return err
		}
	}
	if err := l.commit(nil); err != nil {
		return err
	}
	for _, o := range l.outs {
		o.manifest.End()
	}
	return l.writeManifests()
}

func (l *local) writeManifests() error {
	for name, o := range l.outs {
		data, err := o.manifest.Marshal()
		if err != nil {
			return fmt.Errorf("output %s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(o.dir, "manifest.json"), data, 0o644); err != nil {
			return fmt.Errorf("output %s: %w", name, err)
		}
	}
	return nil
}

func (l *local) appendLog(lines [][]byte) error {
	if len(lines) == 0 {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(l.cfg.OutputDir, ErrorLogFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("error log: %w", err)
	}
	for _, line := range lines {
		f.Write(append(line, '\n'))
	}
	return f.Close()
}

func (l *local) writeSummary() error {
	data, err := json.MarshalIndent(l.obs.summary(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(l.cfg.OutputDir, SummaryFile), append(data, '\n'), 0o644)
}

// File is an output chunk being written: one file from Create to Close
// (GO-104). Write to it, or let another tool write to Path.
type File struct {
	f       *os.File
	path    string
	output  string
	ext     string
	id      int64 // the agent's ID, in agent mode
	onClose func(*File) error
	closed  bool
	size    int64
	sha256  string
}

func (f *File) Write(p []byte) (int, error) { return f.f.Write(p) }

// Path is the file's local path, for tools that write by name.
func (f *File) Path() string { return f.path }

// Close finishes the chunk. Its size and checksum are taken from the file
// as it is now, so they also cover what other tools wrote to Path.
func (f *File) Close() error {
	if f.closed {
		return fmt.Errorf("output %s: chunk closed twice", f.output)
	}
	f.closed = true
	if err := f.f.Close(); err != nil {
		return fmt.Errorf("output %s: %w", f.output, err)
	}
	return f.onClose(f)
}
