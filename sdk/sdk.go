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

// Package sdk is how an Alpheus component is written in Go. A component
// reads its input chunks as files and writes its output chunks as files, in
// any format it likes; Alpheus moves the files but never reads them
// (GO-101). The SDK handles everything else: the connection to the agent,
// which chunk is done, shutdown signals and exit codes (GO-46).
//
//	func main() {
//		sdk.Run(func(ctx context.Context, in sdk.Input, out sdk.Output) error {
//			for chunk, err := range in.Chunks(ctx) {
//				if err != nil {
//					return err
//				}
//				dst, err := out.Create(ctx, sdk.DefaultOutput, "csv")
//				if err != nil {
//					return err
//				}
//				if err := filter(chunk.Path, dst); err != nil {
//					return err
//				}
//				if err := dst.Close(); err != nil {
//					return err
//				}
//			}
//			return nil
//		})
//	}
//
// Components report how much they have processed and what went wrong with
// [Count], [Error], [Warn] and [Progress] (spec 10.1).
//
// The same component runs without Kubernetes in local mode, which reads
// input files from a folder and writes its outputs to another (GO-47); see
// [RunLocal] and [Run].
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
)

// DefaultOutput is the output an edge reads when it names none, and
// DefaultPort the input an edge ends in when it names none (GO-108).
const (
	DefaultOutput = "default"
	DefaultPort   = "default"
)

// Handler is a component: it reads input chunks from in and creates output
// chunks with out. Returning nil means the component has finished
// successfully; it must then have read all of its input.
type Handler func(ctx context.Context, in Input, out Output) error

// Chunk is an input chunk: a file in the component's scratch folder.
type Chunk struct {
	// Port is the input the chunk arrived on (GO-108).
	Port string
	// Step and Replica are the producer, and N the chunk's number in the
	// producer's stream.
	Step    string
	Replica int
	N       int64
	// Path is the local file. The agent deletes it when the chunk is done.
	Path string
	// Ext is the file extension the producer gave, without the dot.
	Ext   string
	Bytes int64
}

// Open opens the chunk's file for reading.
func (c Chunk) Open() (*os.File, error) { return os.Open(c.Path) }

// Input is the component's input: the chunks assigned to this replica from
// every incoming edge, and the step's settings.
type Input interface {
	// Chunks yields input chunks from every port. A chunk is done when
	// the component asks for the next one or the loop ends normally;
	// output files created for it and not closed are closed then (GO-86).
	// Output, counts and log entries made while a chunk was handled are
	// committed with it (GO-90, GO-115, GO-118).
	Chunks(ctx context.Context) iter.Seq2[Chunk, error]

	// Port returns the chunks of one input only (GO-109). A component may
	// read a small port to the end first and then stream another; mark the
	// edge to such a port side: true in the flow, so every replica and
	// every new attempt gets all of it (GO-167).
	Port(name string) Port

	// OnEOS registers a function that runs once all input has been read,
	// before the outputs end. Components with state across chunks write
	// their result here (GO-48).
	OnEOS(fn func(ctx context.Context, out Output) error)

	// Settings decodes the step's settings from the StreamRun into v, as
	// JSON. With no settings, v is left as it is.
	Settings(v any) error

	// Replica is this replica's number, from 0, and Replicas how many the
	// step has (GO-75).
	Replica() int
	Replicas() int
}

// Port is one named input.
type Port interface {
	Chunks(ctx context.Context) iter.Seq2[Chunk, error]
}

// Output is where the component's chunks go.
type Output interface {
	// Create starts a new output chunk on a named output, as a file with
	// the given extension (lower-case letters and digits, at most 16;
	// GO-104). Write to the File, or let another tool write to its Path,
	// and Close it. Create may block while consumers catch up
	// (backpressure, GO-35).
	Create(ctx context.Context, output, ext string) (*File, error)
}

// ErrUsage means the component was started or used wrongly: no agent and no
// local mode, a bad setting or a bad counter unit. Run exits with code 2 for
// it.
var ErrUsage = errors.New("usage")

// Exit codes of Run.
const (
	ExitOK      = 0
	ExitFailed  = 1
	ExitUsage   = 2
	ExitStopped = 143 // SIGTERM, as a shell reports it
)

// Environment variables Run reads.
const (
	// EnvMode selects how the component runs: "local", or empty when it is
	// started by the agent.
	EnvMode = "ALPHEUS_MODE"
	// EnvLocalInput is the folder of the default port in local mode, and
	// EnvLocalOutput the folder the outputs go to.
	EnvLocalInput  = "ALPHEUS_LOCAL_INPUT"
	EnvLocalOutput = "ALPHEUS_LOCAL_OUTPUT"
	// EnvSettings holds the step's settings as JSON.
	EnvSettings = "ALPHEUS_SETTINGS"
	// EnvReplica and EnvReplicas say which replica this is (GO-75).
	EnvReplica  = "ALPHEUS_REPLICA"
	EnvReplicas = "ALPHEUS_REPLICAS"
	// EnvSocket is the agent's Unix socket, set by the controller.
	EnvSocket = "ALPHEUS_SOCKET"
	// EnvLocalInputs gives local mode several ports, as
	// port=folder,port=folder; alpheus replay sets it (GO-141).
	EnvLocalInputs = "ALPHEUS_LOCAL_INPUTS"
	// EnvLogLevel is the level of the log, and of slog's default logger:
	// debug, info (the default), warn or error. alpheus replay sets debug
	// (GO-141).
	EnvLogLevel = "ALPHEUS_LOG_LEVEL"
)

// Run runs the component and exits the process with its exit code. It
// stops the component on SIGTERM or SIGINT by cancelling ctx.
//
// With ALPHEUS_MODE=local it runs in local mode with the folders in
// ALPHEUS_LOCAL_INPUT and ALPHEUS_LOCAL_OUTPUT; otherwise it connects to
// the agent at ALPHEUS_SOCKET.
func Run(h Handler) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv(EnvLogLevel))); err != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("component", component())
	// A component that logs with slog logs at the same level, to the same
	// place.
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	err := run(ctx, log, recovering(log, h))
	// The code first: stop cancels ctx, which would make every error look
	// like SIGTERM.
	code := exitCode(ctx, err)
	stop()
	if err != nil {
		log.Error("component failed", "error", err.Error(), "exit_code", code)
		terminationMessage(err.Error())
	}
	os.Exit(code)
}

// recovering turns a panic in the handler into an error, so a component
// with a bug fails with exit code 1 like any other failure, not 2, which
// means wrong settings. The error says where it panicked, in the
// component's code, and the whole stack goes to the log.
func recovering(log *slog.Logger, h Handler) Handler {
	return func(ctx context.Context, in Input, out Output) (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			log.Error("component panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("panic: %v%s", r, panicSite())
		}()
		return h(ctx, in, out)
	}
}

// panicSite is " at file:line" of the frame that panicked: the first one
// outside the Go runtime and this package.
func panicSite() string {
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	for {
		f, more := frames.Next()
		if !strings.HasPrefix(f.Function, "runtime.") && !strings.HasPrefix(f.Function, "alpheus-dev.org/alpheus/sdk.") && f.File != "" {
			return fmt.Sprintf(" at %s:%d", path.Join(path.Base(path.Dir(f.File)), path.Base(f.File)), f.Line)
		}
		if !more {
			return ""
		}
	}
}

// terminationMessage leaves why the component failed where Kubernetes
// keeps it after the pod is gone, so the run's status can say it (GO-139).
// The file exists only in a container.
func terminationMessage(msg string) {
	const path = "/dev/termination-log"
	if _, err := os.Stat(path); err != nil {
		return
	}
	if len(msg) > 2048 {
		msg = msg[:2048]
	}
	_ = os.WriteFile(path, []byte(msg), 0o644)
}

func run(ctx context.Context, log *slog.Logger, h Handler) error {
	switch mode := os.Getenv(EnvMode); mode {
	case "local":
		cfg, err := localConfigFromEnv()
		if err != nil {
			return err
		}
		cfg.Log = log
		return RunLocal(ctx, cfg, h)
	case "":
		if os.Getenv(EnvSocket) == "" {
			return fmt.Errorf("%w: no agent (%s is not set); for local mode set %s=local, %s and %s",
				ErrUsage, EnvSocket, EnvMode, EnvLocalInput, EnvLocalOutput)
		}
		cfg := AgentConfig{Socket: os.Getenv(EnvSocket), Settings: []byte(os.Getenv(EnvSettings))}
		var err error
		if cfg.Replica, err = intEnv(EnvReplica, 0); err != nil {
			return err
		}
		if cfg.Replicas, err = intEnv(EnvReplicas, 1); err != nil {
			return err
		}
		return RunAgent(ctx, cfg, h)
	default:
		return fmt.Errorf("%w: %s=%q, want local or empty", ErrUsage, EnvMode, mode)
	}
}

func exitCode(ctx context.Context, err error) int {
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, ErrUsage):
		return ExitUsage
	case ctx.Err() != nil:
		return ExitStopped
	}
	return ExitFailed
}

func localConfigFromEnv() (LocalConfig, error) {
	cfg := LocalConfig{
		OutputDir: os.Getenv(EnvLocalOutput),
		Settings:  []byte(os.Getenv(EnvSettings)),
	}
	if dir := os.Getenv(EnvLocalInput); dir != "" {
		cfg.Inputs = map[string]string{DefaultPort: dir}
	}
	if v := os.Getenv(EnvLocalInputs); v != "" {
		cfg.Inputs = map[string]string{}
		for _, pair := range strings.Split(v, ",") {
			port, dir, ok := strings.Cut(pair, "=")
			if !ok || port == "" || dir == "" {
				return cfg, fmt.Errorf("%w: %s=%q, want port=folder,port=folder", ErrUsage, EnvLocalInputs, v)
			}
			cfg.Inputs[port] = dir
		}
	}
	var err error
	if cfg.Replica, err = intEnv(EnvReplica, 0); err != nil {
		return cfg, err
	}
	if cfg.Replicas, err = intEnv(EnvReplicas, 1); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func intEnv(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q is not a number", ErrUsage, name, v)
	}
	return n, nil
}

func component() string {
	if len(os.Args) == 0 {
		return ""
	}
	return filepath.Base(os.Args[0])
}

// decodeSettings decodes JSON settings into v, rejecting unknown fields so
// a misspelt setting is an error and not silently ignored.
func decodeSettings(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: settings: %w", ErrUsage, err)
	}
	return nil
}
