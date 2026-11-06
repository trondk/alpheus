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

// Command grep is the template for an Alpheus component in Go. It keeps the
// lines of each input chunk that contain a text, and writes them as one
// output chunk per input chunk, with the input's file extension. A chunk
// without matching lines gives no output. Lines are copied byte for byte,
// so with an empty match the output is the input.
//
// Settings: {"match": "ERROR", "invert": false}
//
// It counts the units lines_in and lines_out, and warns about an empty
// input chunk with the code empty_chunk. At debug level it logs its
// settings and each chunk: set logLevel: debug on the step, or run
// alpheus run --log-level <step>=debug; alpheus replay always does.
//
// Copy this folder to start a component: replace handle with your own,
// keep sdk.Run in main.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"alpheus-dev.org/alpheus/sdk"
)

// settings are the step's settings in the StreamRun. Unknown fields are an
// error, so a misspelt setting is not silently ignored.
type settings struct {
	Match  string `json:"match"`
	Invert bool   `json:"invert"`
}

func main() {
	// Run connects to the agent next to the component in the pod, or runs
	// in local mode with ALPHEUS_MODE=local, and exits with the right
	// code: 0 done, 1 failed, 2 wrong settings or use.
	sdk.Run(handle)
}

func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s settings
	if err := in.Settings(&s); err != nil {
		return err
	}
	// The SDK sets slog's default logger to ALPHEUS_LOG_LEVEL, so debug
	// lines cost nothing unless the step's logLevel is debug. Log what
	// helps find a bug, not the data itself, which may be personal.
	slog.DebugContext(ctx, "settings", "match", s.Match, "invert", s.Invert, "replica", in.Replica(), "replicas", in.Replicas())
	// Chunks yields the input chunks of this replica, one file at a time.
	// A chunk is done when the loop asks for the next one: what was
	// written and counted for it is then committed together.
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := grep(ctx, s, c, out); err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
	}
	return nil
}

func grep(ctx context.Context, s settings, c sdk.Chunk, out sdk.Output) error {
	if c.Bytes == 0 {
		sdk.Warn(ctx, "empty_chunk", "the input chunk is empty", sdk.Field("n", c.N))
		return nil
	}
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	var dst *sdk.File // created at the first matching line
	var w *bufio.Writer
	var in, kept int64
	r := bufio.NewReader(src)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			in++
			if bytes.Contains(line, []byte(s.Match)) != s.Invert {
				if dst == nil {
					if dst, err = out.Create(ctx, sdk.DefaultOutput, c.Ext); err != nil {
						return err
					}
					w = bufio.NewWriter(dst)
				}
				if _, err := w.Write(line); err != nil {
					return err
				}
				kept++
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	sdk.Count(ctx, "lines_in", in)
	sdk.Count(ctx, "lines_out", kept)
	slog.DebugContext(ctx, "chunk done", "from", c.Step, "replica", c.Replica, "n", c.N, "bytes", c.Bytes, "lines_in", in, "lines_out", kept)
	if dst == nil {
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	// Close hands the file to the agent, which uploads it when the input
	// chunk is done.
	return dst.Close()
}
