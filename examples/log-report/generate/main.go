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

// Command generate is the source of the log-report example: it writes
// chunks of log lines, every seventh line an ERROR.
//
// Settings: {"chunks": 20, "lines": 5000}
package main

import (
	"bufio"
	"context"
	"fmt"

	"alpheus-dev.org/alpheus/sdk"
)

type settings struct {
	Chunks int `json:"chunks"`
	Lines  int `json:"lines"`
}

func main() { sdk.Run(handle) }

// A source has no input: it only creates output chunks. Each closed chunk
// is committed at once. A source must make the same chunks every time it
// runs with the same settings, since a restarted source starts over.
func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s settings
	if err := in.Settings(&s); err != nil {
		return err
	}
	for n := range s.Chunks {
		f, err := out.Create(ctx, sdk.DefaultOutput, "log")
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		for i := range s.Lines {
			level := "INFO"
			if (n*s.Lines+i)%7 == 0 {
				level = "ERROR"
			}
			fmt.Fprintf(w, "%s line %d of chunk %d\n", level, i+1, n+1)
		}
		if err := w.Flush(); err != nil {
			return err
		}
		// Counted before Close, so the count is committed with this chunk.
		sdk.Count(ctx, "lines", int64(s.Lines))
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
