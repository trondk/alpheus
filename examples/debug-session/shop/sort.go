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

package shop

import (
	"bufio"
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"alpheus-dev.org/alpheus/sdk"
)

// SortSettings: Bytes is about how large each output chunk is.
type SortSettings struct {
	Bytes int64 `json:"bytes"`
}

// Sort writes all merged orders sorted by region, then by amount, largest
// first, then by order id. It is stateful: it sorts each input chunk into a
// run in TMPDIR, and merges the runs when all input is read. Only one chunk
// is in memory at a time.
func Sort(ctx context.Context, in sdk.Input, out sdk.Output) error {
	s := SortSettings{Bytes: 32 << 20}
	if err := in.Settings(&s); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "sort-runs-")
	if err != nil {
		return err
	}
	slog.DebugContext(ctx, "settings", "bytes", s.Bytes, "runs", dir)
	var runs []string
	// OnEOS runs after this function returns, so the runs are removed
	// there, once they are merged.
	in.OnEOS(func(ctx context.Context, out sdk.Output) error {
		defer os.RemoveAll(dir)
		return mergeRuns(ctx, runs, s.Bytes, out)
	})
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		run := filepath.Join(dir, fmt.Sprintf("run-%04d.csv", len(runs)+1))
		n, err := sortChunk(c, run)
		if err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
		runs = append(runs, run)
		slog.DebugContext(ctx, "sorted a chunk into a run", "from", c.Step, "n", c.N, "orders", n, "run", filepath.Base(run))
		sdk.Progress(ctx, fmt.Sprintf("%d runs", len(runs)))
	}
	return nil
}

// row is a merged order with its sort key.
type row struct {
	region string
	cents  int64
	id     string
	line   string
}

func parseRow(line string) (row, error) {
	f := strings.Split(line, ",")
	if len(f) != 7 {
		return row{}, fmt.Errorf("%d columns, want 7", len(f))
	}
	whole, frac, _ := strings.Cut(f[3], ".")
	cents, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return row{}, fmt.Errorf("amount %q: %w", f[3], err)
	}
	return row{region: f[5], cents: cents, id: f[0], line: line}, nil
}

func less(a, b row) int {
	return cmp.Or(cmp.Compare(a.region, b.region), cmp.Compare(b.cents, a.cents), cmp.Compare(a.id, b.id))
}

func sortChunk(c sdk.Chunk, run string) (int, error) {
	src, err := c.Open()
	if err != nil {
		return 0, err
	}
	defer src.Close()
	sc := bufio.NewScanner(bufio.NewReaderSize(src, 1<<20))
	if !sc.Scan() || sc.Text() != MergedHeader {
		return 0, fmt.Errorf("the first line is not the header %q", MergedHeader)
	}
	var rows []row
	for line := 2; sc.Scan(); line++ {
		r, err := parseRow(sc.Text())
		if err != nil {
			return 0, fmt.Errorf("line %d: %w", line, err)
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	slices.SortFunc(rows, less)
	f, err := os.Create(run)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for _, r := range rows {
		w.WriteString(r.line)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return 0, err
	}
	return len(rows), f.Close()
}

// runReader is one run being merged.
type runReader struct {
	sc  *bufio.Scanner
	f   *os.File
	cur row
}

type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return less(h[i].cur, h[j].cur) < 0 }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)        { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

func (r *runReader) next() (bool, error) {
	if !r.sc.Scan() {
		return false, r.sc.Err()
	}
	var err error
	r.cur, err = parseRow(r.sc.Text())
	return true, err
}

// mergeRuns writes the runs merged in order, as chunks of about size bytes,
// each with the header.
func mergeRuns(ctx context.Context, runs []string, size int64, out sdk.Output) error {
	h := runHeap{}
	for _, name := range runs {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		r := &runReader{sc: bufio.NewScanner(bufio.NewReaderSize(f, 1<<20)), f: f}
		ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			h = append(h, r)
		}
	}
	heap.Init(&h)
	var (
		dst     *sdk.File
		w       *bufio.Writer
		written int64
		rows    int64
	)
	closeChunk := func() error {
		if dst == nil {
			return nil
		}
		if err := w.Flush(); err != nil {
			return err
		}
		err := dst.Close()
		dst = nil
		return err
	}
	for h.Len() > 0 {
		r := h[0]
		if dst == nil {
			var err error
			if dst, err = out.Create(ctx, sdk.DefaultOutput, "csv"); err != nil {
				return err
			}
			w = bufio.NewWriterSize(dst, 1<<20)
			n, _ := io.WriteString(w, MergedHeader+"\n")
			written = int64(n)
		}
		w.WriteString(r.cur.line)
		w.WriteByte('\n')
		written += int64(len(r.cur.line)) + 1
		rows++
		ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
		if written >= size {
			if err := closeChunk(); err != nil {
				return err
			}
		}
	}
	if err := closeChunk(); err != nil {
		return err
	}
	sdk.Count(ctx, "orders", rows)
	slog.DebugContext(ctx, "merged the runs", "runs", len(runs), "orders", rows)
	return nil
}
