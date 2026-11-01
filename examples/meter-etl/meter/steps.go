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

package meter

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"alpheus-dev.org/alpheus/sdk"
)

// Outputs of clean: DatasetOutput gets the cleaned readings, and the
// default output a partial total per input chunk for aggregate.
const DatasetOutput = "dataset"

// Export is the source: it writes Gen's files as gzipped CSV, one output
// chunk per file. Settings are a Gen. With replicas, each writes the files
// whose number modulo the replicas is its own. It counts files, rows and
// bytes, the uncompressed size.
func Export(ctx context.Context, in sdk.Input, out sdk.Output) error {
	g := Gen{Files: 8, Rows: 1_000_000, Seed: 1, BadPerMille: 5}
	if err := in.Settings(&g); err != nil {
		return err
	}
	if _, err := g.day(); err != nil {
		return fmt.Errorf("%w: day: %v", sdk.ErrUsage, err)
	}
	for i := range g.Files {
		if i%in.Replicas() != in.Replica() {
			continue
		}
		f, err := out.Create(ctx, sdk.DefaultOutput, "gz")
		if err != nil {
			return err
		}
		zw, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
		w := bufio.NewWriterSize(zw, 1<<20)
		w.WriteString(Header + "\n")
		raw := int64(len(Header) + 1)
		err = g.rows(i, func(line []byte, _ bool, _ Key, _ int64) error {
			w.Write(line)
			raw += int64(len(line)) + 1
			return w.WriteByte('\n')
		})
		if err == nil {
			err = w.Flush()
		}
		if err == nil {
			err = zw.Close()
		}
		if err != nil {
			return err
		}
		// Counted before Close, so the counts are committed with this file.
		sdk.Count(ctx, "files", 1)
		sdk.Count(ctx, "rows", int64(g.Rows))
		sdk.Count(ctx, "bytes", raw)
		if err := f.Close(); err != nil {
			return err
		}
		sdk.Progress(ctx, fmt.Sprintf("file %d of %d", i+1, g.Files))
	}
	return nil
}

// splitSettings: Bytes is the uncompressed size to cut at; each output
// chunk ends at the first line end after it.
type splitSettings struct {
	Bytes int64 `json:"bytes"`
}

// Split cuts each gzipped input file into gzipped chunks of about Bytes
// uncompressed, at line ends, each with the header, so the next step can
// work on them in parallel. It counts rows and bytes, uncompressed.
func Split(ctx context.Context, in sdk.Input, out sdk.Output) error {
	s := splitSettings{Bytes: 32 << 20}
	if err := in.Settings(&s); err != nil {
		return err
	}
	if s.Bytes < 1024 {
		return fmt.Errorf("%w: bytes must be at least 1024", sdk.ErrUsage)
	}
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := split(ctx, s, c, out); err != nil {
			return fmt.Errorf("chunk %d: %w", c.N, err)
		}
	}
	return nil
}

func split(ctx context.Context, s splitSettings, c sdk.Chunk, out sdk.Output) error {
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(src, 1<<20))
	if err != nil {
		return err
	}
	r := bufio.NewReaderSize(zr, 1<<20)
	header, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("no header: %w", err)
	}
	var (
		dst  *sdk.File
		zw   *gzip.Writer
		w    *bufio.Writer
		size int64
	)
	closeChunk := func() error {
		if dst == nil {
			return nil
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		err := dst.Close()
		dst = nil
		return err
	}
	var rows, bytes int64
	for {
		line, rerr := r.ReadSlice('\n')
		if rerr == bufio.ErrBufferFull {
			return fmt.Errorf("a line longer than %d bytes", r.Size())
		}
		if len(line) > 0 {
			if dst == nil {
				if dst, err = out.Create(ctx, sdk.DefaultOutput, "gz"); err != nil {
					return err
				}
				zw, _ = gzip.NewWriterLevel(dst, gzip.BestSpeed)
				w = bufio.NewWriterSize(zw, 1<<20)
				w.WriteString(header)
				size = 0
			}
			w.Write(line)
			size += int64(len(line))
			rows++
			bytes += int64(len(line))
			if size >= s.Bytes {
				if err := closeChunk(); err != nil {
					return err
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	sdk.Count(ctx, "rows", rows)
	sdk.Count(ctx, "bytes", bytes)
	return closeChunk()
}

// Clean checks every row of each input chunk. Valid rows go to the dataset
// output, gzipped, with an hour column added; each bad row is logged as an
// error with its code and line. A partial total per region and hour goes
// to the default output. It counts rows_in, rows_out and rows_rejected.
func Clean(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s struct{}
	if err := in.Settings(&s); err != nil {
		return err
	}
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := clean(ctx, c, out); err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
	}
	return nil
}

func clean(ctx context.Context, c sdk.Chunk, out sdk.Output) error {
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(src, 1<<20))
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	if !sc.Scan() || sc.Text() != Header {
		return fmt.Errorf("the first line is not the header %q", Header)
	}

	dst, err := out.Create(ctx, DatasetOutput, "gz")
	if err != nil {
		return err
	}
	zw, _ := gzip.NewWriterLevel(dst, gzip.BestSpeed)
	w := bufio.NewWriterSize(zw, 1<<20)
	w.WriteString(Header + ",hour\n")

	sums := map[Key]Sum{}
	var rowsIn, rowsOut, rejected int64
	var buf []byte
	for line := int64(2); sc.Scan(); line++ {
		rowsIn++
		r, err := Parse(sc.Text())
		if err != nil {
			rejected++
			sdk.Error(ctx, err.Error(), "row rejected", sdk.Field("line", line))
			continue
		}
		rowsOut++
		k := Key{r.Region, r.Time.Hour()}
		s := sums[k]
		s.Readings++
		s.Wh += r.Wh
		sums[k] = s
		buf = append(buf[:0], r.Meter...)
		buf = append(buf, ',')
		buf = append(buf, r.Region...)
		buf = append(buf, ',')
		buf = r.Time.AppendFormat(buf, time.RFC3339)
		buf = append(buf, ',')
		buf = appendKWh(buf, r.Wh)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(k.Hour), 10)
		buf = append(buf, '\n')
		w.Write(buf)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}
	if err := writeSums(ctx, out, sums); err != nil {
		return err
	}
	sdk.Count(ctx, "rows_in", rowsIn)
	sdk.Count(ctx, "rows_out", rowsOut)
	sdk.Count(ctx, "rows_rejected", rejected)
	return nil
}

// SumsHeader is the first line of a file of totals.
const SumsHeader = "region,hour,readings,wh"

// writeSums writes totals as CSV, sorted by region and hour.
func writeSums(ctx context.Context, out sdk.Output, sums map[Key]Sum) error {
	f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, SumsHeader)
	for _, k := range sortedKeys(sums) {
		fmt.Fprintf(w, "%s,%d,%d,%d\n", k.Region, k.Hour, sums[k].Readings, sums[k].Wh)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Close()
}

func sortedKeys(sums map[Key]Sum) []Key {
	return slices.SortedFunc(maps.Keys(sums), func(a, b Key) int {
		return cmp.Or(strings.Compare(a.Region, b.Region), cmp.Compare(a.Hour, b.Hour))
	})
}

// ParseSums reads a file of totals.
func ParseSums(r io.Reader) (map[Key]Sum, error) {
	sums := map[Key]Sum{}
	sc := bufio.NewScanner(r)
	if !sc.Scan() || sc.Text() != SumsHeader {
		return nil, fmt.Errorf("the first line is not the header %q", SumsHeader)
	}
	for sc.Scan() {
		f := strings.Split(sc.Text(), ",")
		if len(f) != 4 {
			return nil, fmt.Errorf("bad line %q", sc.Text())
		}
		hour, err1 := strconv.Atoi(f[1])
		n, err2 := strconv.ParseInt(f[2], 10, 64)
		wh, err3 := strconv.ParseInt(f[3], 10, 64)
		if err := cmp.Or(err1, err2, err3); err != nil {
			return nil, fmt.Errorf("bad line %q: %w", sc.Text(), err)
		}
		s := sums[Key{f[0], hour}]
		s.Readings += n
		s.Wh += wh
		sums[Key{f[0], hour}] = s
	}
	return sums, sc.Err()
}

// Aggregate adds up clean's partial totals and writes the totals per
// region and hour at the end of the stream (a stateful step). It counts
// partials.
func Aggregate(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s struct{}
	if err := in.Settings(&s); err != nil {
		return err
	}
	total := map[Key]Sum{}
	in.OnEOS(func(ctx context.Context, out sdk.Output) error { return writeSums(ctx, out, total) })
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		f, err := c.Open()
		if err != nil {
			return err
		}
		sums, err := ParseSums(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
		for k, s := range sums {
			t := total[k]
			t.Readings += s.Readings
			t.Wh += s.Wh
			total[k] = t
		}
		sdk.Count(ctx, "partials", 1)
	}
	return nil
}
