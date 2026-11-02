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

// Command bench is the test component of testcase01 to testcase06 (SPEC
// 12.1). It has three roles:
//
//   - source writes chunks chunks of size bytes. Each starts with a header
//     of 16 bytes, the chunk's number and the time it was started, as
//     little-endian uint64 and Unix nanoseconds, followed by bytes from a
//     ChaCha8 generator seeded by seed and the number. With fixed, the
//     time is 0, so every attempt makes the same chunks, as a restarted
//     source must (GO-92).
//
//   - pass copies each input chunk to the default output unchanged. With
//     drop, it drops the chunks whose number in its input stream is a
//     multiple of drop, and counts them in the unit dropped. With fail, it
//     fails on the chunk whose header has that number, as a component
//     with a bug does, and logs each chunk at debug level first (GO-141).
//
//   - sink reads each chunk, notes when it got it, and checks its bytes
//     against the generator. It expects every chunk once on each of its
//     ports, except the chunks whose number is a multiple of skip. At EOS it writes the CSV chunk
//
//     port,n,bytes,written_ns,received_ns
//
//     to the default output, one line per chunk, by port and number. It
//     fails if a chunk is wrong, comes twice on a port, comes on a port
//     not in ports, should have been skipped, or is missing.
//
// Settings: {"role": "source", "chunks": 64, "size": 4194304, "seed": 1}.
// sink needs the same chunks, size and seed as source, and takes ports,
// which defaults to ["default"], and skip; pass takes drop. Both take
// delay, milliseconds to wait before each input chunk, to make a step slow.
//
// It counts the units chunks and bytes.
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"time"

	"alpheus-dev.org/alpheus/sdk"
)

// headerSize is the size of a chunk's header.
const headerSize = 16

type settings struct {
	Role   string `json:"role"`
	Chunks int    `json:"chunks"`
	Size   int64  `json:"size"`
	Seed   uint64 `json:"seed"`
	// Ports are the inputs sink expects every chunk on.
	Ports []string `json:"ports"`
	// Drop makes pass drop every chunk whose number is a multiple of it,
	// and Skip makes sink expect that; 0 means none.
	Drop int64 `json:"drop"`
	Skip int64 `json:"skip"`
	// Delay is milliseconds pass and sink wait before each input chunk.
	Delay int `json:"delay"`
	// Fixed makes source write the time 0 in the header.
	Fixed bool `json:"fixed"`
	// Fail makes pass fail on the chunk with this number in its header;
	// 0 means never.
	Fail uint64 `json:"fail"`
}

func main() {
	sdk.Run(handle)
}

func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s settings
	if err := in.Settings(&s); err != nil {
		return err
	}
	if s.Role != "pass" && (s.Chunks < 1 || s.Size < headerSize) {
		return fmt.Errorf("%w: %s needs chunks of at least 1 and size of at least %d bytes", sdk.ErrUsage, s.Role, headerSize)
	}
	if s.Drop < 0 || s.Skip < 0 || s.Delay < 0 {
		return fmt.Errorf("%w: drop, skip and delay must not be negative", sdk.ErrUsage)
	}
	switch s.Role {
	case "source":
		return source(ctx, s, out)
	case "pass":
		return pass(ctx, s, in, out)
	case "sink":
		return sink(ctx, s, in)
	}
	return fmt.Errorf("%w: role must be source, pass or sink, not %q", sdk.ErrUsage, s.Role)
}

// payload returns chunk n's bytes after the header.
func payload(seed uint64, n int64, size int64) io.Reader {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[0:], seed)
	binary.LittleEndian.PutUint64(key[8:], uint64(n))
	return io.LimitReader(rand.NewChaCha8(key), size-headerSize)
}

func source(ctx context.Context, s settings, out sdk.Output) error {
	for n := int64(1); n <= int64(s.Chunks); n++ {
		f, err := out.Create(ctx, sdk.DefaultOutput, "bin")
		if err != nil {
			return err
		}
		var h [headerSize]byte
		binary.LittleEndian.PutUint64(h[0:], uint64(n))
		if !s.Fixed {
			binary.LittleEndian.PutUint64(h[8:], uint64(time.Now().UnixNano()))
		}
		if _, err := f.Write(h[:]); err != nil {
			return err
		}
		if _, err := io.Copy(f, payload(s.Seed, n, s.Size)); err != nil {
			return err
		}
		// Counted before Close, so the counts are committed with this
		// chunk (testcase12).
		sdk.Count(ctx, "chunks", 1)
		sdk.Count(ctx, "bytes", s.Size)
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// wait waits ms milliseconds, or until ctx is done.
func wait(ctx context.Context, ms int) error {
	if ms == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}

// skipped tells whether chunk n is a multiple of every, when every is set.
func skipped(n, every int64) bool {
	return every > 0 && n%every == 0
}

func pass(ctx context.Context, s settings, in sdk.Input, out sdk.Output) error {
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := wait(ctx, s.Delay); err != nil {
			return err
		}
		if skipped(c.N, s.Drop) {
			sdk.Count(ctx, "dropped", 1)
			continue
		}
		src, err := c.Open()
		if err != nil {
			return err
		}
		if s.Fail > 0 {
			var header [8]byte
			_, err := src.ReadAt(header[:], 0)
			number := binary.LittleEndian.Uint64(header[:])
			slog.Debug("read chunk", "n", c.N, "file", c.Path, "bytes", c.Bytes, "header_number", number)
			if err == nil && number == s.Fail {
				src.Close()
				return fmt.Errorf("chunk %d (source chunk %d) is one this component cannot handle", c.N, number)
			}
		}
		dst, err := out.Create(ctx, sdk.DefaultOutput, c.Ext)
		if err == nil {
			_, err = io.Copy(dst, src)
		}
		src.Close()
		if err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		sdk.Count(ctx, "chunks", 1)
		sdk.Count(ctx, "bytes", c.Bytes)
	}
	return nil
}

type timing struct {
	port              string
	n                 int64
	bytes             int64
	written, received int64
}

func sink(ctx context.Context, s settings, in sdk.Input) error {
	if len(s.Ports) == 0 {
		s.Ports = []string{sdk.DefaultPort}
	}
	var got []timing
	in.OnEOS(func(ctx context.Context, out sdk.Output) error {
		want := s.Chunks
		if s.Skip > 0 {
			want -= s.Chunks / int(s.Skip)
		}
		if want *= len(s.Ports); len(got) != want {
			return fmt.Errorf("got %d chunks, want %d", len(got), want)
		}
		slices.SortFunc(got, func(a, b timing) int {
			return cmp.Or(cmp.Compare(a.port, b.port), cmp.Compare(a.n, b.n))
		})
		f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
		if err != nil {
			return err
		}
		fmt.Fprintln(f, "port,n,bytes,written_ns,received_ns")
		for _, t := range got {
			fmt.Fprintf(f, "%s,%d,%d,%d,%d\n", t.port, t.n, t.bytes, t.written, t.received)
		}
		return f.Close()
	})
	type key struct {
		port string
		n    int64
	}
	seen := make(map[key]bool)
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		received := time.Now().UnixNano()
		if err := wait(ctx, s.Delay); err != nil {
			return err
		}
		t, err := check(s, c.Path)
		if err != nil {
			return fmt.Errorf("chunk %d from %s/%d: %w", c.N, c.Step, c.Replica, err)
		}
		if skipped(t.n, s.Skip) {
			return fmt.Errorf("chunk %d came on port %s, but every %dth should be skipped", t.n, c.Port, s.Skip)
		}
		if !slices.Contains(s.Ports, c.Port) {
			return fmt.Errorf("chunk %d came on port %q, want one of %q", t.n, c.Port, s.Ports)
		}
		if seen[key{c.Port, t.n}] {
			return fmt.Errorf("chunk %d came twice on port %s", t.n, c.Port)
		}
		seen[key{c.Port, t.n}] = true
		t.port, t.received = c.Port, received
		got = append(got, t)
		sdk.Count(ctx, "chunks", 1)
		sdk.Count(ctx, "bytes", t.bytes)
	}
	return nil
}

// check reads a chunk's header and compares the rest with the generator.
func check(s settings, path string) (timing, error) {
	f, err := os.Open(path)
	if err != nil {
		return timing{}, err
	}
	defer f.Close()
	var h [headerSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return timing{}, fmt.Errorf("header: %w", err)
	}
	t := timing{n: int64(binary.LittleEndian.Uint64(h[0:])), written: int64(binary.LittleEndian.Uint64(h[8:]))}
	if t.n < 1 || t.n > int64(s.Chunks) {
		return t, fmt.Errorf("number %d is not from 1 to %d", t.n, s.Chunks)
	}
	want := payload(s.Seed, t.n, s.Size)
	a, b := make([]byte, 256<<10), make([]byte, 256<<10)
	t.bytes = headerSize
	for {
		k, err := io.ReadFull(f, a)
		if k > 0 {
			if _, werr := io.ReadFull(want, b[:k]); werr != nil || !bytes.Equal(a[:k], b[:k]) {
				return t, fmt.Errorf("content differs after byte %d", t.bytes)
			}
			t.bytes += int64(k)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return t, err
		}
	}
	if t.bytes != s.Size {
		return t, fmt.Errorf("%d bytes, want %d", t.bytes, s.Size)
	}
	return t, nil
}
