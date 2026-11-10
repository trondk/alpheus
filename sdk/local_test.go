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

package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/sdk"
)

// inputs writes one file per content, named so they sort in order.
func inputs(t *testing.T, ext string, contents ...string) string {
	t.Helper()
	dir := t.TempDir()
	for i, c := range contents {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("in-%03d.%s", i+1, ext)), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// manifest reads and validates an output's manifest.
func manifest(t *testing.T, out, output string) *chunk.Manifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(out, output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	m, err := chunk.ParseManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// contents returns the content of each chunk of an output, in order.
func contents(t *testing.T, out, output string) []string {
	t.Helper()
	m := manifest(t, out, output)
	var got []string
	for _, c := range m.Chunks {
		data, err := os.ReadFile(filepath.Join(out, output, fmt.Sprintf("part-%06d.%s", c.N, c.Ext)))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(data))
	}
	return got
}

func create(ctx context.Context, out sdk.Output, output, ext, content string) error {
	f, err := out.Create(ctx, output, ext)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(f, content); err != nil {
		return err
	}
	return f.Close()
}

// copyThrough writes each input chunk unchanged to the default output.
func copyThrough(ctx context.Context, in sdk.Input, out sdk.Output) error {
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		data, err := os.ReadFile(c.Path)
		if err != nil {
			return err
		}
		if err := create(ctx, out, sdk.DefaultOutput, c.Ext, string(data)); err != nil {
			return err
		}
	}
	return nil
}

// TestCopyThrough runs a component that copies its input in local mode:
// every chunk comes out unchanged, with its extension and source chunk, and
// the output ends with EOS.
func TestCopyThrough(t *testing.T) {
	in, out := inputs(t, "csv", "a\n", "b\n", "c\n"), t.TempDir()
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out}
	if err := sdk.RunLocal(context.Background(), cfg, copyThrough); err != nil {
		t.Fatal(err)
	}
	if got := contents(t, out, "default"); !slices.Equal(got, []string{"a\n", "b\n", "c\n"}) {
		t.Errorf("chunks %q", got)
	}
	m := manifest(t, out, "default")
	if !m.EOS || m.Chunks[0].Ext != "csv" || m.Chunks[2].Src.N != 3 || !m.IsConsumed("default/0", 3) {
		t.Errorf("manifest %+v", m)
	}
}

// An output folder, with its manifest, can be the next step's input: the
// manifest is not a chunk.
func TestChainedSteps(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	in := inputs(t, "csv", "a\n", "b\n")
	if err := sdk.RunLocal(context.Background(), sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: first}, copyThrough); err != nil {
		t.Fatal(err)
	}
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: filepath.Join(first, "default")}, OutputDir: second}
	if err := sdk.RunLocal(context.Background(), cfg, copyThrough); err != nil {
		t.Fatal(err)
	}
	if got := contents(t, second, "default"); !slices.Equal(got, []string{"a\n", "b\n"}) {
		t.Errorf("chunks of the second step %q", got)
	}
}

// A source's chunks are committed when closed, with the counts reported
// since the last one (GO-91, GO-115).
func TestSourceCommitsOnClose(t *testing.T) {
	out := t.TempDir()
	source := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for part := range 3 {
			sdk.Count(ctx, "rows", 10)
			if err := create(ctx, out, sdk.DefaultOutput, "jsonl", fmt.Sprintf("part %d\n", part)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := sdk.RunLocal(context.Background(), sdk.LocalConfig{OutputDir: out}, source); err != nil {
		t.Fatal(err)
	}
	m := manifest(t, out, "default")
	if len(m.Chunks) != 3 || m.Chunks[1].Counts["rows"] != 10 || m.Chunks[0].Src != nil || !m.EOS {
		t.Errorf("manifest %+v", m)
	}
}

// Files left open are closed when the input chunk is done (GO-86), and
// another tool may write to the file's path.
func TestAutoCloseAndPath(t *testing.T) {
	in, out := inputs(t, "csv", "x"), t.TempDir()
	h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for _, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			f, err := out.Create(ctx, "parts", "txt")
			if err != nil {
				return err
			}
			if err := os.WriteFile(f.Path(), []byte("written by name"), 0o644); err != nil {
				return err
			}
		}
		return nil
	}
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out}
	if err := sdk.RunLocal(context.Background(), cfg, h); err != nil {
		t.Fatal(err)
	}
	if got := contents(t, out, "parts"); !slices.Equal(got, []string{"written by name"}) {
		t.Errorf("chunks %q", got)
	}
}

// Chunks takes ports in turn; Port reads one port only (GO-109).
func TestPorts(t *testing.T) {
	ctx := context.Background()
	big := inputs(t, "csv", "b1", "b2", "b3")
	lookup := inputs(t, "csv", "l1", "l2")

	var order []string
	roundRobin := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for c, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			data, _ := os.ReadFile(c.Path)
			order = append(order, string(data))
		}
		return nil
	}
	cfg := sdk.LocalConfig{Inputs: map[string]string{"big": big, "lookup": lookup}, OutputDir: t.TempDir()}
	if err := sdk.RunLocal(ctx, cfg, roundRobin); err != nil {
		t.Fatal(err)
	}
	if want := []string{"b1", "l1", "b2", "l2", "b3"}; !slices.Equal(order, want) {
		t.Errorf("order %v, want %v", order, want)
	}

	order = nil
	lookupFirst := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for c, err := range in.Port("lookup").Chunks(ctx) {
			if err != nil {
				return err
			}
			order = append(order, c.Port)
		}
		for c, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			order = append(order, c.Port)
		}
		return nil
	}
	if err := sdk.RunLocal(ctx, cfg, lookupFirst); err != nil {
		t.Fatal(err)
	}
	if want := []string{"lookup", "lookup", "big", "big", "big"}; !slices.Equal(order, want) {
		t.Errorf("order %v, want %v", order, want)
	}
}

// A replica reads only its own chunks: chunk n goes to replica n mod
// replicas (GO-76).
func TestReplica(t *testing.T) {
	in, out := inputs(t, "csv", "1", "2", "3", "4", "5", "6"), t.TempDir()
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out, Replica: 1, Replicas: 3}
	if err := sdk.RunLocal(context.Background(), cfg, copyThrough); err != nil {
		t.Fatal(err)
	}
	if got := contents(t, out, "default"); !slices.Equal(got, []string{"1", "4"}) {
		t.Errorf("chunks %q, want [1 4]", got)
	}
}

// State across chunks is written at EOS (GO-48).
func TestOnEOS(t *testing.T) {
	in, out := inputs(t, "csv", "aa", "bbb"), t.TempDir()
	sum := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		var total int64
		in.OnEOS(func(ctx context.Context, out sdk.Output) error {
			return create(ctx, out, "result", "txt", fmt.Sprint(total))
		})
		for c, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			total += c.Bytes
		}
		return nil
	}
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out}
	if err := sdk.RunLocal(context.Background(), cfg, sum); err != nil {
		t.Fatal(err)
	}
	if got := contents(t, out, "result"); !slices.Equal(got, []string{"5"}) {
		t.Errorf("result %q, want [5]", got)
	}
}

// TestErrors checks that local mode fails a component that returns in the
// middle, gives a bad extension, writes a chunk over maxBytes, uses a bad
// counter unit or more than 16 units, or reads a port it does not have.
func TestErrors(t *testing.T) {
	ctx := context.Background()
	in := inputs(t, "csv", "a", "b")
	inputs := map[string]string{sdk.DefaultPort: in}

	t.Run("returns in the middle", func(t *testing.T) {
		early := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			for range in.Chunks(ctx) {
				break
			}
			return nil
		}
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{Inputs: inputs, OutputDir: t.TempDir()}, early); err == nil {
			t.Error("accepted")
		}
	})

	t.Run("bad extension", func(t *testing.T) {
		h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			_, err := out.Create(ctx, sdk.DefaultOutput, "tar.gz")
			return err
		}
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{OutputDir: t.TempDir()}, h); !errors.Is(err, sdk.ErrUsage) {
			t.Errorf("%v, want ErrUsage", err)
		}
	})

	t.Run("chunk over maxBytes", func(t *testing.T) {
		h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			return create(ctx, out, sdk.DefaultOutput, "txt", strings.Repeat("x", 11))
		}
		err := sdk.RunLocal(ctx, sdk.LocalConfig{OutputDir: t.TempDir(), MaxBytes: 10}, h)
		if err == nil || !strings.Contains(err.Error(), "maxBytes 10") {
			t.Errorf("%v, want a maxBytes error", err)
		}
	})

	t.Run("bad counter unit", func(t *testing.T) {
		h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			sdk.Count(ctx, "Rows", 1)
			return nil
		}
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{OutputDir: t.TempDir()}, h); !errors.Is(err, sdk.ErrUsage) {
			t.Errorf("%v, want ErrUsage", err)
		}
	})

	t.Run("more than 16 counter units", func(t *testing.T) {
		h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			for i := range 17 {
				sdk.Count(ctx, fmt.Sprintf("unit_%d", i), 1)
			}
			return nil
		}
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{OutputDir: t.TempDir()}, h); !errors.Is(err, sdk.ErrUsage) {
			t.Errorf("%v, want ErrUsage", err)
		}
	})

	t.Run("unknown port", func(t *testing.T) {
		h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			for _, err := range in.Port("nope").Chunks(ctx) {
				return err
			}
			return nil
		}
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{Inputs: inputs, OutputDir: t.TempDir()}, h); !errors.Is(err, sdk.ErrUsage) {
			t.Errorf("%v, want ErrUsage", err)
		}
	})
}

// An index in the input folder, as a replay writes, gives the chunks it
// names their producer and number; the others keep the defaults.
func TestLocalIndex(t *testing.T) {
	in, out := inputs(t, "csv", "a\n", "b\n"), t.TempDir()
	index := `{"in-001.csv": {"step": "filter", "replica": 2, "n": 6}}`
	if err := os.WriteFile(filepath.Join(in, chunk.LocalIndex), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	var got []string
	h := func(ctx context.Context, in sdk.Input, out sdk.Output) error {
		for c, err := range in.Chunks(ctx) {
			if err != nil {
				return err
			}
			got = append(got, fmt.Sprintf("%s/%d #%d", c.Step, c.Replica, c.N))
		}
		return nil
	}
	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out}
	if err := sdk.RunLocal(context.Background(), cfg, h); err != nil {
		t.Fatal(err)
	}
	if want := []string{"filter/2 #6", "default/0 #2"}; !slices.Equal(got, want) {
		t.Errorf("chunks %q, want %q", got, want)
	}
}
