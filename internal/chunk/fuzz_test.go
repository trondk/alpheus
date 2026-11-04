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

package chunk

import (
	"bytes"
	"strings"
	"testing"
)

// The fuzz tests feed the parsers files as they could come from storage:
// cut off by a crash, written by another version, or changed by hand
// (GO-58). A parser must never panic, and what it accepts must be a file
// the agent can work on: it marshals, parses again to the same bytes, and
// can be appended to and ended. make fuzz runs each for FUZZTIME.

func seedChunk(n int64) Chunk {
	return Chunk{N: n, Ext: "bin", Bytes: 4, SHA256: strings.Repeat("a", 64),
		Counts: map[string]int64{"rows": 3}, Src: &Source{Edge: "a-default-b", Replica: 1, N: n}}
}

func FuzzParseManifest(f *testing.F) {
	m := NewManifest("run", "uid", "a-default-b", 0, 1)
	add := func() {
		data, err := m.Marshal()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	add()
	for n := int64(1); n <= 3; n++ {
		if _, err := m.Append(seedChunk(n)); err != nil {
			f.Fatal(err)
		}
	}
	m.SetConsumed("in", 3)
	m.Observed = &Observed{Seq: 2, Counts: map[string]int64{"rows": 9}, Errors: 1, Segments: 1}
	f.Add([]byte(`{"version":1,"epoch":1,"observed":{"seq":1,"counts":{},"errors":0,"warnings":0,"errorsDropped":0,"segments":2000000000}}`))
	add()
	m.End()
	add()
	f.Add([]byte(`{"version":1,"epoch":1,"segments":["manifest-0000.json"],"chunks":[],"eos":true,"last":1000}`))
	f.Add([]byte(`{"version":2}`))
	f.Add([]byte(`{"version":1,"epoch":1} {}`))
	f.Add([]byte(`{"version":1,"epoch":1,"unknown":true}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := ParseManifest(data)
		if err != nil {
			if m != nil {
				t.Fatal("a manifest with an error")
			}
			return
		}
		first, err := m.Marshal()
		if err != nil {
			t.Fatalf("an accepted manifest does not marshal: %v", err)
		}
		again, err := ParseManifest(first)
		if err != nil {
			t.Fatalf("an accepted manifest does not parse after marshal: %v\n%s", err, first)
		}
		second, err := again.Marshal()
		if err != nil || !bytes.Equal(first, second) {
			t.Fatalf("marshal is not stable (%v):\n%s\n%s", err, first, second)
		}
		if o := m.Observed; o != nil && (o.Segments > MaxLogEntries || o.Segments < 0) {
			t.Fatalf("observed has %d log segments", o.Segments)
		}
		if m.Count() < 0 || m.NextN() != m.Count()+1 {
			t.Fatalf("count %d, next %d", m.Count(), m.NextN())
		}
		for stream, n := range m.Consumed {
			if !m.IsConsumed(stream, n) {
				t.Fatalf("consumed[%s] = %d is not consumed", stream, n)
			}
		}
		// The agent goes on from what it read.
		if m.EOS {
			if _, err := m.Append(seedChunk(m.NextN())); err == nil {
				t.Fatal("appended to an ended stream")
			}
			return
		}
		if _, err := m.Append(seedChunk(m.NextN())); err != nil {
			t.Fatalf("append the next chunk: %v", err)
		}
		m.End()
		if err := m.Validate(); err != nil {
			t.Fatalf("after append and end: %v", err)
		}
	})
}

func FuzzParseSegment(f *testing.F) {
	seg := Segment{Version: ManifestVersion}
	for n := int64(1); n <= SegmentSize; n++ {
		seg.Chunks = append(seg.Chunks, seedChunk(n))
	}
	data, err := seg.Marshal()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data, 0)
	f.Add(data, 1)
	f.Add([]byte(`{"version":1,"chunks":[]}`), 0)

	f.Fuzz(func(t *testing.T, data []byte, i int) {
		if i < 0 || i > 1<<20 {
			return // the agent asks for the segments its manifest names
		}
		s, err := ParseSegment(data, i)
		if err != nil {
			return
		}
		if len(s.Chunks) != SegmentSize || s.Chunks[0].N != int64(i)*SegmentSize+1 {
			t.Fatalf("segment %d with %d chunks from %d", i, len(s.Chunks), s.Chunks[0].N)
		}
		first, err := s.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseSegment(first, i)
		if err != nil {
			t.Fatalf("an accepted segment does not parse after marshal: %v", err)
		}
		if second, _ := again.Marshal(); !bytes.Equal(first, second) {
			t.Fatal("marshal is not stable")
		}
	})
}

func FuzzParseAcks(f *testing.F) {
	a := NewAcks()
	data, _ := a.Marshal()
	f.Add(data)
	for _, n := range []int64{1, 2, 5} {
		if err := a.Ack(n); err != nil {
			f.Fatal(err)
		}
	}
	a.Epoch = 2
	a.Observed = &Observed{Seq: 1, Counts: map[string]int64{"rows": 1}}
	data, _ = a.Marshal()
	f.Add(data)
	a.Closed = true
	data, _ = a.Marshal()
	f.Add(data)
	f.Add([]byte(`{"version":1,"acked":[2,1]}`))
	f.Add([]byte(`{"version":1,"acked":[0]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		a, err := ParseAcks(data)
		if err != nil {
			return
		}
		first, err := a.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseAcks(first)
		if err != nil {
			t.Fatalf("an accepted ack file does not parse after marshal: %v\n%s", err, first)
		}
		if second, _ := again.Marshal(); !bytes.Equal(first, second) {
			t.Fatalf("marshal is not stable:\n%s\n%s", first, second)
		}
		if o := a.Observed; o != nil && (o.Segments > MaxLogEntries || o.Segments < 0) {
			t.Fatalf("observed has %d log segments", o.Segments)
		}
		for _, n := range a.Acked {
			if !a.Has(n) {
				t.Fatalf("acked %d but Has says no", n)
			}
		}
		if a.Has(0) || a.Has(a.Last()+1) {
			t.Fatal("Has a chunk that is not acked")
		}
		last := a.Last()
		if err := a.Ack(last); last > 0 && err != nil {
			t.Fatalf("acking the last chunk again: %v", err)
		}
		if last < 1<<62 {
			if err := a.Ack(last + 1); err != nil {
				t.Fatalf("acking the next chunk: %v", err)
			}
		}
	})
}
