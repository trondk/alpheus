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
	"errors"
	"reflect"
	"strings"
	"testing"
)

const sum0 = "0000000000000000000000000000000000000000000000000000000000000000"

func chunkN(n int64) Chunk { return Chunk{N: n, Ext: "csv", Bytes: 100, SHA256: sum0} }

// TestManifestAppend checks that chunks are added to a manifest in order
// from 1, and that a gap or a chunk added twice is refused.
func TestManifestAppend(t *testing.T) {
	m := NewManifest("sales-dk", "uid", "read-filter", 0, 1)
	for n := int64(1); n <= 3; n++ {
		if seg, err := m.Append(chunkN(n)); err != nil || seg != nil {
			t.Fatalf("Append(%d) = %v, %v", n, seg, err)
		}
	}
	if m.NextN() != 4 {
		t.Errorf("NextN = %d, want 4", m.NextN())
	}
	if _, err := m.Append(chunkN(5)); !errors.Is(err, ErrInvalid) {
		t.Errorf("Append with a gap: %v, want ErrInvalid", err)
	}
	if _, err := m.Append(chunkN(3)); !errors.Is(err, ErrInvalid) {
		t.Errorf("Append twice: %v, want ErrInvalid", err)
	}
}

// TestManifestSealsSegments checks that every 1,000 chunks are sealed into a
// segment with the right name and numbers, and that the count includes them
// (GO-32).
func TestManifestSealsSegments(t *testing.T) {
	m := NewManifest("r", "uid", "a-b", 0, 1)
	var segs []*Segment
	for n := int64(1); n <= 2*SegmentSize+5; n++ {
		seg, err := m.Append(chunkN(n))
		if err != nil {
			t.Fatal(err)
		}
		if seg != nil {
			segs = append(segs, seg)
		}
	}
	if len(segs) != 2 || len(m.Segments) != 2 || len(m.Chunks) != 5 {
		t.Fatalf("%d segments returned, %d listed, %d unsealed", len(segs), len(m.Segments), len(m.Chunks))
	}
	if m.Segments[1] != "manifest-0001.json" || m.Chunks[0].N != 2*SegmentSize+1 {
		t.Errorf("segments %v, first unsealed chunk %d", m.Segments, m.Chunks[0].N)
	}
	if m.Count() != 2*SegmentSize+5 {
		t.Errorf("Count = %d", m.Count())
	}

	data, err := segs[1].Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseSegment(data, 1); err != nil {
		t.Errorf("ParseSegment(1): %v", err)
	}
	if _, err := ParseSegment(data, 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("ParseSegment with the wrong index: %v, want ErrInvalid", err)
	}
	if _, err := ParseManifest(mustMarshal(t, m)); err != nil {
		t.Errorf("ParseManifest after sealing: %v", err)
	}
}

// A replica that drops every chunk it gets ends its stream with no chunks
// and eos (GO-80, testcase03).
func TestManifestEndsEmpty(t *testing.T) {
	m := NewManifest("r", "uid", "filter-sum", 0, 1)
	for _, n := range []int64{10, 20, 30} {
		m.SetConsumed("generate-filter/0", n)
	}
	m.End()
	got, err := ParseManifest(mustMarshal(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if !got.EOS || got.Last == nil || *got.Last != 0 || len(got.Chunks) != 0 {
		t.Errorf("eos %v, last %v, %d chunks", got.EOS, got.Last, len(got.Chunks))
	}
	if !got.IsConsumed("generate-filter/0", 30) || got.IsConsumed("generate-filter/0", 40) {
		t.Errorf("consumed = %v", got.Consumed)
	}
	if _, err := m.Append(chunkN(1)); !errors.Is(err, ErrEOS) {
		t.Errorf("Append after End: %v, want ErrEOS", err)
	}
}

// TestManifestConsumedNeverMovesBack checks that how far an input stream is
// consumed never goes back (SPEC 7.4).
func TestManifestConsumedNeverMovesBack(t *testing.T) {
	m := NewManifest("r", "uid", "a-b", 0, 1)
	m.SetConsumed("x-a/0", 7)
	m.SetConsumed("x-a/0", 3)
	if m.Consumed["x-a/0"] != 7 {
		t.Errorf("consumed = %d, want 7", m.Consumed["x-a/0"])
	}
}

// TestManifestCheckEpoch checks that a manifest from an older attempt and
// one from a newer attempt are told apart from the reader's own (GO-68).
func TestManifestCheckEpoch(t *testing.T) {
	m := NewManifest("r", "uid", "a-b", 0, 2)
	if err := m.CheckEpoch(2); err != nil {
		t.Errorf("same epoch: %v", err)
	}
	if err := m.CheckEpoch(3); !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("older manifest: %v, want ErrStaleEpoch", err)
	}
	if err := m.CheckEpoch(1); !errors.Is(err, ErrNewerEpoch) {
		t.Errorf("newer manifest: %v, want ErrNewerEpoch", err)
	}
}

// TestManifestRoundTrip checks that a manifest with counters, a source chunk
// and a consumed stream is written and read back unchanged.
func TestManifestRoundTrip(t *testing.T) {
	m := NewManifest("sales-dk-0042", "3f0c9a6e", "read-filter", 1, 2)
	c := chunkN(1)
	c.Counts = map[string]int64{"rows": 50000, "files": 3}
	c.Src = &Source{Edge: "source-read", Replica: 0, N: 4711}
	if _, err := m.Append(c); err != nil {
		t.Fatal(err)
	}
	m.SetConsumed("source-read/0", 4711)
	got, err := ParseManifest(mustMarshal(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Errorf("round trip:\n got %+v\nwant %+v", got, m)
	}
}

// TestParseManifestRejects checks that a manifest from a newer version, with
// unknown fields or trailing data, epoch 0, EOS without its last chunk, a
// gap, a short checksum or a bad extension is refused, so a file is never
// half understood.
func TestParseManifestRejects(t *testing.T) {
	valid := `{"version":1,"run":"r","uid":"u","edge":"a-b","replica":0,"epoch":1,"segments":[],"chunks":[],"consumed":{},"eos":false,"last":null}`
	if _, err := ParseManifest([]byte(valid)); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}
	withObserved := strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":3,"counts":{"rows":7},"errors":99999,"warnings":1,"errorsDropped":5,"segments":2}`, 1)
	if _, err := ParseManifest([]byte(withObserved)); err != nil {
		t.Fatalf("valid manifest with observed totals: %v", err)
	}
	for name, tc := range map[string]struct {
		data string
		want error
	}{
		"version 2":        {strings.Replace(valid, `"version":1`, `"version":2`, 1), ErrVersion},
		"unknown field":    {strings.Replace(valid, `"eos":false`, `"eos":false,"extra":1`, 1), ErrInvalid},
		"trailing data":    {valid + "{}", ErrInvalid},
		"epoch 0":          {strings.Replace(valid, `"epoch":1`, `"epoch":0`, 1), ErrInvalid},
		"eos without last": {strings.Replace(valid, `"eos":false`, `"eos":true`, 1), ErrInvalid},
		"chunk gap": {strings.Replace(valid, `"chunks":[]`,
			`"chunks":[{"n":2,"ext":"csv","bytes":1,"sha256":"`+sum0+`"}]`, 1), ErrInvalid},
		"short checksum": {strings.Replace(valid, `"chunks":[]`,
			`"chunks":[{"n":1,"ext":"csv","bytes":1,"sha256":"00"}]`, 1), ErrInvalid},
		"bad extension": {strings.Replace(valid, `"chunks":[]`,
			`"chunks":[{"n":1,"ext":"tar.gz","bytes":1,"sha256":"`+sum0+`"}]`, 1), ErrInvalid},
		"bad counter unit": {strings.Replace(valid, `"chunks":[]`,
			`"chunks":[{"n":1,"ext":"csv","bytes":1,"sha256":"`+sum0+`","counts":{"Rows":1}}]`, 1), ErrInvalid},
		"old schema field":                         {strings.Replace(valid, `"segments"`, `"schema":"","segments"`, 1), ErrInvalid},
		"wrong segment name":                       {strings.Replace(valid, `"segments":[]`, `"segments":["manifest-0001.json"]`, 1), ErrInvalid},
		"observed: more log segments than entries": {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":1,"counts":{},"errors":1,"warnings":0,"errorsDropped":0,"segments":2000000000}`, 1), ErrInvalid},
		"observed: too many log entries":           {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":1,"counts":{},"errors":100000,"warnings":1,"errorsDropped":0,"segments":1}`, 1), ErrInvalid},
		"observed: negative errors":                {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":1,"counts":{},"errors":-1,"warnings":0,"errorsDropped":0,"segments":0}`, 1), ErrInvalid},
		"observed: negative seq":                   {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":-1,"counts":{},"errors":0,"warnings":0,"errorsDropped":0,"segments":0}`, 1), ErrInvalid},
		"observed: bad counter unit":               {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":1,"counts":{"Rows":1},"errors":0,"warnings":0,"errorsDropped":0,"segments":0}`, 1), ErrInvalid},
		"observed: negative counter":               {strings.Replace(valid, `"last":null`, `"last":null,"observed":{"seq":1,"counts":{"rows":-1},"errors":0,"warnings":0,"errorsDropped":0,"segments":0}`, 1), ErrInvalid},
	} {
		if _, err := ParseManifest([]byte(tc.data)); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", name, err, tc.want)
		}
	}
}

func mustMarshal(t *testing.T, m *Manifest) []byte {
	t.Helper()
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
