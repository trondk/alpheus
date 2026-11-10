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
	"slices"
	"testing"
)

// TestAcks checks an ack file: acks in order, a repeated ack ignored, an ack
// out of order refused, and the file's format and round trip.
func TestAcks(t *testing.T) {
	a := NewAcks()
	for _, n := range []int64{1, 11, 21, 21} {
		if err := a.Ack(n); err != nil {
			t.Fatalf("Ack(%d): %v", n, err)
		}
	}
	if !slices.Equal(a.Acked, []int64{1, 11, 21}) {
		t.Errorf("acked = %v", a.Acked)
	}
	if err := a.Ack(5); !errors.Is(err, ErrInvalid) {
		t.Errorf("Ack out of order: %v, want ErrInvalid", err)
	}
	if !a.Has(11) || a.Has(12) || a.Last() != 21 {
		t.Errorf("Has(11) %v, Has(12) %v, Last %d", a.Has(11), a.Has(12), a.Last())
	}

	if _, err := a.Marshal(); err != nil {
		t.Fatal(err)
	}
	a.Epoch = 2
	data, _ := a.Marshal()
	if string(data) != `{"version":1,"acked":[1,11,21],"epoch":2}` {
		t.Errorf("format: %s", data)
	}
	got, err := ParseAcks(data)
	if err != nil || !slices.Equal(got.Acked, a.Acked) {
		t.Errorf("round trip: %v, %v", got, err)
	}
}

// TestParseAcksRejects checks that an ack file with another version, acks
// out of order, chunk 0, a repeated ack or impossible observed totals is
// refused.
func TestParseAcksRejects(t *testing.T) {
	for _, data := range []string{
		`{"version":2,"acked":[]}`,
		`{"version":1,"acked":[3,2]}`,
		`{"version":1,"acked":[0]}`,
		`{"version":1,"acked":[1,1]}`,
		`{"version":1,"acked":[],"observed":{"seq":1,"counts":{},"errors":0,"warnings":0,"errorsDropped":0,"segments":2000000000}}`,
		`{"version":1,"acked":[],"observed":{"seq":1,"counts":{},"errors":-1,"warnings":0,"errorsDropped":0,"segments":0}}`,
	} {
		if _, err := ParseAcks([]byte(data)); err == nil {
			t.Errorf("ParseAcks(%s) accepted", data)
		}
	}
}

// The check testcase03 makes on the filter replicas' ack files
// (GO-84): disjoint, together exactly 1-100, chunk n at replica n mod 10.
func TestAcksTestcase03Check(t *testing.T) {
	files := make([]*Acks, 10)
	for r := range files {
		files[r] = NewAcks()
	}
	for n := int64(1); n <= 100; n++ {
		if err := files[Assign(n, 0, 10)].Ack(n); err != nil {
			t.Fatal(err)
		}
	}
	owner := map[int64]int{}
	for r, a := range files {
		for _, n := range a.Acked {
			if prev, ok := owner[n]; ok {
				t.Fatalf("chunk %d acked by replicas %d and %d", n, prev, r)
			}
			if int(n%10) != r {
				t.Errorf("chunk %d acked by replica %d", n, r)
			}
			owner[n] = r
		}
	}
	for n := int64(1); n <= 100; n++ {
		if _, ok := owner[n]; !ok {
			t.Errorf("chunk %d not acked", n)
		}
	}
}
