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

package table

import (
	"errors"
	"strings"
	"testing"
)

// TestParseNumber checks that amounts with a decimal comma or point and
// thousands separators are read as hundredths, and that bad numbers are
// refused.
func TestParseNumber(t *testing.T) {
	for _, tc := range []struct {
		in   string
		dec  rune
		want Number
	}{
		{"90,00", ',', 9000},
		{"1.260,00", ',', 126000},
		{"-45,00", ',', -4500},
		{"65,5", ',', 6550},
		{" 12 ", ',', 1200},
		{"-0,50", ',', -50},
		{"1,260.75", '.', 126075},
	} {
		got, err := ParseNumber(tc.in, tc.dec)
		if err != nil || got != tc.want {
			t.Errorf("ParseNumber(%q) = %d, %v, want %d", tc.in, got, err, tc.want)
		}
	}
	if _, err := ParseNumber("  ", ','); !errors.Is(err, ErrEmpty) {
		t.Errorf("blank: %v, want ErrEmpty", err)
	}
	for _, bad := range []string{"abc", "1,234", "1,-5", "--1"} {
		if _, err := ParseNumber(bad, ','); err == nil || errors.Is(err, ErrEmpty) {
			t.Errorf("ParseNumber(%q) = %v, want an error", bad, err)
		}
	}
}

// TestFormat checks that hundredths are written as amounts with a decimal
// comma.
func TestFormat(t *testing.T) {
	for n, want := range map[Number]string{147000: "1470,00", 19650: "196,50", -50: "-0,50", 0: "0,00"} {
		if got := n.Format(','); got != want {
			t.Errorf("Format(%d) = %q, want %q", n, got, want)
		}
	}
}

// TestReaderSkipsBOMAndBlankLines checks that the CSV reader skips a byte
// order mark and blank lines.
func TestReaderSkipsBOMAndBlankLines(t *testing.T) {
	r := NewReader(strings.NewReader("\uFEFFa;b\n1;2\n\n3;4\n"), ';')
	recs, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || recs[0][0] != "a" || recs[2][1] != "4" {
		t.Errorf("records %q", recs)
	}
}
