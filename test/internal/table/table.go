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

// Package table reads and writes the CSV files of the test components:
// a header row, any delimiter, an optional UTF-8 byte order mark, and
// numbers with a chosen decimal separator.
package table

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

var bom = []byte("\uFEFF")

// Delimiter returns the delimiter a setting names; empty means a comma.
func Delimiter(s string) (rune, error) {
	if s == "" {
		return ',', nil
	}
	r, n := utf8.DecodeRuneInString(s)
	if n != len(s) || r == utf8.RuneError || r == '"' || r == '\r' || r == '\n' {
		return 0, fmt.Errorf("delimiter %q is not one character", s)
	}
	return r, nil
}

// NewReader reads CSV from r with the delimiter comma, skipping a byte
// order mark. Rows may have any number of fields, so the caller can log a
// bad row instead of failing. Blank lines are skipped.
func NewReader(r io.Reader, comma rune) *csv.Reader {
	br := bufio.NewReader(r)
	if b, err := br.Peek(len(bom)); err == nil && bytes.Equal(b, bom) {
		_, _ = br.Discard(len(bom)) // cannot fail after the Peek
	}
	cr := csv.NewReader(br)
	cr.Comma = comma
	cr.FieldsPerRecord = -1
	return cr
}

// NewWriter writes CSV to w with the delimiter comma.
func NewWriter(w io.Writer, comma rune) *csv.Writer {
	cw := csv.NewWriter(w)
	cw.Comma = comma
	return cw
}

// ErrEmpty means a number field is empty.
var ErrEmpty = errors.New("empty")

// Number is a decimal number with two decimals, in hundredths, so sums are
// exact.
type Number int64

// ParseNumber reads a number with the decimal separator dec; the other of
// '.' and ',' is a thousands separator and is ignored. Spaces around it
// are trimmed. It has at most two decimals.
func ParseNumber(s string, dec rune) (Number, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, ErrEmpty
	}
	thousands := "."
	if dec == '.' {
		thousands = ","
	}
	clean := strings.ReplaceAll(s, thousands, "")
	whole, frac, _ := strings.Cut(clean, string(dec))
	if len(frac) > 2 {
		return 0, fmt.Errorf("%q has more than two decimals", s)
	}
	neg := strings.HasPrefix(whole, "-")
	whole = strings.TrimPrefix(whole, "-")
	if whole == "" || !digits(whole) || !digits(frac) {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	frac += strings.Repeat("0", 2-len(frac))
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q: %w", s, err)
	}
	f, _ := strconv.ParseInt(frac, 10, 64)
	n := Number(w*100 + f)
	if neg {
		n = -n
	}
	return n, nil
}

func digits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Format writes n with two decimals and the decimal separator dec, without
// thousands separators.
func (n Number) Format(dec rune) string {
	sign := ""
	if n < 0 {
		sign, n = "-", -n
	}
	return fmt.Sprintf("%s%d%c%02d", sign, n/100, dec, n%100)
}
