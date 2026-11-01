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

// Package meter is the meter-etl example: large-file ETL of smart-meter
// readings. A utility's nightly exports arrive as large gzipped CSV files;
// they are split, cleaned in parallel and summed per region and hour.
//
//	export ──> split ──> clean (replicas) ──> aggregate ──> totals (result)
//	                        └──> dataset (result): the cleaned readings
//
// The readings are CSV without quotes: meter_id,region,timestamp,kwh, with
// a reading every 15 minutes. export makes them from a seed, with a share
// of bad rows, so a test can compute the exact totals with Expected.
package meter

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Header is the first line of every CSV file of readings.
const Header = "meter_id,region,timestamp,kwh"

// Regions are the grid regions readings come from.
var Regions = []string{"capital", "central", "coast", "east", "islands", "north", "south", "west"}

// Gen describes the exports: Files files of Rows readings each, for Day,
// with BadPerMille bad rows in every thousand. The same Gen always makes
// the same files.
type Gen struct {
	Files       int    `json:"files"`
	Rows        int    `json:"rows"`
	Seed        uint64 `json:"seed"`
	BadPerMille int    `json:"badPerMille"`
	Day         string `json:"day"`
}

func (g Gen) day() (time.Time, error) {
	if g.Day == "" {
		return time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC), nil
	}
	return time.Parse(time.DateOnly, g.Day)
}

// Key is what readings are summed by.
type Key struct {
	Region string
	Hour   int
}

// Sum is the readings of one key: how many, and their energy in Wh, so
// sums are exact.
type Sum struct {
	Readings int64
	Wh       int64
}

// Totals are what a run over Gen's files must give.
type Totals struct {
	Rows int64
	Bad  int64
	Sums map[Key]Sum
}

// rows makes file i's lines, without the header, and calls fn for each.
// Valid lines also get their key and energy.
func (g Gen) rows(i int, fn func(line []byte, valid bool, k Key, wh int64) error) error {
	day, err := g.day()
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(g.Seed, uint64(i)))
	var line []byte
	for range g.Rows {
		meter := rng.IntN(100000)
		region := Regions[meter%len(Regions)]
		slot := rng.IntN(96)
		wh := int64(rng.IntN(5000))
		bad := -1
		if rng.IntN(1000) < g.BadPerMille {
			bad = rng.IntN(4)
		}
		ts := day.Add(time.Duration(slot) * 15 * time.Minute)
		line = append(line[:0], 'M')
		line = appendPadded(line, int64(meter), 7)
		line = append(line, ',')
		if bad != 3 { // 3: a column is missing
			line = append(line, region...)
			line = append(line, ',')
		}
		if bad == 1 {
			line = append(line, "2026-13-40T25:61:00Z"...)
		} else {
			line = ts.AppendFormat(line, time.RFC3339)
		}
		line = append(line, ',')
		switch bad {
		case 0: // the reading is missing
		case 2:
			line = append(line, '-')
			line = appendKWh(line, wh+1)
		default:
			line = appendKWh(line, wh)
		}
		if err := fn(line, bad < 0, Key{region, ts.Hour()}, wh); err != nil {
			return err
		}
	}
	return nil
}

// Expected computes the totals of all files without writing them.
func (g Gen) Expected() (Totals, error) {
	t := Totals{Sums: map[Key]Sum{}}
	for i := range g.Files {
		err := g.rows(i, func(_ []byte, valid bool, k Key, wh int64) error {
			t.Rows++
			if !valid {
				t.Bad++
				return nil
			}
			s := t.Sums[k]
			s.Readings++
			s.Wh += wh
			t.Sums[k] = s
			return nil
		})
		if err != nil {
			return t, err
		}
	}
	return t, nil
}

func appendPadded(b []byte, n int64, width int) []byte {
	s := strconv.FormatInt(n, 10)
	for range width - len(s) {
		b = append(b, '0')
	}
	return append(b, s...)
}

// appendKWh writes wh as kWh with three decimals.
func appendKWh(b []byte, wh int64) []byte {
	b = strconv.AppendInt(b, wh/1000, 10)
	b = append(b, '.')
	return appendPadded(b, wh%1000, 3)
}

// Reading is one valid row.
type Reading struct {
	Meter  string
	Region string
	Time   time.Time
	Wh     int64
}

// Codes of bad rows, as clean logs them.
var (
	ErrColumns   = errors.New("bad_columns")
	ErrMeter     = errors.New("bad_meter")
	ErrRegion    = errors.New("bad_region")
	ErrTimestamp = errors.New("bad_timestamp")
	ErrMissing   = errors.New("missing_kwh")
	ErrNegative  = errors.New("negative_kwh")
	ErrKWh       = errors.New("bad_kwh")
)

// Parse checks one line and returns its reading, or which rule it breaks.
func Parse(line string) (Reading, error) {
	f := strings.Split(line, ",")
	if len(f) != 4 {
		return Reading{}, ErrColumns
	}
	if len(f[0]) != 8 || f[0][0] != 'M' || !digits(f[0][1:]) {
		return Reading{}, ErrMeter
	}
	if f[1] == "" {
		return Reading{}, ErrRegion
	}
	ts, err := time.Parse(time.RFC3339, f[2])
	if err != nil {
		return Reading{}, ErrTimestamp
	}
	wh, err := parseKWh(f[3])
	if err != nil {
		return Reading{}, err
	}
	return Reading{Meter: f[0], Region: f[1], Time: ts.UTC(), Wh: wh}, nil
}

// parseKWh reads kWh with up to three decimals as an exact number of Wh.
func parseKWh(s string) (int64, error) {
	switch {
	case s == "":
		return 0, ErrMissing
	case s[0] == '-':
		return 0, ErrNegative
	}
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" || !digits(whole) || len(frac) > 3 || !digits(frac) || len(whole) > 6 {
		return 0, ErrKWh
	}
	frac += strings.Repeat("0", 3-len(frac))
	w, _ := strconv.ParseInt(whole, 10, 64)
	f, _ := strconv.ParseInt(frac, 10, 64)
	return w*1000 + f, nil
}

func digits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
