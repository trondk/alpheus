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

// Package shop is the debug-session example: a web shop's orders, joined
// with its customers and sorted into a report. It has a bug on purpose, in
// the merge step, to show how a failed run is debugged (docs/debug-example.md).
//
//	orders ──> filter (3 replicas) ──┐
//	                                 ├──> merge ──> sort ──> report
//	customers ───────────────────────┘
//
// Everything is CSV without compression, so a chunk can be read with head.
// The data comes from a seed, so every run gives the same rows.
package shop

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"

	"alpheus-dev.org/alpheus/sdk"
)

// The CSV headers of each stream.
const (
	CustomersHeader = "customer_id,name,region"
	OrdersHeader    = "order_id,customer_id,date,amount,status"
	FilteredHeader  = "order_id,customer_id,date,amount"
	MergedHeader    = "order_id,customer_id,date,amount,name,region,region_code"
)

// Regions are the regions a customer can be in.
var Regions = []string{"north", "south", "east", "west", "central"}

// CustomerSettings are the customers step's settings. Customers number
// BlankFrom to BlankFrom+Blank-1 have no region: they signed up through a
// new form that did not ask for it. That is the data the bug trips on.
type CustomerSettings struct {
	Customers int    `json:"customers"`
	BlankFrom int    `json:"blankFrom"`
	Blank     int    `json:"blank"`
	Seed      uint64 `json:"seed"`
}

// Customers is a source: it writes all customers as one chunk.
func Customers(ctx context.Context, in sdk.Input, out sdk.Output) error {
	s := CustomerSettings{Customers: 50000, BlankFrom: 46001, Blank: 20, Seed: 21}
	if err := in.Settings(&s); err != nil {
		return err
	}
	slog.DebugContext(ctx, "settings", "customers", s.Customers, "blank_from", s.BlankFrom, "blank", s.Blank)
	f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, CustomersHeader)
	r := rand.New(rand.NewPCG(s.Seed, 0))
	for id := 1; id <= s.Customers; id++ {
		region := Regions[r.IntN(len(Regions))]
		if id >= s.BlankFrom && id < s.BlankFrom+s.Blank {
			region = ""
		}
		fmt.Fprintf(w, "C%06d,Customer %d,%s\n", id, id, region)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	sdk.Count(ctx, "customers", int64(s.Customers))
	return f.Close()
}

// OrderSettings are the orders step's settings: Files chunks of Rows
// orders each, one day per chunk from 2026-10-01. The shop grows: the
// orders of day d come from the first 80 + 2·d percent of the customers,
// so with 50,000 customers, those without a region (46,001 to 46,020)
// first order on day 7.
type OrderSettings struct {
	Files             int    `json:"files"`
	Rows              int    `json:"rows"`
	Customers         int    `json:"customers"`
	CancelledPerMille int    `json:"cancelledPerMille"`
	Seed              uint64 `json:"seed"`
}

// Orders is a source: it writes one chunk of orders per day.
func Orders(ctx context.Context, in sdk.Input, out sdk.Output) error {
	s := OrderSettings{Files: 20, Rows: 200000, Customers: 50000, CancelledPerMille: 50, Seed: 21}
	if err := in.Settings(&s); err != nil {
		return err
	}
	slog.DebugContext(ctx, "settings", "files", s.Files, "rows", s.Rows)
	for i := range s.Files {
		f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
		if err != nil {
			return err
		}
		w := bufio.NewWriterSize(f, 1<<20)
		fmt.Fprintln(w, OrdersHeader)
		r := rand.New(rand.NewPCG(s.Seed, uint64(i)+1))
		active := min(s.Customers, s.Customers*(80+2*(i+1))/100)
		day := fmt.Sprintf("2026-10-%02d", i+1)
		for row := range s.Rows {
			status := "ok"
			if r.IntN(1000) < s.CancelledPerMille {
				status = "cancelled"
			}
			cents := 100 + r.IntN(250000)
			fmt.Fprintf(w, "O%08d,C%06d,%s,%d.%02d,%s\n", i*s.Rows+row+1, 1+r.IntN(active), day, cents/100, cents%100, status)
		}
		if err := w.Flush(); err != nil {
			return err
		}
		sdk.Count(ctx, "orders", int64(s.Rows))
		if err := f.Close(); err != nil {
			return err
		}
		slog.DebugContext(ctx, "wrote a day of orders", "day", day, "rows", s.Rows, "customers", active)
		sdk.Progress(ctx, fmt.Sprintf("day %d of %d", i+1, s.Files))
	}
	return nil
}

// Filter drops cancelled orders and the status column.
func Filter(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s struct{}
	if err := in.Settings(&s); err != nil {
		return err
	}
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := filter(ctx, c, out); err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
	}
	return nil
}

func filter(ctx context.Context, c sdk.Chunk, out sdk.Output) error {
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	sc := bufio.NewScanner(bufio.NewReaderSize(src, 1<<20))
	if !sc.Scan() || sc.Text() != OrdersHeader {
		return fmt.Errorf("the first line is not the header %q", OrdersHeader)
	}
	f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, FilteredHeader)
	var in, kept int64
	for sc.Scan() {
		in++
		line := sc.Text()
		rest, status, ok := cutLast(line)
		if !ok {
			return fmt.Errorf("line %d has no status", in+1)
		}
		if status != "ok" {
			continue
		}
		kept++
		w.WriteString(rest)
		w.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	sdk.Count(ctx, "orders_in", in)
	sdk.Count(ctx, "orders_out", kept)
	slog.DebugContext(ctx, "filtered a chunk", "from", c.Step, "n", c.N, "orders_in", in, "orders_out", kept)
	return f.Close()
}

func cutLast(line string) (before, after string, ok bool) {
	i := strings.LastIndexByte(line, ',')
	if i < 0 {
		return "", "", false
	}
	return line[:i], line[i+1:], true
}
