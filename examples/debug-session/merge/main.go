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

// Command merge is the merge step of the debug-session example, version 1:
// it joins each order with its customer and adds the customer's name,
// region and region code. It has a bug on purpose: it does not handle a
// customer with a blank region. merge-v2 is the fixed version.
//
// Input ports: customers, a side input (side: true in the flow) read to the
// end first, and orders.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"strings"

	"alpheus-dev.org/alpheus/examples/debug-session/shop"
	"alpheus-dev.org/alpheus/sdk"
)

type customer struct{ name, region string }

func main() { sdk.Run(handle) }

func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s struct{}
	if err := in.Settings(&s); err != nil {
		return err
	}
	customers, err := load(ctx, in)
	if err != nil {
		return err
	}
	for c, err := range in.Port("orders").Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := join(ctx, customers, c, out); err != nil {
			return fmt.Errorf("chunk %s/%d #%d: %w", c.Step, c.Replica, c.N, err)
		}
	}
	return nil
}

// load reads all customers from the customers port.
func load(ctx context.Context, in sdk.Input) (map[string]customer, error) {
	customers := map[string]customer{}
	regions := map[string]int{}
	for c, err := range in.Port("customers").Chunks(ctx) {
		if err != nil {
			return nil, err
		}
		src, err := c.Open()
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(src)
		if !sc.Scan() || sc.Text() != shop.CustomersHeader {
			src.Close()
			return nil, fmt.Errorf("customers: the first line is not the header %q", shop.CustomersHeader)
		}
		for sc.Scan() {
			f := strings.Split(sc.Text(), ",")
			customers[f[0]] = customer{name: f[1], region: f[2]}
			regions[f[2]]++
		}
		src.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	slog.DebugContext(ctx, "customers loaded", "customers", len(customers), "regions", regions)
	return customers, nil
}

func join(ctx context.Context, customers map[string]customer, c sdk.Chunk, out sdk.Output) error {
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	sc := bufio.NewScanner(bufio.NewReaderSize(src, 1<<20))
	if !sc.Scan() || sc.Text() != shop.FilteredHeader {
		return fmt.Errorf("the first line is not the header %q", shop.FilteredHeader)
	}
	slog.DebugContext(ctx, "joining a chunk", "from", c.Step, "replica", c.Replica, "n", c.N, "bytes", c.Bytes)
	f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, shop.MergedHeader)
	var orders int64
	for line := 2; sc.Scan(); line++ {
		order := sc.Text()
		_, rest, _ := strings.Cut(order, ",")
		id, _, _ := strings.Cut(rest, ",")
		cust, ok := customers[id]
		if !ok {
			sdk.Error(ctx, "unknown_customer", "the order's customer is not in customers", sdk.Field("line", line))
			continue
		}
		code := strings.ToUpper(cust.region[:1])
		fmt.Fprintf(w, "%s,%s,%s,%s\n", order, cust.name, cust.region, code)
		orders++
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	sdk.Count(ctx, "orders", orders)
	slog.DebugContext(ctx, "joined a chunk", "n", c.N, "orders", orders)
	return f.Close()
}
