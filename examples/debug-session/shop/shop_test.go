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

package shop_test

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/examples/debug-session/shop"
	"alpheus-dev.org/alpheus/sdk"
)

// Small is a small version of the example's data: the customers without a
// region first order on day 7, as in the full run.
const (
	SmallCustomers = `{"customers": 1000, "blankFrom": 921, "blank": 20, "seed": 21}`
	SmallOrders    = `{"files": 8, "rows": 2000, "customers": 1000, "cancelledPerMille": 50, "seed": 21}`
)

func step(t *testing.T, h sdk.Handler, settings string, inputs map[string]string) (string, error) {
	t.Helper()
	out := t.TempDir()
	err := sdk.RunLocal(context.Background(), sdk.LocalConfig{Inputs: inputs, OutputDir: out, Settings: []byte(settings)}, h)
	return filepath.Join(out, sdk.DefaultOutput), err
}

// TestSources checks the debug example's generated data: the customers'
// header and count, the 20 customers without a region from C000921, and
// that their first orders come on day 7, the seventh chunk, where merge
// version 1 fails.
func TestSources(t *testing.T) {
	customers, err := step(t, shop.Customers, SmallCustomers, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(customers, "part-000001.csv"))
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1001 || lines[0] != shop.CustomersHeader {
		t.Fatalf("%d lines, header %q", len(lines), lines[0])
	}
	blank := 0
	for _, l := range lines[1:] {
		if strings.HasSuffix(l, ",") {
			blank++
		}
	}
	if blank != 20 || !strings.HasSuffix(lines[921], ",") || strings.HasSuffix(lines[920], ",") {
		t.Errorf("%d customers without a region, want 20 from C000921", blank)
	}

	orders, err := step(t, shop.Orders, SmallOrders, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The blank customers order first on day 7, the seventh chunk.
	for day := 1; day <= 8; day++ {
		f, err := os.Open(filepath.Join(orders, "part-00000"+string(rune('0'+day))+".csv"))
		if err != nil {
			t.Fatal(err)
		}
		hit := false
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			id := strings.Split(sc.Text(), ",")[1]
			if id >= "C000921" && id <= "C000940" {
				hit = true
			}
		}
		f.Close()
		if hit != (day >= 7) {
			t.Errorf("day %d: orders from customers without a region: %v", day, hit)
		}
	}
}

// TestFilterAndSort runs the debug example's filter and sort on their own:
// filter drops cancelled orders and keeps the header, and sort orders merged
// rows across two chunks.
func TestFilterAndSort(t *testing.T) {
	orders, err := step(t, shop.Orders, SmallOrders, nil)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := step(t, shop.Filter, "", map[string]string{"default": orders})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(filtered, "part-000001.csv"))
	if strings.Contains(string(data), "cancelled") || !strings.HasPrefix(string(data), shop.FilteredHeader+"\n") {
		t.Errorf("filter kept a cancelled order or lost the header")
	}

	// Sort reads merged rows; two chunks, sorted across them.
	in := t.TempDir()
	os.WriteFile(filepath.Join(in, "a.csv"), []byte(shop.MergedHeader+"\nO2,C1,d,5.00,n,south,S\nO1,C2,d,9.00,n,north,N\n"), 0o644)
	os.WriteFile(filepath.Join(in, "b.csv"), []byte(shop.MergedHeader+"\nO3,C3,d,7.50,n,None,None\nO4,C4,d,12.00,n,south,S\n"), 0o644)
	sorted, err := step(t, shop.Sort, "", map[string]string{"default": in})
	if err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(filepath.Join(sorted, "part-000001.csv"))
	want := shop.MergedHeader + "\nO3,C3,d,7.50,n,None,None\nO1,C2,d,9.00,n,north,N\nO4,C4,d,12.00,n,south,S\nO2,C1,d,5.00,n,south,S\n"
	if string(data) != want {
		t.Errorf("sorted:\n%s\nwant:\n%s", data, want)
	}
}
