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

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/examples/debug-session/shop"
	"alpheus-dev.org/alpheus/sdk"
)

// Version 2 joins every order, writes None for a blank region, and warns
// about each with the code blank_region.
func TestMergeV2HandlesBlankRegion(t *testing.T) {
	ctx := context.Background()
	run := func(h sdk.Handler, settings string, in map[string]string) (string, string) {
		out := t.TempDir()
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{Inputs: in, OutputDir: out, Settings: []byte(settings)}, h); err != nil {
			t.Fatal(err)
		}
		return filepath.Join(out, sdk.DefaultOutput), out
	}
	customers, _ := run(shop.Customers, `{"customers": 1000, "blankFrom": 921, "blank": 20, "seed": 21}`, nil)
	raw, _ := run(shop.Orders, `{"files": 8, "rows": 2000, "customers": 1000, "cancelledPerMille": 50, "seed": 21}`, nil)
	orders, _ := run(shop.Filter, "", map[string]string{"default": raw})
	merged, out := run(handle, "", map[string]string{"customers": customers, "orders": orders})

	data, _ := os.ReadFile(filepath.Join(merged, "part-000007.csv"))
	if !strings.Contains(string(data), ",None,None\n") {
		t.Error("day 7 has no order with region None")
	}
	errs, _ := os.ReadFile(filepath.Join(out, "_errors.jsonl"))
	if n := strings.Count(string(errs), `"code":"blank_region"`); n == 0 || strings.Count(string(data), ",None,None\n") > n {
		t.Errorf("%d blank_region warnings", n)
	}
	if _, err := os.Stat(filepath.Join(merged, "part-000008.csv")); err != nil {
		t.Errorf("day 8 was not joined: %v", err)
	}
}
