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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/examples/debug-session/shop"
	"alpheus-dev.org/alpheus/sdk"
)

// inputs makes small customers and filtered orders, where the customers
// without a region first order on day 7.
func inputs(t *testing.T) (customers, orders string) {
	t.Helper()
	ctx := context.Background()
	run := func(h sdk.Handler, settings string, in map[string]string) string {
		out := t.TempDir()
		if err := sdk.RunLocal(ctx, sdk.LocalConfig{Inputs: in, OutputDir: out, Settings: []byte(settings)}, h); err != nil {
			t.Fatal(err)
		}
		return filepath.Join(out, sdk.DefaultOutput)
	}
	customers = run(shop.Customers, `{"customers": 1000, "blankFrom": 921, "blank": 20, "seed": 21}`, nil)
	raw := run(shop.Orders, `{"files": 8, "rows": 2000, "customers": 1000, "cancelledPerMille": 50, "seed": 21}`, nil)
	return customers, run(shop.Filter, "", map[string]string{"default": raw})
}

// Version 1 fails on the first order from a customer without a region,
// on day 7, with exit code 1 and where it panicked.
func TestMergeV1FailsOnBlankRegion(t *testing.T) {
	if os.Getenv("MERGE_CHILD") == "1" {
		main()
		return
	}
	customers, orders := inputs(t)
	out := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMergeV1FailsOnBlankRegion$")
	cmd.Env = append(os.Environ(), "MERGE_CHILD=1", "ALPHEUS_MODE=local", "ALPHEUS_LOG_LEVEL=debug",
		"ALPHEUS_LOCAL_INPUTS=customers="+customers+",orders="+orders, "ALPHEUS_LOCAL_OUTPUT="+out)
	log, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != sdk.ExitFailed {
		t.Fatalf("exit %v, want %d:\n%s", err, sdk.ExitFailed, log)
	}
	for _, want := range []string{
		`"msg":"customers loaded"`, `"regions":{"":20,`,
		`panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:`,
	} {
		if !strings.Contains(string(log), want) {
			t.Errorf("no %q in:\n%s", want, log)
		}
	}
	// Days 1 to 6 were joined before it failed.
	if n := strings.Count(string(log), `"msg":"joined a chunk"`); n != 6 {
		t.Errorf("%d chunks joined before the failure, want 6", n)
	}
}
