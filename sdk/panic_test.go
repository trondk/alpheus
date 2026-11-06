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

package sdk_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/sdk"
)

// A component that panics fails with exit code 1, like any other failure,
// not 2, which means wrong settings, and the error says where it panicked
// in the component's code; the stack goes to the log.
func TestPanicExitsWithOne(t *testing.T) {
	if os.Getenv("SDK_PANIC_CHILD") == "1" {
		sdk.Run(func(ctx context.Context, in sdk.Input, out sdk.Output) error {
			var region string
			_ = region[:1] // the bug: a blank field
			return nil
		})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPanicExitsWithOne$")
	cmd.Env = append(os.Environ(), "SDK_PANIC_CHILD=1", "ALPHEUS_MODE=local",
		"ALPHEUS_LOCAL_INPUT="+inputs(t, "csv", "a\n"), "ALPHEUS_LOCAL_OUTPUT="+t.TempDir())
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != sdk.ExitFailed {
		t.Fatalf("exit %v, want %d:\n%s", err, sdk.ExitFailed, out)
	}
	for _, want := range []string{
		`"msg":"component panicked"`, `"stack":"goroutine`,
		`panic: runtime error: slice bounds out of range [:1] with length 0 at sdk/panic_test.go:`,
		`"exit_code":1`,
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
}
