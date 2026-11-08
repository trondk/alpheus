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
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"alpheus-dev.org/alpheus/sdk"
)

// grep keeps the lines with ERROR in them, one output chunk for each
// input chunk, and counts the lines it read and kept.
func grep(ctx context.Context, in sdk.Input, out sdk.Output) error {
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		f, err := c.Open()
		if err != nil {
			return err
		}
		dst, err := out.Create(ctx, sdk.DefaultOutput, "log")
		if err != nil {
			f.Close()
			return err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			sdk.Count(ctx, "lines_in", 1)
			if strings.Contains(sc.Text(), "ERROR") {
				sdk.Count(ctx, "lines_out", 1)
				fmt.Fprintln(dst, sc.Text())
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
	}
	return nil
}

// A component runs in local mode on a folder of input files, without
// Kubernetes or an agent, as in a component's own tests. In a container,
// the component's main calls sdk.Run(grep) instead.
func ExampleRunLocal() {
	dir, err := os.MkdirTemp("", "example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	in, out := filepath.Join(dir, "in"), filepath.Join(dir, "out")
	os.Mkdir(in, 0o755)
	os.WriteFile(filepath.Join(in, "1.log"), []byte("INFO start\nERROR disk full\n"), 0o644)
	os.WriteFile(filepath.Join(in, "2.log"), []byte("ERROR timeout\nINFO done\n"), 0o644)

	cfg := sdk.LocalConfig{Inputs: map[string]string{sdk.DefaultPort: in}, OutputDir: out}
	if err := sdk.RunLocal(context.Background(), cfg, grep); err != nil {
		panic(err)
	}
	for _, name := range []string{"part-000001.log", "part-000002.log"} {
		data, _ := os.ReadFile(filepath.Join(out, sdk.DefaultOutput, name))
		fmt.Printf("%s: %s", name, data)
	}
	summary, _ := os.ReadFile(filepath.Join(out, sdk.SummaryFile))
	fmt.Printf("%s", summary)
	// Output:
	// part-000001.log: ERROR disk full
	// part-000002.log: ERROR timeout
	// {
	//   "counts": {
	//     "lines_in": 4,
	//     "lines_out": 2
	//   },
	//   "errors": 0,
	//   "warnings": 0,
	//   "errorsDropped": 0
	// }
}
