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

package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
)

// details say what a command's help has no room for.
var details = map[string]string{
	"run": "Reads the file strictly, so a misspelt field is an error, and runs the controller's checks of the steps and edges first. " +
		"An image without a digest gives a warning, since the controller refuses it unless it runs with `-allow-tags`.",
	"get": "Without a run, lists the runs in the namespace, or in all namespaces with `-A`. " +
		"With one, shows its steps, replicas, edges with their lag, results and where to fetch them, and error logs.",
	"watch": "Prints the whole status at each change, as text or, with `-o json`, one object per line. " +
		"It ends when the run finishes, with exit code 0 only when it succeeded, so a script can wait for a run with it.",
	"logs":   "Shows the component's log, or the agent's with `-c agent`, of every step or one.",
	"cancel": "Sets `spec.cancel`. The controller stops the pods; chunks and results are cleaned up as for a finished run.",
	"chunks": "Reads the manifests of an edge, such as `filter-sum`, or of a result, `<step>.<output>`, with the keys in the run's storage Secret. " +
		"When `spec.storage.endpoint` is only reachable inside the cluster, pass the address you reach it by with `--endpoint`.",
	"validate": "Runs the same checks as `run` and has the API server check the fields and rules with a dry run, without creating anything. " +
		"`--local` needs no cluster and skips the server's checks.",
	"results": "Lists the results in a bucket and deletes those whose `resultsTTL` has run out since the run finished. " +
		"Results whose StreamRun is gone are kept until their TTL runs out, unless you pass `--orphans`; results with `resultsTTL` 0 are never deleted. " +
		"It asks before deleting, unless you pass `--yes`.",
	"version": "Shows the CLI's version, and the controller's and the CRD's when the cluster can be read.",
}

// shapes say what each command prints with -o json (GO-163). The same
// shape is printed as YAML with -o yaml.
var shapes = map[string]string{
	"run": "The `StreamRun` as created, with `apiVersion` and `kind`, as `kubectl get -o json` prints it.",
	"get": "Without a run, a `StreamRunList` with the runs in `items`; with one, the `StreamRun`. " +
		"Its `status` is described in [Using Alpheus from a program](/docs/api/).",
	"watch": "One `StreamRun` per line, compact, each time its status changes. With `-o yaml`, one YAML document per change, each after a `---` line.",
	"logs": "One object per log line, compact, one per line: `pod`, `step`, `replica`, `container` and `line`. " +
		"With `--kept`, `pod` is `attempt-<n>`, the attempt the log was kept from.",
	"cancel": "The `StreamRun`, as for `get`.",
	"chunks": "A list with one object per producer replica: `stream` (the edge or result), `prefix` in the bucket, `replica`, " +
		"`found` (false while it has no manifest yet), `epoch`, `eos`, `last` (the number of the last chunk, set with `eos`), " +
		"`observed` (counts, `errors`, `warnings` and `errorsDropped` so far), `consumed` (per input stream of the producer, the highest input chunk whose output to this stream is committed) " +
		"and `chunks`, each with `n`, `ext`, `bytes`, `sha256`, `counts` and `src` (`edge`, `replica` and `n` of the input chunk it was made from).",
	"replay": "One object when the replay has run: `failure`, the attempt's `failure.json` as described in [Using Alpheus from a program](/docs/api/#the-failed-attempt); " +
		"`dir`, where the inputs were fetched; `pod`, with `--with cluster`; `command`, the podman or docker command line; " +
		"`exitCode`, the component's; and `failedImage`, when `--image` ran another version than the one that failed. " +
		"What the component prints goes to stderr, so stdout has only the JSON.",
	"validate": "`file`, `valid`, `server` (true when the API server checked it too) and `warnings`, a list.",
	"results": "A list with one object per run in the bucket: `prefix`, `run`, `namespace`, `uid`, `finishedAt`, " +
		"`reason` (why it is deleted or kept) and `action`: `delete`, `deleted` or `keep`.",
	"version": "`cli`; `controllers`, each with `namespace`, `version` and `image`; `crd`, the versions the cluster serves; " +
		"and `errors`, when the cluster could not be read.",
}

// common are the flags every command has; the reference lists them once.
var common = map[string]bool{"kubeconfig": true, "context": true, "namespace": true, "n": true, "output": true, "o": true}

// Reference writes the CLI reference for the website (WWW-4) as Markdown,
// from the commands' own help, so it never differs from the program.
func Reference(w io.Writer) error {
	fmt.Fprintln(w, "---")
	fmt.Fprintln(w, "title: CLI reference")
	fmt.Fprintln(w, "description: Every alpheus command and its flags, generated from the CLI's own help.")
	fmt.Fprintln(w, "weight: 30")
	fmt.Fprintln(w, "---")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "<!-- Generated by make generate from internal/cli; do not edit. -->")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "`alpheus` talks to the cluster with your kubeconfig and has no server or login of its own.")
	fmt.Fprintln(w, "Flags can come before or after the arguments, with one dash or two.")
	fmt.Fprintln(w, "A command that fails exits with 1 and says what you can do; a wrong command line exits with 2.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Every command takes these flags:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Flag | What it does |")
	fmt.Fprintln(w, "|---|---|")
	fmt.Fprintln(w, "| `--kubeconfig` | The kubeconfig file (default: `KUBECONFIG` or `~/.kube/config`) |")
	fmt.Fprintln(w, "| `--context` | The kubeconfig context (default: the current one) |")
	fmt.Fprintln(w, "| `-n`, `--namespace` | The namespace (default: the context's) |")
	fmt.Fprintln(w, "| `-o`, `--output` | `json` or `yaml`, for scripts (default: text) |")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "With `-o json` each command prints the shape its section describes, and nothing else, on stdout.")
	fmt.Fprintln(w, "Errors, and what a replayed component prints, go to stderr.")
	fmt.Fprintln(w, "The shapes are an interface for programs: fields may be added, but a field keeps its name and meaning.")
	fmt.Fprintln(w, "A field that is empty may be left out.")
	for _, c := range commands() {
		args := []string{c.name, "-h"}
		if c.name == "results" {
			args = []string{c.name, "gc", "-h"}
		}
		var out bytes.Buffer
		Main(context.Background(), args, &Env{Stdout: &out, Stderr: &out})
		usage, flags := parseHelp(out.String())
		fmt.Fprintf(w, "\n## alpheus %s\n\n%s.\n\n```\n%s\n```\n", c.name, c.summary, usage)
		if d := details[c.name]; d != "" {
			fmt.Fprintf(w, "\n%s\n", d)
		}
		if sh := shapes[c.name]; sh != "" {
			fmt.Fprintf(w, "\nWith `-o json`: %s\n", sh)
		}
		if len(flags) > 0 {
			fmt.Fprintln(w, "\n| Flag | What it does |")
			fmt.Fprintln(w, "|---|---|")
			for _, f := range flags {
				// A flag's text such as <step>=<image> would be taken for
				// HTML in Markdown and dropped.
				text := strings.NewReplacer("<", "&lt;", ">", "&gt;", "|", "\\|").Replace(f[1])
				fmt.Fprintf(w, "| `%s` | %s |\n", f[0], text)
			}
		}
	}
	return nil
}

// parseHelp reads flag.PrintDefaults output: the usage line, and each
// flag that is not common, with its description. A one-letter bool flag
// has both on one line.
func parseHelp(help string) (usage string, flags [][2]string) {
	var name string
	add := func(spec, desc string) {
		f := strings.Fields(strings.TrimPrefix(spec, "-"))
		if common[f[0]] {
			return
		}
		dashes := "--"
		if len(f[0]) == 1 {
			dashes = "-"
		}
		flags = append(flags, [2]string{dashes + strings.Join(f, " "), strings.TrimSpace(desc)})
	}
	for _, l := range strings.Split(help, "\n") {
		switch {
		case strings.HasPrefix(l, "Usage: "):
			usage = strings.TrimSuffix(strings.TrimPrefix(l, "Usage: "), " [flags]")
		case strings.HasPrefix(l, "  -"):
			spec, desc, oneLine := strings.Cut(strings.TrimSpace(l), "\t")
			if oneLine {
				add(spec, desc)
			} else {
				name = spec
			}
		case strings.HasPrefix(l, "    \t") && name != "":
			add(name, l)
			name = ""
		}
	}
	return usage, flags
}
