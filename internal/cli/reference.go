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
	"results get": "Fetches the committed chunks of every result of the run, or of the ones named, to `./<run>/` or the folder `-d` names, " +
		"with a folder per result and one per replica, and `" + IndexFile + "` with the run, its UID and each chunk's file, size and SHA-256. " +
		"Only chunks the manifests list are fetched; other objects under the prefix were never committed. " +
		"Each chunk is written as `<file>.partial` and renamed when its size and SHA-256 match, so a file without `.partial` is whole. " +
		"A fetch that is stopped goes on where it stopped when run again, if the objects have not changed. " +
		"It writes only into an empty folder or one with the same run's `" + IndexFile + "`; `--force` uses another folder, and still only writes the run's own files. " +
		"It checks the free space first, and asks before more than 10 GiB unless you pass `--yes`. " +
		"When the run did not succeed, or a replica has not ended its stream, the committed chunks are fetched and the results said to be incomplete, " +
		"with `complete: false` in the index and for each such replica; " +
		"`--complete` makes that an error. A result larger than the disk is better read where it is: list it with `results ls`.",
	"results cat": "Writes the chunks of one result to stdout in order, replica 0 first, checking each against its size and SHA-256, " +
		"so `alpheus results cat <run> <step>.<output> --chunks 1 | zcat | head` shows the start of a result.",
	"results ls": "Lists the committed chunks as `s3://<bucket>/<key>` with their size and SHA-256, for tools that read S3 themselves, " +
		"so they get exactly the committed chunks and not everything under the prefix.",
	"results copy": "Copies the committed chunks of every result of the run, or of the ones named, to `--to s3://<bucket>/<prefix>` with the keys in the Secret `--to-secret`, " +
		"laid out as `results get` lays out a folder, with `" + IndexFile + "` naming the copies. " +
		"On the same endpoint the storage copies each chunk, so the data does not pass through here; " +
		"when the target's keys may not read the results, or the target is on another endpoint (`--to-endpoint`), each chunk is streamed through here without touching the disk, " +
		"and its SHA-256 checked before the upload completes. " +
		"Run again, it skips the copies that are already there. " +
		"It writes only to an empty prefix or one with the same run's `" + IndexFile + "`; `--force` uses another prefix, and still only writes the run's own objects. " +
		"Incomplete results are copied and said to be incomplete, as with `results get`; `--complete` makes that an error.",
	"results gc": "Lists the results in a bucket and deletes those whose `resultsTTL` has run out since the run finished. " +
		"Results whose StreamRun is gone are kept until their TTL runs out, unless you pass `--orphans`; results with `resultsTTL` 0 are never deleted. " +
		"It asks before deleting, unless you pass `--yes`.",
	"errors": "Shows the error log the components wrote with the chunks they committed: time, step, replica, level, code, input chunk and message, " +
		"and then a count per code. With `-d` it saves the log's files under `<dir>/_errors/<step>/<replica>/` instead.",
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
	"results get": "The index it wrote to `" + IndexFile + "`, as for `results ls`, with `file` set on each chunk " +
		"and `errors`, the error log's files with `--errors`. With `-d -` the chunks themselves.",
	"results cat": "Does not apply: the chunks are written as they are.",
	"results ls": "An index: `version`, `run`, `namespace`, `uid`, `phase`, `bucket`; " +
		"`complete`, true when the run succeeded and every replica ended its stream; `replicas`, each with `result`, `replica`, " +
		"`complete` (it ended its stream) and `chunks` (how many it has committed); " +
		"and `chunks`, each with `result`, `replica`, `n`, `key`, `url`, `bytes` and `sha256`.",
	"results copy": "The index it wrote to `" + IndexFile + "` in the target, as for `results get`, with `bucket` the target's and each chunk's `key` and `url` its copy.",
	"results gc": "A list with one object per run in the bucket: `prefix`, `run`, `namespace`, `uid`, `finishedAt`, " +
		"`reason` (why it is deleted or kept) and `action`: `delete`, `deleted` or `keep`.",
	"errors": "One entry per line, compact, as in the error log: `time`, `level`, `code`, `message`, `fields`, `step`, `replica` and `chunk`. " +
		"The count per code is only in the text.",
	"version": "`cli`; `controllers`, each with `namespace`, `version` and `image`; `crd`, the versions the cluster serves; " +
		"and `errors`, when the cluster could not be read.",
}

// subcommands are the parts of a command that has them, each a section
// of the reference.
var subcommands = map[string][]command{
	"results": {
		{name: "results get", summary: "Fetch a run's results to a folder"},
		{name: "results cat", summary: "Write one result to stdout"},
		{name: "results ls", summary: "List the committed chunks of a run's results as S3 paths"},
		{name: "results copy", summary: "Copy a run's results to another bucket or prefix"},
		{name: "results gc", summary: "Delete results whose TTL has run out, or whose StreamRun is gone"},
	},
}

// sections are the reference's sections: each command, or each of its
// subcommands.
func sections() []command {
	var out []command
	for _, c := range commands() {
		if subs, ok := subcommands[c.name]; ok {
			out = append(out, subs...)
			continue
		}
		out = append(out, c)
	}
	return out
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
	for _, c := range sections() {
		var out bytes.Buffer
		Main(context.Background(), append(strings.Fields(c.name), "-h"), &Env{Stdout: &out, Stderr: &out})
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
