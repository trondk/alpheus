---
title: Writing components
description: What a component is, how a run moves its files step by step, and how to write one in Go or Python, with templates to start from.
weight: 10
lastmod: 2026-10-31
---

A component is a container image that does one step of a flow. It reads
its input chunks as files and writes its output chunks as files, in any
format it likes. Alpheus moves the files between steps through object
storage, but never reads them.

The component never talks to S3 and holds no keys. An agent runs next to it
in the same pod and does all of that. The component talks to the agent over
a Unix socket, through the Go SDK or, in other languages, through a small
gRPC contract.

This page has four parts:

1. [A run, step by step](#a-run-step-by-step): what happens to a flow from
   `alpheus run` until its result is in the bucket.
2. [The rules a component follows](#the-rules-a-component-follows).
3. [Components in Go](#components-in-go) and
   [components in Python](#components-in-python), with the same three
   examples in both languages.
4. [Testing without Kubernetes](#testing-without-kubernetes) and
   [other languages](#other-languages).

## Start from a template

There are two templates. Each is a complete, tested component, `grep`,
which keeps the lines of each input chunk that contain a text:

| Template | What is in it |
|---|---|
| {{< repolink "templates/component-go/" >}} | `main.go`, a test in local mode, and a Dockerfile that builds a static binary on `scratch` |
| {{< repolink "templates/component-python/" >}} | `component.py`, a small implementation of the contract in `alpheus/`, a Dockerfile, and a test in local mode |

Copy one, replace the `handle` function, and build the image. The example
on this page, {{< repolink "examples/log-report/" >}}, uses `grep` as its
middle step, in both languages. [Getting started](/docs/getting-started/)
builds and runs it.

## A run, step by step

The example is a log report with three steps:

```
generate ──> grep (2 replicas) ──> count ──> result
```

- `generate` writes chunks of log lines. It is a *source*: it has no input.
- `grep` keeps the ERROR lines. It runs as two replicas that share the
  work.
- `count` totals the lines per level. It needs all of its input before it
  can write anything, so it is *stateful*. Its output goes to no other
  step, so it is the run's *result*.

The flow is one `StreamRun` resource:

```yaml
apiVersion: alpheus-dev.org/v1alpha1
kind: StreamRun
metadata:
  name: log-report
spec:
  steps:
    - name: generate
      image: registry.example/generate@sha256:…
      settings: {chunks: 20, lines: 5000}
    - name: grep
      image: registry.example/grep@sha256:…
      replicas: 2
      settings: {match: ERROR}
    - name: count
      image: registry.example/count@sha256:…
  edges:
    - {from: generate, to: grep}
    - {from: grep, to: count}
  chunks: {maxBytes: 64Mi, maxUnacked: 4}
  storage:
    endpoint: http://s3.example:3900
    bucket: flows
    secretRef: alpheus-s3
```

`settings` is free JSON for the component; Alpheus only passes it on.
Images are pinned by digest, so a restarted pod runs exactly the same
code.

### 1. You create the run

```console
$ alpheus validate -f flow.yaml
$ alpheus run -f flow.yaml
```

`validate` checks the steps and edges the same way the controller does,
and has the API server check the fields and rules without creating
anything. `run` creates the `StreamRun`. With `kubectl`,
`kubectl apply -f flow.yaml` does the same as `run`.

### 2. The controller checks it and starts the pods

The controller checks that the edges form a graph without cycles and that
every image is pinned by digest. It then creates one pod per replica: four
pods here. In each pod:

- The **component** is the only regular container. It gets its settings in
  `ALPHEUS_SETTINGS`, its replica number in `ALPHEUS_REPLICA` and
  `ALPHEUS_REPLICAS`, and the agent's socket in `ALPHEUS_SOCKET`. It gets
  no S3 keys and no Kubernetes token.
- The **agent** runs next to it as a sidecar. It gets the S3 keys from the
  run's Secret, and a certificate for this attempt of the pod, for the
  signal hub in the controller.
- The two share a folder, `/run/alpheus`, with the socket and the scratch
  folder where chunk files live.

Both run as a user without privileges, with a read-only root file system.
The component can only write in the scratch folder, in the files the agent
gives it.

### 3. The source writes chunks

`generate` asks the agent for an output file (`Create`), writes 5,000 log
lines to it, and closes it (`Close`). For a source, each closed chunk is
committed at once:

1. The agent uploads the file to
   `runs/<uid>/generate-grep/0/part-000001.log`.
2. It adds the chunk, with its size and SHA-256, to the stream's
   *manifest* in the same folder. A chunk exists once the manifest lists
   it. A file that was uploaded but never listed is ignored.
3. It tells the hub that a chunk is ready. The hub passes that on to the
   agents of `grep`.

A source must write the same chunks every time it runs with the same
settings. A restarted source starts from the beginning, and its agent
discards the chunks it already committed.

### 4. The chunks are shared between replicas

Chunk *n* from producer replica *p* goes to consumer replica
(*n* + *p*) mod `replicas`. Here, `grep` replica 0 gets chunks 2, 4, 6, …
and replica 1 gets 1, 3, 5, …. The rule is fixed, so a restarted replica
gets exactly the same chunks again.

### 5. A step reads a chunk and writes its output

`grep`'s agent downloads the next chunk to the scratch folder. When the
component asks for it (`Next`), it gets the path to the file. The
component reads the file, writes the matching lines to a new output file
(`Create`, then `Close`), and asks for the next chunk. The SDK then tells
the agent that the chunk is done (`Done`).

At `Done`, everything made from that input chunk is committed together:
the output chunks, the counters, and the error log entries, in one write
of the outgoing manifest. Next to them the manifest records that this
input chunk is *consumed*. A crash in the middle of a chunk therefore never
leaves half its output visible, and nothing is counted twice.

The agent then *acks* the input chunk to `generate`'s agent through an ack
file. A producer may be at most `maxUnacked` chunks ahead of each
consumer replica (4 here). When it gets that far, its `Create` waits.
That is *backpressure*: a slow step slows the steps before it, and the
scratch folder never fills up.

### 6. A stateful step writes at the end

`count` adds up the lines of every chunk in memory. It registers an
*end-of-stream* function (`OnEOS` in Go, `on_eos` in Python) before it
reads. That tells the agent the step is stateful: it acks chunks as it
goes, so `grep` is not held back, but it commits nothing until the
end-of-stream output is written. If `count` restarts before that, its
agent gives it all of its input again, and it rebuilds its totals.

### 7. The end of the stream

When `generate`'s handler returns, the SDK calls `Finish`, and the agent
marks each outgoing manifest as ended (`eos`), with the number of its last
chunk. When `grep` has read every chunk from every producer, `Next`
returns end of stream, its handler returns, and it ends its outputs the
same way. Last, `count` runs its end-of-stream function, writes the CSV
file, and finishes.

### 8. The result and the status

`count`'s output goes to no other step, so the agent writes it as a
result: `results/log-report/<uid>/count/default/0/part-000001.csv`, with a
manifest, like an edge. The controller reads the manifests and writes the
run's status: chunks, bytes and acks per edge, exact counters per step,
and where each result is:

```console
$ alpheus get log-report
Phase:           Succeeded
…
STEP      PHASE      REPLICAS  RESTARTS  ERRORS  WARNINGS  COUNTS
generate  Succeeded  1/1       0         0       0         lines=100000
grep      Succeeded  2/2       0         0       0         lines_in=100000 lines_out=14286
count     Succeeded  1/1       0         0       0         -

RESULT         CHUNKS  BYTES  COUNTS
count.default  1       24 B   -
```

`alpheus watch log-report` shows the same while the run is going, with the
lag of each edge. `alpheus chunks log-report grep-count` lists the chunks
of an edge, and `alpheus logs log-report grep` shows the components' logs.

### 9. When a pod fails

If a pod dies, the controller waits until it is gone, then starts a new
attempt of the same replica, up to `maxRestarts` times (3 by default). The
new agent reads the manifests and goes on from the last committed chunk.
The chunk that was being handled is given to the component again, so a
component must write the same output for the same input. The old attempt
is shut out: its certificate no longer works at the hub, and its writes
to the manifests are refused.

### 10. Cleaning up

The run's chunks under `runs/<uid>/` are deleted after `ttlAfterFinished`,
or when the `StreamRun` is deleted. Results are not: while the run exists,
the controller deletes them when `resultsTTL` has run out, 7 days by
default. Once the run is deleted, `alpheus results gc` does that instead.

## The rules a component follows

- **Read input only through the chunks you are given.** Each chunk is a
  local file. The agent deletes it once the chunk is done, so don't keep
  its path.
- **Write output only to files from `Create`.** Choose the file extension
  (lower-case letters and digits, at most 16). Close each file when it is
  written. A file larger than `spec.chunks.maxBytes` is refused when you
  close it.
- **Output made while handling a chunk belongs to that chunk.** It is
  committed when the chunk is done, together with what you counted and
  logged. A chunk may give no output, one chunk or many.
- **Be deterministic.** A restarted replica handles its uncommitted chunk
  again, and a restarted source writes all its chunks again. The same
  input and settings must give the same output.
- **Effects outside Alpheus happen at least once.** If a component writes
  to a database, for example, a restart can write the same thing twice.
  Make such writes idempotent.
- **Return when all input is read.** Returning in the middle of a chunk is
  an error. Returning an error fails the attempt, and the replica is
  restarted.
- **Settings are strict.** Reject settings you don't know, so a misspelt
  setting fails the run instead of being ignored. That is a usage error,
  exit code 2.
- **Report, don't print, what went wrong with the data.** Count with
  units you choose (lower-case letters, digits and `_`, at most 32
  characters, at most 16 units per step), and log bad records as errors or
  warnings with a code. They go to the run's error log, are committed with
  the chunk, and show in `alpheus get`. Progress text (at most once a
  second) shows in `alpheus watch`.

The SDK exits with these codes, and the Python template does the same:

| Code | Meaning |
|---|---|
| 0 | The component finished and read all of its input |
| 1 | It failed; the replica is restarted |
| 2 | Wrong settings or wrong use of the API |
| 143 | It was stopped (SIGTERM) |

## Components in Go

The Go SDK is the package `alpheus-dev.org/alpheus/sdk`. `sdk.Run` connects
to the agent, runs your handler, and exits with the right code. Your code
sees files and nothing else.

### A step: grep

This is the core of {{< repolink "templates/component-go/main.go" >}}.
`in.Chunks` yields this replica's input chunks. A chunk is done when the
loop asks for the next one.

```go
type settings struct {
	Match  string `json:"match"`
	Invert bool   `json:"invert"`
}

func main() { sdk.Run(handle) }

func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s settings
	if err := in.Settings(&s); err != nil { // unknown fields are an error
		return err
	}
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		if err := grep(ctx, s, c, out); err != nil {
			return err
		}
	}
	return nil
}

func grep(ctx context.Context, s settings, c sdk.Chunk, out sdk.Output) error {
	if c.Bytes == 0 {
		sdk.Warn(ctx, "empty_chunk", "the input chunk is empty", sdk.Field("n", c.N))
		return nil
	}
	src, err := c.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	var dst *sdk.File // created at the first matching line
	var w *bufio.Writer
	var in, kept int64
	r := bufio.NewReader(src)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			in++
			if bytes.Contains(line, []byte(s.Match)) != s.Invert {
				if dst == nil {
					if dst, err = out.Create(ctx, sdk.DefaultOutput, c.Ext); err != nil {
						return err
					}
					w = bufio.NewWriter(dst)
				}
				w.Write(line)
				kept++
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	sdk.Count(ctx, "lines_in", in)
	sdk.Count(ctx, "lines_out", kept)
	if dst == nil {
		return nil // no matching lines: no output for this chunk
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return dst.Close()
}
```

### A source: generate

A source never reads input. It creates output chunks and returns when it
is done. From {{< repolink "examples/log-report/generate/main.go" >}}:

```go
func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	var s settings // {"chunks": 20, "lines": 5000}
	if err := in.Settings(&s); err != nil {
		return err
	}
	for n := range s.Chunks {
		f, err := out.Create(ctx, sdk.DefaultOutput, "log")
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		for i := range s.Lines {
			level := "INFO"
			if (n*s.Lines+i)%7 == 0 {
				level = "ERROR"
			}
			fmt.Fprintf(w, "%s line %d of chunk %d\n", level, i+1, n+1)
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if err := f.Close(); err != nil { // a source's chunk is committed here
			return err
		}
		sdk.Count(ctx, "lines", int64(s.Lines))
	}
	return nil
}
```

### A stateful step: count

`count` keeps totals across chunks and writes them in `OnEOS`, once all
input is read. Register it before reading. From
{{< repolink "examples/log-report/count/main.go" >}}:

```go
func handle(ctx context.Context, in sdk.Input, out sdk.Output) error {
	levels := map[string]int64{}
	in.OnEOS(func(ctx context.Context, out sdk.Output) error {
		f, err := out.Create(ctx, sdk.DefaultOutput, "csv")
		if err != nil {
			return err
		}
		fmt.Fprintln(f, "level,lines")
		for _, level := range slices.Sorted(maps.Keys(levels)) {
			fmt.Fprintf(f, "%s,%d\n", level, levels[level])
		}
		return f.Close()
	})
	for c, err := range in.Chunks(ctx) {
		if err != nil {
			return err
		}
		src, err := c.Open()
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(src)
		for sc.Scan() {
			level, _, _ := strings.Cut(sc.Text(), " ")
			levels[level]++
		}
		src.Close()
		if err := sc.Err(); err != nil {
			return err
		}
	}
	return nil
}
```

### More in the SDK

- **Several inputs.** An edge can end in a named input, a *port*.
  `in.Port("lookup").Chunks(ctx)` reads one port only, so a component can
  read a small lookup table to the end first and then stream a large input.
- **Several outputs.** `out.Create(ctx, "rejected", "csv")` writes to a
  named output. An edge from it names the output: `{from: grep, output:
  rejected, to: audit}`.
- **Replicas.** `in.Replica()` and `in.Replicas()` say which replica this
  is. You seldom need them: the agent already gives each replica its own
  chunks.
- **Tools that write files themselves.** `File.Path()` is the output
  file's path, so another program can write to it before you close it.

### Build the image

The template's Dockerfile builds a static binary on `scratch`. Build from
your own module:

```console
$ cp -r templates/component-go grep && cd grep
$ go mod init example.com/grep
$ go get alpheus-dev.org/alpheus@latest
$ go test ./...
$ podman build -t registry.example/grep:v1 .
$ podman push registry.example/grep:v1
```

Use the pushed image's digest in the flow:
`image: registry.example/grep@sha256:…`.

## Components in Python

There is no Python SDK. The template
{{< repolink "templates/component-python/" >}} has a small module,
`alpheus`, of about 300 lines, which implements the same contract over
gRPC, and local mode. Copy it with your component. The Dockerfile
generates the gRPC code from the contract, so the folder holds no
generated code. The module needs `grpcio` and `protobuf`, pinned in
`requirements.txt`.

The API mirrors Go's:

| Go | Python |
|---|---|
| `sdk.Run(handle)` | `alpheus.run(handle)` |
| `in.Settings(&s)` | `s = inp.settings()` (a dict) |
| `in.Chunks(ctx)` | `inp.chunks()`, or `inp.chunks("port")` |
| `in.OnEOS(fn)` | `inp.on_eos(fn)` |
| `out.Create(ctx, output, ext)` | `out.create(output, ext)`, a file you can use with `with` |
| `sdk.Count`, `sdk.Error`, `sdk.Warn`, `sdk.Progress` | `alpheus.count`, `alpheus.error`, `alpheus.warn`, `alpheus.progress` |
| `sdk.ErrUsage` | `raise alpheus.UsageError(...)` |

### A step: grep

From {{< repolink "templates/component-python/component.py" >}}:

```python
import alpheus


def handle(inp, out):
    s = inp.settings()
    unknown = set(s) - {"match", "invert"}
    if unknown:
        raise alpheus.UsageError(f"unknown settings: {', '.join(sorted(unknown))}")
    match = s.get("match", "").encode()
    invert = bool(s.get("invert", False))

    for chunk in inp.chunks():
        if chunk.bytes == 0:
            alpheus.warn("empty_chunk", "the input chunk is empty", n=chunk.n)
            continue
        dst = None  # created at the first matching line
        lines_in = lines_out = 0
        with open(chunk.path, "rb") as src:
            for line in src:
                lines_in += 1
                if (match in line) != invert:
                    if dst is None:
                        dst = out.create("default", chunk.ext)
                    dst.write(line)
                    lines_out += 1
        if dst is not None:
            dst.close()
        alpheus.count("lines_in", lines_in)
        alpheus.count("lines_out", lines_out)


if __name__ == "__main__":
    alpheus.run(handle)
```

### A source: generate

From {{< repolink "examples/log-report/python/generate.py" >}}:

```python
def handle(inp, out):
    s = inp.settings()  # {"chunks": 20, "lines": 5000}
    for n in range(s["chunks"]):
        with out.create("default", "log") as f:  # committed when closed
            for i in range(s["lines"]):
                level = "ERROR" if (n * s["lines"] + i) % 7 == 0 else "INFO"
                f.write(f"{level} line {i + 1} of chunk {n + 1}\n".encode())
        alpheus.count("lines", s["lines"])
```

### A stateful step: count

From {{< repolink "examples/log-report/python/count.py" >}}:

```python
def handle(inp, out):
    levels = {}

    def write_totals(out):
        with out.create("default", "csv") as f:
            f.write(b"level,lines\n")
            for level in sorted(levels):
                f.write(f"{level},{levels[level]}\n".encode())

    inp.on_eos(write_totals)  # before reading, so the agent knows
    for chunk in inp.chunks():
        with open(chunk.path, "rb") as src:
            for line in src:
                level = line.split(b" ", 1)[0].decode()
                levels[level] = levels.get(level, 0) + 1
```

The Go and Python versions of the log report give byte-for-byte the same
result. {{< repolink "examples/log-report/test.sh" >}} checks that in
local mode. On the test cluster, testcase07 runs both templates in agent
mode, one after the other, and kills a pod of each in the middle of the
stream.

### Build the image

```console
$ cp -r templates/component-python grep && cd grep
$ ./test.sh
$ podman build -t registry.example/grep:v1 .
```

The image runs as uid 65532 with a read-only root file system, as the pod
requires, so Python is told not to write bytecode.

## Testing without Kubernetes

Both the SDK and the Python module have a *local mode*. The component
reads input chunks from a folder and writes its outputs to another, with
no agent and no cluster:

```console
$ ALPHEUS_MODE=local \
  ALPHEUS_LOCAL_INPUT=./in \
  ALPHEUS_LOCAL_OUTPUT=./out \
  ALPHEUS_SETTINGS='{"match": "ERROR"}' \
  ./grep
```

- Every file in the input folder is a chunk, read in name order. The
  `n`-th file is chunk `n`.
- Each output gets a folder, `out/default/`, with the chunks as
  `part-000001.<ext>` and so on.
- `out/_summary.json` has the counters and the number of errors and
  warnings, and `out/_errors.jsonl` has the error log.
- An output folder can be the next step's input folder, so a whole flow
  can be run step by step. That is what the log-report test does.
- `ALPHEUS_REPLICA` and `ALPHEUS_REPLICAS` make the component read only
  the chunks that replica would get.

In Go, tests call `sdk.RunLocal` with the handler directly. See
{{< repolink "templates/component-go/main_test.go" >}}.

## Other languages

A component in any language can talk to the agent directly. The contract
is {{< repolink "proto/component.proto" >}}: gRPC over the Unix socket in
`ALPHEUS_SOCKET`, with no TLS. Data never goes over the socket, only file
paths. The calls, in the order a component makes them:

| Call | When |
|---|---|
| `Observe` | Open once at the start: a stream of counters, log entries and progress, each with a sequence number and the ID of the input chunk it belongs to (0 for none) |
| `Next` | Get the next input chunk, or end of stream. Set `stateful` if the component writes at the end of the stream |
| `Create` | Start an output chunk. Give the ID of the input chunk it is made from, or 0 for a source or at the end of the stream. It returns the path to write to |
| `Close` | The output file is written |
| `Done` | The input chunk is handled. Give the last `Observe` sequence number, so nothing reported for the chunk is left out |
| `Finish` | All input is read and all output closed. The agent ends every output |

The Python module, {{< repolink "templates/component-python/alpheus/__init__.py" >}},
is a short, complete example of these calls.
