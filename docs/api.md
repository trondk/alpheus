---
title: "The API: using Alpheus from a program"
description: Start a run, follow it, fetch its result, find out why it failed and run the failure again from your own program, through the Kubernetes API and S3 alone, with kubectl, Go or plain HTTP, and the rights the program needs.
weight: 12
lastmod: 2026-11-08
---

Alpheus has no API of its own. A run is a `StreamRun` object in
Kubernetes, and its result is files in S3, so a program uses Alpheus the
way `kubectl` and the `alpheus` CLI do:

1. it creates a `StreamRun` through the Kubernetes API;
2. it reads the run's `status` until the run has finished;
3. it fetches the result from the bucket that `status.results` names.

Any Kubernetes client works: kubectl, client-go, the official clients for
Python, Java and other languages, or plain HTTP. The controller only
reacts to the objects; its one network port, the signal hub, is for the
run's own agents.

The examples use the log-report example from
[Getting started](/docs/getting-started/), in the namespace `flows`.

## The rights a program needs

A program that runs flows in one namespace needs a Role like this, bound
to its ServiceAccount or user:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: alpheus-runner
  namespace: flows
rules:
  - apiGroups: [alpheus-dev.org]
    resources: [streamruns]
    verbs: [create, get, list, watch, patch, delete]
  # The key to fetch the result with; leave this out if the program gets
  # the key another way.
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [alpheus-s3]
    verbs: [get]
```

`patch` is for cancelling and `delete` for cleaning up; leave them out if
the program does neither. It needs no rights on pods: the controller
creates them. For a key that can only read the results, give the run
`spec.storage.resultsBucket` and `resultsSecretRef`; `status.results`
then names that Secret.

## Start a run

Create the object. With kubectl:

```console
$ kubectl -n flows create -f flow.yaml
streamrun.alpheus-dev.org/log-report created
```

Over HTTP, it is a `POST` of the same object as JSON:

```console
$ curl -X POST -H 'Content-Type: application/json' --data @flow.json \
    $APISERVER/apis/alpheus-dev.org/v1alpha1/namespaces/flows/streamruns
```

with the program's token (`-H "Authorization: Bearer $TOKEN"`) and the API
server's CA (`--cacert`), or through `kubectl proxy`. A `201` means the
run exists; the controller starts it within a second.

The spec cannot change after the run is created, except `spec.cancel`. A
run's name is at most 40 characters and must be new in its namespace. The
API server rejects an invalid graph and an image without a digest with a
`422` and a message that names the problem, so nothing is stored. A cycle
in the edges is the exception: the run is stored and fails at once with
the reason `InvalidSpec`. `alpheus validate` checks all of it
beforehand.

## Follow it

The run has finished when its condition `Progressing` is `False`. Then
`status.phase` says how it ended:

| `status.phase` | Meaning | What to read |
|---|---|---|
| `Succeeded` | every step succeeded | `status.results` |
| `PartiallySucceeded` | finished, but a step with `failurePolicy: continue` gave up on some chunks | `status.results`, and `status.steps[].replicas[].failedChunks` for what is missing |
| `Failed` | the run stopped | `status.reason`, `status.message`, `status.failedStep` |
| `Cancelled` | `spec.cancel` was set | |

`status.reason` is a code a program can rely on, such as
`ComponentFailed` or `StorageFull`; [When a run fails](/docs/debugging/)
lists them all. `status.message` says the same in words for a person.

kubectl waits for the end with:

```console
$ kubectl -n flows wait streamrun/log-report --for=condition=Progressing=False --timeout=1h
streamrun.alpheus-dev.org/log-report condition met
$ kubectl -n flows get streamrun log-report -o jsonpath='{.status.phase}'
Succeeded
```

A program can poll the object or watch it
(`GET …/streamruns?watch=true&fieldSelector=metadata.name=log-report`).
While it runs, `status.steps` has each step's phase, counters and
restarts, and `status.edges` each edge's chunks and lag; the controller
updates them at most once a second.

## Fetch the result

Each entry in `status.results` is one output of a step that no other step
reads:

```json
{
  "step": "count",
  "output": "default",
  "endpoint": "http://garage.storage.svc.cluster.local:3900",
  "region": "garage",
  "bucket": "alpheus-data",
  "prefix": "results/log-report/448394aa-…/count/default/",
  "secretRef": "alpheus-s3",
  "chunks": 1,
  "bytes": 24
}
```

The Secret has the keys as `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. Each replica of the step has a folder under the
prefix, `0/`, `1/` and so on, with a manifest and the chunks:

```
results/log-report/<uid>/count/default/0/manifest.json
results/log-report/<uid>/count/default/0/part-000001.csv
```

The manifest is the only list to trust. A file that is in the folder but
not in the manifest was never committed, for example after a crash, so
read only the chunks it lists:

```json
{
  "version": 1,
  "segments": [],
  "chunks": [
    {"n": 1, "ext": "csv", "bytes": 24, "sha256": "…"}
  ],
  "eos": true
}
```

- Chunk `n` is the file `part-<n, six digits>.<ext>`.
- `eos` is `true` when the replica has written its last chunk.
- Check each chunk's size and SHA-256 against `bytes` and `sha256`.
- After 1,000 chunks, the manifest moves them to a sealed segment
  (`manifest-0000.json`, then `manifest-0001.json`), listed in `segments`,
  oldest first. A segment has `chunks` in the same form. Read the segments
  first, then `chunks`.

Any S3 client can read them. Use path-style requests (`endpoint/bucket/key`)
and the region from the result. The endpoint is the address inside the
cluster; a program outside it reaches the storage by another address.
Whoever runs the cluster can put that address on the storage Secret as
the annotation `alpheus-dev.org/external-endpoint`, and a CA for it, as
PEM, as `alpheus-dev.org/external-ca`. The CLI uses them; controller and
agents do not.

`alpheus results ls <run> -o json` lists exactly the committed chunks of a
run's results as `s3://<bucket>/<key>` with their size and SHA-256,
`alpheus results get` fetches them, and `alpheus results copy` copies them
with `alpheus-results.json` to another bucket; the
[CLI reference](/docs/cli/#alpheus-results-ls) has the shape.

## In Go

{{< repolink "examples/api-client/main.go" >}} does all of the above in
under 300 lines: it creates a run from a flow
file, waits for it, and downloads each result with its chunks checked. It
uses the types in `alpheus-dev.org/alpheus/api/v1alpha1`, the
controller-runtime client and minio-go, and nothing else from Alpheus:

```go
var r v1alpha1.StreamRun
yaml.UnmarshalStrict(data, &r)
r.Namespace = "flows"
k8s.Create(ctx, &r)

for {
	k8s.Get(ctx, client.ObjectKeyFromObject(&r), &r)
	c := meta.FindStatusCondition(r.Status.Conditions, v1alpha1.ConditionProgressing)
	if c != nil && c.Status == metav1.ConditionFalse {
		break
	}
	time.Sleep(2 * time.Second)
}
```

On the test cluster:

```console
$ go run ./examples/api-client -f _out/log-report.yaml -n flows \
    -endpoint https://$(vagrant/talos.sh ip):30900 -ca-file vagrant/_out/ca.crt
created flows/log-report
Running 0/3
Succeeded 3/3
count.default replica 0: 1 chunks in results/count.default/0
$ cat results/count.default/0/part-000001.csv
level,lines
ERROR,14286
```

{{% planned %}}`alpheus-dev.org/alpheus` cannot be fetched with `go get`
until the site and the code are online. Until then, clone the repository
and point your module at it with
`replace alpheus-dev.org/alpheus => ../alpheus`.{{% /planned %}}

## Cancel and clean up

Cancel a run by setting `spec.cancel`, the one field that can change:

```console
$ kubectl -n flows patch streamrun log-report --type merge -p '{"spec":{"cancel":true}}'
```

The run ends `Cancelled` and its pods go. Deleting the run deletes its
pods and its chunks, but not its result, so a program can delete the run
as soon as it has fetched what it needs. While the run exists, the
controller deletes the result when `resultsTTL` has run out, 7 days by
default; after that, `alpheus results gc` does.

## When a run fails

Everything the CLI and `alpheus ui` show about a failure, a program can
read too: there is no other way in. The JSON formats below each have a
`version`; a format only changes in a way that older readers can still
read, or else with a new version number, so reject a version you do not
know.

### What the status says

| Field | What it holds |
|---|---|
| `status.reason`, `status.message` | Why the run stopped, as a code and in words |
| `status.failedStep`, `status.failedReplica` | The replica whose failure stopped the run |
| `status.steps[].replicas[]` | Each replica's `phase`, `attempt`, `restarts`, `reason`, `message` and `pod` |
| `status.steps[].logs` | Where the logs of the step's failed attempts are kept |
| `status.steps[].errorLog` | Where the step's error log is, with `errors`, `warnings` and `errorsDropped` |
| `status.steps[].failures` | Where the step's failed attempts are kept for a replay |

The controller also writes Kubernetes events on the run: when it starts,
at each failed attempt and restart, when a replica gives up, and when the
run ends. [When a run fails](/docs/debugging/) has the reason codes.

All the paths are in the run's results bucket, under
`results/<run>/<uid>/`, read with the Secret in `status.results`.

### The logs of failed attempts

Before the controller replaces or deletes a failed attempt's pod, it
keeps the end of each container's log, at most 4 MiB:

```
_logs/<step>/<replica>/attempt-<n>/component.log
                                   agent.log
                                   agent-previous.log   (if the agent restarted in place)
```

### The error log

Bad records a component reported, committed with the chunks:

```
_errors/<step>/<replica>/manifest.json
                         log-000001.jsonl
                         log-000002.jsonl …
```

`manifest.json` lists the segments, oldest first, and the replica's
totals:

```json
{"version": 1, "run": "meter-etl", "uid": "…", "step": "clean", "replica": 0,
 "epoch": 1, "seq": 24, "segments": ["log-000001.jsonl", "…", "log-000024.jsonl"],
 "counts": {"rows_in": 6522839, "rows_out": 6490308, "rows_rejected": 32531},
 "errors": 32531, "warnings": 0, "errorsDropped": 0}
```

Each line of a segment is one entry, here from the
[ETL example](/docs/etl/):

```json
{"time": "2026-11-06T09:40:41.103742555Z", "level": "error", "code": "bad_timestamp",
 "message": "row rejected", "fields": {"line": 94}, "step": "clean", "replica": 0,
 "chunk": "default/split/4"}
```

`chunk` is the input chunk the entry came from: its port, the step that
made it and its number.

Read only the segments the manifest lists.

### The failed attempt

The inputs of a failed attempt are kept so it can run again on its own:

```
_failures/<step>/<replica>/attempt-<n>/failure.json
                                       input/<file>
```

The highest `attempt-<n>` is the last one. `failure.json`:

```json
{
  "version": 1,
  "run": "debug-demo", "namespace": "flows", "uid": "…",
  "step": "pass", "replica": 0, "replicas": 1, "attempt": 2,
  "reason": "ComponentFailed",
  "message": "the component exited with code 1: chunk 5 (source chunk 5) is one this component cannot handle",
  "time": "…",
  "image": "registry.alpheus.test:30500/alpheus-test/bench@sha256:…",
  "settings": {"role": "pass", "fail": 5, "delay": 1000},
  "inputs": [
    {"port": "default", "edge": "source-pass", "from": "source", "producerReplica": 0, "n": 5,
     "bytes": 65536, "sha256": "…", "file": "source-pass-0-000005.bin"}
  ],
  "logs": "results/debug-demo/<uid>/_logs/pass/0/attempt-2/"
}
```

Check each file in `input/` against `bytes` and `sha256` before you use
it. The chunks of a side input (an edge with `side: true`) have
`"side": true`, and all of them are there, since the component reads its
lookup data whole.

### Running it again

With the CLI, a program gets the result as JSON:

```console
$ alpheus replay debug-demo pass -n flows --with cluster -o json
{"failure": {…}, "pod": "debug-demo-pass-0-replay-2-cfcdc2", "exitCode": 1}
```

Without it, put each input in `in/<port>/` and run the image in
[local mode](/docs/components/#testing-without-kubernetes). So that the
component sees each chunk's producer and number as in the run, write
`in/<port>/_chunks.json` with `{"<file>": {"step": <from>, "replica":
<producerReplica>, "n": <n>}}` for each of the port's inputs. Run it with:

| Variable | Value |
|---|---|
| `ALPHEUS_MODE` | `local` |
| `ALPHEUS_LOCAL_INPUT` | `in/default`, for a step with one input port |
| `ALPHEUS_LOCAL_INPUTS` | `<port>=in/<port>,…`, for several ports |
| `ALPHEUS_LOCAL_OUTPUT` | `out` |
| `ALPHEUS_SETTINGS` | `settings` from `failure.json` |
| `ALPHEUS_REPLICA`, `ALPHEUS_REPLICAS` | `0` and `1`: the inputs are already this replica's |
| `ALPHEUS_LOG_LEVEL` | `debug` |
| `TMPDIR`, `HOME` | `/tmp` |

Run it as the pod does: as uid 65532, with a read-only root file system,
a writable `/tmp`, no network and no capabilities. A step with no inputs
(`"source": true`) runs with none.

To run it in the cluster instead, so the data stays there, create a pod
in the run's namespace like the one `alpheus replay --with cluster`
makes:

- owned by the run, with the labels `alpheus-dev.org/replay: <run>`,
  `alpheus-dev.org/network: <run>`, `alpheus-dev.org/step` and
  `alpheus-dev.org/replica`, so the run's NetworkPolicy applies;
- the ServiceAccount `alpheus-run-<run>`, without a token, and
  `restartPolicy: Never`;
- an `emptyDir` at `/replay`, with the step's `scratch` as its size;
- an init container with the agent's image, args
  `-fetch-failure <the attempt's prefix> -dest /replay`, and the S3
  variables the step's agent gets (`ALPHEUS_S3_ENDPOINT`,
  `ALPHEUS_S3_BUCKET`, `ALPHEUS_S3_REGION`, and `ALPHEUS_S3_ACCESS_KEY` and
  `ALPHEUS_S3_SECRET_KEY` from the Secret). It fetches and checks the
  inputs into `/replay/in/` and makes `/replay/out/`;
- the component container with the image from `failure.json`, or a new
  version to try a fix, the variables above with `/replay/in` and
  `/replay/out`, `/tmp` as a `subPath: tmp` of the same volume, and the
  step's `resources` and `envFrom`.

The component's log is the replay's result, and its exit code says
whether the failure came back.

{{% planned %}}The agent's image will be in `status.agentImage`. Until
then, it is the `-agent-image` argument of the Deployment
`alpheus-system/alpheus-controller`.{{% /planned %}}

A program that does this needs more than the Role at the top of this
page: `get` on `pods/log`, and `create`, `get` and `delete` on `pods` for a
replay in the cluster.

## What a run can reach

A run's pods can reach only DNS, the storage and the signal hub. A step
that has to reach your systems, such as a source that reads from your
database, needs a NetworkPolicy of your own: see
[What a run can reach](/docs/isolation/).
