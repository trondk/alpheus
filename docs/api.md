---
title: Using Alpheus from a program
description: Start a run, follow it, fetch its result and cancel it from your own program, through the Kubernetes API and S3 alone, with kubectl, Go or plain HTTP, and the rights the program needs.
weight: 12
lastmod: 2026-11-06
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
run's name is at most 40 characters and must be new in its namespace; an
invalid graph or an image without a digest makes the run fail at once
with the reason `InvalidSpec`, which `alpheus validate` checks
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

## What a run can reach

A run's pods can reach only DNS, the storage and the signal hub. A step
that has to reach your systems, such as a source that reads from your
database, needs a NetworkPolicy of your own: see
[What a run can reach](/docs/isolation/).
