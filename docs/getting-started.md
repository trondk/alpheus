---
title: Getting started
description: Install Alpheus, run the log-report example, follow it and find its result.
weight: 5
lastmod: 2026-11-06
---

This page installs Alpheus from source, runs the example flow from
[Writing components](/docs/components/), and shows how to follow it and
find its result. [How a run runs on Kubernetes](/docs/kubernetes/)
explains what you will see in the cluster.

{{% planned %}}Nothing is released yet, so there are no images to pull and
no published chart. This page builds everything from the repository and
uses the local test cluster from [Development](/development/), which has
an S3 store (Garage) and a registry (Zot). Once `v0.1.0` is out, one
`helm install` replaces the first steps.{{% /planned %}}

## What you need

- A Kubernetes cluster, version 1.29 or newer, since the agent runs as a
  native sidecar. The test cluster is two VMs; see
  [Development](/development/) to set it up.
- An S3-compatible bucket and keys that can read and write it. The store
  must have strong read-after-write consistency for new objects,
  overwrites and listings: Alpheus is tested with AWS S3, Garage and
  MinIO. With any other store, the agent warns in its log when it starts.
- A container registry the cluster can pull from.
- Go, Helm and podman on your machine, and a clone of the repository.

## 1. Install the controller

Build the agent and the controller, push them to the registry, and install
them with the Helm chart. With the test cluster, the defaults push to Zot:

```console
$ hack/images.sh
$ . _out/images.env
$ helm upgrade --install alpheus charts/alpheus \
    --namespace alpheus-system --create-namespace \
    --set registry=${CONTROLLER_IMAGE%%/*} \
    --set controller.image.digest=${CONTROLLER_IMAGE#*@} \
    --set agent.image.digest=${AGENT_IMAGE#*@}
```

The chart installs the `StreamRun` CRD, the controller and its signal hub.
The agent's image must be pinned by digest. Each run gets a NetworkPolicy,
which needs a network plugin that enforces them, such as Cilium (see
[What a run can reach](/docs/isolation/)). `make e2e` does the same before
it runs the tests.

Then build the CLI and check that it reaches the cluster:

```console
$ make cli
$ export PATH=$PWD/_out/bin:$PATH
$ alpheus version
CLI:         v0.0.0-…
Controller:  v0.1.0 in alpheus-system (registry.alpheus.test:30500/alpheus-dev/controller@sha256:…)
CRD:         v1alpha1 (storage)
```

## 2. Give a namespace the storage keys

Runs live in any namespace. Each run names a Secret with the keys for its
bucket. Only the agents get the keys, never the components.

```console
$ kubectl create namespace flows
$ kubectl label namespace flows pod-security.kubernetes.io/enforce=restricted
$ . vagrant/_out/secrets.env
$ kubectl -n flows create secret generic alpheus-s3 \
    --from-literal=AWS_ACCESS_KEY_ID=$GARAGE_DEFAULT_ACCESS_KEY \
    --from-literal=AWS_SECRET_ACCESS_KEY=$GARAGE_DEFAULT_SECRET_KEY
```

The pods of a run meet Pod Security level `restricted`, so the label can
stay.

The Secret names the storage as the cluster reaches it. For the CLI on
your machine, add the address it reaches the storage by, and the CA for
it, as annotations. Controller and agents do not use them:

```console
$ kubectl -n flows annotate secret alpheus-s3 \
    alpheus-dev.org/external-endpoint=https://$(vagrant/talos.sh ip):30900 \
    alpheus-dev.org/external-ca="$(cat vagrant/_out/ca.crt)"
```

Without them, give the CLI `--endpoint` and `--ca-file`, or set
`ALPHEUS_S3_ENDPOINT` and `ALPHEUS_S3_CA_FILE`, each time it reads the
bucket.

## 3. Build the example's images

The example has three steps: `generate` writes chunks of log lines,
`grep` keeps the ERROR lines in two replicas, and `count` totals the lines
per level. This script builds the three images, pushes them, and writes the
flow with their digests to `_out/log-report.yaml`:

```console
$ examples/log-report/images.sh          # in Go
$ examples/log-report/images.sh python   # or in Python
```

For another cluster, set `REGISTRY`, `PUSH_REGISTRY`, `ENDPOINT`,
`BUCKET` and `REGION`; the script's header says what each is.

## 4. Run it

```console
$ alpheus validate -f _out/log-report.yaml -n flows
_out/log-report.yaml is valid
$ alpheus run -f _out/log-report.yaml -n flows
streamrun flows/log-report created
follow it with: alpheus watch log-report -n flows
$ alpheus watch log-report -n flows
```

`watch` prints the run's status each time it changes and ends when the run
finishes. The lag of an edge is how many chunks its consumers have not yet
acked. On the test cluster the example takes a few seconds.

## 5. Look at the result

```console
$ alpheus get log-report -n flows
Name:            log-report
Phase:           Succeeded
…
STEP      PHASE      REPLICAS  RESTARTS  ERRORS  WARNINGS  COUNTS
generate  Succeeded  1/1       0         0       0         lines=100000
grep      Succeeded  2/2       0         0       0         lines_in=100000 lines_out=14286
count     Succeeded  1/1       0         0       0         -

RESULT         CHUNKS  BYTES  COUNTS
count.default  1       24 B   -

Fetch the results with the keys in their Secret:
  count.default  http://garage.storage.svc.cluster.local:3900/alpheus-data/results/log-report/<uid>/count/default/  region garage  secret alpheus-s3
```

The counters are exact, even when pods restart: every line was read once,
and 14,286 of the 100,000 were errors.

The result is a file in the bucket, under the prefix shown, in a folder
per replica: `0/part-000001.csv`. Get it with the CLI:

```console
$ alpheus results get log-report -n flows
Fetched 1 chunk (24 B) to log-report/
$ cat log-report/count.default/0/part-000001.csv
```

`results get` fetches every result of the run to `./<run>/`, or to the
folder `-d` names, checks each chunk against its size and SHA-256, and
writes `alpheus-results.json` with what it fetched. A fetch that is
stopped goes on where it stopped when you run it again.
`alpheus results cat log-report count -n flows` writes the result to
stdout instead, and `alpheus results ls` lists its chunks as S3 paths for
tools that read S3 themselves. Any S3 client can read them too, with the
keys in the Secret. `alpheus chunks log-report count -n flows` shows the
result's manifest.

When a run ends, `alpheus watch` and `alpheus get` list the commands that
fit it, with the run's name and namespace filled in.

`alpheus logs log-report grep -n flows` shows the `grep` components' logs,
and `-c agent` the agents'.

## 6. Clean up

```console
$ kubectl -n flows delete streamrun log-report
```

Deleting the run deletes its chunks, but not its result. While a run
exists, the controller deletes its result when `resultsTTL` has run out,
7 days by default. Once the run is deleted, `alpheus results gc` does
that: it finds results whose TTL has run out and deletes them.

## Next

- [Writing components](/docs/components/) explains what happened above,
  step by step, and how to write your own steps in Go or Python.
- The [CLI reference](/docs/cli/) lists every command and flag.
- [Using Alpheus from a program](/docs/api/) does the same as this page
  from your own code, through the Kubernetes API and S3.
