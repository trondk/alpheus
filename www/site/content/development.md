---
title: Development and testing
description: How Alpheus is built and tested, and the local Kubernetes cluster the end-to-end tests run on.
lastmod: 2026-10-31
---

Everything here runs on one Linux machine. Nothing needs a cloud
account.

## Build and unit tests

You need Go, in the version pinned in {{< repolink "go.mod" >}}. The
code generators, `controller-gen` and the protobuf plugins, are Go tools
in the same file, so `go tool` fetches them. Regenerating the gRPC code
also needs [buf](https://buf.build/docs/installation).

| Command | What it does |
|---|---|
| `make generate` | Writes the CRD, deepcopy functions, RBAC role and gRPC code from their sources |
| `make check-generated` | Fails when generated code is not up to date |
| `make vet` | Runs `go vet` |
| `make check-deps` | Fails if Alpheus depends on Parquet or Arrow; it never reads a chunk's content |
| `make test` | Runs every test with the race detector |
| `make test-chart` | Lints the Helm chart and checks what it renders; needs [Helm](https://helm.sh/docs/intro/install/) but no cluster |
| `make test-templates` | Runs the Python component template and the log-report example in local mode, in containers; needs podman or docker |
| `make cli` | Builds the `alpheus` CLI in `_out/bin` |

`make test` needs no cluster and no S3. The agents and components run in
one process against storage in memory, through pipelines of a few
steps, one of them with ten replicas, and through fan-out and fan-in. The
tests crash steps halfway and check that the result is still right. One
of them stops the pipeline at every write to storage in turn, both before
and after the write lands, then starts it again and checks that nothing
is missing and nothing is there twice: not in the data, not in the
counters, and not in the error log. It does that for a pipeline with
replicas and for one with fan-out, where a step writes each chunk to two
streams one after the other. The
controller's tests run against a real Kubernetes API server and etcd
from envtest, which `make test` downloads the first time.

## The test cluster

{{< repolink "vagrant/" >}} sets up a single-node Kubernetes cluster on
Talos Linux in a local VM, with the services the end-to-end tests
need. It uses Vagrant with the libvirt provider, so you need libvirt
and KVM, the vagrant-libvirt plugin, `talosctl`, `kubectl` and, for the
end-to-end tests, `helm` and `podman`.

```console
$ cd vagrant
$ ./deploy.sh               # about 5 minutes the first time
$ ./credentials.sh          # keys, addresses and kubectl get nodes
$ source ./credentials.sh   # sets KUBECONFIG and TALOSCONFIG
$ ./destroy.sh              # removes the VM
```

| Service | Purpose | From your machine |
|---|---|---|
| Garage | S3-compatible storage: chunks in `alpheus-data`, results in `alpheus-results` | `https://<VM IP>:30900` |
| garage-ui | Garage's dashboard | `https://<VM IP>:30901` |
| Zot | Container registry for the test images | `https://registry.alpheus.test:30500` |
| HAProxy | HTTPS in front of the three | |

`deploy.sh` makes a test CA, and Talos trusts it, so the cluster pulls
images from Zot over HTTPS. Flow files name images
`registry.alpheus.test:30500/…`, which Talos resolves to the VM itself,
so they work whatever IP the VM gets. Every new VM gets new keys and a
new CA.

The registry has no login, and `credentials.sh` prints the keys in plain
text. That is acceptable only because the VM is on libvirt's host-only
network, which other machines cannot reach. Do not attach it to a
bridged or public network.

## Tests against the cluster

With the cluster running:

| Command | What it does |
|---|---|
| `make test-crd` | Checks the CRD's schema, defaults and validation rules against the cluster's API server |
| `make test-s3` | Runs the storage tests against Garage over HTTPS |
| `make test-agent-s3` | Runs the agent pipeline against Garage, with signals through the hub and a restart |
| `make e2e` | Builds the images, pushes them to Zot, installs the controller and runs the end-to-end tests |
| `make bench` | Runs testcase01, testcase02 and testcase03 and saves their measurements |
| `make bench-etl` | Runs the flagship ETL flow with 1, 2, 4 and 8 replicas of export, split and clean, and prints a table of the results |

## End-to-end tests

`make e2e` runs {{< repolink "hack/e2e.sh" >}}. It builds the agent, the
controller and the test components as static binaries, each in an image
with nothing else in it, and pushes them to Zot under the same paths as
on GitHub Container Registry. It installs the CRD and the controller with
the Helm chart in {{< repolink "charts/alpheus/" >}}, the way a user
does, with the `registry` value set to Zot and the images pinned by
digest. Then it runs the
tests in {{< repolink "test/e2e/" >}} as ordinary Go tests against the
cluster.

The first test case, testcase01, is simple, and also shows how the
system performs. Three steps run the test component `bench`:

```
source ──> pass ──> sink ──> result
```

1. `source` writes 64 chunks of 4 MiB. Each starts with its number and
   the time it was written, followed by bytes from a generator with a
   fixed seed.
2. `pass` copies each chunk unchanged, so every chunk goes through
   object storage twice.
3. `sink` checks every byte against the generator and notes when the
   chunk arrived. At the end it writes one line per chunk as the result.

The test waits for the run to succeed and fetches the result using only
what the run's status says about it. A missing, repeated or changed
chunk fails the run. Then it reports:

| Measurement | From |
|---|---|
| Startup | The run is created until every component runs |
| Stream | The first chunk is written until the last is received |
| Total | The run is created until it has succeeded |
| Throughput | Bytes divided by the stream's duration |
| Latency | Per chunk, from written to received: median, 95th percentile and maximum |

On the single-node test cluster, with the images already pulled, startup
takes about a second and data moves at about 40 MB/s through the two
hops.

The second, testcase02, uses the same component for fan-out and fan-in:

```
            ┌──> left  (3 replicas) ──┐
source ─────┤                         ├──> sink ──> result
            └──> right (2 replicas) ──┘
```

Every chunk from `source` goes to both `left` and `right`, and `sink`
must get each chunk exactly once from each of them. The test reads the
acknowledgements in object storage to check that each branch split the
chunks between its replicas as it should, and that `sink` finished only
after both branches had ended.

The third, testcase03, tests replicas, and in particular a replica that
gets work but has nothing to pass on:

```
source ──> filter (10 replicas) ──> sink ──> result
```

`filter` drops every chunk whose number is a multiple of 10. Chunks are
split between the replicas by number, so replica 0 gets exactly chunks
10, 20, 30 … and drops them all. It must still end its stream properly,
or `sink` would wait for it forever. The test checks from the
acknowledgements in object storage that the ten replicas split the
chunks between them without overlap, that replica 0's stream ended with
no chunks, and that `sink` got exactly the other chunks.

testcase03 also keeps its result in a bucket of its own. The run's
status points readers to a key that can only read that bucket, not to
the agents' key, which can write everything. The test fetches the result
with that key and checks that it cannot write.

`make bench` runs all three with the images from the last `make e2e`,
and saves the measurements as JSON in `_out/bench/`. Change their sizes
with `BENCH_CHUNKS`, `BENCH_SIZE` (bytes per chunk) and
`BENCH_MAX_UNACKED`, and the replicas with `BENCH_PASS_REPLICAS`,
`BENCH_LEFT_REPLICAS`, `BENCH_RIGHT_REPLICAS` and
`BENCH_FILTER_REPLICAS`:

```console
$ BENCH_CHUNKS=256 BENCH_PASS_REPLICAS=4 make bench
```

## Restarts, backpressure and chaos

Three more test cases check that a run survives failures. They kill pods
by deleting them with a one-second grace period: the agent and the
component stop together and cut off any write in progress, as in a crash.
Alpheus starts a new attempt once Kubernetes confirms that the old
containers have stopped, and the test checks the result as before.

- testcase04 runs `source`, `pass` and `sink` three times and kills one
  of them halfway each time. A source starts over and the agent skips
  the chunks it already has, after checking that they are the same. A
  step in the middle carries on from the first chunk it had not
  finished. `sink` keeps its state in memory, so it reads all of its
  input again. Every chunk must arrive exactly once, and the counters
  must be exact.
- testcase05 makes a step wait 500 ms before each chunk and samples
  object storage while the run goes. No replica of the slow step may
  ever have more chunks waiting than `maxUnacked`, and the slow step
  must set the pace for the whole run.
- testcase06 runs testcase02's fan-out and fan-in and kills a random pod
  about every three seconds, twelve pods in all unless
  `BENCH_CHAOS_KILLS` says otherwise, then lets the run finish. The
  result and the counters must still be exact. `BENCH_CHAOS_SEED`
  repeats a run's choice of pods.

## The component templates

testcase07 runs the two component templates in agent mode, the way users
will run their own components:

```
source ──> go (2 replicas) ──> python ──> sink ──> result
```

`go` is {{< repolink "templates/component-go/" >}} and `python` is
{{< repolink "templates/component-python/" >}}, both with an empty match,
so every byte passes and `sink` checks the chunks as in testcase01. The
test kills `go`'s replica 0 when it has committed a quarter of its chunks,
and `python` when it has committed half. The run must succeed with every
chunk once and unchanged, both steps must count the same lines in and out,
and each must have restarted once. `hack/images.sh` builds the Python
template with its own Dockerfile.

## The flagship: large-file ETL

testcase08 runs the flagship use case,
[large-file ETL on-prem](/docs/etl/), with
{{< repolink "examples/meter-etl/" >}}: `export` writes gzipped CSV files
of smart-meter readings from a seed, `split` cuts them into chunks,
`clean` checks every row in four replicas, and `aggregate` sums them per
region and hour. The test computes the expected totals with the same
generator and requires everything to be exact: every row read once, every
bad row rejected and logged, one dataset chunk per split chunk, and the
totals equal to the generator's, while it kills a `clean` pod in the
middle. Each file gives more chunks than `maxUnacked`, which once
deadlocked the agent; the test keeps it that way.

`make bench-etl` runs the same flow without killing anything, with 16 files
of 2 million readings and `export`, `split` and `clean` at 1, 2, 4 and 8
replicas each, and prints a table of the processing time and throughput
with the node it ran on.
`REPLICAS`, `ETL_FILES`, `ETL_ROWS` and `ETL_SPLIT_BYTES` change it.

`make test-templates` runs the Python template and the log-report example
from [Writing components](/docs/components/) in local mode, in
containers, without the cluster.

`make e2e` runs them with the others. Every end-to-end test runs in its
own namespace and deletes it at the end. The test fails if the namespace
is not gone within three minutes, since a run must clean up its data
even when its storage keys are deleted with it.

Run every end-to-end test again without rebuilding the images with
`SKIP_IMAGES=1 hack/e2e.sh`. Go images are built with fixed timestamps,
so unchanged code gives the same digests.
