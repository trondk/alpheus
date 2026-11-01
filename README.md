# Alpheus

Alpheus streams files between containers on Kubernetes, with nothing
but object storage in between. It is a file mover, not a data engine: a
chunk is a file in any format, and Alpheus never opens it.

```
   ~~~~~~~~~~~\                                  /~~~~~~~~~>
 ______________\________________________________/____________
                 '- - - - - - - - - - - - - - -'
                       chunks in object storage
```

> **Status: design phase.** The API, the chunk format, the component
> SDK, the agent, the controller, the `alpheus` CLI and the Helm chart
> exist, and thirteen test flows run end to end on a test cluster,
> including restarts, pods killed at random, storage that goes down and a
> controller that dies. Nothing is released and no images
> are published. Nothing here is ready for use, and everything may change.


## Why "Alpheus"

In Greek mythology, Alpheus is a river god. His river runs through the
limestone of Arcadia, disappears underground, flows unseen beneath the
sea and rises again in Sicily. That is what Alpheus does with data:
it leaves one step, travels out of sight through object storage, and
appears in the next.


## What it does

- **Every step runs at the same time.** A flow is a graph of
  containers, and data flows through it continuously as chunks: files
  in any format, which Alpheus moves but never reads.
- **No message broker for data.** Steps exchange chunks through any
  S3-compatible store. Only small signals ("chunk 17 is ready") pass
  between them, and even those are optional: the manifest in object
  storage is the source of truth.
- **Parallel steps.** A step can run as several replicas that share
  its input: each chunk goes to exactly one replica.
- **Restartable by design.** A step that dies restarts and continues
  from the first chunk it had not acknowledged.
- **Backpressure built in.** A fast producer waits when its consumer
  falls behind, instead of filling the store.
- **Components only see files.** A sidecar agent handles storage,
  signals and retries, so a component reads input files and writes
  output files, in any format and any language.
- **Kubernetes-native.** A run is one `StreamRun` custom resource.
  Create it to start the run, read its status to follow it, delete it
  to stop it and clean up.
- **Results stay.** A step's output that no other step reads is a
  result. It is kept after the run is cleaned up, and the run's status
  says where it is and which credentials read it.

**Alpheus moves files; it does not process data.** A chunk is a file
in whatever format the components agree on: CSV, Parquet, JSON lines,
images, archives. Alpheus stores it, lists it in a manifest with its size
and checksum, and hands it to the next step, but never opens it. It has
no schema, no rows and no Arrow or Parquet code, and CI checks that it
never gets any. What is inside a chunk is entirely up to the components.

Alpheus needs only the Kubernetes API and an S3 endpoint: no host
access, no privileged containers, no node changes. It is designed to
run on immutable distributions such as Talos.


## Why Alpheus

Alpheus streams data concurrently between steps. It works with files in
any format and needs only an S3 endpoint: no broker and no privileged
components.

That fits environments where every extra service is a cost: sovereign
and air-gapped clusters, where each component has to be approved and
run in-house, and immutable infrastructure, where nothing may touch the
nodes. If you can run Kubernetes and an S3-compatible store such as
Garage or MinIO, you can run Alpheus.

The flagship use case is **large-file ETL on a sovereign on-prem
cluster**: a utility's nightly smart-meter exports, gzipped CSV files of
millions of readings, split, checked in parallel and summed on its own
hardware. [`examples/meter-etl/`](examples/meter-etl/) is the code,
testcase08 checks that every row counts exactly once even when a pod is
killed, and `make bench-etl` measures it;
[Large-file ETL on-prem](https://alpheus-dev.org/docs/etl/) has the
walk-through and the first results.


## How it compares

| | Steps run concurrently | What passes between steps | Runs besides Kubernetes and S3 | Lifetime |
|---|---|---|---|---|
| **Alpheus** | Yes, chunk by chunk | Files of any format and size, in object storage | Nothing | A run, which starts and ends |
| [Argo Workflows](https://argoproj.github.io/workflows/) | No: a step starts when the steps before it have finished | Artifacts in object storage | Argo's controller | A workflow run |
| [Pachyderm](https://github.com/pachyderm/pachyderm) | A pipeline starts on its input's finished commits | Versioned files in object storage | Pachyderm's services and a database | Long-lived pipelines over versioned data |
| [Numaflow](https://numaflow.numaproj.io/) | Yes, message by message | Messages in an inter-step buffer | Numaflow's controller and a JetStream cluster | Long-running pipelines |
| Diskless Kafka | Producers and consumers run at the same time | Records in a log kept in object storage | Brokers or agents and a metadata store | Long-lived topics |

"Diskless Kafka" means Kafka-compatible brokers that keep their log in
object storage instead of on local disks: products such as WarpStream,
AutoMQ and Bufstream, and Apache Kafka's own diskless topics
([KIP-1150](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150%3A+Diskless+Topics)),
which were accepted in March 2026 but are not yet in a stable release.

Use one of them instead when it fits better:

- **Argo Workflows**, when a step needs all of its input before it can
  start, as a sort, a global join or model training does, or when you need
  a general workflow engine: schedules, conditions, loops, retries per step
  and approvals. Alpheus only streams; it has none of that.
- **Pachyderm**, when you need versioned data and lineage: which version
  of which input made which output, and reprocessing only what changed
  when an input changes. Alpheus keeps no history between runs. Its last
  release, v2.12.2, came out in January 2025, and its website no longer
  answers, so check that it is still maintained.
- **Numaflow**, when your data is a stream of small events rather than
  files, and you need low latency, windowed aggregation or pipelines that
  scale with their load and run for ever. Alpheus moves chunks of
  megabytes to gigabytes, with latency per chunk in the hundreds of
  milliseconds, and does not autoscale.
- **Diskless Kafka**, when you need a durable log that many independent
  consumers read and replay at their own pace, ordering by key, retention,
  or the Kafka ecosystem: Connect, Streams and Flink. A run in Alpheus has
  one set of consumers, its own steps, and its streams are deleted after
  the run; only its results stay.

Use Alpheus when large files should flow through a pipeline of
containers as they are produced, each chunk processed exactly once even
when pods die, and object storage is the only shared service you want to
run. Alpheus is also young: it is in its design phase, and the other
tools above are in production use.


## What it looks like

A flow is one `StreamRun`. This one is the example in the component
guide: `generate` writes chunks of log lines, `grep` keeps the ERROR
lines in two replicas, and `count` totals the lines per level.

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
  storage: {endpoint: http://s3.example:3900, bucket: flows, secretRef: alpheus-s3}
```

```sh
alpheus run -f flow.yaml
alpheus watch log-report
```

[Getting started](https://alpheus-dev.org/docs/getting-started/) installs
Alpheus from source and runs it on the local test cluster.


## Writing a component

A component reads input chunks as files and writes output chunks as
files; the agent next to it does everything else. Start from a template:

- [`templates/component-go/`](templates/component-go/) uses the Go SDK in
  [`sdk/`](sdk/).
- [`templates/component-python/`](templates/component-python/) has a small
  Python module that speaks the gRPC contract in
  [`proto/component.proto`](proto/component.proto).

Both have a local mode, which runs the component on folders of files
without Kubernetes. [Writing components](https://alpheus-dev.org/docs/components/)
walks through a run step by step and shows the same examples in both
languages; the code is in [`examples/log-report/`](examples/log-report/).


## What it is not

- **Not a dataframe or Arrow engine.** Alpheus never reads the data.
  Components that exchange Parquet or Arrow IPC files can use Alpheus to
  move them, but the processing is theirs; for query engines over Arrow,
  look at DataFusion, Polars or DuckDB.
- **Not a low-latency event processor.** Data moves in chunks through
  object storage. On the test cluster a chunk of 4 MiB takes a few
  hundred milliseconds from one step to the next. For event streams with
  latency in milliseconds, look at Numaflow, Apache Flink or Kafka.
- **Not a batch scheduler.** For jobs where one step finishes before the
  next starts, Argo Workflows already does that well.
- **Not a data platform.** Catalogs, access policies, credentials and
  lineage belong to the system that uses Alpheus. MCPipe is the first
  such system.


## Repository layout

The Go module `alpheus-dev.org/alpheus` is the repository root.

| Path | Contents |
|---|---|
| [`SPEC.md`](SPEC.md) | Implementation specification for the Go program: controller, agent, CLI and component SDK |
| [`api/`](api/) | The `StreamRun` API types (public) |
| [`sdk/`](sdk/) | The component SDK (public) |
| [`internal/`](internal/) | Chunk protocol, object storage, the agent, signals and the rest of the program (private) |
| [`cmd/`](cmd/) | The binaries `alpheus`, the CLI, `alpheus-controller` and `alpheus-agent` |
| [`proto/`](proto/) | The component API and the signal hub's protocol, as gRPC |
| [`config/`](config/) | Generated CRD and RBAC, and samples |
| [`charts/alpheus/`](charts/alpheus/) | The Helm chart: CRD, RBAC, controller and signal hub |
| [`templates/`](templates/) | Component templates in Go and Python |
| [`examples/`](examples/) | The log-report example from the component guide, in Go and Python, and the flagship, large-file ETL |
| [`test/`](test/) | Test components, fixtures and end-to-end tests |
| [`hack/`](hack/) | Scripts: CRD and chart tests, image builds, end-to-end tests, benchmarks and releases |
| [`vagrant/`](vagrant/) | Local test cluster: Talos in a VM with Garage and Zot |
| [`docs/`](docs/) | The documentation pages of [alpheus-dev.org](https://alpheus-dev.org); the website's own repository, with its specification and the logo, fetches them when it builds |

The specifications are currently written in Danish. English versions
will follow before the first release.


## Where the code lives

The primary repositories are on the project's own Forgejo instance. Its
address will be announced here and on alpheus-dev.org once it is running.

The repositories on GitHub are read-only mirrors, updated automatically
from Forgejo. They have issues and pull requests turned off; open them
on Forgejo instead. Until the Forgejo instance is running, this GitHub
repository is the only copy, and issues can be opened here.

Releases are never published on GitHub. Everything a release consists of
stays on the project's own infrastructure in the EU, as the Cyber
Resilience Act requires the project to be able to deliver its releases
and security updates itself: images and the Helm chart in the registry at
`registry.alpheus-dev.org`, and binaries, checksums, SBOMs and signatures
at `https://alpheus-dev.org/releases/`. Everything is signed with the
project's own cosign key, published at `https://alpheus-dev.org/cosign.pub`.


## Roadmap

1. Agree on the specifications.
2. API types and generated CRD. *Done.*
3. Chunk and manifest handling against an S3-compatible store. *Done.*
4. Component SDK with a local test mode. *Done.*
5. Agent, then controller. *Done.*
6. End-to-end tests on Talos: a first flow. *Done.*
7. End-to-end tests with fan-out, fan-in, replicas, restarts, backpressure
   and chaos. *Done.*
8. The `alpheus` CLI, the Helm chart, component templates in Go and
   Python, and a guide to writing components. *Done.*
9. First release, `v0.1.0`.


## Go module

The module path is `alpheus-dev.org/alpheus`, served as a vanity import
from the website, so imports stay the same wherever the code is hosted.
Only `api/` and `sdk/` are public; everything under `internal/` may
change without notice.


## Contributing

The project is in the design phase. Comments on the specifications are
welcome as issues on Forgejo. How to build and test is on
[Development](https://alpheus-dev.org/development/). See
`CONTRIBUTING.md` when it is added.


## Security

Please do not report security issues in public issues. A private way to
report them will be described in `SECURITY.md` and on
alpheus-dev.org/security before the first release.


## License

Apache License 2.0. See [`LICENSE`](LICENSE).
