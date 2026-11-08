---
title: How it compares
description: Alpheus next to Argo Workflows, Pachyderm, Numaflow and diskless Kafka, and when to use one of them instead.
weight: 3
lastmod: 2026-11-01
---

Alpheus is a file mover, not a data engine: a chunk is a file in any
format, and Alpheus never opens it. That puts it next to these tools:

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

- **Argo Workflows**, when most steps need all of their input before
  they can start, as model training does, or when you need a general
  workflow engine: schedules, conditions, loops, retries per step and
  approvals. Alpheus can have such a step, a stateful one such as the
  sort in the [debug example](/docs/debug-example/), but if every step is
  like that, nothing runs at the same time and streaming gains nothing.
  Alpheus only streams; it has none of the rest.
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
