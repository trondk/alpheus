---
title: "Large-file ETL on-prem"
description: The flagship use case, run step by step on the test cluster, and what it measures.
weight: 7
lastmod: 2026-11-06
---

This is the use case Alpheus is built and measured around: large files,
processed on your own cluster, with nothing but Kubernetes and an S3 store.

A utility's smart meters send a reading every 15 minutes. Every night the
readings arrive as large gzipped CSV exports. They must be checked, cleaned
and summed per region and hour, on the utility's own cluster: the data may
not leave it, and every service on it has to be approved and run in-house.

The example is in {{< repolink "examples/meter-etl/" >}}. Its four
components use only Go's standard library.

```
export ──> split ──> clean (4 replicas) ──> aggregate ──> totals (result)
                        └──> dataset (result): the cleaned readings
```

1. `export` writes 16 gzipped CSV files of 2 million readings each,
   `meter_id,region,timestamp,kwh`, made from a seed. Five rows in every
   thousand are bad: a missing reading, an impossible timestamp, a negative
   reading or a missing column. In real use, this step would read the
   night's exports: a fixed list of files, named in its settings, since a
   source must give the same chunks when it is restarted
   ([Components](/docs/components/#3-the-source-writes-chunks)).
2. `split` decompresses each file and cuts it at line ends into gzipped
   chunks of about 16 MiB uncompressed, so many chunks can be cleaned at
   once.
3. `clean` checks every row, in four replicas. Valid rows go, gzipped and
   with an hour column, to its `dataset` output. Each bad row goes to the
   run's error log, with its code and line. A small partial total per
   region and hour goes on to `aggregate`.
4. `aggregate` adds up the partial totals and writes the totals per region
   and hour when all of its input is read.

`dataset` and the totals go to no other step, so they are the run's
results. Every step runs at the same time: `clean` works on the first
chunks while `export` is still writing.

## Build it

Nothing is released yet, so Alpheus is built from the repository. You need
Go, Helm and podman, a clone of the repository, and a cluster with an S3
store and a registry; the test cluster from
[Development](/development/#the-test-cluster) has both.

1. Build the agent and the controller, install them with the Helm chart,
   and build the `alpheus` CLI with `make cli`:
   [Getting started, step 1](/docs/getting-started/#1-install-the-controller).
2. Give the namespace `flows` the storage keys:
   [step 2](/docs/getting-started/#2-give-a-namespace-the-storage-keys).

[Build and unit tests](/development/#build-and-unit-tests) lists the other
`make` targets, such as `make test`.

## Run it

Build the example's four images, push them, and write the flow with their
digests:

```console
$ examples/meter-etl/images.sh
wrote _out/meter-etl.yaml
$ alpheus run -f _out/meter-etl.yaml -n flows
$ alpheus watch meter-etl -n flows
```

When it has finished:

```console
$ alpheus get meter-etl -n flows
Phase:           Succeeded
Started:         2026-10-31T10:02:34Z
Finished:        2026-10-31T10:03:26Z
…
STEP       PHASE      REPLICAS  RESTARTS  ERRORS  WARNINGS  COUNTS
export     Succeeded  1/1       0         0       0         bytes=1359581488 files=16 rows=32000000
split      Succeeded  1/1       0         0       0         bytes=1359581008 rows=32000000
clean      Succeeded  4/4       0         160070  0         rows_in=32000000 rows_out=31839930 rows_rejected=160070
aggregate  Succeeded  1/1       0         0       0         partials=96

EDGE             CHUNKS  BYTES      ACKED  LAG  EOS
export-split     16      321.0 MiB  16     0    true
split-clean      96      321.0 MiB  96     0    true
clean-aggregate  96      393.7 KiB  96     0    true

RESULT             CHUNKS  BYTES      COUNTS
clean.dataset      96      359.5 MiB  -
aggregate.default  1       4.9 KiB    -
…
Error log of clean: s3://alpheus-data/results/meter-etl/<uid>/_errors/clean/ (160070 errors, 0 warnings, 0 dropped)
```

That run, on the test cluster, took 52 seconds from start to finish for
1.36 GB of readings, 337 MB gzipped.

The flow keeps its results for a day (`resultsTTL: 24h`), since the
dataset is as large as the input.

The counters are exact, also when a pod dies in the middle: every one of
the 32 million rows was read once, and every rejected row is in the error
log, which `alpheus get` names. The totals per region and hour are the
`aggregate.default` result, a small CSV file, and the cleaned readings are
the `clean.dataset` result, one gzipped file per chunk.

Try killing a `clean` pod while it runs:

```console
$ kubectl -n flows delete pod meter-etl-clean-0-1 --grace-period=1
```

The replica starts again from its last committed chunk, and the result is
the same, row for row.

## What it measures

Two tests run this flow on the test cluster.

- **testcase08** checks that it is exact. It computes the expected totals
  from the same generator, and requires every row to be read once, every
  bad row to be rejected and logged, one dataset chunk for every chunk
  `split` made, and the totals to be equal to the generator's, while it
  kills a `clean` pod in the middle of the run.
- **`make bench-etl`** runs the flow without killing anything, with
  `export`, `split` and `clean` at 1, 2, 4 and 8 replicas each, and
  measures how long the processing takes:
  from when every component runs until the run has succeeded, so pulling
  images is not counted.

Results from 31 October 2026, on the test cluster as it was then: one
Talos node in a VM with 4 CPUs and 8 GiB of memory, with Garage on the
same node. The test cluster has two nodes now. The flow is
16 files of 2 million readings, 1360 MB raw and 337 MB gzipped, cut into
96 chunks. Each row counts once: 32 million rows in every run, 160,070
rejected and logged, totals equal to the generator's.

| Replicas of export, split and clean | Processing | Raw MB/s | Rows/s | Done after: export, split, clean |
|---|---|---|---|---|
| 1 | 62 s | 21.9 | 516,000 | 44 s, 56 s, 59 s |
| 2 | 56 s | 24.3 | 571,000 | 18 s, 47 s, 53 s |
| 4 | 54 s | 25.2 | 593,000 | 17 s, 30 s, 50 s |
| 8 | 56 s | 24.3 | 571,000 | 17 s, 32 s, 51 s |

What the numbers say:

- **The steps run at the same time.** With one replica each, `export`
  takes 44 seconds to write the files, and the whole run is done 15
  seconds later: `split` and `clean` work on the first files while the
  last ones are still being written.
- **On this machine, the CPUs are the limit.** More replicas make
  `export` and `split` finish sooner, but `clean` still finishes after
  about 50 seconds. During a run with 4 replicas, the node's CPUs were
  busy 92 % of the time at the median and 98 % at the 90th percentile:
  every row is decompressed, checked and compressed again, and Garage
  stores every chunk on the same machine. More replicas cannot help when
  there is no CPU left.
- **A killed pod does not change the result.** testcase08 kills a
  `clean` pod in the middle of the run and gets the same rows, rejects and
  totals as a run without one.

These numbers come from one small machine. Everything runs on a single
node, so the steps share its four CPUs with each other and with Garage,
which stores every chunk on the same disk. They show how the flow behaves,
not what a real cluster can do. Run `make bench-etl` on your own cluster to
see that.
