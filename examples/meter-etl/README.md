# Example: large-file ETL on-prem (meter-etl)

The flagship use case: a utility's nightly smart-meter exports arrive as
large gzipped CSV files, and are cleaned, checked and summed on its own
cluster, with nothing but Kubernetes and an S3 store, and nothing leaving
the building.

```
export ──> split ──> clean (4 replicas) ──> aggregate ──> totals (result)
                        └──> dataset (result): the cleaned readings
```

1. `export` is the source. It writes 16 gzipped CSV files of 2 million
   readings each (`meter_id,region,timestamp,kwh`), made from a seed, with
   5 bad rows in every thousand: missing readings, impossible timestamps,
   negative readings, missing columns.
2. `split` decompresses each file and cuts it into chunks of 16 MiB at line
   ends, so the next step can work on many chunks at once.
3. `clean` checks every row, in four replicas. Valid rows go, gzipped and
   with an hour column, to the `dataset` output, which is a result. Each bad
   row is logged in the run's error log with its code and line. A small
   partial total per region and hour goes on to `aggregate`.
4. `aggregate` adds up the partial totals and writes the totals per region
   and hour when all input is read.

Every step runs at the same time: `clean` starts on the first chunk while
`export` is still writing the second file. If a pod dies, its replica
starts again from its last committed chunk, and every row is still counted
exactly once.

| Path | What it is |
|---|---|
| `meter/` | The four steps, the generator and the row checks, in one package |
| `export/`, `split/`, `clean/`, `aggregate/` | One `main` per component |
| `flow.yaml` | The StreamRun. Put your images' digests in it. |
| `images.sh` | Builds and pushes the images and writes the flow with their digests |

The components use only Go's standard library. `go test ./examples/meter-etl/...`
runs the whole flow in local mode, with `clean` as two replicas, and checks
the totals against the generator. On the test cluster, testcase08 runs it
with a killed `clean` pod, and `make bench-etl` measures it with 1, 2, 4
and 8 replicas of `export`, `split` and `clean`.

The walk-through and the benchmark's results are at
https://alpheus-dev.org/docs/etl/.
