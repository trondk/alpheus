---
title: Bad records and bad chunks
description: The dead-letter pattern in Alpheus. A bad record goes to the error log or to an output of its own, and a chunk a component cannot handle is set aside with failurePolicy continue, so the rest of the run goes on. What is kept, where, for how long, and how to run it again.
weight: 6
lastmod: 2026-11-04
---

Some data cannot be handled: a row with a missing column, or a file the
component crashes on. A queue would put such a message in a *dead-letter
queue* and go on with the next one. Alpheus has no queue, but it has the
same choice at two levels:

| What is bad | Where it goes | The run |
|---|---|---|
| A record in a chunk | The step's error log, or an output of its own, which is a result | Succeeds |
| A whole chunk the component fails on | Kept with the failed attempt; the replica gives up with `failurePolicy: continue` | `PartiallySucceeded` |

The first is up to the component, and is the one to use when you can.
The second is for the data you did not foresee.

## A bad record

A component that can tell a bad record from a good one should not fail on
it. It has two places to put it, and can use both.

**The error log.** `sdk.Error` and `sdk.Warn` log a record with a code, a
message and fields. The entry is committed with the chunk, together with
the output, so it is there exactly once, also after restarts. This is what
the meter-etl example does: each bad reading is an error with its line.

```go
r, err := meter.Parse(line)
if err != nil {
	sdk.Error(ctx, "bad_row", err.Error(), sdk.Field("line", n))
	continue
}
```

`alpheus get` shows the number of errors and warnings per step, and
`status.steps[].errorLog` says where the log is:
`results/<run>/<uid>/_errors/<step>/`. It is kept as long as the results,
`resultsTTL` (7 days by default). Each replica keeps at most 100,000
entries; `status.steps[].errorsDropped` counts those beyond.

**An output of its own.** The error log has what you log about a record,
not the record. To keep the records themselves, write them to a second
output. An output that no edge goes from is a result, kept like the
other results:

```go
bad, err := out.Create(ctx, "rejected", "csv")
…
fmt.Fprintln(bad, line)
…
bad.Close()
```

The bad records are then the result `<step>.rejected`, under
`results/<run>/<uid>/<step>/rejected/`, and `alpheus get` says where to
fetch them, like any other result. Like all output, they are committed with the input
chunk they came from, so a restart neither loses nor repeats them. Create
the file only when a chunk has a bad record, and a run without any has no
such files. An edge from the output, such as
`{from: clean, output: rejected, to: audit}`, sends them on to a step
instead.

## A bad chunk

A component that fails on a chunk fails the attempt. The replica is
restarted from its last committed chunk, gets the same chunk first, and
fails again, up to `maxRestarts` times. Then:

- With `failurePolicy: stop`, the default, the run fails with the reason
  `ComponentFailed`.
- With `failurePolicy: continue`, the replica gives up, and the run goes on
  without it.

```yaml
- name: clean
  image: registry.example/clean@sha256:…
  replicas: 4
  maxRestarts: 1
  failurePolicy: continue
```

When a replica gives up, the controller closes its input and output
streams. The step before stops waiting for it, the steps after reach the
end of their input, and the other replicas of the step go on with their
own chunks. The chunks that were the replica's and are not done are not
handled by anyone, also those the step before writes after it gave up:
each chunk belongs to one replica. The run ends `PartiallySucceeded`, with
the reason `FailedReplicas`.

```console
$ alpheus get meter-etl
Phase:           PartiallySucceeded: FailedReplicas (…)
…
STEP       REPLICA  PHASE      ATTEMPT  POD  REASON           MESSAGE
clean      1        Failed     2        -    ComponentFailed  … (30 chunks not processed)
```

`status.steps[].replicas[].failedChunks` is the number of chunks that were
not processed. A tool that runs flows should treat `PartiallySucceeded` as
neither a success nor a failure: the results are correct for the chunks
that were processed, and `failedChunks` says how many were not.

### What is kept

Each failed attempt is kept in the run's bucket, as in
[When a run fails](/docs/debugging/#replaying-a-failed-attempt):

```
results/<run>/<uid>/_failures/<step>/<replica>/attempt-<n>/failure.json
                                                           input/<chunks>
results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/component.log
```

`input/` has copies of the chunks the attempt had open and had not
finished, the chunk it failed on first: the first `maxOpen` of each input
stream, at most 16. They are kept for `resultsTTL`, like the results.

That is not every chunk the replica did not process. The rest are where
they were written, under `runs/<uid>/`, until the run's chunks are deleted:
after `ttlAfterFinished`, or with the `StreamRun`. Alpheus does not list
which they are. To keep them for longer, don't set `ttlAfterFinished` and
keep the `StreamRun`.

### Running it again

`alpheus replay` runs the failed attempt again on its own, on the kept
chunks, at debug level, so you can see why the component fails on them:

```console
$ alpheus replay meter-etl clean --replica 1
```

Once the component is fixed, a new run with the fixed image processes the
data again, all of it. Alpheus does not run only the chunks that were
not processed.

## Which to use

- Use **the error log** for what you want to count and read: codes,
  reasons, line numbers.
- Use **an output** for what you want to keep, fix and load again: the bad
  records themselves.
- Use **`failurePolicy: continue`** for a step where some data lost is
  better than no result, and keep `maxRestarts` low, since each restart
  fails on the same chunk. Leave it at `stop` where every chunk must be
  processed, as it must when the results are summed.

A bucket that is full fails the run at once, with `continue` too: no step
can go on without room to write. See
[When a pod fails](/docs/components/#9-when-a-pod-fails).
