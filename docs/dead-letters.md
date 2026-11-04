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
| A whole chunk the component fails on | The replica gives up with `failurePolicy: continue`, and every chunk it never processed is kept for a replay | `PartiallySucceeded` |

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
[When a run fails](/docs/debugging/#replaying-a-failed-attempt), with
copies of the chunks it had open, the chunk it failed on first. When the
run has finished, and nothing more can come to the replica, the controller
also keeps every chunk the replica never processed, those that came after
it gave up too, in `skipped/`:

```
results/<run>/<uid>/_failures/<step>/<replica>/attempt-<n>/failure.json
                                                           input/<chunks>
results/<run>/<uid>/_failures/<step>/<replica>/skipped/failure.json
                                                       input/<chunks>
results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/component.log
```

`skipped/failure.json` lists them, each with its edge, number, size and
SHA-256, along with the image, the settings, and the reason the replica's
last attempt failed with. They are copies made by the storage itself, so
the data does not pass through the controller, and they are kept for
`resultsTTL`, like the results, after the run's own chunks under
`runs/<uid>/` are deleted. They take as much room as the chunks did.

At most 10,000 chunks are kept per replica. `notKept` in
`skipped/failure.json` counts those beyond, and any the storage could not
copy; the controller tries once, when the run finishes. The event
`KeepSkipped` says how many it kept, and `alpheus get` shows the command
that fetches them.

### Running it again

`alpheus replay` runs the failed attempt again on its own, on the chunks
it had open, at debug level, so you can see why the component fails on
them:

```console
$ alpheus replay meter-etl clean --replica 1
```

With `--skipped` it runs the component on all the chunks the replica
never processed instead, in order, in local mode:

```console
$ alpheus replay meter-etl clean --replica 1 --skipped
Step clean replica 1, attempt 2: ComponentFailed
  …
Skipped: the 30 chunks the replica never processed, after it gave up
…
```

That is how to finish the job once the component is fixed: replay the
skipped chunks with the fixed image (`--image`), and the output is in the
replay folder's `out/`. `--with none` only fetches the chunks, checked
against their SHA-256, for a program of your own. A new run with the
fixed image is the other way, and processes all the data again.

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
