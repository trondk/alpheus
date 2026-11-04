---
title: Reading alpheus ui
description: What each part of alpheus ui shows, from the list of runs to one replica's logs, what the numbers on the flow's arrows mean, and what the colours and phases say.
weight: 7
lastmod: 2026-11-04
---

`alpheus ui` shows the runs live: the list of runs, one run with its flow,
and one replica with its counts, logs and failed attempts. This page goes
through what each part shows. The same guide is in the page itself, under
**help** at the top right. How to start it, and how to run it in the
cluster for a team, is in [When a run fails](/docs/debugging/#alpheus-ui).

## The header

- **alpheus** goes back to the list of runs.
- **namespace** is where the runs are read from. Type another one and
  press Enter. `*` shows the runs of every namespace you may read.
- **●** is green while the page gets live updates from the cluster, and
  grey when the connection is lost. The browser connects again by itself,
  and the page catches up.
- **help** opens this guide, and **dark** or **light** switches the look.
  The browser remembers the look.

## The runs

One row per run, newest first. The buttons above the table show **all**
runs, the **running** ones (also those still pending), or the **failed**
ones (also those that partly succeeded).

| Column | What it shows |
|---|---|
| Namespace | Only with `*`: the run's namespace |
| Run | The run's name; click the row to open it |
| Phase | Where the run is; see [Phases and colours](#phases-and-colours) |
| Steps | Steps finished out of all steps, such as `2/3` |
| Started | How long ago the run started |
| Duration | From the start until the run finished, or until now |
| Reason | Why the run failed, as a code a program can rely on; see [Why it failed](/docs/debugging/#why-it-failed) |

## Phases and colours

A run has one of these phases:

| Phase | Mark | Meaning |
|---|---|---|
| Pending | ○ grey | Created; the first pods are not running yet |
| Running | ● amber | Chunks are moving |
| Succeeded | ✓ green | Every step finished and every chunk was processed |
| PartiallySucceeded | ◐ amber | It finished, but a replica with `failurePolicy: continue` gave up, so some input chunks were not processed |
| Failed | ✗ red | A step failed; the reason and message say why |
| Cancelled | ■ grey | Someone cancelled it, with `alpheus cancel` or `spec.cancel` |

Steps and replicas are Pending, Running, Succeeded or Failed, with the
same marks and colours. The page adds one more, **Stopped** (■ grey): a
step or replica that had not finished when its run ended, for example
because another step failed. Its pods were deleted with the run.

## A run

The card at the top has the run's phase and reason, its message, how many
steps have finished, when it started and how long it has taken. Below it:

- **Flow**: the steps and the edges between them, live. See
  [The flow](#the-flow).
- **Replicas**: one row per replica of every step. **Attempt** is the
  replica's current attempt; it grows by one each time the replica is
  restarted, and **restarts** says how many times that happened.
  **Reason** and **Message** say why the last attempt failed. While a
  replica runs, the message is its component's latest progress report.
  "*n* chunks not processed" means the replica gave up with
  `failurePolicy: continue`. Click a row to open the replica.
- **Results**: what the run's last steps wrote, with the number of chunks,
  the bytes, and the bucket and prefix they are in. `alpheus get <run>`
  shows how to fetch them.
- **Events**: what happened when, oldest first: started, restarted, failed,
  finished.
  Warnings are red. Kubernetes keeps events for an hour by default; the
  reasons and logs of failed attempts stay in the bucket longer.

## The flow

Each **box** is a step. The sources are on the left, and each step is in
the column after the last of the steps it reads from. The box's border has the
step's colour: amber while it runs, green when it has succeeded, red when
it has failed, grey while it waits or after it was stopped.

Each **dot** in a box is one replica of the step, coloured by its phase. A
hollow dot is pending or stopped. Point at a dot to see the replica's
phase and reason, and click it to open the replica.

Each **arrow** is an edge: the chunks one step writes and the next one
reads. It has two numbers:

- **chunks**: how many chunks the step before has committed on the edge
  so far. A chunk is one file that Alpheus moves between steps without
  reading it. It is committed once it is in storage and listed in the
  edge's manifest, and from then on it is never lost or counted twice.
- **lag**: chunks committed on the edge that the step after has not
  finished yet. A downstream replica acks a chunk when it has processed it
  and committed its own output for it, so lag is committed minus acked:
  the chunks waiting to be read plus those being worked on.

**· done** after the lag means the step before has committed its last
chunk on the edge. With lag 0 and done, the edge is finished.

Lag stays small by design. A step may get only `maxUnacked` chunks ahead
of each replica that reads from it (4 by default, in the run's `chunks`).
When it gets that far, it waits. That is backpressure: a slow step slows
down the steps before it, and nothing piles up in storage or on disk. So:

- **A lag of a few chunks** is normal while the run goes.
- **A lag that stays at the limit** means the step after the arrow is the
  slowest one, and the step before is waiting for it. More replicas for
  the slow step usually help.
- **A lag that does not change at all** while nothing is running
  downstream means the replicas there are restarting or pending. Their
  dots and the events say why.

The same numbers are in `alpheus get <run>` and `alpheus watch <run>`,
and as the metric `alpheus_edge_lag`.

## A replica

The card has the replica's phase and reason, its message, the attempt, the
number of restarts and, while it runs, the name of its pod. Four tabs
show the rest:

- **Counts**: the counters its step's component keeps, such as rows or
  bytes, summed over all the step's replicas, and its errors and warnings.
  They are committed together with the chunks, so they are exact, also
  after restarts.
- **Error log**: the errors and warnings the component wrote with the
  SDK's `Error` and `Warn`, each with its level, code, message, fields and
  the input chunk it was working on.
- **Logs**: the component's or the agent's log, chosen in the second
  list. The first list chooses the attempt. *live* is the running pod's
  last 2000 lines. *kept* is the log of a failed attempt, saved in the
  bucket, so it is there after the pod is gone.
- **Failures**: a failed attempt, kept with its reason, message, image and
  the input chunks it had not finished. **Replay** runs the same image on
  the same chunks again, on its own and at debug level, and shows its
  output as it comes. On your machine it runs with podman or docker. In
  the cluster it runs as a pod next to the run, and it can be turned off;
  then the page shows the `alpheus replay` command to run yourself. See
  [Replaying a failed attempt](/docs/debugging/#replaying-a-failed-attempt).

## Words

| Word | Meaning |
|---|---|
| Step | One component in the flow, with its image and settings |
| Replica | One copy of a step; a step with three replicas splits its chunks between three pods |
| Attempt | One pod's try at a replica; a restarted replica gets a new attempt, which goes on from the last committed chunk |
| Edge | The chunks one step writes for the next |
| Chunk | One file moved between steps; Alpheus never reads it |
| Commit | Writing a chunk and listing it in the edge's manifest; a chunk exists from then on |
| Ack | A downstream replica saying it has processed a chunk and committed what it made from it |
| Lag | Chunks committed on an edge and not acked yet |
| Backpressure | A step waiting because it is `maxUnacked` chunks ahead of a replica that reads from it |
| Done | The step before has committed its last chunk on the edge |
| Result | What a last step wrote, kept in the bucket for you to fetch |
