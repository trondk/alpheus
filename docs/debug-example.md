---
title: "A debug session, step by step"
description: A flow fails in its merge step. Follow it with the CLI, read the kept logs, replay the failed attempt at debug level on your machine and in the cluster, try a fixed version on the same data, and run the flow again.
weight: 6
lastmod: 2026-11-08
---

This page follows one failure from start to fix, with the real output of
each command from the test cluster. The flow is a web shop's report:
twenty days of orders, joined with the customers and sorted by region
and amount. Its merge step has a bug on purpose.

{{< debug-flow "run" >}}

| Step | Does | Size |
|---|---|---|
| `orders` | Writes the orders, one chunk per day: `order_id,customer_id,date,amount,status` | 4,000,000 orders, 152 MiB in 20 chunks |
| `customers` | Writes the customers: `customer_id,name,region` | 50,000 customers, 1.4 MiB |
| `filter` | Drops cancelled orders and the `status` column, in three replicas | |
| `merge` | Joins each order with its customer and adds the name, region and region code | |
| `sort` | Sorts everything by region, then amount, in its `TMPDIR` | |

`customers` reaches `merge` as a [side input](/docs/components/#more-in-the-sdk)
(`side: true`): lookup data that `merge` reads whole before the orders.

**The bug:** twenty customers who signed up through a new form have no
region, and their first orders come on day 7. Version 1 of merge takes
the region's first letter as its code, `strings.ToUpper(region[:1])`,
which panics on an empty region.

The example is in {{< repolink "examples/debug-session/" >}}, with
version 1 of merge in `merge/` and the fix in `merge-v2/`. Each component
is written as in the [Go template](/docs/components/#start-from-a-template),
and its image is built the same way: a static binary on `scratch`, run as
user 65532.

## 0. Build it

On a cluster set up as in [Getting started](/docs/getting-started/),
steps 1 and 2:

```console
$ examples/debug-session/images.sh
wrote _out/debug-session.yaml and _out/debug-session.env
```

`_out/debug-session.yaml` is the flow with version 1 of merge.
`_out/debug-session.env` has `MERGE_V2_IMAGE`, the fixed version, pinned
by digest, for later.

## 1. Start the flow and follow it

```console
$ alpheus run -f _out/debug-session.yaml -n flows
streamrun flows/shop-report created
follow it with: alpheus watch shop-report -n flows
$ alpheus watch shop-report -n flows
```

`watch` prints the whole status at each change. Four seconds in, every
step is running at once: `merge` has joined 570,074 orders while `orders`
is still writing. Counts and acks are written one after the other, so a
snapshot can show a chunk counted before it is acked: here three chunks
of orders counted and two acked.

```
--- 11:54:28
Phase:           Running
Steps finished:  0/5

STEP       PHASE    REPLICAS  RESTARTS  ERRORS  WARNINGS  COUNTS
orders     Running  0/1       0         0       0         orders=1200000
customers  Running  0/1       0         0       0         customers=50000
filter     Running  0/3       0         0       0         orders_in=1000000 orders_out=950113
merge      Running  0/1       0         0       0         orders=570074
sort       Running  0/1       0         0       0         -

EDGE             CHUNKS  BYTES     ACKED  LAG  EOS
orders-filter    6       45.7 MiB  5      1    false
customers-merge  1       1.4 MiB   1      0    true
filter-merge     5       33.1 MiB  2      3    false
merge-sort       2       21.5 MiB  2      0    false
```

## 2. It fails

{{< debug-flow "failed" >}}

`merge` fails, is restarted twice, and fails the same way each time. The
run stops, and `watch` ends with exit code 1 and the reason:

```
STEP       REPLICA  PHASE      ATTEMPT  POD                        REASON           MESSAGE
merge      0        Failed     3        shop-report-merge-0-3      ComponentFailed  the component exited with code 1: panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:119

Logs of merge's failed attempts: s3://alpheus-data/results/shop-report/c05553b3-…/_logs/merge/
  alpheus logs --kept shop-report merge
Replay merge's last failed attempt on its inputs: alpheus replay shop-report merge

error: streamrun shop-report finished as Failed: step merge replica 0 failed after 2 restarts: the component exited with code 1: panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:119
```

The message already says where: `merge/main.go:119`. The SDK turns a
panic into an error with exit code 1 and the place in the component's
code, so it is not taken for wrong settings (exit code 2). The last lines
of `watch` say what to do next.

[`alpheus ui`](/docs/ui/) shows the same: `merge` in red, the three
attempts in the events.

{{< screenshot src="/docs/debug-example/ui-run.png" width="1100" height="1415" alt="alpheus ui showing the run shop-report as Failed with ComponentFailed: the flow drawing has merge in red, the replicas table has merge failed in attempt 3 with the panic at merge/main.go:119, and the events list the three failed attempts." caption="The failed run in alpheus ui." >}}

## 3. Read the logs

{{< debug-flow "logs" >}}

The pods of a failed run are gone, but the end of each attempt's log is
kept in the bucket:

```console
$ alpheus logs --kept shop-report merge -n flows
==> merge replica 0 attempt 1, component <==
{"level":"ERROR","msg":"component panicked","component":"merge","panic":"runtime error: slice bounds out of range [:1] with length 0","stack":"goroutine 1 [running]:\n…main.join(…)\n\t…/examples/debug-session/merge/main.go:119 …"}
{"level":"ERROR","msg":"component failed","component":"merge","error":"panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:119","exit_code":1}
==> merge replica 0 attempt 2, component <==
…
```

The whole stack is in the log. In the UI it is under the replica's Logs:

{{< screenshot src="/docs/debug-example/ui-replica.png" width="1100" height="760" alt="alpheus ui showing merge, replica 0: Failed, ComponentFailed, attempt 3 with 2 restarts, and the Logs tab with the kept log of attempt 3, the panic and its stack." caption="The kept log of attempt 3 in alpheus ui, read from the bucket after the pod is gone." >}}

## 4. Replay the failed attempt at debug level

{{< debug-flow "replay-v1" >}}

The line is known, but not why the region is empty. When an attempt
fails, the controller keeps its inputs: the chunks it had not finished,
and all of the side input. `alpheus replay` fetches them, checks each
against its SHA-256, and runs the same image on them with podman, with
no network and the log level at debug:

```console
$ alpheus replay shop-report merge -n flows
Step merge replica 0, attempt 3: ComponentFailed
  the component exited with code 1: panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:119
Image:  …/debug-session/merge@sha256:1081930a…
Inputs: 13 chunks, checked against their SHA-256
Logs of the attempt: s3://alpheus-data/results/shop-report/c05553b3-…/_logs/merge/0/attempt-3/
Fetched to alpheus-replay/shop-report-merge-0-3/in

$ podman run --rm --network=none --read-only --tmpfs /tmp --userns=keep-id:uid=65532,gid=65532 … -e ALPHEUS_LOG_LEVEL=debug …

{"level":"DEBUG","msg":"customers loaded","component":"merge","customers":50000,"regions":{"":20,"central":10212,"east":9993,"north":9918,"south":9906,"west":9951}}
{"level":"DEBUG","msg":"joining a chunk","component":"merge","from":"filter","replica":0,"n":3,"bytes":6947660}
{"level":"ERROR","msg":"component panicked","component":"merge","panic":"runtime error: slice bounds out of range [:1] with length 0","stack":"goroutine 1 [running]:\n…main.join(…)\n\t…/examples/debug-session/merge/main.go:119 …"}
{"level":"ERROR","msg":"component failed","component":"merge","error":"panic: runtime error: slice bounds out of range [:1] with length 0 at merge/main.go:119","exit_code":1}

The component exited with code 1, as in the run. Its output is in alpheus-replay/shop-report-merge-0-3/out.
```

The failure comes back, on your machine, on the first chunk of orders:
chunk 3 of `filter`'s replica 0. A replay gives each chunk the producer
and number it had in the run, from the `_chunks.json` it writes in each
port's folder. And the debug line that merge writes when it has loaded the
customers gives it away: `"":20`, twenty customers with an empty region.

## 5. Look at the data

The inputs are plain files:

```console
$ cd alpheus-replay/shop-report-merge-0-3
$ ls in/*
in/customers:
_chunks.json  customers-merge-0-000001.csv

in/orders:
_chunks.json  filter-merge-0-000003.csv  filter-merge-0-000004.csv  …  filter-merge-2-000006.csv

$ grep ",$" in/customers/*.csv | head -3
C046001,Customer 46001,
C046002,Customer 46002,
C046003,Customer 46003,

$ grep -m1 -n "C046001" in/orders/filter-merge-1-000003.csv
18817:O01219826,C046001,2026-10-07,1405.49

$ cd ../..
```

Customers 46001 to 46020 have no region, and the first of their orders is
from 7 October.

## 6. Fix it

Version 2 writes `None` for a blank region and region code, and warns
about each one in the [error log](/docs/debugging/#the-error-log), so it
can be counted and followed up:

```diff
-    code := strings.ToUpper(cust.region[:1])
-    fmt.Fprintf(w, "%s,%s,%s,%s\n", order, cust.name, cust.region, code)
+    region, code := "None", "None"
+    if cust.region != "" {
+        region, code = cust.region, strings.ToUpper(cust.region[:1])
+    } else {
+        // Customers from the new sign-up form have no region.
+        sdk.Warn(ctx, "blank_region", "the customer has no region; written as None", sdk.Field("line", line), sdk.Field("customer", id))
+        sdk.Count(ctx, "blank_region", 1)
+    }
+    fmt.Fprintf(w, "%s,%s,%s,%s\n", order, cust.name, region, code)
```

`images.sh` has already built and pushed it, as `MERGE_V2_IMAGE`.

## 7. Try the fix on the same data

{{< debug-flow "replay-v2" >}}

`--image` runs another version of the component on the inputs that made
the old one fail:

```console
$ . _out/debug-session.env
$ alpheus replay shop-report merge -n flows --image $MERGE_V2_IMAGE --dir alpheus-replay-v2
Image:  …/debug-session/merge@sha256:a0136a59…
        another version than the one that failed: …/debug-session/merge@sha256:1081930a…
Inputs: 13 chunks, checked against their SHA-256
…
This version of the component succeeded on the inputs the failed attempt had; the attempt failed with: the component exited with code 1: panic: …
Its output is in alpheus-replay-v2/out, and what it counted and logged in _summary.json and _errors.jsonl there.
To run the flow with it: alpheus run -f <flow file> --name <a new name> --image merge=…/debug-session/merge@sha256:a0136a59…
```

The output is yours to read:

```console
$ grep -h ",None,None" alpheus-replay-v2/out/default/*.csv | head -3
O01606399,C046001,2026-10-09,261.46,Customer 46001,None,None
O01606521,C046001,2026-10-09,932.52,Customer 46001,None,None
O01611825,C046016,2026-10-09,129.31,Customer 46016,None,None

$ cat alpheus-replay-v2/out/_summary.json
{
  "counts": {
    "blank_region": 868,
    "orders": 2280292
  },
  "errors": 0,
  "warnings": 868,
  "errorsDropped": 0
}
```

868 warnings here, and 1,014 in the whole run in step 9: the failed
attempt had twelve chunks of orders, days 7 to 18. Days 1 to 6 were done
and acked, and days 19 and 20 were not yet written. The flow has
`chunks: {maxUnacked: 4}` ([backpressure](/docs/components/#5-a-step-reads-a-chunk-and-writes-its-output)): a `filter` replica waits while four of its
chunks are not acked by merge, and merge acked none after day 6.
Replicas 1 and 2 of `filter` had written four more each and waited with
days 19 and 20; replica 0 had written all of its six.
That backpressure keeps a failing step from piling up data in the
bucket. A replay has only the inputs of the attempt it replays.

## 8. Or try it in the cluster

{{< debug-flow "replay-cluster" >}}

When the data may not leave the cluster, `--with cluster` runs the same
replay as a pod in the run's namespace, under the run's NetworkPolicy:
the agent's image fetches and checks the inputs there, and only the log
comes to your terminal. The output stays in the pod and is deleted with
it when the replay ends; to read the output, replay with podman as in
step 7. Keeping a cluster replay's output in the bucket is planned.

```console
$ alpheus replay shop-report merge -n flows --with cluster --image $MERGE_V2_IMAGE
…
Replaying in pod flows/shop-report-merge-0-replay-3-bff63e, at debug level:

{"level":"INFO","msg":"fetched the failed attempt","component":"alpheus-agent","step":"merge","replica":0,"attempt":3,"inputs":13,"reason":"ComponentFailed"}
{"level":"DEBUG","msg":"customers loaded","component":"merge","customers":50000,"regions":{"":20,"central":10212,…}}
…
This version of the component succeeded on the inputs the failed attempt had; …
```

The replica's Failures tab in `alpheus ui` lists the kept inputs, the
side input first, and its button runs the same replay with the failed
attempt's image:

{{< screenshot src="/docs/debug-example/ui-failures.png" width="1100" height="700" alt="alpheus ui showing the Failures tab of merge, replica 0: attempt 3, ComponentFailed with the panic, the image, the 13 kept input chunks starting with customers-merge-0-000001.csv, and a button Replay attempt 3 that runs alpheus replay with cluster." caption="The kept failure, with a button that replays it in the cluster." >}}

## 9. Run the flow again with the fix

{{< debug-flow "rerun" >}}

The run itself cannot change, so the fix runs as a new run. `--image`
and `--log-level` change one step without editing the flow file;
`--log-level merge=debug` shows more of what merge does:

```console
$ alpheus run -f _out/debug-session.yaml -n flows --name shop-report-v2 \
    --image merge=$MERGE_V2_IMAGE --log-level merge=debug
streamrun flows/shop-report-v2 created
$ alpheus watch shop-report-v2 -n flows
…
STEP       PHASE      REPLICAS  RESTARTS  ERRORS  WARNINGS  COUNTS
orders     Succeeded  1/1       0         0       0         orders=4000000
customers  Succeeded  1/1       0         0       0         customers=50000
filter     Succeeded  3/3       0         0       0         orders_in=4000000 orders_out=3800639
merge      Succeeded  1/1       0         0       1014      blank_region=1014 orders=3800639
sort       Succeeded  1/1       0         0       0         orders=3800639

RESULT        CHUNKS  BYTES      COUNTS
sort.default  7       215.1 MiB  -

Error log of merge: s3://alpheus-data/results/shop-report-v2/f56a5d7a-…/_errors/merge/ (0 errors, 1014 warnings, 0 dropped)
```

Every one of the 3,800,639 orders that passed `filter` is joined, and
sorted. 1,014 of them are from customers without a region; the error log
has each one with its line and customer. The debug log shows merge at
work:

```console
$ alpheus logs shop-report-v2 merge -n flows
{"level":"DEBUG","msg":"customers loaded","component":"merge","customers":50000,"regions":{"":20,…}}
{"level":"DEBUG","msg":"joining a chunk","component":"merge","from":"filter","replica":1,"n":1,"bytes":6946549}
{"level":"DEBUG","msg":"joined a chunk","component":"merge","n":1,"orders":190019}
…
```

## What made this work

- **The failure is kept.** The end of each attempt's log, and the inputs
  of each failed attempt, are in the bucket for `resultsTTL`, after the
  pods are gone.
- **A replay is the attempt again.** The same image, by digest, the same
  settings and the same inputs, checked against their SHA-256, without
  network. A failure that depends on the data comes back every time.
- **The lookup data is a side input.** Every attempt of merge, and every
  replay, gets all of the customers again. Without `side: true`, a new
  attempt would start with the customers already done and so without
  them: it would reject every order as an unknown customer, and the run
  could end Succeeded with nothing joined.
- **Debug on demand.** `logLevel: debug`, or `alpheus run --log-level`,
  turns on a step's debug log without a new image; a replay always runs
  at debug.
- **A fix is tried before it is run.** `alpheus replay --image` runs a new
  version on the inputs that made the old one fail.

## On the test cluster

Two things are needed on the test cluster:

- The CLI must reach the storage from your machine: the run's storage
  address only works inside the cluster. Annotate the Secret once, as in
  [Getting started](/docs/getting-started/#2-give-a-namespace-the-storage-keys),
  or add `--endpoint https://$(vagrant/talos.sh ip):30900 --ca-file
  vagrant/_out/ca.crt` to `logs --kept` and `replay`, as below.
  This goes for `replay --with cluster` too: the CLI reads the failure
  from the bucket before it starts the pod.
- For a replay with podman, pull each image from the VM's address first
  and pass that with `--image`: `registry.alpheus.test` is the cluster's
  name for the registry. That is version 1 in step 4 and
  `$MERGE_V2_IMAGE` in step 7. A replay with `--with cluster` pulls in
  the cluster and needs no pull.

```console
$ ip=$(vagrant/talos.sh ip)
$ podman pull --cert-dir _out/certs $ip:30500/debug-session/merge@sha256:1081930a…
$ alpheus replay shop-report merge -n flows --image $ip:30500/debug-session/merge@sha256:1081930a… \
    --endpoint https://$ip:30900 --ca-file vagrant/_out/ca.crt

$ . _out/debug-session.env      # MERGE_V2_IMAGE=registry.alpheus.test:30500/debug-session/merge@sha256:…
$ v2=${MERGE_V2_IMAGE/registry.alpheus.test/$ip}   # the port stays: $ip:30500/…
$ podman pull --cert-dir _out/certs $v2
$ alpheus replay shop-report merge -n flows --image $v2 --dir alpheus-replay-v2 \
    --endpoint https://$ip:30900 --ca-file vagrant/_out/ca.crt
```
