---
title: When a run fails
description: Why a run or a step failed, as codes a program can rely on and in words; the events, kept logs and error log; replaying a failed attempt on its own; alpheus ui; and the metrics.
weight: 6
lastmod: 2026-11-04
---

When something goes wrong, Alpheus tries to answer three questions without
anyone digging through the cluster: what failed, why, and on which data.
Everything here is in the `StreamRun`'s status and in the run's bucket,
so it is there for `alpheus`, for `kubectl`, for your own tools and for
[`alpheus ui`](#alpheus-ui), also after the pods are gone.

## Why it failed

A failed run has a **reason**, a code that is part of the API, and a
**message** in words. Messages may be reworded between versions; reasons
are not. A tool that runs flows should act on the reason and show the
message.

| Reason | What happened | What helps |
|---|---|---|
| `ComponentFailed` | The component exited with an error | Its message, its kept logs and a [replay](#replaying-a-failed-attempt) |
| `AgentFailed` | The agent failed for another reason, or restarted too often | The agent's kept log |
| `StorageRefused` | The storage refused the keys in the run's Secret | Fix the Secret; no restart helps, so the run fails at once |
| `StorageUnreachable` | The storage did not answer for `spec.storage.timeout` | Check the storage; shorter outages are waited out |
| `StorageFull` | The bucket's quota is reached | More quota, or smaller results; no restart helps, so the run fails at once |
| `StorageError` | The storage answered with another error | The message has the storage's answer |
| `ScratchFull` | A chunk did not fit in the step's scratch folder | Raise `scratch` for the step, or make chunks smaller |
| `PodDeleted` | The pod disappeared: evicted, a lost node, or deleted | Kubernetes' events for the pod |
| `PodFailed` | The pod failed without a container saying why | Kubernetes' events for the pod |
| `InvalidSpec` | The graph is invalid, or an image is not pinned | The message names the problem |
| `Cancelled` | `spec.cancel` was set | |
| `FailedReplicas` | The run finished, but replicas with `failurePolicy: continue` gave up | `failedChunks` of each replica says how much was not processed; see [Bad records and bad chunks](/docs/dead-letters/) |

Where to find it:

- `status.reason` and `status.message` for the run, and the `Ready`
  condition's reason.
- `status.failedStep` and `status.failedReplica`: the replica whose failure
  stopped the run.
- `status.steps[].replicas[].reason` and `.message` for each replica. While
  a replica runs again after a failure, its reason is why the last attempt
  failed.
- `kubectl get sr` shows the reason in a column, and `alpheus get <run>`
  shows all of it.

A component's message is in its own words. The Go SDK and the Python
module write the error a component fails with to Kubernetes' termination
message, and a component that writes nothing there still gets the end of
its log, so the status can say "the component exited with code 1: open
input.csv: no such file" rather than only "code 1".

## What happened when

The controller records Kubernetes events on the `StreamRun`: when it
starts, every failed attempt and its restart, a replica that gives up, and
the end of the run, each with the same reasons as above.

```console
$ kubectl events --for streamrun/sales
```

## The logs of failed attempts

Pods are deleted when a run stops, and a restarted replica gets a new pod,
so a failed attempt's log would be gone. Before the controller replaces or
deletes a failed attempt's pod, it saves the end of each container's log,
up to 4 MiB, in the run's bucket next to the error log:

```
results/<run>/<uid>/_logs/<step>/<replica>/attempt-<n>/component.log
                                                       agent.log
```

`status.steps[].logs` says where they are, and they are kept as long as the
run's results (`resultsTTL`).

```console
$ alpheus logs sales filter --kept            # the component's
$ alpheus logs sales filter --kept -c agent   # the agent's
```

## The error log

What a component reports about the data, with `Log` in the SDK, goes to the
step's error log, committed with the chunk it is about: level, code,
message, fields and the input chunk. `status.steps[].errors`, `warnings`
and `errorLog` say how many there are and where. See
[Writing components](/docs/components/#the-rules-a-component-follows).

## Replaying a failed attempt

When an attempt fails, the controller also keeps what is needed to run it
again on its own:

```
results/<run>/<uid>/_failures/<step>/<replica>/attempt-<n>/failure.json
                                                           input/<the chunks>
```

`failure.json` has the reason and message, the image, pinned by digest,
and the settings. `input/` has copies of the input chunks the attempt had
been given and had not finished: the first `maxOpen` of each input stream,
at most 16, the same chunks a new attempt gets first. The storage copies
them itself, so the data does not pass through the controller. A failure
of the storage itself keeps nothing, since it is not about the data.

`alpheus replay` fetches the chunks, checks each against its SHA-256, and
runs the same image on them in [local mode](/docs/components/#testing-without-kubernetes),
with the log level at debug:

```console
$ alpheus replay sales filter
Step filter replica 1, attempt 4: ComponentFailed
  step filter replica 1 failed after 3 restarts: the component exited with code 1: …
Image:  registry.example/filter@sha256:…
Inputs: 2 chunks in alpheus-replay/sales-filter-1-4/in, checked against their SHA-256

$ podman run --rm --network=none -v …/alpheus-replay/sales-filter-1-4:/replay:Z -e ALPHEUS_MODE=local … registry.example/filter@sha256:…
{"level":"DEBUG","msg":"read chunk","n":1, …}
…
```

- `--replica` and `--attempt` choose another failure than the last one of
  the replica that stopped the run.
- `--with docker` uses Docker; `--with none` only fetches the inputs and
  prints the command, so you can run the component yourself, for example
  in a debugger.
- `--with cluster` runs it as a pod in the run's namespace instead: the
  agent's image fetches and checks the chunks, then the step's image runs
  on them with the step's resources, `envFrom` and scratch size, and the
  logs come to your terminal. The data stays in the cluster, and a failure
  that depends on the node's resources can show again. The pod is deleted
  afterwards, unless you pass `--keep`.
- `--image` pulls the same digest from another registry, and `-e KEY=VALUE`
  passes what the step's `envFrom` gives in the cluster, which replay does
  not copy.
- `--endpoint` and `--ca-file` reach the storage from your machine, as for
  `alpheus chunks`.
- The component runs as replica 0 of 1, since the inputs are already this
  replica's, without network.

The SDKs set the log level from `ALPHEUS_LOG_LEVEL`: in Go, slog's default
logger logs at that level, and in Python, the `logging` module does. Log
at debug what helps you find a bug, and it shows in a replay without
filling the logs of every run.

A replay that succeeds where the run failed is also an answer: the failure
depends on something else, such as chunks before these in a component that
keeps state, the environment, or the resources. A component with several
input ports replays only in Go, which reads `ALPHEUS_LOCAL_INPUTS`.

The kept chunks are copies of the run's data. They have the run's
`resultsTTL` and are read with the same Secret, but if the data is
personal, keep that in mind before you fetch it to a laptop.

## alpheus ui

```console
$ alpheus ui
alpheus ui on http://127.0.0.1:7171/ (namespace flows); Ctrl-C stops it
```

The CLI serves a page on your machine and opens it in the browser. It
starts in a light look; the button at the top right switches to a dark
one, and the browser remembers it. It
shows the runs, one run with its flow, replicas, results and events, and
one replica with its counts, error log, logs (live, or kept from a failed
attempt) and failures, with a button that runs `alpheus replay`. It updates
as the status changes. [The UI](/ui/) has a sketch of it, and
[Reading alpheus ui](/docs/ui/) says what each part shows, such as the
chunks and lag on the flow's arrows; the page has the same guide under
**help**.

- It is read-only, apart from replay, and uses your kubeconfig, so it sees
  what you may see. Nothing is installed in the cluster.
- It answers only on localhost, and only to requests addressed to
  localhost, so a web page cannot use it to reach your cluster. Replay
  needs a token only the page has.
- `--endpoint` and `--ca-file` reach the storage, as for `alpheus replay`.

### In the cluster

The chart can run the same page as a Deployment, for a team that shares
it. It is off unless you turn it on:

```console
$ helm upgrade alpheus oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus \
    --namespace alpheus-system --reuse-values \
    --set ui.enabled=true --set 'ui.namespaces={flows}'
$ kubectl port-forward -n alpheus-system svc/alpheus-ui 7171
```

Then open <http://localhost:7171/>. In the cluster the page shows the runs
of every namespace, and its replay button runs `alpheus replay --with
cluster`, so the replay is a pod next to the run.

- **What it may see.** Runs and their events, in every namespace. The
  runs' logs, error logs and failures only in the namespaces in
  `ui.namespaces`, since reading them means reading the runs' storage
  Secrets there; `["*"]` is every namespace. Replay pods are started only
  there, and `ui.replay: false` turns the button off.
- **Who may see it.** The page has no login of its own: everyone who
  reaches it sees what its service account may see. Through
  `kubectl port-forward`, only those with access to the cluster reach it.
  With `ui.ingress`, put an authenticating proxy in front of it. The page
  still answers only to requests for localhost or the Ingress's host.

## Metrics

Each agent serves live metrics on port 9090 of its pod, `/metrics`, and the
controller on port 9090 of its own. Step pods have `prometheus.io/scrape`
annotations; with the Prometheus Operator, set
`metrics.serviceMonitor.enabled` in the chart for a ServiceMonitor and a
PodMonitor. A component must not listen on port 9090.

| Metric | From | What it is |
|---|---|---|
| `alpheus_chunks_total{direction}` | agent | Chunks downloaded (`in`) and uploaded (`out`) |
| `alpheus_bytes_total{direction}` | agent | Their bytes |
| `alpheus_backpressure_seconds_total` | agent | Time the output waited for consumers |
| `alpheus_storage_retries_total` | agent | Storage requests retried during an outage |
| `alpheus_component_count_total{unit}` | agent | What the component counted, live |
| `alpheus_component_log_entries_total{level}` | agent | Errors and warnings it reported |
| `alpheus_runs_started_total` | controller | Runs started |
| `alpheus_runs_finished_total{phase,reason}` | controller | Runs finished |
| `alpheus_step_restarts_total{step,reason}` | controller | Failed attempts restarted |
| `alpheus_edge_lag{run,edge}` | controller | Chunks committed on an edge and not acked yet |

The agents' metrics have the labels `run`, `step` and `replica`, and the
controller's `namespace`. They are best effort and start from zero when an
agent restarts; the run's status has the exact, committed numbers.

The chart has a Grafana dashboard with these metrics; set
`metrics.grafanaDashboard.enabled` to install it as a ConfigMap for
Grafana's sidecar, or import
{{< repolink "charts/alpheus/files/dashboard.json" >}}.
