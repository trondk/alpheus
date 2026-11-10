# Changelog

Every change to Alpheus that a user, an operator or a component author
notices, newest first. It follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/). A change
and its entry go in the same pull request, under Unreleased; a release
moves them to a section of its own, which becomes the release's notes.

Each section has three parts: **Changes**, what is new or different;
**Upgrade notes**, what to do when upgrading to it; and **CRD changes**,
what changed in the `StreamRun` custom resource.

## [Unreleased]

### Changes

- A flow is a `StreamRun` custom resource: a graph of steps, each a
  container image pinned by digest, that run at the same time and pass
  files to each other as chunks through any S3-compatible store.
- A sidecar agent in each step's pod handles storage, signals, retries
  and backpressure, so a component only reads and writes files. An SDK
  for Go and templates for Go and Python implement the component's side.
- Steps can run as several replicas; each chunk goes to one of them.
- A step that dies restarts and goes on from the first chunk it had not
  acknowledged; a stateful step reads its input again.
- The agents wait for the storage while it is down, with backoff, and a
  run fails at once when the storage refuses its keys or its bucket is
  full.
- A step's output that no other step reads is a result. It is kept after
  the run, for `resultsTTL`, and `status.results` says where it is.
- A failed run says why, as a reason code and in the component's words.
  The logs of failed attempts and the chunks they had open are kept, and
  `alpheus replay` runs such an attempt again on its own, on your machine
  or as a pod in the cluster.
- `failurePolicy: continue` lets the other replicas go on when one gives
  up, and the run ends `PartiallySucceeded`.
- The `alpheus` CLI: `run`, `get`, `watch`, `logs`, `cancel`, `chunks`,
  `validate`, `replay`, `errors`, `version`, `ui` and `results get`,
  `cat`, `ls`, `copy` and `gc`. When a run ends, `watch` and `get` list
  the commands that fit it.
- `alpheus ui` shows runs, their flows, logs and failures in the browser,
  on your machine or shared in the cluster.
- The Helm chart installs the CRD, the controller with its signal hub,
  and optionally the UI, with metrics for Prometheus and a Grafana
  dashboard.
- Each run gets a NetworkPolicy and a ServiceAccount without a token.
- The agent measures how long the component takes with each input chunk,
  from `Next` to `Done`, as the histogram `alpheus_chunk_duration_seconds`,
  and the dashboard shows its median and 95th percentile per step.

### Upgrade notes

- There is no earlier release to upgrade from.
- A running run keeps the agent it started with, `status.agentImage`,
  when the chart is upgraded. Keep the old agent's image in the registry
  until those runs are done.

### CRD changes

- The `StreamRun` custom resource, `alpheus-dev.org/v1alpha1`.
- `status.agentImage`: the agent's image, pinned by digest, that every
  step of the run uses.
