---
title: How a run runs on Kubernetes
description: What the chart installs, what the controller creates for each run, what is in a pod, and why Alpheus creates pods itself instead of using Jobs.
weight: 4
lastmod: 2026-11-06
---

A run is one `StreamRun` object. Everything else on this page the
controller creates from it, owns, and deletes again. This is what you see
with `kubectl` while a run is going, and why it looks the way it does.

{{< how-it-works >}}

## What the chart installs

In its own namespace, `alpheus-system` in these examples:

| Object | What it is for |
|---|---|
| Deployment `alpheus-controller` | Watches `StreamRun` objects in every namespace and creates their pods. With more than one replica, one is the leader and the others stand by. |
| Service `alpheus-signal-hub` | The leader's signal hub on port 7070, where the agents tell each other that chunks are ready. |
| Service `alpheus-controller-metrics` | The controller's metrics on port 9090. |
| Deployment `alpheus-ui` | `alpheus ui` in the cluster, if `ui.enabled` is set. |

And for the whole cluster: the `StreamRun` CRD, a ClusterRole for the
controller, and the ValidatingAdmissionPolicy `alpheus-image-digest`,
which rejects a run whose images are not pinned by digest. See
[Getting started](/docs/getting-started/) for the install.

## What a run creates

For the run `k8s-doc`, made from the log-report example with two
replicas of `grep`, the controller creates in the run's namespace:

| Object | Name | One per |
|---|---|---|
| ServiceAccount | `alpheus-run-k8s-doc` | run |
| NetworkPolicy | `alpheus-run-k8s-doc` | run |
| Pod | `k8s-doc-<step>-<replica>-<attempt>` | replica and attempt |
| Secret | `k8s-doc-<step>-<replica>-<attempt>-signal` | replica and attempt |

All of them are owned by the `StreamRun`, so Kubernetes deletes them when
the run is deleted. The run's chunks in S3 are deleted by the controller
first; the run cannot disappear until they are gone.

```console
$ kubectl -n flows get pods -l alpheus-dev.org/run=k8s-doc
NAME                   READY   STATUS      RESTARTS   AGE
k8s-doc-count-0-1      0/2     Completed   0          5s
k8s-doc-generate-0-1   0/2     Completed   0          5s
k8s-doc-grep-0-1       0/2     Completed   0          5s
k8s-doc-grep-1-1       0/2     Completed   0          5s
```

Every step's pods start at once: `count` runs while `generate` is still
writing. When the run succeeds, the pods stay, with their logs, until the
run is deleted or `ttlAfterFinished` runs out. When it fails or is
cancelled, the controller deletes them at once, so nothing goes on
running; the logs of failed attempts are kept in S3 (below). The labels
`alpheus-dev.org/run`, `step`, `replica` and `attempt` find them.

## One pod up close

{{< k8s-pod >}}

- **Two containers.** The component is the pod's only ordinary
  container. The agent is a native sidecar, an init container with
  `restartPolicy: Always`: it starts before the component and stops after
  it, which needs Kubernetes 1.29 or newer. Every pod of a run has the
  agent the run started with, `status.agentImage`. When the chart is
  upgraded, running runs keep their agent, and new runs get the new one,
  so keep the old agent's image in the registry until those runs are done.
- **Files, not network.** Component and agent share an `emptyDir` with
  the agent's Unix socket and the scratch files the component reads and
  writes. `spec.steps[].scratch` limits its size.
- **Keys only for the agent.** The agent gets the S3 keys from the run's
  Secret and its certificate for the signal hub. The component gets only
  the Secrets the step names in `envFrom`.
- **No rights in Kubernetes.** The pod runs as the run's ServiceAccount,
  which has no rights and no token. It meets the Pod Security level
  `restricted`: not root, a read-only root file system, no capabilities.
- **A fence around it.** The NetworkPolicy lets the pod reach only DNS,
  the storage and the signal hub; [What a run can reach](/docs/isolation/)
  has the details.
- **Scheduled like any pod.** `resources`, `nodeSelector`, `tolerations`
  and `affinity` from the step go into the pod as they are.

## Pods, not Jobs

A Kubernetes Job, or a StatefulSet, restarts its pods on its own: when a
pod fails, or its node stops answering, it starts a new one. For Alpheus
that is the wrong thing to do. A pod on a node that stops answering may
still be running and writing chunks, and a new pod beside it would be a
second writer to the same stream.

So every pod has `restartPolicy: Never`, and the controller creates them
itself, one for each attempt:

{{< k8s-attempts >}}

1. When an attempt fails, the controller first keeps the end of each
   container's log in S3, so `alpheus logs --kept` can show it after the
   pod is gone.
2. It starts the next attempt only when the old pod is confirmed gone:
   deleted, or with every container stopped. A pod on a node that does not
   answer holds the next attempt back until it is deleted.
3. The new pod has the next attempt number in its name, a new certificate
   for the signal hub, and that number as its epoch in the manifests. An
   older attempt that still runs is turned away by the hub, and stops as
   soon as it sees a manifest with the higher epoch.
4. The new attempt picks up where the old one left off, from the last
   chunk it committed. After `maxRestarts` restarts (3 by default) the
   replica gives up, and with `failurePolicy: continue` only that replica
   does: the rest of the run goes on.

This is also why `RESTARTS` in `kubectl get pods` stays at `0`: a restart
is a new pod, not a restarted container. `status.steps[].replicas[]` has
each replica's attempt and the reason its last attempt failed.

## Clean-up

| When | What goes |
|---|---|
| The run succeeds | Nothing yet; the pods stay so you can read their logs |
| The run fails or is cancelled | The pods; the logs and inputs of failed attempts are kept in S3 |
| `ttlAfterFinished` runs out | The pods and the run's chunks |
| The run is deleted | The pods, the Secrets, the ServiceAccount, the NetworkPolicy and the chunks |
| `resultsTTL` runs out, 7 days by default | The results, error logs and kept logs |

The results outlive the run on purpose: a program can delete the run as
soon as it has started reading them. See
[Using Alpheus from a program](/docs/api/).
