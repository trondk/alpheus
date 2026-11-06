---
title: What a run can reach
description: Each run gets a ServiceAccount with no rights and a NetworkPolicy that lets its pods reach only DNS, the storage and the signal hub. What that means for components, how to give a step more network, and what the policy does not cover.
weight: 9
lastmod: 2026-11-05
---

A component is code you did not necessarily write. Alpheus gives it as
little as it needs: local files in its scratch folder, and the settings
and Secrets the run names. The controller also gives every run two
objects of its own, both named `alpheus-run-<run>` and deleted with the
run.

## The ServiceAccount

The run's pods use a ServiceAccount with no token and no roles. A
component cannot use the Kubernetes API, not even as the namespace's
`default` ServiceAccount, which someone may have given rights to.

If an object with that name exists and is not the run's, the run fails
with the reason `InvalidSpec` before any pod starts.

## The NetworkPolicy

The run's pods may reach:

| What | Port | Why |
|---|---|---|
| DNS | 53, UDP and TCP, any address | to find the storage and the hub |
| The storage | the port in `spec.storage.endpoint`, or 443 or 80 by its scheme | the agent reads and writes the chunks there |
| The signal hub | the hub's port, on the controller pod running the hub | the agent's signals |

They can be reached only on port 9090, the agent's metrics, for a
Prometheus that scrapes the pods. Nothing else gets in or out: not the
Kubernetes API, not other pods of the run, not other namespaces, not
the internet. The pods of a run never need to talk to each other, since
everything goes through the storage.

The agent and the component share the pod's network, so the component
can reach what the agent can. That is why the storage key is the agent's
alone: the component can reach the storage, but has no key for it.

The policy selects the pods by the label `alpheus-dev.org/network`, which
step pods and the pods of [`alpheus replay --with cluster`](/docs/debugging/#replaying-a-failed-attempt)
both have.

### Your network must enforce it

A NetworkPolicy is only a request: the cluster's network plugin enforces
it, or ignores it. Cilium and Calico enforce them; flannel, Talos'
default, does not. The test cluster runs Cilium for that reason. On a
cluster that cannot enforce them, or that has its own policies, turn them
off:

```console
$ helm upgrade --install alpheus charts/alpheus --set networkPolicy.enabled=false ...
```

### When a step needs more

A source that reads from a database, or a step that calls an API, needs
more than the policy gives. NetworkPolicies add up: a pod may do what any
policy that selects it allows. So give the step a policy of your own next
to the run's, selected by the labels every step pod has,
`alpheus-dev.org/run` and `alpheus-dev.org/step`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: export-to-postgres
  namespace: etl
spec:
  podSelector:
    matchLabels:
      alpheus-dev.org/step: export
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels: {kubernetes.io/metadata.name: databases}
      ports:
        - {protocol: TCP, port: 5432}
```

Leave out `alpheus-dev.org/run` to cover every run with a step named
`export` in the namespace, as above, or add it for one run only. Your
policy is yours: the run does not own it and does not delete it.

### The storage

By default the storage rule allows the storage's port on any address,
because the controller cannot know where a name like `s3.example.com`
points. Two chart values make it narrower:

| Value | Example | Effect |
|---|---|---|
| `networkPolicy.storageTo` | `[{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: storage}}}]` or `[{ipBlock: {cidr: 10.0.20.0/24}}]` | the storage port only to there |
| `networkPolicy.storagePorts` | `[9000]` | these ports instead of the endpoint's |

Set `storagePorts` when the storage is a Service in the cluster whose
`port` differs from its `targetPort`: a NetworkPolicy sees the pod's port,
not the Service's.

## What it does not cover

- DNS is open to any address, so a component could send data out in DNS
  queries to a server it controls. Block that with your network plugin's
  own DNS policies, such as Cilium's, if it matters to you.
- Without `networkPolicy.storageTo`, a component can reach any host on the
  storage's port, such as 443 on the internet when the storage is on 443.
- The metrics port is open to every pod in the cluster. It serves only
  the agent's counters.
- `alpheus ui` in the cluster has no login of its own: it shows anyone
  who reaches it what its ServiceAccount may see, so keep it behind
  `kubectl port-forward` or an authenticating proxy (see
  [When a run fails](/docs/debugging/#in-the-cluster)).
