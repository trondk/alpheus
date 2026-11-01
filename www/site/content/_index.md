---
title: Alpheus
description: Alpheus streams files between containers on Kubernetes, with nothing but object storage in between.
myth: "In Greek myth, the river Alpheus runs through the limestone of Arcadia, disappears underground and rises again in Sicily. Alpheus does the same with data: it leaves one step, travels out of sight through object storage, and appears in the next."
---

## What it does

- Every step runs at the same time; data moves between them as chunks in S3.
- A chunk is a file in any format: CSV, Parquet, images, archives. Alpheus moves it and never opens it; it is not a dataframe or Arrow engine.
- No message broker for data, only Kubernetes and an S3 endpoint.

## How it works

{{< how-it-works >}}

- The controller runs each step of a `StreamRun` as pods: one per
  replica, with the step's component as the only container next to an
  agent.
- The component reads its input as files and writes its output as files,
  in a folder it shares with the agent. It never talks to S3.
- The agent moves the files: it fetches input chunks from S3, uploads
  output chunks, and commits them in a manifest for each stream. A chunk
  exists once the manifest lists it.
- Agents tell each other that a chunk is ready through a small hub in
  the controller, over mutual TLS with a certificate for each attempt of
  a pod, so an old attempt is shut out. A lost signal only delays
  things: agents also read the manifests.
- A step that crashes starts again from its manifests, without losing
  or repeating output. Its counters and error log are committed with
  the output, so they stay exact too.

## A minimal flow

A run is one `StreamRun` resource. {{% planned %}}Seven test flows run end to end on a test cluster, including restarts and pods killed at random, but nothing is released yet: the CLI is built from source with `make cli`, and `kubectl apply -f flow.yaml` does the same as `alpheus run`.{{% /planned %}}

```yaml
apiVersion: alpheus-dev.org/v1alpha1
kind: StreamRun
metadata:
  name: log-report
spec:
  steps:
    - name: generate
      image: registry.example/generate@sha256:…
      settings: {chunks: 20, lines: 5000}
    - name: grep
      image: registry.example/grep@sha256:…
      replicas: 2
      settings: {match: ERROR}
    - name: count
      image: registry.example/count@sha256:…
  edges:
    - {from: generate, to: grep}
    - {from: grep, to: count}
  storage: {endpoint: http://s3.example:3900, bucket: flows, secretRef: alpheus-s3}
```

```console
$ alpheus run -f flow.yaml
$ alpheus watch log-report
```

The three steps are the example in [Writing components](/docs/components/),
in Go and in Python.

## Install

One Helm chart installs the controller and the CRD. {{% planned %}}The chart is built and tested on the test cluster, but not published yet.{{% /planned %}}

```console
$ helm install alpheus oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus \
    --namespace alpheus-system --create-namespace
```

Images and the chart come from the project's own registry,
`registry.alpheus-dev.org`, unless you set your own, such as a Zot mirror
in an air-gapped cluster, with `--set registry=registry.example:5000`.

{{% ifpage "/docs/getting-started/" %}}[Getting started](/docs/getting-started/) walks through a first flow.{{% /ifpage %}}
{{% ifpage "/docs/components/" %}}[Writing components](/docs/components/) shows how to write the steps of a flow in Go or Python.{{% /ifpage %}}
