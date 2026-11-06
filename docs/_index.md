---
title: Documentation
description: How to write components for Alpheus and run flows with them.
lastmod: 2026-11-06
---

{{% planned %}}These pages describe the code on `main`. Nothing is released
yet, so commands that install from `registry.alpheus-dev.org` do not work
until the first release.{{% /planned %}}

New to Alpheus? Start with [How a run runs on Kubernetes](/docs/kubernetes/)
and [Getting started](/docs/getting-started/). When a run fails,
[A debug session, step by step](/docs/debug-example/) shows how to find
out why and try a fix. To drive Alpheus from your
own program, with no CLI, see [The API](/docs/api/): a run is a Kubernetes
object and its results and debug data are files in S3, so any language
can use it.
