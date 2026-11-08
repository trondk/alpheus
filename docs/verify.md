---
title: Verify a release
description: Check a release's files, images and Helm chart against the project's signing key before you install it.
weight: 8
lastmod: 2026-11-01
---

{{% planned %}}Nothing is released yet. The release script is built and
tested on the test cluster, but the project's key, the registry and
`/releases/` are not online until the first release.{{% /planned %}}

Every release is built from a tag on the project's own Forgejo, on the
project's own machines in the EU, and nothing of it is published on
GitHub. A release consists of:

| Where | What |
|---|---|
| `https://alpheus-dev.org/releases/vX.Y.Z/` | The `alpheus` CLI for Linux and macOS, the Helm chart as a file, an SPDX SBOM for every binary and image (`sbom/`), `images.txt`, `SHA256SUMS` and its signature `SHA256SUMS.bundle` |
| `registry.alpheus-dev.org/alpheus-dev/agent`, `…/controller` and `…/ui` | The images for `linux/amd64` and `linux/arm64`, signed, each with its SBOM attached |
| `oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus` | The Helm chart, signed, with the images' digests in it |

Everything is signed with the project's own key, with
[cosign](https://docs.sigstore.dev/cosign/system_config/installation/).
The public key is at `https://alpheus-dev.org/cosign.pub`. No signature
depends on a transparency log or any other outside service, so you can
check a release in a cluster with no internet access, as long as you have
the key.

## The files

Download the release's folder, or the files you need together with
`SHA256SUMS` and `SHA256SUMS.bundle`, and check the signature, then the
files:

```console
$ cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true \
    --bundle SHA256SUMS.bundle SHA256SUMS
Verified OK
$ sha256sum --ignore-missing -c SHA256SUMS
alpheus_v0.1.0_linux_amd64.tar.gz: OK
```

`--insecure-ignore-tlog=true` tells cosign not to look for the signature
in Sigstore's public transparency log, where Alpheus does not put it. The
key alone is what you trust.

## The images and the chart

`images.txt` names the images and the chart by digest. Check each one's
signature:

```console
$ cosign verify --key cosign.pub --insecure-ignore-tlog=true \
    registry.alpheus-dev.org/alpheus-dev/controller@sha256:…
$ cosign verify --key cosign.pub --insecure-ignore-tlog=true \
    registry.alpheus-dev.org/alpheus-dev/charts/alpheus@sha256:…
```

Each platform's image carries its SBOM as a signed attestation. To check
it and read it:

```console
$ cosign verify-attestation --key cosign.pub --insecure-ignore-tlog=true \
    --type spdxjson registry.alpheus-dev.org/alpheus-dev/agent@sha256:…
```

The chart installs the images by digest, so a verified chart only ever
pulls the images that were released with it.

## A mirror

If you copy the images and the chart into your own registry, copy their
signatures and attestations with them (for example with
`cosign copy`), and check them there with the same key. Install the chart
with `--set registry=<your registry>`: the images keep their paths and
digests.

## Rebuilding a release

The release is reproducible: building the tag again with
{{< repolink "hack/release.sh" >}} gives the same binaries, archives,
SBOMs, chart and image digests, so you can check that a release was built
from the code it says. The script and its settings are described on
[Development](/development/#releases).
