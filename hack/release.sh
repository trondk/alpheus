#!/usr/bin/env bash
# Builds a release: the CLI for Linux and macOS, the agent and controller
# images for linux/amd64 and linux/arm64, the Helm chart with their digests
# in it, an SPDX SBOM for every binary and image, and SHA256SUMS. Signs the
# images, the chart and SHA256SUMS with the project's cosign key, without a
# transparency log, and attaches each image's SBOM to it (SPEC 14).
#
# The files end up in _out/release/<version>/, ready for
# https://alpheus-dev.org/releases/<version>/. Images and the chart are
# pushed to the registry. hack/verify-release.sh checks the result.
#
# Usage: hack/release.sh
#   VERSION          vX.Y.Z; defaults to the tag on HEAD, which must exist
#   REGISTRY         the name images are pulled by (registry.alpheus-dev.org)
#   PUSH_REGISTRY    where they are pushed from here (REGISTRY)
#   CA_FILE          CA of the registry's certificate, if not a public one
#   COSIGN_KEY       the private key (cosign.key); COSIGN_PASSWORD unlocks it
#   COSIGN_PUB       the public key published with the release (cosign.pub)
#   ALLOW_DIRTY      1 builds from uncommitted changes and without the tag
#                    (testing only)
#   HELM, SYFT, COSIGN  the tools, if not on PATH
#
# Log in to the registry with podman, helm and cosign first, unless it
# needs no login (like Zot in the test cluster).
#
# No tool may reach a service outside the project's own infrastructure
# (GO-130): cosign and syft run behind a proxy that does not exist, with
# only the registry let through, so a call to Sigstore or an update check
# fails the release instead of happening quietly.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VERSION="${VERSION:-$(git describe --exact-match --tags --match 'v*' 2>/dev/null || true)}"
REGISTRY="${REGISTRY:-registry.alpheus-dev.org}"
PUSH_REGISTRY="${PUSH_REGISTRY:-$REGISTRY}"
CA_FILE="${CA_FILE:-}"
COSIGN_KEY="${COSIGN_KEY:-cosign.key}"
COSIGN_PUB="${COSIGN_PUB:-cosign.pub}"
HELM="${HELM:-helm}"
SYFT="${SYFT:-syft}"
COSIGN="${COSIGN:-cosign}"

die() { echo "release: $*" >&2; exit 1; }
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
  die "VERSION must be vX.Y.Z, or HEAD must be tagged (got '${VERSION}')"
if [[ "${ALLOW_DIRTY:-}" != 1 ]]; then
  [[ -z "$(git status --porcelain)" ]] || die "the tree has uncommitted changes"
  # Go stamps the tag into the binaries, so it must be the one released.
  [[ "$(git tag --points-at HEAD)" == *"$VERSION"* ]] || die "HEAD is not tagged $VERSION"
fi
[[ -f "$COSIGN_KEY" && -f "$COSIGN_PUB" ]] || die "COSIGN_KEY and COSIGN_PUB must be files"
[[ -n "${COSIGN_PASSWORD+set}" ]] || die "COSIGN_PASSWORD must be set (it may be empty)"
for t in "$HELM" "$SYFT" "$COSIGN" podman skopeo go; do
  command -v "$t" >/dev/null || die "$t not found"
done

OUT="$ROOT/_out/release/$VERSION"
WORK="$ROOT/_out/release/.work-$VERSION"
rm -rf "$OUT" "$WORK"
mkdir -p "$OUT/sbom" "$WORK/certs"

# Everything carries the commit's time, so the same commit gives the same
# binaries, archives and image digests (GO-4).
export SOURCE_DATE_EPOCH
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)"
COMMIT="$(git rev-parse HEAD)"

# Registry access for podman, skopeo, helm, cosign and syft.
PODMAN_TLS=() HELM_TLS=() COSIGN_TLS=()
if [[ -n "$CA_FILE" ]]; then
  cp "$CA_FILE" "$WORK/certs/ca.crt"
  PODMAN_TLS=(--cert-dir "$WORK/certs")
  HELM_TLS=(--ca-file "$CA_FILE")
  COSIGN_TLS=(--registry-cacert "$CA_FILE")
  export SSL_CERT_FILE="$CA_FILE"  # syft
fi
PUSH_HOST="${PUSH_REGISTRY%%/*}"
offline() {
  HTTPS_PROXY=http://127.0.0.1:9 HTTP_PROXY=http://127.0.0.1:9 \
    NO_PROXY="${PUSH_HOST%%:*}" no_proxy="${PUSH_HOST%%:*}" \
    SYFT_CHECK_FOR_APP_UPDATE=false COSIGN_EXPERIMENTAL=0 "$@"
}
# A signing config with no services: signatures use the key alone, with no
# transparency log, timestamp authority or certificate authority (GO-131).
offline "$COSIGN" signing-config create --out "$WORK/signing-config.json" >/dev/null
SIGNING=(--yes --key "$COSIGN_KEY" --signing-config "$WORK/signing-config.json")
# quietly <command>: its output only if it fails.
quietly() {
  local log
  log="$("$@" 2>&1)" || { echo "$log" >&2; die "failed: ${*:1:2}"; }
}
sign() { quietly offline "$COSIGN" sign "${SIGNING[@]}" "${COSIGN_TLS[@]}" "$@"; }

# sbom <source> <name> <file>: an SPDX SBOM. syft stamps the time and a
# random namespace into it; both are fixed here, so the same commit gives
# the same SBOM.
CREATED="$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"
sbom() {
  offline "$SYFT" scan -q "$1" --source-name "$2" --source-version "$VERSION" -o "spdx-json=$WORK/sbom.json"
  sed -e "s|\"created\":\"[^\"]*\"|\"created\":\"$CREATED\"|" \
    -e "s|\"documentNamespace\":\"[^\"]*\"|\"documentNamespace\":\"https://alpheus-dev.org/releases/$VERSION/sbom/$(basename "$3")\"|" \
    "$WORK/sbom.json" >"$3"
}

echo "release $VERSION from $COMMIT"

# --- Binaries -------------------------------------------------------------

build() { # build <goos> <goarch> <package>...
  local dir="$WORK/bin/$1_$2"
  mkdir -p "$dir"
  CGO_ENABLED=0 GOOS="$1" GOARCH="$2" go build -trimpath -buildvcs=true \
    -ldflags='-s -w' -o "$dir/" "${@:3}"
}

ARCHES=(amd64 arm64)
for a in "${ARCHES[@]}"; do
  build linux "$a" ./cmd/alpheus ./cmd/alpheus-agent ./cmd/alpheus-controller
  build darwin "$a" ./cmd/alpheus
done

for os in linux darwin; do
  for a in "${ARCHES[@]}"; do
    name="alpheus_${VERSION}_${os}_${a}"
    tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner \
      -C "$WORK/bin/${os}_$a" -cf - alpheus | gzip -n -9 >"$OUT/$name.tar.gz"
    sbom "file:$WORK/bin/${os}_$a/alpheus" alpheus "$OUT/sbom/$name.spdx.json"
    echo "  $name.tar.gz"
  done
done

# --- Images ---------------------------------------------------------------

: >"$OUT/images.txt"
declare -A DIGEST
for img in agent controller; do
  bin="alpheus-$img"
  repo="alpheus-dev/$img"
  ref="$REGISTRY/$repo:$VERSION"
  podman manifest rm "$ref" >/dev/null 2>&1 || true
  for a in "${ARCHES[@]}"; do
    printf 'FROM scratch\nCOPY %s /%s\nUSER 65532:65532\nENTRYPOINT ["/%s"]\n' "$bin" "$bin" "$bin" |
      podman build --quiet --platform "linux/$a" \
        --label "org.opencontainers.image.version=$VERSION" \
        --label "org.opencontainers.image.revision=$COMMIT" \
        --label "org.opencontainers.image.licenses=Apache-2.0" \
        --label "org.opencontainers.image.source=https://alpheus-dev.org" \
        --manifest "$ref" -f - "$WORK/bin/linux_$a" >/dev/null
  done
  podman manifest push --quiet --all "${PODMAN_TLS[@]}" --digestfile "$WORK/digest" \
    "$ref" "docker://$PUSH_REGISTRY/$repo:$VERSION"
  podman manifest rm "$ref" >/dev/null
  d="$(cat "$WORK/digest")"
  DIGEST[$img]="$d"
  echo "$REGISTRY/$repo@$d" >>"$OUT/images.txt"
  sign --recursive "$PUSH_REGISTRY/$repo@$d"

  # One SBOM per platform, attached to that platform's image.
  for a in "${ARCHES[@]}"; do
    pd="$(skopeo inspect --override-os linux --override-arch "$a" "${PODMAN_TLS[@]}" \
      --format '{{.Digest}}' "docker://$PUSH_REGISTRY/$repo@$d")"
    file="$OUT/sbom/${img}_${VERSION}_linux_$a.spdx.json"
    sbom "registry:$PUSH_REGISTRY/$repo@$pd" "$repo" "$file"
    quietly offline "$COSIGN" attest "${SIGNING[@]}" "${COSIGN_TLS[@]}" \
      --type spdxjson --predicate "$file" "$PUSH_REGISTRY/$repo@$pd"
  done
  echo "  $REGISTRY/$repo@$d"
done

# --- Helm chart -----------------------------------------------------------

# A released chart installs the images by digest (GO-24): the first
# `digest: ""` in values.yaml is the controller's, the second the agent's.
cp -r charts/alpheus "$WORK/chart"
awk -v c="${DIGEST[controller]}" -v a="${DIGEST[agent]}" '
  /^    digest: ""/ { n++; sub(/""/, "\"" (n == 1 ? c : a) "\"") } { print }
' charts/alpheus/values.yaml >"$WORK/chart/values.yaml"
[[ "$(grep -c "digest: \"sha256:" "$WORK/chart/values.yaml")" == 2 ]] || die "digests not set in values.yaml"
"$HELM" package "$WORK/chart" --version "${VERSION#v}" --app-version "$VERSION" -d "$WORK" >/dev/null
# helm stamps the current time on every file; packed again with the
# commit's time, the same commit gives the same chart.
chart="$OUT/alpheus-${VERSION#v}.tgz"
mkdir "$WORK/unpacked"
tar -xzf "$WORK/alpheus-${VERSION#v}.tgz" -C "$WORK/unpacked"
tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner \
  -C "$WORK/unpacked" -cf - alpheus | gzip -n -9 >"$chart"
# helm push takes the manifest's creation time from the file's mtime.
touch -d "@$SOURCE_DATE_EPOCH" "$chart"
pushed="$(TZ=UTC "$HELM" push "$chart" "oci://$PUSH_REGISTRY/alpheus-dev/charts" "${HELM_TLS[@]}" 2>&1)"
chart_digest="$(grep -o 'sha256:[0-9a-f]*' <<<"$pushed" | head -1)" || die "helm push: $pushed"
echo "oci://$REGISTRY/alpheus-dev/charts/alpheus@$chart_digest" >>"$OUT/images.txt"
sign "$PUSH_REGISTRY/alpheus-dev/charts/alpheus@$chart_digest"
echo "  oci://$REGISTRY/alpheus-dev/charts/alpheus:${VERSION#v}"

# --- Checksums and signature ----------------------------------------------

cp "$COSIGN_PUB" "$OUT/cosign.pub"
(cd "$OUT" && find . -type f ! -name SHA256SUMS ! -name 'SHA256SUMS.*' | sed 's|^\./||' | LC_ALL=C sort |
  xargs sha256sum >SHA256SUMS)
quietly offline "$COSIGN" sign-blob "${SIGNING[@]}" --bundle "$OUT/SHA256SUMS.bundle" "$OUT/SHA256SUMS"

rm -rf "$WORK"
echo "wrote $OUT"
