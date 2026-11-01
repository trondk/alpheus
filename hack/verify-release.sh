#!/usr/bin/env bash
# Checks a release the way a user does (GO-133): SHA256SUMS against its
# signature and the files, and the signatures and SBOM attestations of the
# images and the chart in the registry, all with the project's public key.
#
# Usage: hack/verify-release.sh <release folder>
#   COSIGN_PUB      the public key to trust (cosign.pub in the folder)
#   PULL_REGISTRY   reach the registry by this name instead of the one in
#                   images.txt, such as an IP (testing)
#   CA_FILE         CA of the registry's certificate, if not a public one
#   COSIGN          the tool, if not on PATH; skopeo finds each platform's image
set -euo pipefail

DIR="${1:?usage: hack/verify-release.sh <release folder>}"
COSIGN_PUB="${COSIGN_PUB:-$DIR/cosign.pub}"
COSIGN="${COSIGN:-cosign}"
TLS=() SKOPEO_TLS=()
if [[ -n "${CA_FILE:-}" ]]; then
  TLS=(--registry-cacert "$CA_FILE")
  mkdir -p "${TMPDIR:-/tmp}/verify-release-certs.$$"
  cp "$CA_FILE" "${TMPDIR:-/tmp}/verify-release-certs.$$/ca.crt"
  SKOPEO_TLS=(--cert-dir "${TMPDIR:-/tmp}/verify-release-certs.$$")
  trap 'rm -rf "${TMPDIR:-/tmp}/verify-release-certs.$$"' EXIT
fi
FAILED=0
ok() { echo "ok    $1"; }
fail() { echo "FAIL  $1"; FAILED=1; }

if "$COSIGN" verify-blob --key "$COSIGN_PUB" --insecure-ignore-tlog=true \
  --bundle "$DIR/SHA256SUMS.bundle" "$DIR/SHA256SUMS" >/dev/null 2>&1; then
  ok "SHA256SUMS is signed by the key"
else
  fail "SHA256SUMS signature"
fi
if (cd "$DIR" && sha256sum --quiet -c SHA256SUMS); then
  ok "every file matches SHA256SUMS"
else
  fail "checksums"
fi
listed="$(cut -d' ' -f3 "$DIR/SHA256SUMS" | LC_ALL=C sort)"
present="$(cd "$DIR" && find . -type f ! -name 'SHA256SUMS*' | sed 's|^\./||' | LC_ALL=C sort)"
[[ "$listed" == "$present" ]] && ok "SHA256SUMS lists every file" || fail "files missing from SHA256SUMS"

while read -r ref; do
  ref="${ref#oci://}"
  pull="$ref"
  [[ -n "${PULL_REGISTRY:-}" ]] && pull="$PULL_REGISTRY/${ref#*/}"
  if "$COSIGN" verify --key "$COSIGN_PUB" --insecure-ignore-tlog=true "${TLS[@]}" "$pull" >/dev/null 2>&1; then
    ok "signed: $ref"
  else
    fail "signature of $ref"
  fi
  [[ "$ref" == */charts/* ]] && continue
  # Every platform's image carries its SBOM.
  for a in amd64 arm64; do
    pd="$(skopeo inspect --override-os linux --override-arch "$a" "${SKOPEO_TLS[@]}" \
      --format '{{.Digest}}' "docker://$pull")"
    if "$COSIGN" verify-attestation --key "$COSIGN_PUB" --insecure-ignore-tlog=true "${TLS[@]}" \
      --type spdxjson "${pull%@*}@$pd" >/dev/null 2>&1; then
      ok "SBOM attached: $ref (linux/$a)"
    else
      fail "SBOM of $ref (linux/$a)"
    fi
  done
done <"$DIR/images.txt"

exit "$FAILED"
