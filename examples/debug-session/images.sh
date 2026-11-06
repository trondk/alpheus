#!/usr/bin/env bash
# Builds the debug-session example's images, pushes them, and writes the
# flow with their digests to _out/debug-session.yaml, with version 1 of
# merge, the one with the bug. The fixed version 2 is pushed as
# debug-session/merge:v2, and its reference, pinned by digest, written to
# _out/debug-session.env as MERGE_V2_IMAGE. By default it uses the Vagrant
# test cluster: Zot as the registry and Garage as the storage.
#
# Usage: examples/debug-session/images.sh
#   REGISTRY       the name images are pulled by (registry.alpheus.test:30500)
#   PUSH_REGISTRY  where they are pushed from here (the VM's IP, port 30500)
#   CA_FILE        CA of the registry's certificate (vagrant/_out/ca.crt)
#   ENDPOINT, BUCKET, REGION
#                  the run's storage (Garage in the test cluster)
set -euo pipefail
shopt -s inherit_errexit

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/_out"
REGISTRY="${REGISTRY:-registry.alpheus.test:30500}"
PUSH_REGISTRY="${PUSH_REGISTRY:-$("$ROOT/vagrant/talos.sh" ip):30500}"
CA_FILE="${CA_FILE:-$ROOT/vagrant/_out/ca.crt}"
ENDPOINT="${ENDPOINT:-http://garage.storage.svc.cluster.local:3900}"
BUCKET="${BUCKET:-alpheus-data}"
REGION="${REGION:-garage}"

mkdir -p "$OUT/debug-session" "$OUT/certs"
cp "$CA_FILE" "$OUT/certs/ca.crt"
cd "$ROOT"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o "$OUT/debug-session/" \
  ./examples/debug-session/orders ./examples/debug-session/customers ./examples/debug-session/filter \
  ./examples/debug-session/merge ./examples/debug-session/merge-v2 ./examples/debug-session/sort

# push <binary> <repository> <tag>: prints the image pinned by digest. The
# binary keeps a step's name, which the SDK logs as the component.
push() {
  name=${2##*/}
  printf 'FROM scratch\nCOPY %s /%s\nUSER 65532:65532\nENTRYPOINT ["/%s"]\n' "$1" "$name" "$name" |
    podman build --quiet --timestamp 0 -f - -t "$REGISTRY/$2:$3" "$OUT/debug-session" >/dev/null
  podman push --quiet --cert-dir "$OUT/certs" --digestfile "$OUT/debug-session/digest" \
    "$REGISTRY/$2:$3" "docker://$PUSH_REGISTRY/$2:$3"
  echo "$REGISTRY/$2@$(cat "$OUT/debug-session/digest")"
}

flow=$(cat "$ROOT/examples/debug-session/flow.yaml")
for step in orders customers filter sort; do
  flow=${flow//registry.example\/debug-session\/$step@sha256:…/$(push "$step" "debug-session/$step" v1)}
done
flow=${flow//registry.example\/debug-session\/merge@sha256:…/$(push merge debug-session/merge v1)}
echo "MERGE_V2_IMAGE=$(push merge-v2 debug-session/merge v2)" > "$OUT/debug-session.env"
rm -f "$OUT/debug-session/digest"
flow=${flow//endpoint: http:\/\/s3.example:3900/endpoint: $ENDPOINT}
flow=${flow//bucket: flows/bucket: $BUCKET
    region: $REGION}
printf '%s\n' "$flow" > "$OUT/debug-session.yaml"
echo "wrote $OUT/debug-session.yaml and $OUT/debug-session.env"
