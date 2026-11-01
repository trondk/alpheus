#!/usr/bin/env bash
# Builds the meter-etl example's four images, pushes them, and writes the
# flow with their digests to _out/meter-etl.yaml. By default it uses the
# Vagrant test cluster: Zot as the registry and Garage as the storage.
#
# Usage: examples/meter-etl/images.sh
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

mkdir -p "$OUT/meter-etl" "$OUT/certs"
cp "$CA_FILE" "$OUT/certs/ca.crt"
cd "$ROOT"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o "$OUT/meter-etl/" \
  ./examples/meter-etl/export ./examples/meter-etl/split ./examples/meter-etl/clean ./examples/meter-etl/aggregate

flow=$(cat "$ROOT/examples/meter-etl/flow.yaml")
for step in export split clean aggregate; do
  printf 'FROM scratch\nCOPY %s /c\nUSER 65532:65532\nENTRYPOINT ["/c"]\n' "$step" |
    podman build --quiet --timestamp 0 -f - -t "$REGISTRY/meter-etl/$step:dev" "$OUT/meter-etl" >/dev/null
  podman push --quiet --cert-dir "$OUT/certs" --digestfile "$OUT/meter-etl/digest" \
    "$REGISTRY/meter-etl/$step:dev" "docker://$PUSH_REGISTRY/meter-etl/$step:dev"
  flow=${flow//registry.example\/meter-$step@sha256:…/$REGISTRY/meter-etl/$step@$(cat "$OUT/meter-etl/digest")}
done
rm -f "$OUT/meter-etl/digest"
flow=${flow//endpoint: http:\/\/s3.example:3900/endpoint: $ENDPOINT}
flow=${flow//bucket: flows/bucket: $BUCKET
    region: $REGION}
printf '%s\n' "$flow" > "$OUT/meter-etl.yaml"
echo "wrote $OUT/meter-etl.yaml"
