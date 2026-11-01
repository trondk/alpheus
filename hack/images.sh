#!/usr/bin/env bash
# Builds the agent, the controller, the test components and the component
# templates as images and
# pushes them to the registry, by default Zot in the Vagrant test cluster.
# The binaries are static, so each image is one file on scratch, running as
# uid 65532. The agent and the controller get the same paths as on GitHub
# Container Registry, so the Helm chart's registry value is all that differs.
# Writes the images, pinned by digest, to _out/images.env.
#
# Usage: hack/images.sh
#   REGISTRY       the name images are pulled by (registry.alpheus.test:30500)
#   PUSH_REGISTRY  where they are pushed from here (the VM's IP, port 30500)
#   CA_FILE        CA of the registry's certificate (vagrant/_out/ca.crt)
#   TAG            the tag pushed with each image (dev)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/_out"
REGISTRY="${REGISTRY:-registry.alpheus.test:30500}"
PUSH_REGISTRY="${PUSH_REGISTRY:-$("$ROOT/vagrant/talos.sh" ip):30500}"
CA_FILE="${CA_FILE:-$ROOT/vagrant/_out/ca.crt}"
TAG="${TAG:-dev}"

# binary, repository and variable in images.env
IMAGES=(
  "alpheus-agent alpheus-dev/agent AGENT_IMAGE"
  "alpheus-controller alpheus-dev/controller CONTROLLER_IMAGE"
  "bench alpheus-test/bench BENCH_IMAGE"
  "component-go alpheus-test/grep-go GREP_GO_IMAGE"
  "export alpheus-test/meter-export METER_EXPORT_IMAGE"
  "split alpheus-test/meter-split METER_SPLIT_IMAGE"
  "clean alpheus-test/meter-clean METER_CLEAN_IMAGE"
  "aggregate alpheus-test/meter-aggregate METER_AGGREGATE_IMAGE"
)

mkdir -p "$OUT/bin" "$OUT/certs"
cp "$CA_FILE" "$OUT/certs/ca.crt"
cd "$ROOT"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o "$OUT/bin/" \
  ./cmd/alpheus-agent ./cmd/alpheus-controller \
  ./test/components/bench ./templates/component-go \
  ./examples/meter-etl/export ./examples/meter-etl/split ./examples/meter-etl/clean ./examples/meter-etl/aggregate

: > "$OUT/images.env.tmp"
for entry in "${IMAGES[@]}"; do
  read -r bin repo var <<<"$entry"
  printf 'FROM scratch\nCOPY %s /%s\nUSER 65532:65532\nENTRYPOINT ["/%s"]\n' "$bin" "$bin" "$bin" |
    podman build --quiet --timestamp 0 -f - -t "$REGISTRY/$repo:$TAG" "$OUT/bin" >/dev/null
  podman push --quiet --cert-dir "$OUT/certs" --digestfile "$OUT/digest" \
    "$REGISTRY/$repo:$TAG" "docker://$PUSH_REGISTRY/$repo:$TAG"
  echo "$var=$REGISTRY/$repo@$(cat "$OUT/digest")" | tee -a "$OUT/images.env.tmp"
done
# The Python component template, built with its own Dockerfile
# (testcase07).
podman build --quiet -t "$REGISTRY/alpheus-test/grep-python:$TAG" "$ROOT/templates/component-python" >/dev/null
podman push --quiet --cert-dir "$OUT/certs" --digestfile "$OUT/digest" \
  "$REGISTRY/alpheus-test/grep-python:$TAG" "docker://$PUSH_REGISTRY/alpheus-test/grep-python:$TAG"
echo "GREP_PYTHON_IMAGE=$REGISTRY/alpheus-test/grep-python@$(cat "$OUT/digest")" | tee -a "$OUT/images.env.tmp"
rm -f "$OUT/digest"
mv "$OUT/images.env.tmp" "$OUT/images.env"
