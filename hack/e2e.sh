#!/usr/bin/env bash
# Runs the end-to-end tests (SPEC 12.1) on the Vagrant test cluster: builds
# and pushes the images, installs the CRD and the controller with the Helm
# chart and those images, and runs test/e2e against Garage.
#
# Usage: hack/e2e.sh [go test flags], for example -run TestTestcase01
#   SKIP_IMAGES=1  use the images in _out/images.env from the last run
#   BENCH_*        the testcases' sizes (see test/e2e/e2e_test.go)
#   HELM           the helm binary (helm)
#   CHART          a released chart to install instead of charts/alpheus,
#                  such as oci://<registry>/alpheus-dev/charts/alpheus, with
#                  CHART_VERSION; its own image digests are used
#                  (hack/release.sh)
# Measurements are written to _out/bench.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/_out"
VAGRANT="$ROOT/vagrant"
# Absolute, since go test runs in the package's folder.
export KUBECONFIG="$(realpath "${KUBECONFIG:-$VAGRANT/_out/kubeconfig}")"
HELM="${HELM:-helm}"

[[ ${SKIP_IMAGES:-} == 1 ]] || "$ROOT/hack/images.sh"
set -a
. "$OUT/images.env"
. "$VAGRANT/_out/secrets.env"
set +a

# The Helm chart with the images pinned by digest, from the test registry.
REGISTRY_NAME="${CONTROLLER_IMAGE%%/*}"
if [[ -n ${CHART:-} ]]; then
  "$HELM" upgrade --install alpheus "$CHART" --version "${CHART_VERSION:?CHART_VERSION is required with CHART}" \
    --ca-file "$VAGRANT/_out/ca.crt" \
    --namespace alpheus-system --create-namespace --wait --timeout 180s --hide-notes \
    --set registry="$REGISTRY_NAME"
else
  "$HELM" upgrade --install alpheus "$ROOT/charts/alpheus" \
    --namespace alpheus-system --create-namespace --wait --timeout 180s --hide-notes \
    --set registry="$REGISTRY_NAME" \
    --set controller.image.digest="${CONTROLLER_IMAGE#*@}" \
    --set agent.image.digest="${AGENT_IMAGE#*@}"
fi
kubectl label namespace alpheus-system --overwrite pod-security.kubernetes.io/enforce=restricted >/dev/null
kubectl -n alpheus-system rollout status deployment/alpheus-controller --timeout=180s

# The key that can only read the results bucket, made by
# vagrant/services.sh in Garage (GO-129).
info=$(kubectl -n storage exec deploy/garage -c garage -- /garage key info --show-secret results-reader 2>/dev/null)
RESULTS_ACCESS_KEY=$(sed -n -E 's/^Key ID:[[:space:]]+//p' <<<"$info")
RESULTS_SECRET_KEY=$(sed -n -E 's/^Secret key:[[:space:]]+//p' <<<"$info")
[[ -n $RESULTS_ACCESS_KEY && -n $RESULTS_SECRET_KEY ]] || { echo "no key results-reader in Garage: run vagrant/services.sh" >&2; exit 1; }

cd "$ROOT"
ALPHEUS_E2E=1 \
ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY="$RESULTS_ACCESS_KEY" \
ALPHEUS_E2E_S3_RESULTS_SECRET_KEY="$RESULTS_SECRET_KEY" \
ALPHEUS_E2E_S3_ACCESS_KEY="$GARAGE_DEFAULT_ACCESS_KEY" \
ALPHEUS_E2E_S3_SECRET_KEY="$GARAGE_DEFAULT_SECRET_KEY" \
ALPHEUS_E2E_S3_ENDPOINT="https://$("$VAGRANT/talos.sh" ip):30900" \
ALPHEUS_E2E_S3_CA_FILE="$VAGRANT/_out/ca.crt" \
ALPHEUS_E2E_OUT="$OUT/bench" \
  go test -count=1 -v -timeout 20m ./test/e2e "$@"
