#!/usr/bin/env bash
# Runs the end-to-end tests (SPEC 12.1) on the Vagrant test cluster: builds
# and pushes the images, installs the CRD, the controller and the UI with
# the Helm chart and those images, and runs test/e2e against Garage, or
# MinIO through hack/e2e-minio.sh. The
# UI stays running on port 30717 of the VM (GO-143).
#
# Usage: hack/e2e.sh [go test flags], for example -run TestTestcase01
#   SKIP_IMAGES=1  use the images in _out/images.env from the last run
#   DEPLOY_ONLY=1  stop after installing, without the tests (make deploy)
#   SKIP_DEPLOY=1  run the tests against what is installed
#                  (hack/test-upgrade.sh)
#   REPORT=0       no report afterwards; by default hack/report.sh makes
#                  one and serves it on port 30718 of the control plane
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

IP="$("$VAGRANT/talos.sh" ip)"

# The Helm chart with the images pinned by digest, from the test registry.
REGISTRY_NAME="${CONTROLLER_IMAGE%%/*}"
if [[ ${SKIP_DEPLOY:-} == 1 ]]; then
  :
elif [[ -n ${CHART:-} ]]; then
  "$HELM" upgrade --install alpheus "$CHART" --version "${CHART_VERSION:?CHART_VERSION is required with CHART}" \
    --ca-file "$VAGRANT/_out/ca.crt" \
    --namespace alpheus-system --create-namespace --wait --timeout 180s --hide-notes \
    --set registry="$REGISTRY_NAME"
else
  "$HELM" upgrade --install alpheus "$ROOT/charts/alpheus" \
    --namespace alpheus-system --create-namespace --wait --timeout 180s --hide-notes \
    --set registry="$REGISTRY_NAME" \
    --set controller.image.digest="${CONTROLLER_IMAGE#*@}" \
    --set agent.image.digest="${AGENT_IMAGE#*@}" \
    --set ui.enabled=true --set ui.image.digest="${UI_IMAGE#*@}" --set 'ui.namespaces={*}' \
    --set ui.service.type=NodePort --set ui.service.nodePort=30717 --set "ui.hosts={$IP}" \
    --set 'controller.nodeSelector.kubernetes\.io/hostname=alpheus-talos' \
    --set 'ui.nodeSelector.kubernetes\.io/hostname=alpheus-talos'
fi
kubectl label namespace alpheus-system --overwrite pod-security.kubernetes.io/enforce=restricted >/dev/null
kubectl -n alpheus-system rollout status deployment/alpheus-controller --timeout=180s
if [[ -z ${CHART:-} && ${SKIP_DEPLOY:-} != 1 ]]; then
  kubectl -n alpheus-system rollout status deployment/alpheus-ui --timeout=180s
  echo "alpheus ui on http://$IP:30717/"
fi
[[ ${DEPLOY_ONLY:-} == 1 ]] && exit 0

# The storage the testcases use. Garage is always there, and its keys come
# from vagrant/_out/secrets.env and Garage; for MinIO, hack/e2e-minio.sh
# sets the keys, the endpoint and ALPHEUS_E2E_STORAGE=minio.
export ALPHEUS_E2E_STORAGE="${ALPHEUS_E2E_STORAGE:-garage}"
if [[ $ALPHEUS_E2E_STORAGE == garage ]]; then
  # The key that can only read the results bucket, made by
  # vagrant/services.sh in Garage (GO-129).
  info=$(kubectl -n storage exec deploy/garage -c garage -- /garage key info --show-secret results-reader 2>/dev/null)
  ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY=$(sed -n -E 's/^Key ID:[[:space:]]+//p' <<<"$info")
  ALPHEUS_E2E_S3_RESULTS_SECRET_KEY=$(sed -n -E 's/^Secret key:[[:space:]]+//p' <<<"$info")
  [[ -n $ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY && -n $ALPHEUS_E2E_S3_RESULTS_SECRET_KEY ]] || { echo "no key results-reader in Garage: run vagrant/services.sh" >&2; exit 1; }
  ALPHEUS_E2E_S3_ACCESS_KEY="$GARAGE_DEFAULT_ACCESS_KEY"
  ALPHEUS_E2E_S3_SECRET_KEY="$GARAGE_DEFAULT_SECRET_KEY"
  ALPHEUS_E2E_S3_ENDPOINT="https://$IP:30900"
  ALPHEUS_E2E_S3_CA_FILE="$VAGRANT/_out/ca.crt"
fi
export ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY ALPHEUS_E2E_S3_RESULTS_SECRET_KEY \
  ALPHEUS_E2E_S3_ACCESS_KEY ALPHEUS_E2E_S3_SECRET_KEY ALPHEUS_E2E_S3_ENDPOINT ALPHEUS_E2E_S3_CA_FILE

# testcase20 needs a network that enforces NetworkPolicies. A cluster
# built before vagrant/ installed Cilium still has flannel, which ignores
# them; rebuild it with vagrant/destroy.sh and vagrant/deploy.sh.
if kubectl -n kube-system get daemonset kube-flannel >/dev/null 2>&1; then
  echo "the cluster has flannel, which ignores NetworkPolicies: testcase20 is skipped" >&2
  export ALPHEUS_E2E_NETWORK_POLICY=0
fi

# The output is kept for the report (hack/report.sh), which is built and
# served in the cluster when the tests are done, unless REPORT=0.
mkdir -p "$OUT/e2e"
LOG="$OUT/e2e/go-test.log"
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cd "$ROOT"
set +e
ALPHEUS_E2E=1 \
ALPHEUS_E2E_WORKER_VM=alpheus-talos-worker \
ALPHEUS_E2E_OUT="$OUT/bench" \
  go test -count=1 -v -timeout 45m ./test/e2e "$@" 2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}
set -e
if [[ ${REPORT:-1} == 1 ]]; then
  STARTED="$STARTED" STORAGE="$ALPHEUS_E2E_STORAGE" ARGS="$*" "$ROOT/hack/report.sh" "$LOG" ||
    echo "the report could not be made; the tests' own result stands" >&2
fi
exit "$rc"
