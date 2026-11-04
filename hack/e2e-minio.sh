#!/usr/bin/env bash
# Runs the end-to-end tests against MinIO instead of Garage. Installs a
# MinIO in the namespace minio of the Vagrant test cluster with the same
# buckets as Garage (test/e2e/minio), runs hack/e2e.sh against it with
# ALPHEUS_E2E_STORAGE=minio, and deletes the namespace again afterwards,
# with MinIO, its data and its keys, also when the tests fail. The report
# (hack/report.sh) stays.
#
# Usage: hack/e2e-minio.sh [go test flags], for example -run TestTestcase01
#   KEEP_MINIO=1   leave MinIO running afterwards, to look at it
#   SKIP_IMAGES, SKIP_DEPLOY, REPORT, HELM  as for hack/e2e.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VAGRANT="$ROOT/vagrant"
MANIFESTS="$ROOT/test/e2e/minio"
export KUBECONFIG="$(realpath "${KUBECONFIG:-$VAGRANT/_out/kubeconfig}")"
# Chainguard's build of MinIO's client, pinned like the server.
MC_IMAGE="cgr.dev/chainguard/minio-client@sha256:1dccd2a6fd59914a98ac2d9dad95e552c91243cb0bd7242aaa224802df4b3432"

log() { printf '==> %s\n' "$*" >&2; }

IP="$("$VAGRANT/talos.sh" ip)"
ENDPOINT="http://$IP:30903"

remove() {
  local rc=$?
  if [[ ${KEEP_MINIO:-} == 1 ]]; then
    log "MinIO stays on $ENDPOINT; kubectl delete namespace minio removes it"
  else
    log "removing MinIO"
    kubectl delete namespace minio --ignore-not-found --wait=true --timeout=5m >/dev/null || rc=1
  fi
  exit $rc
}
trap remove EXIT

# Keys for this run only: MinIO's root, the agents' key and the key that
# can only read the results bucket (GO-129). They live in MinIO and in the
# test namespaces' Secrets, and go with them.
rand() { openssl rand -hex "$1"; }
ROOT_USER="root$(rand 4)" ROOT_PASSWORD="$(rand 24)"
export ALPHEUS_E2E_S3_ACCESS_KEY="alpheus$(rand 6)" ALPHEUS_E2E_S3_SECRET_KEY="$(rand 24)"
export ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY="reader$(rand 6)" ALPHEUS_E2E_S3_RESULTS_SECRET_KEY="$(rand 24)"

log "installing MinIO"
kubectl apply -f "$MANIFESTS/namespace.yaml" >/dev/null
kubectl -n minio create secret generic minio-root \
  --from-literal=user="$ROOT_USER" --from-literal=password="$ROOT_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -k "$MANIFESTS" >/dev/null
kubectl -n minio rollout status deployment/minio --timeout=5m

# mc runs here, against the NodePort, with the policies mounted.
mc() {
  podman run --rm --network host -v "$MANIFESTS:/policies:ro,Z" \
    -e "MC_HOST_m=http://$ROOT_USER:$ROOT_PASSWORD@$IP:30903" "$MC_IMAGE" "$@" >/dev/null
}
log "buckets and keys"
mc mb --ignore-existing m/alpheus-data m/alpheus-results m/alpheus-quota
# A small bucket that fills up, for testcase12.
mc quota set m/alpheus-quota --size 20MiB
mc admin policy create m alpheus-rw /policies/alpheus-rw.json
mc admin policy create m results-read /policies/results-read.json
mc admin user add m "$ALPHEUS_E2E_S3_ACCESS_KEY" "$ALPHEUS_E2E_S3_SECRET_KEY"
mc admin policy attach m alpheus-rw --user "$ALPHEUS_E2E_S3_ACCESS_KEY"
mc admin user add m "$ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY" "$ALPHEUS_E2E_S3_RESULTS_SECRET_KEY"
mc admin policy attach m results-read --user "$ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY"

log "running the testcases against MinIO on $ENDPOINT"
export ALPHEUS_E2E_STORAGE=minio ALPHEUS_E2E_S3_ENDPOINT="$ENDPOINT" ALPHEUS_E2E_S3_CA_FILE=""
"$ROOT/hack/e2e.sh" "$@"
