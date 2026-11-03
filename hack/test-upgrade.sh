#!/usr/bin/env bash
# Tests a chart upgrade in the middle of a run (GO-66, testcase18) on the
# Vagrant test cluster. Builds an older commit's images, installs its chart
# with them, and runs testcase18, which starts a run under the old
# controller and agent and upgrades to this commit's chart and images while
# it runs. The cluster is left with this commit's chart, as after
# make deploy, also if the test fails.
#
# Usage: hack/test-upgrade.sh
#   UPGRADE_FROM   the commit or tag upgraded from: the latest v* tag, or
#                  HEAD~1 while there is none
#   SKIP_IMAGES=1  use this commit's images in _out/images.env from the last
#                  run
#   HELM           the helm binary (helm)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/_out"
VAGRANT="$ROOT/vagrant"
export KUBECONFIG="$(realpath "${KUBECONFIG:-$VAGRANT/_out/kubeconfig}")"
HELM="${HELM:-helm}"
export HELM

cd "$ROOT"
FROM="${UPGRADE_FROM:-$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo HEAD~1)}"
FROM_SHA="$(git rev-parse --short "$FROM^{commit}")"
echo "upgrading from $FROM ($FROM_SHA) to $(git rev-parse --short HEAD)$(git diff --quiet HEAD || echo ' with changes')"

[[ ${SKIP_IMAGES:-} == 1 ]] || hack/images.sh
IP="$("$VAGRANT/talos.sh" ip)"

# The older commit in a worktree of its own, with its images.
SRC="$OUT/upgrade/src"
git worktree remove --force "$SRC" 2>/dev/null || rm -rf "$SRC"
git worktree prune
mkdir -p "$OUT/upgrade"
git worktree add --quiet --detach "$SRC" "$FROM_SHA"
deploy() { DEPLOY_ONLY=1 SKIP_DEPLOY=0 SKIP_IMAGES=1 "$ROOT/hack/e2e.sh" >"$OUT/upgrade/deploy.log" 2>&1; }
cleanup() {
  local rc=$?
  git -C "$ROOT" worktree remove --force "$SRC" 2>/dev/null || true
  # Back to this commit's chart, whatever happened.
  deploy || { echo "deploying this commit's chart failed: $OUT/upgrade/deploy.log" >&2; rc=1; }
  exit $rc
}
trap cleanup EXIT
PUSH_REGISTRY="$IP:30500" CA_FILE="$VAGRANT/_out/ca.crt" TAG=upgrade-from "$SRC/hack/images.sh" >/dev/null
OLD_ENV="$SRC/_out/images.env"
old() { sed -n "s/^$1=//p" "$OLD_ENV"; }
OLD_CONTROLLER="$(old CONTROLLER_IMAGE)" OLD_AGENT="$(old AGENT_IMAGE)" OLD_BENCH="$(old BENCH_IMAGE)"
[[ -n $OLD_CONTROLLER && -n $OLD_AGENT && -n $OLD_BENCH ]] || { echo "no images in $OLD_ENV" >&2; exit 1; }

# The older chart, as a user had it installed: no UI, the controller on
# the control plane like hack/e2e.sh puts it.
"$HELM" upgrade --install alpheus "$SRC/charts/alpheus" \
  --namespace alpheus-system --create-namespace --wait --timeout 180s --hide-notes \
  --set registry="${OLD_CONTROLLER%%/*}" \
  --set controller.image.digest="${OLD_CONTROLLER#*@}" \
  --set agent.image.digest="${OLD_AGENT#*@}" \
  --set ui.enabled=false \
  --set 'controller.nodeSelector.kubernetes\.io/hostname=alpheus-talos'
kubectl -n alpheus-system rollout status deployment/alpheus-controller --timeout=180s

SKIP_IMAGES=1 SKIP_DEPLOY=1 \
ALPHEUS_E2E_UPGRADE="DEPLOY_ONLY=1 SKIP_DEPLOY=0 SKIP_IMAGES=1 $ROOT/hack/e2e.sh" \
ALPHEUS_E2E_UPGRADE_AGENT_IMAGE="$OLD_AGENT" \
ALPHEUS_E2E_UPGRADE_BENCH_IMAGE="$OLD_BENCH" \
  hack/e2e.sh -run 'TestTestcase18$'
