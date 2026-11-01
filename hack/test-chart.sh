#!/usr/bin/env bash
# Lints the Helm chart and checks what it renders: images from GitHub
# Container Registry or the registry value, pinned by digest, the agent's
# digest required, the hub's address in the release's namespace, and the CRD
# kept on uninstall. Needs no cluster.
#
# Usage: hack/test-chart.sh
#   HELM  the helm binary (helm)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHART="$ROOT/charts/alpheus"
HELM="${HELM:-helm}"
FAILED=0
D1=sha256:1111111111111111111111111111111111111111111111111111111111111111
D2=sha256:2222222222222222222222222222222222222222222222222222222222222222
PINNED=(--set "agent.image.digest=$D1" --set "controller.image.digest=$D2")

render() { "$HELM" template alpheus "$CHART" --namespace flows "$@" 2>&1; }

# check <description> <pattern> <output>: the output contains the pattern
check() {
  if grep -qF -- "$2" <<<"$3"; then echo "ok    $1"; else echo "FAIL  $1: no '$2'"; FAILED=1; fi
}
# reject <description> <pattern> <output>: the output does not contain it
reject() {
  if grep -qF -- "$2" <<<"$3"; then echo "FAIL  $1: found '$2'"; FAILED=1; else echo "ok    $1"; fi
}

if "$HELM" lint --strict "$CHART" "${PINNED[@]}" >/dev/null; then echo "ok    helm lint"; else echo "FAIL  helm lint"; FAILED=1; fi

out=$(render "${PINNED[@]}")
check "controller from registry.alpheus-dev.org by digest" "image: registry.alpheus-dev.org/alpheus-dev/controller@$D2" "$out"
check "agent from registry.alpheus-dev.org by digest" "-agent-image=registry.alpheus-dev.org/alpheus-dev/agent@$D1" "$out"
check "hub in the release's namespace" "-hub-address=alpheus-signal-hub.flows.svc:7070" "$out"
check "hub Service" "name: alpheus-signal-hub" "$out"
check "CRD" "name: streamruns.alpheus-dev.org" "$out"
check "CRD kept on uninstall" "helm.sh/resource-policy: keep" "$out"
check "ClusterRole" "kind: ClusterRole" "$out"
check "binding to the release's namespace" "namespace: flows" "$out"
reject "no tags allowed" "-allow-tags" "$out"

out=$(render "${PINNED[@]}" --set registry=registry.example:5000/)
check "registry value for the controller" "image: registry.example:5000/alpheus-dev/controller@$D2" "$out"
check "registry value for the agent" "-agent-image=registry.example:5000/alpheus-dev/agent@$D1" "$out"
reject "nothing from registry.alpheus-dev.org" "registry.alpheus-dev.org" "$out"

out=$(render --set "controller.image.digest=$D2")
check "agent digest required" "agent.image.digest is required" "$out"

out=$(render --set controller.allowTags=true --set agent.image.tag=dev)
check "tags when allowed" "-agent-image=registry.alpheus-dev.org/alpheus-dev/agent:dev" "$out"
check "controller tag from appVersion" "image: registry.alpheus-dev.org/alpheus-dev/controller:v0.1.0" "$out"
check "allow-tags flag" "-allow-tags" "$out"

out=$(render "${PINNED[@]}" --set signalHub.enabled=false --set crds.install=false)
check "hub disabled" "-hub-listen=" "$out"
reject "no hub Service" "name: alpheus-signal-hub" "$out"
reject "no hub address" "-hub-address" "$out"
reject "no CRD" "kind: CustomResourceDefinition" "$out"

exit $FAILED
