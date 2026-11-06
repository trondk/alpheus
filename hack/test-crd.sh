#!/usr/bin/env bash
# Checks the StreamRun CRD's schema and CEL rules against a real API server.
# Installs the CRD from config/crd/bases, then creates, updates and deletes
# test objects in a temporary namespace.
#
# Usage: KUBECONFIG=... hack/test-crd.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS="alpheus-crd-test-$$"
SAMPLE="$ROOT/config/samples/streamrun.yaml"
FAILED=0

kubectl apply --server-side --force-conflicts -f "$ROOT/config/crd/bases/alpheus-dev.org_streamruns.yaml" >/dev/null || exit 1
kubectl wait --for=condition=Established crd/streamruns.alpheus-dev.org --timeout=30s >/dev/null || exit 1
kubectl create namespace "$NS" >/dev/null || exit 1
trap 'kubectl delete namespace "$NS" --wait=false >/dev/null' EXIT

# sample <yq-like sed expression> : the sample with one change, on stdout
sample() { sed "$1" "$SAMPLE"; }

# expect ok|<message> <description> <kubectl args...> (manifest on stdin)
expect() {
  local want=$1 what=$2; shift 2
  local out
  out=$(kubectl -n "$NS" "$@" -f - 2>&1)
  local rc=$?
  if [[ $want == ok && $rc == 0 ]] || [[ $want != ok && $rc != 0 && $out == *"$want"* ]]; then
    echo "ok    $what"
  else
    echo "FAIL  $what"
    echo "      want: $want"
    echo "      got:  $out"
    FAILED=1
  fi
}

create() { expect "$1" "$2" create --dry-run=server; }

# Creating
create ok "sample is valid" < "$SAMPLE"
create "every edge must go from and to an existing step" "edge to unknown step" \
  < <(sample 's/{from: filter, to: sum}/{from: filter, to: nope}/')
create "a step cannot read its own output" "edge from a step to itself" \
  < <(sample 's/{from: filter, to: sum}/{from: sum, to: sum}/')
create "Duplicate value" "duplicate step name" \
  < <(sample 's/- name: sum/- name: read/')
create "Duplicate value" "duplicate edge" \
  < <(sample 's/{from: filter, to: sum}/{from: read, to: filter}/')
create ok "same edge with another output is allowed" \
  < <(sample 's/{from: filter, to: sum}/{from: read, output: rejects, to: filter}/')
create "should match" "step name with capitals" \
  < <(sample 's/- name: sum/- name: Sum/')
create "name must be at most 40 characters" "run name over 40 characters" \
  < <(sample 's/name: sales-dk/name: sales-dk-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/')
create "should match" "storage endpoint without scheme" \
  < <(sample 's|endpoint: http://s3.example:3900|endpoint: s3.example:3900|')
create "Unsupported value" "stomp signal driver, which was removed" \
  < <(sample '$a\  signals: {driver: stomp}')
create "Unsupported value" "unknown signal driver" \
  < <(sample '$a\  signals: {driver: kafka}')
create "resultsSecretRef needs resultsBucket" "results key without a results bucket" \
  < <(sample 's/secretRef: alpheus-s3/secretRef: alpheus-s3, resultsSecretRef: reader/')
create ok "results bucket with its own read key" \
  < <(sample 's/secretRef: alpheus-s3/secretRef: alpheus-s3, resultsBucket: alpheus-results, resultsSecretRef: reader/')
create "should be greater than or equal to 1" "maxUnacked 0" \
  < <(sample 's/maxUnacked: 4/maxUnacked: 0/')
create ok "testcase03 sample is valid" < "$ROOT/config/samples/testcase03.yaml"
create "should be greater than or equal to 1" "replicas 0" \
  < <(sample 's/- name: sum/- name: sum\n      replicas: 0/')
create "should be less than or equal to 64" "replicas 65" \
  < <(sample 's/- name: sum/- name: sum\n      replicas: 65/')
create "unknown field" "maxRows is gone (GO-86)" \
  < <(sample 's/maxBytes: 512Mi/maxRows: 50000/')
create "Unsupported value" "unknown failurePolicy" \
  < <(sample 's/- name: sum/- name: sum\n      failurePolicy: retry/')
create "Unsupported value" "unknown inputOrder" \
  < <(sample 's/- name: sum/- name: sum\n      inputOrder: random/')
create "should be greater than or equal to 1" "maxOpen 0" \
  < <(sample 's/- name: sum/- name: sum\n      maxOpen: 0/')
create ok "named port on an edge" \
  < <(sample 's/{from: filter, to: sum}/{from: filter, to: sum, port: lookup}/')
create ok "resultsTTL 0 keeps results" \
  < <(sample '$a\  resultsTTL: 0s')

# Defaults
kubectl -n "$NS" create -f "$SAMPLE" >/dev/null || exit 1
# check_defaults <run> <description> <jsonpath> <want>
check_defaults() {
  local got
  got=$(kubectl -n "$NS" get sr "$1" -o jsonpath="$3")
  if [[ $got == "$4" ]]; then
    echo "ok    defaults: $2"
  else
    echo "FAIL  defaults: $2: got '$got', want '$4'"; FAILED=1
  fi
}
check_defaults sales-dk "maxRestarts, replicas, output, port, signals.driver" \
  '{.spec.steps[0].maxRestarts} {.spec.steps[0].replicas} {.spec.edges[0].output} {.spec.edges[0].port} {.spec.signals.driver}' \
  "3 1 default default builtin"
check_defaults sales-dk "failurePolicy, scratch, maxOpen, prefetch, inputOrder, resultsTTL" \
  '{.spec.steps[0].failurePolicy} {.spec.steps[0].scratch} {.spec.steps[0].maxOpen} {.spec.steps[0].prefetch} {.spec.steps[0].inputOrder} {.spec.resultsTTL}' \
  "stop 10Gi 8 2 arrival 168h"
sample '/chunks:/d;s/name: sales-dk/name: no-chunks/' | kubectl -n "$NS" create -f - >/dev/null || exit 1
check_defaults no-chunks "when chunks is left out" '{.spec.chunks.maxUnacked}' "4"

# Status: the phase from GO-97 and the counters from GO-115 are accepted.
if kubectl -n "$NS" patch sr sales-dk --subresource=status --type=merge -p \
  '{"status":{"phase":"PartiallySucceeded","steps":[{"name":"filter","counts":{"rows_in":57},"errors":1,"replicas":[{"index":0,"failedChunks":3}]}]}}' >/dev/null; then
  echo "ok    status with PartiallySucceeded, counts and failedChunks"
else
  echo "FAIL  status with PartiallySucceeded, counts and failedChunks"; FAILED=1
fi

# Updating
update() { expect "$1" "$2" replace; }
current() { kubectl -n "$NS" get sr sales-dk -o yaml; }

update "steps is immutable" "changing a step image" \
  < <(current | sed 's/input_csv@/input_tsv@/')
update "edges is immutable" "removing all edges" \
  < <(current | sed '/^  edges:/,/^  [a-z]/{/^  edges:/d;/^  - /d;/^    /d}')
update "storage is immutable" "changing the bucket" \
  < <(current | sed 's/bucket: flows/bucket: other/')
update "resultsTTL is immutable" "changing resultsTTL" \
  < <(current | sed 's/resultsTTL: 168h/resultsTTL: 1h/')
update ok "setting cancel" \
  < <(current | sed 's/^spec:/spec:\n  cancel: true/')
update "cancel cannot be undone" "unsetting cancel" \
  < <(current | sed 's/cancel: true/cancel: false/')

if [[ $FAILED == 0 ]]; then echo "all passed"; else echo "some failed"; fi
exit $FAILED
