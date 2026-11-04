#!/usr/bin/env bash
# Makes a report of an end-to-end run and serves it in the test cluster:
# turns go test's output into a page (hack/testreport), keeps it in
# _out/reports/ with the earlier ones, builds an image with all of them and
# runs it in the namespace reports, on port 30718 of the control plane.
# hack/e2e.sh calls it when the tests are done.
#
# Usage: hack/report.sh <go test -v output>
#        hack/report.sh --serve    only serve the reports in _out/reports
#                                  again, after a new cluster
#   STARTED  when the tests started (RFC 3339)
#   STORAGE  the storage they ran against (garage)
#   ARGS     the go test flags they ran with
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/_out"
VAGRANT="$ROOT/vagrant"
export KUBECONFIG="$(realpath "${KUBECONFIG:-$VAGRANT/_out/kubeconfig}")"
LOG="${1:?usage: hack/report.sh <go test -v output> | --serve}"
REGISTRY="${REGISTRY:-registry.alpheus.test:30500}"
IP="$("$VAGRANT/talos.sh" ip)"
STORAGE="${STORAGE:-garage}"
ID="$(date -u +%Y%m%dT%H%M%SZ)-$STORAGE"
REPORTS="$OUT/reports"
BUILD="$OUT/report-image"

cd "$ROOT"
if [[ $LOG != --serve ]]; then
images() { [[ -f $OUT/images.env ]] && sed -n "s/^$1=//p" "$OUT/images.env"; }
nodes="$(kubectl get nodes --no-headers -o custom-columns=N:.metadata.name,V:.status.nodeInfo.kubeletVersion 2>/dev/null | awk '{printf "%s%s (%s)", sep, $1, $2; sep=", "}')"
go tool test2json -t < "$LOG" > "$OUT/e2e/events.json" || true
go run ./hack/testreport render -root "$REPORTS" -id "$ID" -events "$OUT/e2e/events.json" -log "$LOG" -tests test/e2e \
  ${STARTED:+-started "$STARTED"} \
  -meta "storage=$STORAGE" \
  -meta "commit=$(git describe --always --dirty)" \
  -meta "go test flags=${ARGS:-(none)}" \
  -meta "controller=$(images CONTROLLER_IMAGE)" \
  -meta "agent=$(images AGENT_IMAGE)" \
  -meta "nodes=$nodes"
fi
[[ -d $REPORTS ]] || { echo "no reports in $REPORTS" >&2; exit 1; }

# The image: the server and every report, on scratch.
rm -rf "$BUILD" && mkdir -p "$BUILD"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o "$BUILD/testreport" ./hack/testreport
cp -r "$REPORTS" "$BUILD/reports"
printf 'FROM scratch\nCOPY testreport /testreport\nCOPY reports /reports\nUSER 65532:65532\nENTRYPOINT ["/testreport"]\n' |
  podman build --quiet --timestamp 0 -f - -t "$REGISTRY/alpheus-test/reports:latest" "$BUILD" >/dev/null
mkdir -p "$OUT/certs"
cp "$VAGRANT/_out/ca.crt" "$OUT/certs/ca.crt"
podman push --quiet --cert-dir "$OUT/certs" --digestfile "$BUILD/digest" \
  "$REGISTRY/alpheus-test/reports:latest" "docker://$IP:30500/alpheus-test/reports:latest"
REPORT_IMAGE="$REGISTRY/alpheus-test/reports@$(cat "$BUILD/digest")"

sed "s|\${REPORT_IMAGE}|$REPORT_IMAGE|" "$ROOT/test/report/report.yaml" | kubectl apply -f - >/dev/null
kubectl -n reports rollout status deployment/reports --timeout=3m >/dev/null
if [[ $LOG == --serve ]]; then echo "test reports on http://$IP:30718/"; else echo "test report on http://$IP:30718/$ID/"; fi
