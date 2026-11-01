#!/usr/bin/env bash
# The flagship benchmark (SPEC 12.1, testcase08): runs the meter-etl flow
# on the Vagrant test cluster with export, split and clean each at every
# number of replicas in REPLICAS, without killing a pod, and prints a table
# of the results, with when each step finished and the node it ran on.
# Uses the images from the last make e2e.
#
# Usage: hack/bench-etl.sh
#   REPLICAS         replicas of export, split and clean to try (1 2 4 8)
#   ETL_FILES        exported files (16)
#   ETL_ROWS         readings per file (2000000)
#   ETL_SPLIT_BYTES  uncompressed size of split's chunks (16 MiB)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export KUBECONFIG="$(realpath "${KUBECONFIG:-$ROOT/vagrant/_out/kubeconfig}")"
export ETL_FILES="${ETL_FILES:-16}" ETL_ROWS="${ETL_ROWS:-2000000}" ETL_SPLIT_BYTES="${ETL_SPLIT_BYTES:-16777216}"
export ETL_KILL=0 SKIP_IMAGES=1
REPLICAS="${REPLICAS:-1 2 4 8}"
OUT="$ROOT/_out/bench"
mkdir -p "$OUT"
since=$(date +%s)

for n in $REPLICAS; do
  BENCH_EXPORT_REPLICAS=$n BENCH_SPLIT_REPLICAS=$n BENCH_CLEAN_REPLICAS=$n "$ROOT/hack/e2e.sh" -run 'TestTestcase08$' | grep --line-buffered -E 'testcase08_test.go|--- (PASS|FAIL)' | grep --line-buffered -vE ': (Pending|Running|Succeeded) [0-9]'
done

node=$(kubectl get nodes -o jsonpath='{.items[0].status.capacity.cpu} CPUs, {.items[0].status.capacity.memory} memory, {.items[0].status.nodeInfo.osImage}')
echo
echo "Node: $node"
echo
python3 - "$OUT" "$since" <<'PY'
import glob, json, os, sys
out, since = sys.argv[1], float(sys.argv[2])
runs = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(out, "testcase08-*.json"))) if os.path.getmtime(f) >= since]
runs = [r for r in runs if not r["kills"]]
if not runs:
    sys.exit("no results")
r0 = runs[0]
print(f"{r0['files']} files of {r0['rowsPerFile']:,} readings: {r0['rawBytes']/1e6:.0f} MB raw, {r0['gzipBytes']/1e6:.0f} MB gzipped, {r0['chunks']} chunks")
print()
print("| replicas per step | processing | raw MB/s | rows/s | speed-up | export, split, clean, aggregate done after |")
print("|---|---|---|---|---|---|")
base = None
for r in sorted(runs, key=lambda r: r["cleanReplicas"]):
    base = base or r["processSeconds"]
    steps = r.get("stepSeconds") or {}
    done = ", ".join(f"{steps[s]:.0f} s" for s in ("export", "split", "clean", "aggregate") if s in steps)
    print(f"| {r['cleanReplicas']} | {r['processSeconds']:.0f} s | {r['rawMBPerSecond']:.1f} | {r['rowsPerSecond']:,.0f} | {base / r['processSeconds']:.2f}× | {done} |")
PY
