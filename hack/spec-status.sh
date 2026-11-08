#!/usr/bin/env bash
# Prints the status of the requirements in SPEC.md (see "Status på
# kravene"): how many have each label, and every requirement that fails
# its check or is only partly built.
set -euo pipefail
cd "$(dirname "$0")/.."
rows=$(grep -E '^\| GO-[0-9]+ \|' SPEC.md | awk -F' \\| ' '{print $1 "\t" $3}' | sed 's/^| //')
total=$(wc -l <<<"$rows")
count() { grep -cP "\t$1" <<<"$rows" || true; }
printf '%-16s %3s\n' requirements "$total"
printf '%-16s %3s\n' closed "$(count '.*closed \(')"
printf '%-16s %3s\n' 'logic checked' "$(count '.*logic checked')"
printf '%-16s %3s\n' 'done (in all)' "$(count 'done')"
printf '%-16s %3s\n' 'done (delvis)' "$(count 'done \(delvis\)')"
printf '%-16s %3s\n' fail "$(count 'fail:')"
printf '%-16s %3s\n' 'not built (—)' "$(count '—$')"
printf '%-16s %3s\n' udgået "$(count 'udgået')"
echo
echo "fail:"
grep -P '\tfail:' <<<"$rows" | sed 's/\tfail: */  /' || echo "  none"
echo
echo "done (delvis):"
grep -P '\tdone \(delvis\)' <<<"$rows" | sed 's/\tdone (delvis)[^:]*: */  /' || echo "  none"
