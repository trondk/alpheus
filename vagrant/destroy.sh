#!/bin/bash
# Removes the Talos VMs and the generated cluster config in vagrant/_out/
# (the destroy trigger runs talos.sh clean).
#
# Usage: ./destroy.sh            the VMs and vagrant/_out/; the test reports,
#                                benchmarks and logs in the repository's
#                                _out/ stay, and the next make e2e serves
#                                the old reports again
#        ./destroy.sh --clean    also what earlier test runs left in _out/,
#                                so the next deploy starts with no history
#
# --clean keeps _out/release/ (release builds) and _out/vulndb/ (the
# vulnerability database), which have nothing to do with the cluster, and
# vagrant/.cache/ (the Talos ISO), which saves a download.

cd "$(dirname "$0")" || exit 1

CLEAN=0
case "${1:-}" in
  "") ;;
  --clean) CLEAN=1 ;;
  *) echo "usage: $0 [--clean]" >&2; exit 2 ;;
esac

export VAGRANT_DEFAULT_PROVIDER=libvirt
vagrant destroy -f || exit

[[ $CLEAN == 1 ]] || exit 0

# What make e2e, make e2e-minio, make bench, make deploy and
# make test-upgrade leave in the repository's _out/.
OUT="$(cd .. && pwd)/_out"
[[ -d $OUT ]] || exit 0
removed=()
for p in "$OUT"/*; do
  [[ -e $p ]] || continue
  case "${p##*/}" in
    release | vulndb) continue ;;
  esac
  if [[ ${p##*/} == upgrade && -d $p/src ]]; then
    # test-upgrade's git worktree: let git forget it too.
    git -C .. worktree remove --force "$p/src" 2>/dev/null
  fi
  rm -rf "$p" && removed+=("${p##*/}")
done
git -C .. worktree prune 2>/dev/null
if ((${#removed[@]})); then
  echo "removed from $OUT: ${removed[*]}"
else
  echo "nothing to remove in $OUT"
fi
echo "kept: release/, vulndb/ and vagrant/.cache/ (the Talos ISO)"
