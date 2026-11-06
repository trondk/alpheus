#!/usr/bin/env bash
# Host-side setup of the two Talos VMs. Called by Vagrant triggers, but each
# command can also be run by hand:
#
#   ./talos.sh fetch-iso   download the Talos ISO
#   ./talos.sh bootstrap   install Talos on the VM and bootstrap Kubernetes
#   ./talos.sh ip          print the control plane's IP address
#   ./talos.sh ip worker   print the worker's IP address
#   ./talos.sh clean       remove generated config
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TALOS_VERSION="${TALOS_VERSION:-v1.14.2}"
VM_NAME="${VM_NAME:-alpheus-talos}"
WORKER_NAME="${WORKER_NAME:-alpheus-talos-worker}"
TALOS_ISO="${TALOS_ISO:-$HERE/.cache/talos-$TALOS_VERSION-metal-amd64.iso}"
CLUSTER_NAME="alpheus"
OUT="$HERE/_out"
LIBVIRT_URI="qemu:///system"
REGISTRY_HOST="registry.alpheus.test:30500"
# Cilium enforces the runs' NetworkPolicies; the chart is checked against
# the SHA-256 in helm.cilium.io's index.
CILIUM_VERSION="1.20.3"
CILIUM_SHA256="0805a5273118c436c9772bb9566d87ec558d8b25cc2ce5dc32918ecdd36f2c7c"
CILIUM_CHART="$HERE/.cache/cilium-$CILIUM_VERSION.tgz"

export TALOSCONFIG="$OUT/talosconfig"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() {
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null || die "$cmd not found in PATH"
  done
}

# wait_for <seconds> <description> <command...>
wait_for() {
  local timeout=$1 what=$2; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || die "timed out after ${timeout}s waiting for $what"
    sleep 5
  done
}

fetch_iso() {
  need curl
  [[ -s $TALOS_ISO ]] && return
  mkdir -p "$(dirname "$TALOS_ISO")"
  log "downloading Talos $TALOS_VERSION ISO"
  curl -fL --progress-bar -o "$TALOS_ISO.part" \
    "https://github.com/siderolabs/talos/releases/download/$TALOS_VERSION/metal-amd64.iso"
  mv "$TALOS_ISO.part" "$TALOS_ISO"
}

# A VM's only NIC is on the Vagrant management network, which hands out
# addresses by DHCP.
# virsh's whole answer is read before awk picks the first address: awk
# stopping early in a pipe would kill virsh with SIGPIPE, which pipefail
# turns into exit code 141. No lease yet prints nothing.
vm_ip() {  # [vm]
  local leases
  leases=$(virsh -c "$LIBVIRT_URI" domifaddr "${1:-$VM_NAME}" --source lease 2>/dev/null) || return 0
  awk '$3 == "ipv4" { sub(/\/.*/, "", $4); print $4; exit }' <<<"$leases"
}

has_ip() { [[ -n $(vm_ip "$1") ]]; }

# The cluster has no network until Cilium runs (cni-patch.yaml), so the
# nodes are not ready and talosctl health would wait for ever. Cilium is
# installed as soon as the API server answers.
install_cilium() {
  if [[ ! -s $CILIUM_CHART ]]; then
    log "downloading the Cilium $CILIUM_VERSION chart"
    mkdir -p "$(dirname "$CILIUM_CHART")"
    curl -fsSL -o "$CILIUM_CHART.part" "https://helm.cilium.io/cilium-$CILIUM_VERSION.tgz"
    mv "$CILIUM_CHART.part" "$CILIUM_CHART"
  fi
  echo "$CILIUM_SHA256  $CILIUM_CHART" | sha256sum -c --quiet - ||
    { rm -f "$CILIUM_CHART"; die "the Cilium chart does not match its SHA-256"; }
  log "installing Cilium $CILIUM_VERSION"
  KUBECONFIG="$OUT/kubeconfig" helm upgrade --install cilium "$CILIUM_CHART" \
    --namespace kube-system --values "$HERE/services/cilium/values.yaml" --wait --timeout 10m >/dev/null
}

# Bootstrap is refused until Talos has installed to disk and started etcd,
# so this is retried. A cluster that is already bootstrapped counts as done.
try_bootstrap() {
  local out
  out=$(talosctl bootstrap 2>&1) && return 0
  [[ $out == *AlreadyExists* ]]
}

# The test CA signs the gateway's certificate (services.sh). Talos must trust
# it before it is installed, so it is made here, before the Talos config.
make_ca() {
  [[ -f $OUT/ca.crt && -f $OUT/ca.key ]] && return
  log "creating the test CA"
  ( umask 077
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
      -keyout "$OUT/ca.key" -out "$OUT/ca.crt" -days 3650 \
      -subj "/O=Alpheus test/CN=Alpheus test CA" \
      -addext "basicConstraints=critical,CA:TRUE" \
      -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null )
  chmod 0644 "$OUT/ca.crt"
}

# The node pulls images named registry.alpheus.test:30500/... from Zot
# through the gateway: the name points at the VM itself, and the gateway's
# certificate is trusted through the test CA.
registry_patch() {  # ip
  cat > "$OUT/registry-patch.yaml" <<EOF
apiVersion: v1alpha1
kind: StaticHostConfig
name: $1
hostnames:
  - ${REGISTRY_HOST%:*}
---
apiVersion: v1alpha1
kind: RegistryTLSConfig
name: $REGISTRY_HOST
ca: |
$(sed 's/^/  /' "$OUT/ca.crt")
EOF
}

bootstrap() {
  need talosctl kubectl virsh openssl flock helm curl sha256sum

  # One bootstrap at a time: two would each generate a config with new
  # secrets, and a node installed with one could not be reached with the
  # other.
  mkdir -p "$OUT"
  exec 9>"$OUT/.bootstrap.lock"
  flock 9

  log "waiting for the VMs to get IP addresses"
  wait_for 300 "a DHCP lease for $VM_NAME" has_ip "$VM_NAME"
  wait_for 300 "a DHCP lease for $WORKER_NAME" has_ip "$WORKER_NAME"
  local ip wip
  ip=$(vm_ip "$VM_NAME")
  wip=$(vm_ip "$WORKER_NAME")
  log "control plane: $ip, worker: $wip"

  if [[ -f $TALOSCONFIG ]] && talosctl -n "$ip" -e "$ip" version >/dev/null 2>&1; then
    log "Talos is already configured"
  else
    mkdir -p "$OUT"
    make_ca
    registry_patch "$ip"
    log "generating Talos config"
    talosctl gen config "$CLUSTER_NAME" "https://$ip:6443" \
      --talos-version "$TALOS_VERSION" \
      --install-disk /dev/vda \
      --config-patch-control-plane @"$HERE/patch.yaml" \
      --config-patch-worker @"$HERE/worker-patch.yaml" \
      --config-patch @"$OUT/registry-patch.yaml" \
      --config-patch @"$HERE/cni-patch.yaml" \
      --additional-sans "$ip" \
      --output "$OUT" --force

    log "waiting for Talos maintenance mode on both nodes"
    wait_for 300 "Talos API on $ip" \
      talosctl -n "$ip" -e "$ip" get disks --insecure
    wait_for 300 "Talos API on $wip" \
      talosctl -n "$wip" -e "$wip" get disks --insecure

    log "applying config; Talos installs to disk and reboots"
    talosctl apply-config --insecure -n "$ip" -e "$ip" --file "$OUT/controlplane.yaml"
    talosctl apply-config --insecure -n "$wip" -e "$wip" --file "$OUT/worker.yaml"

    talosctl config endpoint "$ip"
    talosctl config node "$ip"

    log "waiting for Talos to install and accept bootstrap"
    wait_for 900 "etcd bootstrap" try_bootstrap
  fi

  talosctl kubeconfig "$OUT/kubeconfig" --force --merge=false
  log "waiting for the API server"
  wait_for 600 "the API server" kubectl --kubeconfig "$OUT/kubeconfig" get --raw /readyz
  install_cilium

  log "waiting for Kubernetes on both nodes"
  talosctl health --wait-timeout 10m --control-plane-nodes "$ip" --worker-nodes "$wip"

  log "cluster ready"
  KUBECONFIG="$OUT/kubeconfig" kubectl get nodes -o wide >&2
}

clean() {
  rm -rf "$OUT"
}

case "${1:-}" in
  fetch-iso) fetch_iso ;;
  bootstrap) bootstrap ;;
  ip) if [[ ${2:-} == worker ]]; then vm_ip "$WORKER_NAME"; else vm_ip "$VM_NAME"; fi ;;
  clean) clean ;;
  *) die "usage: $0 fetch-iso|bootstrap|ip [worker]|clean" ;;
esac
