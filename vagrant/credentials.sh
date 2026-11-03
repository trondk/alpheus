#!/bin/bash
# Prints the admin credentials of the Alpheus test cluster and how to
# connect to it. Runs after `vagrant up`, and can be run by hand at any time.
#
# Usage: ./credentials.sh          everything, then kubectl get nodes
#        ./credentials.sh --hide   only the first 4 characters of each secret
#                                  (for screen sharing)
#        source ./credentials.sh   only export KUBECONFIG and TALOSCONFIG in
#                                  your own shell
#
# Sources: _out/ (made by talos.sh bootstrap, kept on this machine). Nothing
# here is stored anywhere else; never paste the output into git or a chat.

# Sourced: set the variables in the caller's shell and stop.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  export KUBECONFIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_out/kubeconfig"
  export TALOSCONFIG="${KUBECONFIG%/kubeconfig}/talosconfig"
  echo "KUBECONFIG and TALOSCONFIG set for the Alpheus test cluster"
  return 0
fi

set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUT="$ROOT/_out"
HIDE=0
[[ "${1:-}" == "--hide" ]] && HIDE=1

export KUBECONFIG="$OUT/kubeconfig" TALOSCONFIG="$OUT/talosconfig"

CP_IP="$("$ROOT/talos.sh" ip 2>/dev/null)"
CP_IP="${CP_IP:-<VM IP>}"
WORKER_IP="$("$ROOT/talos.sh" ip worker 2>/dev/null)"
WORKER_IP="${WORKER_IP:-<not running>}"

show() {  # value
  if [[ -z "$1" ]]; then echo "(not found)"
  elif [[ "$HIDE" == 1 ]]; then echo "${1:0:4}…  (${#1} characters)"
  else echo "$1"
  fi
}
file_status() {  # file
  if [[ -f "$1" ]]; then echo "$1"
  else echo "$1   (missing: run vagrant up)"
  fi
}

from_env() {  # file key
  [[ -f "$1" ]] && sed -n -E "s/^$2=//p" "$1"
}
from_secret() {  # namespace secret key
  kubectl -n "$1" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d 2>/dev/null
}
from_garage_key() {  # key field ("Key ID" or "Secret key")
  kubectl -n storage exec deploy/garage -c garage -- /garage key info --show-secret "$1" 2>/dev/null |
    sed -n -E "s/^$2:[[:space:]]+//p"
}

CLUSTER=1
kubectl version --request-timeout=5s >/dev/null 2>&1 || CLUSTER=0

GARAGE="$OUT/secrets.env"
REGISTRY="registry.alpheus.test:30500"

# alpheus ui in the cluster, which make deploy and make e2e install (GO-143).
UI_STATUS="not installed: make deploy"
if [[ "$CLUSTER" == 1 ]]; then
  ready="$(kubectl -n alpheus-system get deployment alpheus-ui -o jsonpath='{.status.readyReplicas}' 2>/dev/null)"
  if [[ -n "$ready" && "$ready" != 0 ]]; then UI_STATUS="running"
  elif kubectl -n alpheus-system get deployment alpheus-ui >/dev/null 2>&1; then UI_STATUS="installed, not ready"
  fi
fi

# The cluster admin's bearer token is not stored; the kubeconfig uses a client
# certificate. Show a fingerprint of it so two kubeconfigs can be compared.
CLIENT_CERT="$(kubectl config view --raw -o jsonpath='{.users[0].user.client-certificate-data}' 2>/dev/null \
  | base64 -d 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null | sed 's/^.*=//')"

cat <<EOF
Alpheus test cluster credentials$([[ "$HIDE" == 1 ]] && echo " (hidden)")$([[ "$CLUSTER" == 0 ]] && printf '\n%s' "The cluster is not reachable: is the VM running (vagrant status)?")

VMs:            alpheus-talos (control plane, runs workloads too), IP $CP_IP
                alpheus-talos-worker (worker), IP $WORKER_IP
                (libvirt, qemu:///system)
                console: virsh -c qemu:///system console alpheus-talos[-worker]

Kubernetes and Talos: kubectl and talosctl (admin, client certificates inside)
  kubeconfig:   $(file_status "$OUT/kubeconfig")
  talosconfig:  $(file_status "$OUT/talosconfig")
  client cert:  $(show "$CLIENT_CERT")
  API server:   https://$CP_IP:6443   (Talos API: $CP_IP:50000)
  Without one of these, kubectl tries localhost:8080 and fails.
  In this terminal only:
    source $ROOT/credentials.sh
    (same as: export KUBECONFIG=$OUT/kubeconfig TALOSCONFIG=$OUT/talosconfig)
  For every terminal (links, so a rebuild's new certificates are used at once):
    mkdir -p ~/.kube ~/.talos
    for f in ~/.kube/config ~/.talos/config; do [ -f "\$f" ] && [ ! -L "\$f" ] && mv "\$f" "\$f.before-alpheus"; done
    ln -sf $OUT/kubeconfig ~/.kube/config
    ln -sf $OUT/talosconfig ~/.talos/config
  When your shell has HTTPS_PROXY set, kubectl must not use it for the VM:
    export NO_PROXY=$CP_IP no_proxy=$CP_IP
  Check:        kubectl get nodes      talosctl -n $CP_IP,$WORKER_IP version
  Dashboard:    talosctl -n $CP_IP dashboard
  After vagrant destroy the files are gone; vagrant up makes new ones in the
  same place.

Test CA (every web service below is HTTPS through the gateway, with its certificates)
  certificate:  $(file_status "$OUT/ca.crt")   (import it in your browser)
  private key:  $OUT/ca.key   (never share it)
  Registry name: images are named $REGISTRY/..., so flow files do not
  depend on the VM's IP. The node resolves it itself; on this machine add
    $CP_IP ${REGISTRY%:*}
  to /etc/hosts (it changes when the VM gets a new IP), and for podman:
    sudo mkdir -p /etc/containers/certs.d/$REGISTRY
    sudo cp $OUT/ca.crt /etc/containers/certs.d/$REGISTRY/ca.crt

Garage (storage)                               from _out/secrets.env
  dashboard:    https://$CP_IP:30901   log in with the admin token
  admin token:  $(show "$(from_env "$GARAGE" GARAGE_ADMIN_TOKEN)")
  S3:           https://$CP_IP:30900   region garage, bucket alpheus-data
                inside the cluster: http://garage.storage.svc.cluster.local:3900
  access key:   $(show "$(from_env "$GARAGE" GARAGE_DEFAULT_ACCESS_KEY)")
  secret key:   $(show "$(from_env "$GARAGE" GARAGE_DEFAULT_SECRET_KEY)")
                the same key is the Secret alpheus-s3 in namespace default
                (spec.storage.secretRef for test runs)
  rpc secret:   $(show "$(from_env "$GARAGE" GARAGE_RPC_SECRET)")   (Garage-internal)
  results:      bucket alpheus-results, which the key above writes and a
                key of its own, results-reader, can only read (GO-129):
    access key: $(show "$(from_garage_key results-reader "Key ID")")
    secret key: $(show "$(from_garage_key results-reader "Secret key")")

Alpheus                                        make deploy installs it
  ui:           http://$CP_IP:30717/   ($UI_STATUS)
                the runs of every namespace, their logs, failures and
                replay; no login (the VM's network only)
  controller:   kubectl -n alpheus-system get deploy,pods

Zot (registry)                                 Garage key from the cluster
  web UI:       https://$REGISTRY   no login (this machine only, README.md)
  push:         podman push <image> $REGISTRY/<name>:<tag>
  its own S3 key for bucket alpheus-registry (Zot-internal):
    access key: $(show "$(from_secret registry zot-s3 AWS_ACCESS_KEY_ID)")
    secret key: $(show "$(from_secret registry zot-s3 AWS_SECRET_ACCESS_KEY)")
EOF

[[ "$CLUSTER" == 1 ]] || exit 1

echo
echo "\$ kubectl get nodes"
kubectl get nodes
