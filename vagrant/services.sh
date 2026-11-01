#!/bin/bash
# Installs the services Alpheus is tested with, the same ones and versions
# as MCPipe uses: local-path (disks), Garage (S3) with its dashboard, Zot
# (container registry) and HAProxy (HTTPS on ports 30500, 30900, 30901).
# Runs after talos.sh bootstrap; safe to run again.
#
# The first run on a new cluster creates _out/secrets.env with Garage's keys
# and the gateway's certificate from the test CA (_out/ca.crt). Nothing is
# kept outside _out/, and destroy.sh removes it.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/_out"
SERVICES="$HERE/services"
REGISTRY_NAME="registry.alpheus.test"
DATA_BUCKET="alpheus-data"
RESULTS_BUCKET="alpheus-results"
RESULTS_KEY="results-reader"
QUOTA_BUCKET="alpheus-quota"
REGISTRY_BUCKET="alpheus-registry"
REGISTRY_KEY="zot-registry"

export KUBECONFIG="$OUT/kubeconfig"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

[[ -f $KUBECONFIG ]] || die "no kubeconfig in $OUT: run talos.sh bootstrap first"
[[ -f $OUT/ca.crt && -f $OUT/ca.key ]] || die "no test CA in $OUT: run talos.sh bootstrap first"

# apply_secret <yaml> <name>: only applied when it differs, because kubectl
# apply always says "configured" for a Secret.
apply_secret() {
  if kubectl diff -f - <<<"$1" >/dev/null 2>&1; then
    echo "secret/$2 unchanged"
  else
    kubectl apply -f - <<<"$1"
  fi
}

garage() {
  kubectl -n storage exec deploy/garage -c garage -- /garage "$@" 2>/dev/null
}

secrets() {
  local file=$OUT/secrets.env
  [[ -f $file ]] && return
  log "creating Garage keys in $file"
  ( umask 077
    cat > "$file" <<EOF
GARAGE_RPC_SECRET=$(openssl rand -hex 32)
GARAGE_ADMIN_TOKEN=$(openssl rand -hex 32)
GARAGE_DEFAULT_ACCESS_KEY=GK$(openssl rand -hex 12)
GARAGE_DEFAULT_SECRET_KEY=$(openssl rand -hex 32)
EOF
  )
}

# The gateway's certificate, for the VM's IP and the registry name. Made
# again when the IP changes.
gateway_cert() {
  local ip sans
  ip=$("$HERE/talos.sh" ip)
  [[ -n $ip ]] || die "cannot find the VM's IP"
  sans="DNS:$REGISTRY_NAME,DNS:localhost,IP:127.0.0.1,IP:$ip"
  if [[ -f $OUT/gateway.crt && "$(cat "$OUT/gateway.sans" 2>/dev/null)" == "$sans" ]]; then
    return
  fi
  log "issuing the gateway certificate for $sans"
  ( umask 077
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
      -keyout "$OUT/gateway.key" -out "$OUT/gateway.csr" \
      -subj "/O=Alpheus test/CN=gateway" 2>/dev/null
    openssl x509 -req -in "$OUT/gateway.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
      -CAcreateserial -CAserial "$OUT/ca.srl" -out "$OUT/gateway.crt" -days 825 \
      -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature\n' "$sans") 2>/dev/null
    rm -f "$OUT/gateway.csr"
    echo "$sans" > "$OUT/gateway.sans" )
}

install_local_path() {
  log "local-path (disks)"
  kubectl apply -k "$SERVICES/local-path"
  kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=5m
}

install_storage() {
  log "Garage (S3) and its dashboard"
  secrets
  kubectl apply -f "$SERVICES/storage/namespace.yaml"
  apply_secret "$(kubectl -n storage create secret generic garage-secrets \
    --from-env-file="$OUT/secrets.env" --dry-run=client -o yaml)" garage-secrets
  kubectl apply -k "$SERVICES/storage"
  kubectl -n storage rollout status deployment/garage --timeout=5m
  kubectl -n storage rollout status deployment/garage-ui --timeout=5m
  garage bucket info "$DATA_BUCKET" | grep -E '^(Bucket|Global alias)' || die "bucket $DATA_BUCKET is missing"

  # The same key for test runs, as the Secret their StreamRuns name in
  # spec.storage.secretRef (namespace default).
  # shellcheck disable=SC1091
  source "$OUT/secrets.env"
  apply_secret "$(kubectl -n default create secret generic alpheus-s3 \
    --from-literal=AWS_ACCESS_KEY_ID="$GARAGE_DEFAULT_ACCESS_KEY" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$GARAGE_DEFAULT_SECRET_KEY" \
    --dry-run=client -o yaml)" alpheus-s3

  # A bucket for results, which the agents' key writes and a key of its
  # own can only read (GO-129). hack/e2e.sh reads that key from Garage.
  garage bucket info "$RESULTS_BUCKET" >/dev/null || garage bucket create "$RESULTS_BUCKET" >/dev/null
  garage bucket allow --read --write --key "$GARAGE_DEFAULT_ACCESS_KEY" "$RESULTS_BUCKET" >/dev/null
  garage key info "$RESULTS_KEY" >/dev/null || garage key create "$RESULTS_KEY" >/dev/null
  garage bucket allow --read --key "$RESULTS_KEY" "$RESULTS_BUCKET" >/dev/null

  # A small bucket that fills up, for testcase12: a run must fail cleanly
  # when the storage refuses a write.
  garage bucket info "$QUOTA_BUCKET" >/dev/null || garage bucket create "$QUOTA_BUCKET" >/dev/null
  garage bucket allow --read --write --key "$GARAGE_DEFAULT_ACCESS_KEY" "$QUOTA_BUCKET" >/dev/null
  garage bucket set-quotas --max-size 20MiB "$QUOTA_BUCKET" >/dev/null
}

# Zot's bucket and key live in Garage. The key is read back from Garage on
# every run, so it is not kept anywhere else.
install_registry() {
  log "Zot (registry)"
  garage bucket info "$REGISTRY_BUCKET" >/dev/null || garage bucket create "$REGISTRY_BUCKET" >/dev/null
  garage key info "$REGISTRY_KEY" >/dev/null || garage key create "$REGISTRY_KEY" >/dev/null
  garage bucket allow --read --write --owner "$REGISTRY_BUCKET" --key "$REGISTRY_KEY" >/dev/null

  local info key_id secret_key
  info=$(garage key info --show-secret "$REGISTRY_KEY")
  key_id=$(sed -n -E 's/^Key ID:[[:space:]]+//p' <<<"$info")
  secret_key=$(sed -n -E 's/^Secret key:[[:space:]]+//p' <<<"$info")
  [[ -n $key_id && -n $secret_key ]] || die "could not read key $REGISTRY_KEY from Garage"

  kubectl apply -f "$SERVICES/registry/namespace.yaml"
  apply_secret "$(kubectl -n registry create secret generic zot-s3 \
    --from-literal=AWS_ACCESS_KEY_ID="$key_id" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$secret_key" \
    --dry-run=client -o yaml)" zot-s3
  kubectl apply -k "$SERVICES/registry"
  kubectl -n registry rollout status deployment/zot --timeout=10m
}

install_gateway() {
  log "HAProxy (HTTPS on 30500, 30900, 30901)"
  gateway_cert
  kubectl apply -f "$SERVICES/gateway/namespace.yaml"
  # HAProxy wants certificate and key in one file.
  apply_secret "$(kubectl -n gateway create secret generic gateway-tls \
    --from-file=gateway.pem=<(cat "$OUT/gateway.crt" "$OUT/gateway.key") \
    --dry-run=client -o yaml)" gateway-tls
  kubectl apply -k "$SERVICES/gateway"
  # A new certificate needs a restart (HAProxy reads it at start).
  local sha
  sha=$(sha256sum "$OUT/gateway.crt" | cut -c1-16)
  if [[ "$(kubectl -n gateway get deploy haproxy -o jsonpath='{.spec.template.metadata.annotations.alpheus-dev\.org/tls-sha}')" != "$sha" ]]; then
    kubectl -n gateway patch deploy haproxy --type=merge \
      -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"alpheus-dev.org/tls-sha\":\"$sha\"}}}}}" >/dev/null
  fi
  kubectl -n gateway rollout status deployment/haproxy --timeout=5m
}

install_local_path
install_storage
install_registry
install_gateway
log "services ready"
