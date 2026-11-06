# Vagrant setup

A two-node Kubernetes cluster on Talos Linux in two local VMs, used for
the end-to-end tests:

| VM | Role | Default size |
|---|---|---|
| `alpheus-talos` | control plane, also runs workloads; the test services and Alpheus are pinned here | 4 CPUs, 8 GiB, 40 GB (`ALPHEUS_VM_CPUS`, `_MEMORY`, `_DISK`) |
| `alpheus-talos-worker` | worker | 2 CPUs, 4 GiB, 20 GB (`ALPHEUS_WORKER_CPUS`, `_MEMORY`, `_DISK`) |

Runs span both nodes, and testcase16 and testcase17 need the worker: one
runs a flow across the two nodes, the other powers the worker off in the
middle of a run and on again.

## Prerequisites

1. Vagrant with the libvirt plugin. On Fedora: `vagrant plugin install vagrant-libvirt`
2. libvirt/KVM, with your user in the `libvirt` group
3. `talosctl` (same minor version as `TALOS_VERSION`), `kubectl` and `helm`
4. Run `deploy.sh` in this folder

## Usage

```sh
./deploy.sh                 # about 5 minutes the first time
./credentials.sh            # credentials and kubectl get nodes
source ./credentials.sh     # only set KUBECONFIG and TALOSCONFIG in your shell
./destroy.sh                # remove the VMs and _out/
./destroy.sh --clean        # also the test reports, benchmarks and logs
```

`deploy.sh` runs `vagrant up` with the libvirt provider. It downloads
the Talos ISO to `.cache/`, boots both VMs from it, and runs
`talos.sh bootstrap` once both are up. That generates the Talos config in
`_out/`, installs Talos to both disks, bootstraps Kubernetes on the control
plane, joins the worker, writes the kubeconfig and installs the network. At the end
`credentials.sh` prints where the kubeconfig and talosconfig are and how
to use them. Running `deploy.sh` again on a running cluster only checks
health and refreshes the kubeconfig.

`destroy.sh` removes the VMs and this folder's `_out/`, with every key
and certificate. What the tests left in the repository's `_out/` stays:
the reports, the benchmarks and the logs, so the next `make e2e` serves
the earlier reports again on the new cluster. `destroy.sh --clean` removes
those too, so the next deploy starts with no history. It keeps
`_out/release/`, `_out/vulndb/` and the ISO in `.cache/`.

The network is Cilium 1.20.3, not Talos' default flannel
(`cni-patch.yaml`), because flannel ignores NetworkPolicies and every run
gets one (SPEC 6.1). `talos.sh` downloads the chart to `.cache/`, checks
its SHA-256 and installs it with `services/cilium/values.yaml` as soon as
the API server answers; until then the nodes are not ready. A cluster
from before this change still has flannel, which does not enforce the
policies; `destroy.sh` and `deploy.sh` give it Cilium.

The VMs get their IPs by DHCP on the Vagrant management network.
`./talos.sh ip` prints the control plane's, which the services below are
reached by, and `./talos.sh ip worker` the worker's.

## Services

After Kubernetes, `services.sh` installs the same services and versions
MCPipe uses (manifests in `services/`):

| Service | From this machine | Inside the cluster |
|---|---|---|
| Garage S3 v2.4.1 | `https://<VM IP>:30900`, buckets `alpheus-data` (chunks), `alpheus-results` (results) and `alpheus-quota` (20 MiB quota, for testcase12) | `http://garage.storage.svc.cluster.local:3900` |
| garage-ui v0.13.0 | `https://<VM IP>:30901` | |
| Zot v2.1.22 | `https://registry.alpheus.test:30500` | `http://zot.registry.svc.cluster.local:5000` |

`make deploy` (or `make e2e`) installs Alpheus itself with the Helm chart,
and with it `alpheus ui` running in the cluster, which stays up between
test runs:

| Service | From this machine |
|---|---|
| alpheus ui | `http://<VM IP>:30717/`, the runs of every namespace, with their logs, failures and replay |

The UI has no login, like the rest of this test cluster; it is on the VM's
private network only.

HTTPS goes through HAProxy with a certificate from a test CA that
`talos.sh` makes in `_out/ca.crt`. Talos trusts the CA and resolves
`registry.alpheus.test` to the VM itself, so flow files name images
`registry.alpheus.test:30500/...` whatever the VM's IP is.
`credentials.sh` prints the keys, the `/etc/hosts` line and the podman
setup. Every `deploy.sh` on a new VM makes new keys and a new CA.

`vagrant ssh` and `vagrant provision` do not work, because Talos has no
SSH. Use `talosctl` instead, for example `talosctl dashboard`.

## Security

The test cluster is for this machine only. The registry (Zot) has no
login, and the credentials are printed in plain text. That is acceptable
only because the VM sits on libvirt's host-only/NAT management network
and cannot be reached from other machines. Do not attach the VM to a
bridged or public network, and do not forward its ports.

## Settings

| Variable | Default |
|---|---|
| `TALOS_VERSION` | `v1.14.2` |
| `ALPHEUS_VM_CPUS` | `4` |
| `ALPHEUS_VM_MEMORY` | `8192` (MiB) |
| `ALPHEUS_VM_DISK` | `40G` |
