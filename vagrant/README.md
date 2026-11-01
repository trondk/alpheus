# Vagrant setup

A single-node Kubernetes cluster on Talos Linux in a local VM, used for
the end-to-end tests.

## Prerequisites

1. Vagrant with the libvirt plugin. On Fedora: `vagrant plugin install vagrant-libvirt`
2. libvirt/KVM, with your user in the `libvirt` group
3. `talosctl` (same minor version as `TALOS_VERSION`) and `kubectl`
4. Run `deploy.sh` in this folder

## Usage

```sh
./deploy.sh                 # about 5 minutes the first time
./credentials.sh            # credentials and kubectl get nodes
source ./credentials.sh     # only set KUBECONFIG and TALOSCONFIG in your shell
./destroy.sh                # remove the VM and _out/
```

`deploy.sh` runs `vagrant up` with the libvirt provider. It downloads
the Talos ISO to `.cache/`, boots it, and runs `talos.sh bootstrap`.
That generates the Talos config in `_out/`, installs Talos to the VM
disk, bootstraps Kubernetes and writes the kubeconfig. At the end
`credentials.sh` prints where the kubeconfig and talosconfig are and how
to use them. Running `deploy.sh` again on a running cluster only checks
health and refreshes the kubeconfig.

The VM gets its IP by DHCP on the Vagrant management network.
`./talos.sh ip` prints it.

## Services

After Kubernetes, `services.sh` installs the same services and versions
MCPipe uses (manifests in `services/`):

| Service | From this machine | Inside the cluster |
|---|---|---|
| Garage S3 v2.4.1 | `https://<VM IP>:30900`, buckets `alpheus-data` (chunks) and `alpheus-results` (results) | `http://garage.storage.svc.cluster.local:3900` |
| garage-ui v0.13.0 | `https://<VM IP>:30901` | |
| Zot v2.1.22 | `https://registry.alpheus.test:30500` | `http://zot.registry.svc.cluster.local:5000` |

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
