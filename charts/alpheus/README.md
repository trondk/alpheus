# Alpheus Helm chart

Installs Alpheus: the `StreamRun` CRD, the controller with its signal
hub, and the RBAC they need. One release per cluster, since the CRD and the
ClusterRole are cluster-wide.

```console
$ helm install alpheus oci://registry.alpheus-dev.org/alpheus-dev/charts/alpheus \
    --namespace alpheus-system --create-namespace
```

Nothing is published before `v0.1.0`; until then install from this folder
with your own images, as https://alpheus-dev.org/docs/getting-started/
shows.

## Values

| Value | Default | What it does |
|---|---|---|
| `registry` | `""` (registry.alpheus-dev.org) | Where images are pulled from. Set it to a mirror, such as Zot in an air-gapped cluster; the repositories keep their paths there. |
| `imagePullSecrets` | `[]` | Secrets for pulling the controller's image. A private registry also needs a pull secret in each run's namespace. |
| `controller.image.repository` | `alpheus-dev/controller` | The controller's repository in the registry |
| `controller.image.tag` | the chart's appVersion | Used when no digest is set |
| `controller.image.digest` | `""` | `sha256:…`; used instead of the tag. A released chart has it set. |
| `controller.image.pullPolicy` | `IfNotPresent` | |
| `controller.replicas` | `1` | More than one gives a standby; only the leader reconciles and runs the hub. |
| `controller.resources` | 100m CPU, 128Mi requested; 512Mi limit | |
| `controller.allowTags` | `false` | Allows component images without a digest, and the agent's image by tag. For development only. Without it, the chart installs the ValidatingAdmissionPolicy `alpheus-image-digest`, so the API server rejects a run whose image has no digest. |
| `controller.nodeSelector`, `tolerations`, `affinity` | empty | Where the controller runs |
| `agent.image.repository` | `alpheus-dev/agent` | The agent's repository in the registry |
| `agent.image.tag` | the chart's appVersion | Only with `controller.allowTags` |
| `agent.image.digest` | `""` | Required unless `controller.allowTags` is set: the agent runs next to every component and is pinned by digest. |
| `signalHub.enabled` | `true` | Without the hub, agents only poll the manifests: slower, but correct. |
| `signalHub.port` | `7070` | |
| `networkPolicy.enabled` | `true` | Each run gets a NetworkPolicy: its pods may reach DNS, the storage and the signal hub, and only the agent's metrics port can be reached. Needs a network plugin that enforces NetworkPolicies; flannel does not. |
| `networkPolicy.storagePorts` | `[]` | The ports the storage is reached on. Empty means the port in each run's endpoint; set it when the storage is a Service whose port differs from its target port. |
| `networkPolicy.storageTo` | `[]` | Where the storage is, as NetworkPolicy peers (`namespaceSelector`, `ipBlock`). Empty means any address on the storage ports. |
| `crds.install` | `true` | Installs and upgrades the CRD. The CRD is kept when the release is uninstalled, since deleting it deletes every StreamRun. |

The CRD and the ClusterRole in `files/` are copies of the generated files
in `config/`, kept up to date by `make generate`. `make test-chart` checks
what the chart renders.
