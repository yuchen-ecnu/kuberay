# Federation Lab

Open `/federation` to edit a FederatedRayCluster (FRC) and its primary
RayCluster (PRC), inspect member RayClusters (MRC), and view their actual Pods in a
[React Flow](https://reactflow.dev/) topology. Existing groups preview the live
PRC targets in both scaling modes; FRC replicas seed new groups only.
Nodes can be dragged, and the canvas supports zooming, panning and layout reset.
Federation connectivity probe Pods are omitted because they are not Ray nodes.

The toolbar provides status, refresh, Terminal and Ray Dashboard. The demo UI
and its diagnostics are in English; Kubernetes resource names and external
diagnostics retain their original text.

FRC and RayCluster YAML previews highlight the nested autoscaler settings,
worker-group management and head addresses. Field labels link to the corresponding lines. These visual
annotations do not change copied or downloaded YAML.

## Setup

Requirements: Node.js 22, Go (the version required by `ray-operator/go.mod`),
Python 3, Docker, kind, kubectl, OpenSSL and ripgrep (`rg`). Use a machine where
two kind clusters can route between their Pod and Service networks. The demo is
intended for dedicated test clusters.

From a fresh checkout, prepare the clusters, install matching CRDs and operators,
and run the federation tests. The runner retains both clusters and writes their
kubeconfigs to `STATE_DIR`:

```bash
export STATE_DIR=/tmp/kuberay-federation-kind
export KIND_PREFIX=frc
export RAY_IMAGE=rayproject/ray:2.56.0-py311-cpu
./ray-operator/test/federation/run-kind.sh
```

Then, from the repository root, start the demo with one command:

```bash
./dashboard/demo/federation/run.sh \
  --primary-kubeconfig "$STATE_DIR/primary.kubeconfig" \
  --member-kubeconfig "$STATE_DIR/member.kubeconfig" \
  --state-dir /tmp/kuberay-federation-demo \
  --image "$RAY_IMAGE"
```

The script creates or refreshes demo resources, installs dashboard dependencies,
builds the production app, and starts the custom WebSocket server. Keep the process
running. Open <http://127.0.0.1:3000/federation>; the full-page terminal is at
<http://127.0.0.1:3000/terminal>. Re-running the script refreshes the member
credential and preserves an existing FRC spec.

To use an existing pair of kind clusters, skip `run-kind.sh` and pass their
absolute kubeconfig paths to `run.sh`. Both operators need `RayFederation=true`,
the same federation CRDs, a working FRC validating webhook, and a route between
the Pod and Service CIDRs. The Ray image must be available in both clusters.
Use `--member-node` if the member kind node has a different container name, and
`--head-address` if the primary Service CIDR does not include `10.96.0.110`.

The setup script creates:

- Namespace `ray-federation-demo` and FRC `ray-demo`.
- One head and one local worker in the primary cluster, and two member workers.
- A head Service at `10.96.0.110`, within the test runner's primary Service CIDR.
- A member ServiceAccount, Role and RoleBinding, plus its credential Secret in
  the primary cluster.
- A ClusterRole allowing that ServiceAccount to read the `kube-system` Namespace
  UID for member identity verification.
- A `federation-terminal` Deployment in each cluster, with its own ServiceAccount
  scoped to the demo namespace.

`setup.py` also accepts `--namespace` and `--name` when invoked directly.
The default member node is `frc-member-control-plane`.
Rerunning setup preserves an existing FRC spec and refreshes its credentials.
The requested token lifetime is 24 hours; Kubernetes determines the actual
expiration. Kubeconfig paths and credentials stay on the server.

## Walkthrough

1. Drag nodes, zoom the canvas, and use **Reset layout**.
   Click a control-plane edge to inspect the relationship.
2. Select **Primary RayCluster**. Remote worker groups use
   `managedBy: ray.io/federated-raycluster-controller`.
3. Select **Member RayCluster**. It is a regular RayCluster with no
   `headGroupSpec`; workers connect through `rayStartParams.address`.
4. Edit an existing managed group's `replicas` in the PRC; the graph previews
   the extra Pod. Validate and apply the PRC draft.
5. Wait for the member worker. FRC status and the graph show the PRC target
   and actual Pods. Click a Pod to inspect its node, IP, image and resources.
6. Change the PRC target back to observe scale-down. Editing the existing
   group's FRC `replicas` changes its seed value, not its live target.
7. The default demo shows `spec.primaryCluster.enableInTreeAutoscaling: false`.
   For an autoscaled FRC, start with the separate
   [autoscaling sample](../../../ray-operator/config/samples/ray-federation-autoscaling.yaml).
   Its `spec.primaryCluster.autoscalerOptions` configures the single autoscaler;
   Ray writes runtime replicas to PRC; manual PRC edits may be superseded by
   the next autoscaler decision.

YAML supports formatting, folding, highlighting and downloads. Polling preserves
unsaved drafts; concurrent spec changes prompt a reload or merge. Pod detail
panels show the snapshot captured when opened. The topology refreshes every five
seconds while preserving dragged positions and the viewport. Dashed preview nodes
have no Pod identity and are excluded from running counts.

## kubectl terminal

The terminal supports shell pipelines, redirection, variables, scripts, long-running
commands and Bash history. `kubectl` and its `k` alias use the selected cluster and
demo namespace. Switching clusters starts a shell in the corresponding utility
Pod. The drawer can expand to fullscreen or open the `/terminal` page.

The WebSocket service runs `kubectl exec -it` against the `federation-terminal`
Deployment. Its ServiceAccount is scoped to the demo namespace; `/workspace` and
shell history persist for the lifetime of the Pod. Setup mounts the kind node's
kubectl binary read-only. Override its node path with
`--terminal-kubectl-host-path`.

## Ray Dashboard

Select **Ray Dashboard** to open `/ray-dashboard/` in a new tab, or visit
<http://127.0.0.1:3000/ray-dashboard/>. This is the native Dashboard on the primary
head. Its Cluster page includes local and remote workers because the PRC and MRCs
form one Ray runtime.

On first access, the server starts a loopback-only `kubectl port-forward` and
proxies Dashboard assets, API requests and HTTP log streams. Concurrent requests
share the connection. A head replacement retires it; subsequent requests retry
closed connections. The target is the configured primary RayCluster, using its
`rayStartParams.dashboard-port` or port `8265` by default.
Keep the trailing slash so relative assets resolve correctly; see
[Ray's reverse-proxy guidance](https://docs.ray.io/en/latest/cluster/configure-manage-dashboard.html#running-behind-a-reverse-proxy).

The primary kubeconfig needs `create` permission for `pods/portforward` in the demo
namespace. HTTP methods and bodies are forwarded to Ray Dashboard. This demo does
not deploy Prometheus or Grafana, so their metric panels are unavailable; nodes,
jobs and logs use Ray's own APIs.

## Server configuration

`FEDERATION_DEMO_CONFIG` points to a JSON file containing one primary and one or
more members:

```json
{
  "name": "ray-demo",
  "namespace": "ray-federation-demo",
  "clusters": [
    {
      "id": "primary",
      "label": "frc-primary",
      "role": "primary",
      "namespace": "ray-federation-demo",
      "kubeconfig": "/absolute/path/primary.kubeconfig",
      "terminalTarget": "deployment/federation-terminal",
      "terminalContainer": "shell"
    },
    {
      "id": "member-b",
      "label": "frc-member",
      "role": "member",
      "memberName": "member-b",
      "namespace": "ray-federation-demo",
      "kubeconfig": "/absolute/path/member.kubeconfig",
      "terminalTarget": "deployment/federation-terminal",
      "terminalContainer": "shell"
    }
  ]
}
```

Each entry can specify `context`; otherwise it uses the kubeconfig's
current-context. `terminalTarget` accepts a Pod or Deployment name and defaults to
`deployment/federation-terminal`. Restart the server after changing this file.

`memberName` maps an observed cluster to an FRC member. Members absent from this
registry can still be submitted to Kubernetes, but have no live RayCluster or Pod
view. Manual members can be observed with server-side read-only credentials;
the FRC itself still does not access them.

For managed members, the server reads the actual RayCluster name from FRC status
and queries Pods with `ray.io/cluster=<actual-name>`. This supports retained member
names and multiple members in one namespace. The registry controls observation
sources, not which members the FRC can declare.

The server forwards editable FRC and PRC specs and metadata to Kubernetes,
adding UID and resourceVersion checks for concurrent updates. MRC YAML remains
read-only because managed MRCs are derived from the PRC. It validates YAML
transport and preview safety; Kubernetes CRD schemas, CEL rules and the installed
operator webhook determine resource validity. Structurally valid YAML that cannot
be previewed can still be submitted for server validation.

This demo has no login or Host/Origin restrictions. Connect it to dedicated demo
clusters. Use SSH forwarding for remote access, or bind with `HOSTNAME=0.0.0.0`.

### Workspace proxy paths

For a workspace proxy that strips a prefix such as `/workspace/proxy/3000/`,
pass that prefix to `run.sh`:

```bash
./dashboard/demo/federation/run.sh \
  --primary-kubeconfig "$STATE_DIR/primary.kubeconfig" \
  --member-kubeconfig "$STATE_DIR/member.kubeconfig" \
  --state-dir /tmp/kuberay-federation-demo \
  --image "$RAY_IMAGE" \
  --proxy-path /workspace/proxy/3000 \
  --bind 0.0.0.0
```

Open `https://workspace.example/workspace/proxy/3000/federation`. The script
sets `FEDERATION_DEMO_PROXY_PATH` for both build and runtime; these values must
match. A mismatched production build now fails at startup instead of serving an
unstyled page. YAML API and Ray Dashboard links retain the browser's prefix.
Changing the prefix requires a rebuild. Set
`FEDERATION_DEMO_PROXY_URL=https://workspace.example/workspace/proxy/3000` when
running browser tests to verify assets, validation and Dashboard redirects through
the proxy.

## Tests

```bash
cd dashboard
node .yarn/releases/yarn-4.9.2.cjs test:federation
# With the server running and setup.py resources ready:
node .yarn/releases/yarn-4.9.2.cjs exec playwright install chromium
node .yarn/releases/yarn-4.9.2.cjs test:federation:e2e
```

Run browser tests in a dedicated demo namespace: they apply FRC and PRC changes
and restore the original specs. Tests cover manual and autoscaled replica
ownership, topology, Kubernetes admission, draft preservation, terminals,
native Ray Dashboard assets and request forwarding. Set `FEDERATION_DEMO_URL`
for a different server or
`PLAYWRIGHT_CHROMIUM_EXECUTABLE` to use an existing Chromium binary. Screenshots
and failure traces are saved under `demo/federation/test-results/`, which is
excluded from Git.

Topology edges describe controller relationships. Readiness comes from observed
conditions. In the default demo, users edit PRC replicas and the federation
controller forwards managed-group targets to members. With autoscaling enabled,
Ray writes those PRC targets. FRC replicas initialize new groups in both modes.
Switching an existing runtime between these modes requires a new FRC.

## Cleanup

Delete the FRC in the primary cluster and wait for its finalizer to clean up the
managed members before removing both demo namespaces:

```bash
kubectl --kubeconfig /absolute/path/primary.kubeconfig -n ray-federation-demo \
  delete frc ray-demo --wait=true --timeout=180s
kubectl --kubeconfig /absolute/path/primary.kubeconfig delete namespace ray-federation-demo
kubectl --kubeconfig /absolute/path/member.kubeconfig delete namespace ray-federation-demo
```

The kind clusters and operators remain available after namespace cleanup.
