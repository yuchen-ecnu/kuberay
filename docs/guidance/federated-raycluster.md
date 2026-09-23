# Federated RayCluster

FederatedRayCluster runs one Ray runtime across a primary Kubernetes cluster and
member Kubernetes clusters. The primary owns the Ray head; members run workers
that connect to that head over a routed private network.

This feature is alpha and requires `--feature-gates=RayFederation=true` on the
primary and member operators. Install matching CRDs and operator versions.
`RayClusterStatusConditions=true` is required. See the [PoC contract and migration
guide](federated-raycluster-contract.md) before upgrading existing alpha resources.
Autoscaler settings are part of `spec.primaryCluster`. The PRC holds runtime
replica targets in both scaling modes; FRC values initialize new groups.

## Resources and responsibilities

Only **FederatedRayCluster** is a new CRD. Both primary and member resources use
`kind: RayCluster`. PRC and MRC are role abbreviations, not additional API kinds.

| Resource | Location | Responsibility |
| --- | --- | --- |
| FederatedRayCluster (FRC) | Primary | Configuration, credentials, member lifecycle and aggregate status |
| Primary RayCluster (PRC) | Primary | Head and local workers; remote groups carry `managedBy: ray.io/federated-raycluster-controller` |
| Member RayCluster (MRC) | Member | Local workers connected to the external head |

The existing RayCluster controller manages all business Pods. The federation
controller manages RayClusters and observes their owned workers. Member resources
do not have cross-cluster ownerReferences. Their local children use ownerReferences
to the member RayCluster.

RayCluster adds `workerGroupSpecs[].managedBy`, independently of the existing
cluster-level `spec.managedBy`:

```yaml
managedBy: ray.io/federated-raycluster-controller
```

| Worker group `managedBy` | Responsibility |
| --- | --- |
| Omitted or `ray.io/raycluster-controller` | KubeRay manages local worker Pods |
| `ray.io/federated-raycluster-controller` | The federation controller distributes the group to a member RayCluster |

The field accepts only these two controller names; empty strings and unknown
managers are rejected. Omission and explicit local management have identical
accounting and upgrade behavior. The local RayCluster controller excludes
delegated groups from Pod provisioning, capacity accounting and upgrade decisions.
Delegated groups require the federation autoscaler adapter described below;
the unmodified KubeRay autoscaler cannot observe remote Pods.

Unlike the immutable cluster-level `spec.managedBy`, the worker group field is
mutable. Switching from local to federation management drains worker Pods owned
by the current RayCluster. Switching back resumes local provisioning. Pods owned
by another controller are never adopted, counted or deleted, including during
suspension, group removal, rename and `workersToDelete` handling. Deletion uses
Pod UID preconditions to protect same-name replacements. The federation controller
propagates a delegated group's replica targets and `workersToDelete` to its member.
The field selects responsibility; it does not provide an atomic migration of a
running Ray workload. Coordinate member cleanup before returning a group to local
management.

The local delegation behavior needs no FRC reference, member name, cluster
reference or credential, and works independently of the `RayFederation` feature
gate. Setting the field alone does not create an FRC or provision remote workers.
FRC owns the topology and maps federation-wide unique `groupName` values to members.
In FRC input `workerGroups`, omit `managedBy` or use `ray.io/raycluster-controller`;
FRC assigns the manager when generating primary and member RayClusters. The
existing cluster-level `spec.managedBy` still delegates the entire RayCluster,
including its head, and cannot be used to delegate individual worker groups.
Its existing default controller name remains `ray.io/kuberay-operator`; the
worker group values identify controllers within the KubeRay operator.

Its existing `headGroupSpec` is now optional:

- With `headGroupSpec`, ordinary RayCluster behavior is preserved.
- Without it, the RayCluster maintains only workers. Every group must set the
  same literal `rayStartParams.address`, including the GCS port.
- An empty head object is invalid. Adding or removing the head on an existing
  RayCluster is rejected; create a new resource to change its role.
- RayJob and RayService still require a RayCluster with a head. A workers-only
  RayCluster cannot be selected as a RayJob's existing cluster.

The CRD enforces structural requirements and role transitions. Detailed endpoint
format, cross-group address consistency and feature-gate checks also run in the
webhook and controller. With webhooks disabled, invalid detailed configuration
produces an `InvalidRayClusterSpec` event and no workers are provisioned.

## Network prerequisites

Configure private, bidirectional routing between the head and all workers, and
between workers in different clusters. Exposing only the GCS port is insufficient
for task execution and object transfer. Configure DNS and NetworkPolicies too.

The initial implementation uses a platform-provided stable private endpoint:

```yaml
networking:
  headEndpoint:
    mode: UserProvided
    address: ray-head.internal.example
    gcsPort: 6379
```

The endpoint must route to the primary GCS. The operator does not provision a
private load balancer, VPN or cross-cluster DNS.

Network connectivity is a platform prerequisite. Configure `rayStartParams` and
network rules for the actual Ray ports, including worker-to-worker traffic. FRC
preserves the provided port settings and does not inject fixed port defaults or
reserve a control port. Ordinary Ray defaults apply when ports are omitted;
allow the dynamic ports or explicitly configure ports that match your network rules.

Members create and maintain workers from their local RayCluster spec. The normal
`wait-gcs-ready` init container waits for the configured head endpoint before
starting Ray. Pod readiness and `WorkersReady` describe observed worker capacity;
they do not certify every cross-cluster data path. Diagnose connectivity with
platform network tools and real Ray workloads.

Loss of the federation controller or its member credentials prevents new desired
state from being delivered or observed. The member operator keeps scaling and
repairing workers from the last received spec. One member's observation failure
does not freeze another member's local Pod lifecycle.

## Managed members

Start with [ray-federation.yaml](../../ray-operator/config/samples/ray-federation.yaml).
Set the private head endpoint, member namespaces and worker templates. A member
entry looks like this:

```yaml
memberClusters:
- name: member-b
  namespace: ray-federation
  kubeconfigSecretRef:
    name: member-b-kubeconfig
  workerGroups:
  - groupName: member-cpu
    replicas: 2
    # Full worker template is in the sample.
```

Create each target namespace first. Place the referenced Secret in the **FRC's
namespace on the primary cluster**, using key `kubeconfig`. An administrator must
approve it with `ray.io/federation-credential=true`:

```bash
kubectl --context primary -n ray-federation create secret generic member-b-kubeconfig \
  --from-file=kubeconfig=member-b.kubeconfig
kubectl --context primary -n ray-federation label secret member-b-kubeconfig \
  ray.io/federation-credential=true
```

Use verified HTTPS and embedded credentials/CA data. Executable credential
plugins, local file references, impersonation and proxy settings are rejected.
Restrict who can create or approve these Secrets. Credentials stay on the primary
and are refreshed when the Secret changes.

The member credential needs this namespace-scoped Role, bound to its identity:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: federation-client
  namespace: ray-federation
rules:
- apiGroups: [ray.io]
  resources: [rayclusters]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
```

It also needs `get` on the member cluster's `kube-system` Namespace to verify the
cluster identity. This does not require listing namespaces or accessing resources
inside `kube-system`. Bind the following ClusterRole to the same credential
identity. This example binding uses a `federation-client` ServiceAccount in
`ray-federation`; adjust the subject to match your credential:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: federation-cluster-identity
rules:
- apiGroups: [""]
  resources: [namespaces]
  resourceNames: [kube-system]
  verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: federation-cluster-identity
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: federation-cluster-identity
subjects:
- kind: ServiceAccount
  name: federation-client
  namespace: ray-federation
```

Read-only Pod access supports capacity observation. The federation controller
does not create worker Pods or need Pod exec permission.

Apply FRC to the primary. It creates a same-name primary RayCluster and a
`<frc-name>-<member-name>` RayCluster for each managed member. Multiple members may
share a Kubernetes cluster and namespace, including the primary's namespace.
Names longer than RayCluster's 53-character limit are shortened with a stable hash suffix.
If the readable name belongs to another FRC, a suffix derived from the source FRC UID
avoids collisions between FRCs from different primary namespaces.
The actual name is persisted in `status.memberClusterStatuses[].rayClusterName`
before remote writes; creation, observation and cleanup use that name. Existing
members retain their original names and worker identities when the operator is upgraded.
For example, members `member-b` and `member-c` of FRC `ray-federation` create
`ray-federation-member-b` and `ray-federation-member-c` in the same namespace.
Worker group names must still be unique across the federation.
The primary's remote
groups receive `managedBy: ray.io/federated-raycluster-controller` and do not create local Pods. FRC
matches each group to `memberClusters[].workerGroups` by `groupName`; target
cluster names and credentials remain in FRC.
The corresponding member groups omit `managedBy` and receive the shared
`networking.headEndpoint` as their existing `rayStartParams.address`.

## Manual members: optional kubeconfig

Omit `kubeconfigSecretRef` to manage a member yourself. Do not put its workerGroups
on FRC; the member RayCluster is their only source of desired state.

```yaml
spec:
  memberClusters:
  - name: member-b
    namespace: ray-federation
```

Use [ray-federation-manual.yaml](../../ray-operator/config/samples/ray-federation-manual.yaml)
and [ray-member-standalone.yaml](../../ray-operator/config/samples/ray-member-standalone.yaml).
The member is an ordinary RayCluster:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: ray-federation
  labels:
    ray.io/federation-member: member-b
spec:
  rayVersion: "2.56.0"
  enableInTreeAutoscaling: false
  workerGroupSpecs:
  - groupName: member-cpu
    replicas: 2
    minReplicas: 0
    maxReplicas: 10
    rayStartParams:
      address: ray-head.internal.example:6379
      num-cpus: "1"
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.56.0-py311-cpu
          resources:
            requests: {cpu: "1", memory: "1Gi"}
            limits: {cpu: "1", memory: "2Gi"}
```

The member identity label is optional and supplies the prefix for Ray cloud
instance IDs: `member-b/<pod-name>` in this example. Without the label, the
workers-only RayCluster UID is used as the prefix. Primary workers use plain Pod
names. Kubernetes Pod names themselves are unchanged.
Do not set `ray.io/federation-owner` on a manually managed member; that label
records ownership by a specific FRC for synchronization and cleanup.

The member operator maintains declared replicas and replaces failed Pods without
any dependency on the federation controller. Workers can be created before the head;
the GCS init container waits for the configured endpoint. Head loss alone does
not cause the operator to delete every existing worker.

There is no autoscaler in this mode. Users may change `replicas` manually;
`minReplicas` and `maxReplicas` remain the existing replica bounds. FRC does not
propagate scale targets or targeted deletions to manual members. An FRC with any
manual member cannot enable federation autoscaling.

The federation never contacts a manual member's Kubernetes API. Its FRC member status
contains only the `ManuallyManaged=True` condition, without health conditions, capacity
observations or an observation timestamp. Manual members are excluded from aggregate readiness:
FRC `Ready=True` requires the primary and managed members to be ready, and does not indicate
manual member health. With only manual members, readiness depends on the primary alone.
Inspect a manual member's local readiness with:

```bash
kubectl --context member-b -n ray-federation wait raycluster/ray-federation \
  --for=condition=WorkersReady --timeout=180s
```

Manual RayClusters use ordinary Ray port defaults unless you configure explicit
start parameters. Allow their dynamic ports or pin them to match your network
rules; FRC does not inject port settings into manual members.

Worker readiness and the GCS init check are not a full-mesh network test. A zero-replica member reports matching
local capacity without claiming that the external head is reachable.

## Federation autoscaling

Start with
[ray-federation-autoscaling.yaml](../../ray-operator/config/samples/ray-federation-autoscaling.yaml).
Enable autoscaling when creating the FRC:

```yaml
spec:
  primaryCluster:
    rayVersion: "2.56.0"
    enableInTreeAutoscaling: true
    autoscalerOptions:
      version: v2
      idleTimeoutSeconds: 60
      upscalingMode: Default
    # headGroupSpec and complete worker templates are in the sample.
    workerGroups:
    - groupName: primary-cpu
      replicas: 0
      minReplicas: 0
      maxReplicas: 2
      resources: {CPU: "1", primary: "1"}
```

Configure each managed member group's bounds and resources the same way. All
members must provide `kubeconfigSecretRef`. Groups can start at zero; Ray task,
actor and placement-group resource demand drives scale-up. The existing Ray v2
scheduler and instance manager choose capacity, enforce bounds and identify idle
workers for scale-down. Workers holding an actor's requested resources remain
busy. Custom resources such as `primary` and `member` can direct workloads to
specific groups. The adapter does not implement a separate scaling algorithm.

Exactly one autoscaler runs as a sidecar on the primary head. Its configuration
contains all local and managed member groups. The autoscaler updates the PRC's
existing `replicas` and `scaleStrategy.workersToDelete` fields. The federation
controller forwards remote targets to their MRCs, and each local RayCluster
controller manages its own Pods. MRCs remain workers-only with
`enableInTreeAutoscaling: false`. No additional RayCluster field or custom resource
kind is introduced for autoscaling.

With autoscaling enabled, scheduling bounds, group idle timeout and priority stay
on the PRC for Ray's scheduler. Generated MRCs clear the scheduling policy and use
`minReplicas: 0` / `maxReplicas: 2147483647`; they execute the PRC's explicit replica
targets and Pod deletion list. This prevents a lower user maximum from causing
an MRC operator to clamp replicas and delete an arbitrary busy worker before Ray
selects which worker to drain. With autoscaling disabled, member min/max bounds
continue to be copied from FRC.

### Observations and failure behavior

The federation controller alone reads member kubeconfig Secrets and contacts
member Kubernetes APIs. It creates one FRC-owned ConfigMap containing the Python
adapter and a combined Pod snapshot, plus an owned Role and RoleBinding. The head
ServiceAccount receives `get` permission for that one ConfigMap, in addition to
the ordinary KubeRay autoscaler permissions on the primary cluster. Member
credentials are not mounted into the head or shared with the autoscaler.

The controller refreshes the snapshot on its 10-second reconciliation interval
and relevant events. It includes only Ray head/worker Pods owned by the PRC or
bound MRCs. The snapshot contains identity, group labels and the minimal Pod
status consumed by Ray's provider; it excludes Pod specs, container environment
values and credentials. Unrelated and probe Pods are excluded. The adapter
preserves Ray's existing member-qualified instance IDs (`member-b/<pod-name>`),
while primary instances use plain Pod names. It maps these identities to and from
the raw Pod names stored in PRC/MRC `workersToDelete`. Different members can have
the same Pod name; only duplicate qualified instance IDs make a snapshot unusable.

The adapter supplies this snapshot to Ray's existing KubeRay provider in place of
its local-only Pod listing. Before observing Pods or patching the PRC, it checks
that the snapshot is complete, matches the PRC UID and generation, and is at most
30 seconds old, uses snapshot schema v2, and matches the running adapter source
revision. Before Ray mutates its instance state, every live GCS instance must be
present in the Pod observation. A missing instance pauses the iteration until the
snapshot catches up, or GCS reports it DEAD after an actual deletion. The drain
RPC boundary rechecks the observation; an RPC already sent cannot be recalled.
Scaling patches also check the PRC UID and resourceVersion, so a
concurrent update cannot silently overwrite another scaling decision.

An unavailable member API, invalid credential, incomplete inventory, unmatched
member replica intent, generation mismatch or expired snapshot pauses global
autoscaler observations and writes. Unknown capacity is never treated as zero.
The controller also refuses snapshots larger than 900 KiB; it publishes an error
instead of truncating worker inventory. This is a snapshot-size limit, not a fixed
maximum worker count. Recovery requires a complete, current snapshot before
autoscaling resumes. The member operators continue maintaining the last delivered
targets and replacing failed Pods during an observation outage.

FRC `AutoscalerObservationReady` reports whether the controller could publish a complete
snapshot at its last reconciliation. Inspect the head's `autoscaler` container
logs for runtime failures or freshness checks after the controller stops. This
condition does not certify the autoscaler process or the cross-cluster data plane.
An unavailable observation also makes aggregate `Ready=False`. Collection taking
over 30 seconds reports an error instead of publishing an already expired view
as ready. Native Kubernetes connection/timeout failures during autoscaler startup
are retried every five seconds.

### Configuration and current limits

- The adapter currently supports **Ray 2.56.0 and autoscaler v2** only.
  Set the Ray version and use matching head, worker and autoscaler images.
  `autoscalerOptions.version` may be omitted; federation selects `v2`.
  Prereleases, v1 and untested patch/minor versions are rejected. The tested Ray
  commit is `637fd062205393b9e1929996bfe1d49bd3f8469d`; a custom build reporting
  the same version still needs the native-provider contract tests.
- Use `networking.headEndpoint.gcsPort: 6379`; the primary's local GCS port
  (`headGroupSpec.rayStartParams.port`, if set) must also be 6379. All members must be managed;
  optional-credential manual members remain a non-autoscaling mode. Worker groups
  must be single-host and cannot be suspended while autoscaling is enabled.
- Set `autoscalerOptions` only with `enableInTreeAutoscaling: true`. The operator
  supplies the adapter command and args; user overrides of these fields, reserved
  identity variables and the adapter volume/mount are rejected. Keep
  `/etc/kuberay/federation` available for the generated adapter mount.
- Configure Ray resource capacity using worker `resources` or the corresponding
  `rayStartParams`, without defining the same resource in both places. Native Ray
  v2 group `priority` and per-group `idleTimeoutSeconds` are supported. In-place
  Pod resizing is not supported by the adapter.

The following changes have different update paths:

| Change | Update path |
| --- | --- |
| Group `minReplicas` / `maxReplicas`, `priority`, or `idleTimeoutSeconds` | Edit FRC; the autoscaler reloads the policy |
| Global `autoscalerOptions.idleTimeoutSeconds` / `upscalingMode` | Edit FRC; no head restart is needed |
| Runtime group `replicas` / `workersToDelete` | Written to PRC by the autoscaler; FRC preserves and distributes them |
| Enable/disable autoscaling, change Ray/autoscaler version or sidecar image, resources, environment, mounts or security settings | Create a new FRC because the head runtime configuration changes |
| Head configuration or an existing primary worker's runtime/template | Create a new FRC |

In both modes, FRC group `replicas` initializes newly created PRC groups. Editing
that value for an existing group does not override its live PRC target, and live
targets are not copied back into FRC. With autoscaling enabled, a manual PRC
replica edit may be superseded by the next autoscaler decision; change FRC
min/max bounds to constrain capacity instead.
Enabling autoscaling on an existing non-autoscaling FRC is rejected rather than
restarting its head in place.

## Start commands, updates and status

The endpoint is resolved once for command generation, GCS init checks and related
environment variables. Worker groups may have different resource/start options,
but must use the same `address`. No `externalHead` or shared start-parameter field
is added to RayCluster.

Custom initialization can reuse the existing generated-command mechanism:

```yaml
template:
  metadata:
    annotations:
      ray.io/overwrite-container-cmd: "true"
  spec:
    containers:
    - name: ray-worker
      image: rayproject/ray:2.56.0-py311-cpu
      command: ["/bin/bash", "-c", "--"]
      args: ['ulimit -n 65536; exec /bin/bash -c "$KUBERAY_GEN_RAY_START_CMD"']
```

`KUBERAY_GEN_RAY_START_CMD` is operator-generated output; configure its inputs with
`rayStartParams`. The inner shell preserves quoted JSON resource arguments.
A fully handwritten command must preserve the intended endpoint and Ray settings.

Endpoint or member worker template updates replace affected member workers. Replica-only
updates preserve their templates. Ready counts exclude old templates, terminating
Pods and targeted deletions. Member status uses existing replica counters and
`WorkersReady`; it does not invent a local HeadInfo or HeadPodReady condition.
FRC aggregates `HeadEndpointReady`, `MemberControlPlaneReachable`, `WorkersReady`
and `Ready` for the primary and managed members. Manual members only report `ManuallyManaged=True`.
These conditions report observations and do not authorize provisioning.
`RayClusterProvisioned` remains a first-provisioning record, not a live health test.

FRC does not yet coordinate upgrades of the entire Ray runtime. On an existing
federation, `primaryCluster.rayVersion`, `primaryCluster.headGroupSpec`, autoscaler
runtime settings and the runtime configuration of existing primary worker groups
cannot change in place.
Create a new FRC for these changes. Without autoscaling, edit replica targets and
`workersToDelete` on the PRC; Ray writes those PRC fields when autoscaling is enabled.
Bounds, worker suspension and adding/removing groups remain
managed through the FRC; suspension requires autoscaling to be disabled. Keep Ray and Python
versions compatible across the head and member images when editing member templates.

The FRC webhook and reconciler validate the final generated PRC and MRC specs
before creating children or recording a runtime revision. Previously persisted
invalid PRCs can be corrected only while they have no owned Pods. A missing
revision annotation is reconstructed after removing generated adapter wiring.
For runtime edits, admission checks the accepted PRC configuration, so reverting
an unapplied FRC change is allowed without granting an in-place runtime upgrade.
The FRC webhook rejects unsafe updates at admission. With webhooks disabled, the
controller reports `Ready=False` with `ReconcileFailed` before modifying primary
configuration or cleaning up members. Restore the accepted configuration to resume
reconciliation. A revision annotation on the primary lets the controller repair
configuration drift without treating that repair as a requested runtime upgrade.

Replica ownership is explicit:

| Mode | Desired replicas and targeted deletions | User operation |
| --- | --- | --- |
| Autoscaling disabled | User → PRC → managed MRC | Edit PRC workerGroupSpecs |
| Autoscaling enabled | Ray autoscaler → PRC → managed MRC | Edit FRC bounds and scheduling policy; replicas only initialize new groups |
| Manual member | User → standalone MRC | Edit the MRC directly |

FRC replica edits for existing groups do not replace PRC targets. The federation
controller preserves PRC `replicas` and `workersToDelete` while reconciling FRC
policy and forwards those targets to managed MRCs. Local controllers still apply
bounds and suspension. FRC status reports observed targets without copying them
back into its seed values. Removing a local group still removes its Pods.
Removing a managed member uses the cleanup policy,
which is separate from Ray autoscaler's graceful scale-down protocol.

When upgrading an existing non-autoscaling federation from FRC-owned replicas,
wait until the FRC has reconciled and record the current PRC targets. The new
controller preserves those targets; subsequent FRC replica edits affect only
new groups. A missing PRC is created from the FRC initial values.

Autoscaling defaults to disabled. Enabling it on FRC adds the federation adapter
to the primary autoscaler; generated MRCs always keep their own autoscaler disabled.

A missing or invalid referenced Secret is a managed-member failure, not a switch
to manual mode. Switching an existing member between managed and manual ownership
requires an explicit remove/re-add lifecycle; credential rotation stays supported.

Each managed member's `status.memberClusterStatuses[].clusterUID` records the
`kube-system` Namespace UID before remote writes. The operator verifies it when
using credentials, including during cleanup. Rotating a Secret or switching to a
new Secret for the same cluster preserves the member RayCluster and its workers;
the old credential may already have expired. Replacement Secret references also
work while FRC is deleting: verify the same cluster UID, persist the new reference,
then resume cleanup. A member removed from spec retains its original reference;
restore that Secret if it is needed to finish cleanup.

To move a member to another cluster, remove it, wait for its cleanup inventory
entry to disappear, then re-add it with the new destination. Retargeting an existing
Secret cannot complete this migration: an identity mismatch blocks writes and
cleanup, even if the member RayCluster is absent at the new target. Restore the
original credentials to finish cleanup. Keep original credentials and member
RayClusters available when upgrading older alpha FRCs without `clusterUID`; those
entries are bound only after verifying the existing member's federation ownership.

## Cleanup and current limits

`memberCleanupPolicy: Delete` waits for owned remote RayClusters and their children
to be deleted. Missing credentials or unavailable member APIs block finalization
until connectivity returns. Other reachable members are still cleaned up.
The persisted destination inventory survives retries. Member deletes use UID and
resourceVersion preconditions. Observed child UIDs distinguish failed creation
from a later ownership change; foreign name collisions are never deleted.
`Orphan` removes federation ownership, preserving the member's node identity and
declared worker capacity for local maintenance.

Deleting FRC never deletes manual member RayClusters. Delete their workers before
removing the primary when you want to shut down the entire runtime. Deleting a
member RayCluster garbage-collects its local children.

The initial workers-only path supports single-host replicas. It does not support
batch schedulers, local head services/storage/history-server configuration or
operator-generated member PKI. If Ray token auth is enabled manually, reference
a pre-created Secret with the same token as the head; kubeconfig is a separate
control-plane credential. TLS and production CNI/GPU/multi-host combinations are
not part of the initial federation test matrix.

## Local multi-cluster verification

The reproducible runner builds the operator and first verifies worker group delegation
with federation disabled, including Pod ownership and local suspension. It then
runs managed and manual federation scenarios on two kind clusters with distinct
Pod/Service CIDRs:

```bash
cd ray-operator
STATE_DIR=/tmp/ray-federation-test ./test/federation/run-kind.sh
```

Set `RAY_IMAGE` or `KIND_NODE_IMAGE` for a local mirror. Existing test clusters
require `REUSE_KIND_CLUSTERS=1`. The runner adds routing between its kind nodes;
this is a test topology, not production networking automation. Artifacts include
Ray task/actor/object-transfer output, resource snapshots and operator logs.

After preparing two clusters with matching autoscaling-capable CRDs and operators,
run the dedicated autoscaling scenario separately:

```bash
cd ray-operator
python3 test/federation/autoscaling_kind_e2e.py \
  --primary-kubeconfig /tmp/ray-federation-test/primary.kubeconfig \
  --member-kubeconfig /tmp/ray-federation-test/member.kubeconfig \
  --member-node frc-member-control-plane \
  --image rayproject/ray:2.56.0-py311-cpu \
  --artifacts /tmp/ray-federation-autoscaling-results
```

This script reuses the existing network and operator deployments. It creates an
isolated namespace, a local group and two managed members sharing the member
cluster and namespace. Detached Ray actors drive `0 -> 1 -> 2` scale-up, the
`maxReplicas` check, and exact idle-worker removal while busy actors keep their
Pod identities. It also checks credential failure freezes scaling, recovery
resumes it, all idle groups return to zero, and `minReplicas` changes retain idle
capacity without restarting the head. FRC deletion then cleans up the members.

Use `--namespace` for a different unused namespace and `--member-server` when the
member API endpoint is not derived from a kind node. The script restores test
credentials and cleans up its own namespaces by default; `--keep-on-failure`
retains a failed test for inspection. Artifacts contain observations, Ray driver
output, resource snapshots and logs, without Secrets or kubeconfig credentials.
The script does not change demo workloads, operator deployments or node routes.

Run fault injection separately, after the basic runner completes:

```bash
python3 test/federation/faults_kind_e2e.py \
  --primary-kubeconfig /tmp/ray-federation-test/primary.kubeconfig \
  --member-kubeconfig /tmp/ray-federation-test/member.kubeconfig \
  --namespace ray-federation-faults --name ray-faults \
  --artifacts /tmp/ray-federation-fault-results
```

This runner temporarily changes primary operator replica/leader settings and
restores them in `finally`. Its network rules target only the test head. It
covers leader handoff, fresh snapshot lag versus real GCS, adapter revision
mismatch/rollback, a 100-second data-plane partition with actor and object
transfer checks, sidecar exit/head replacement, independent member convergence,
and credential-reference rotation during deletion. A head replacement without
GCS fault tolerance loses actor state; the test verifies recovery of a new
runtime, not preservation of the old one.
