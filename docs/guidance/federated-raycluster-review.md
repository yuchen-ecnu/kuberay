# FederatedRayCluster PoC: historical design review

> This document records the counterexamples found **before** the September
> 2026 fixes. R1–R5 have since been addressed and tested. The current API puts
> autoscaler settings under FRC `spec.primaryCluster`. A later ownership revision
> made the PRC the runtime replica target in both modes. See the [design contract](federated-raycluster-contract.md)
> and [implementation report](federated-raycluster-implementation.md) for the
> present behavior. A passing reproduction of the old bug was evidence of the
> bug, not evidence that the fix had landed.

## Architecture assessment

The basic controller split is sound:

| Component | Responsibility |
| --- | --- |
| Federation controller | Member identity, credentials, PRC/MRC projection, remote cleanup, aggregate observation |
| Ray autoscaler | Global resource demand, target replicas, selected drain nodes |
| Each RayCluster controller | Local Pod creation, replacement, exact deletion, and status |

Only FRC is a new CRD. PRC and MRC use the existing RayCluster controller, and
one Ray runtime runs one autoscaler. This follows the
[Ray autoscaler model](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/k8s-autoscaler.html).
Persisting a remote destination before writing there is appropriate because
Kubernetes owner references do not cross clusters. The design also keeps
member kubeconfigs out of the head and avoids reimplementing Ray's scheduler.

The initial happy-path kind tests proved that cross-cluster workers could join
and scale. They did not cover several observation and lifecycle races.

## Counterexamples found before the fixes

### R1: Pod snapshot lag could create a wrong deletion intent (P1)

A worker could join GCS before appearing in the federation controller's next
Pod snapshot. The snapshot still satisfied its 30-second TTL and matched the
PRC UID/generation. In an offline run of the complete Ray 2.56 reconciler:

1. At logical t=2s, GCS reported the new worker as running while the provider
   snapshot did not yet include it.
2. At t=7s, another read of the same valid snapshot caused the instance
   manager to mark the worker terminated.
3. A later snapshot included the Pod, but a new allocated record could not
   restore the original Ray node association.
4. Advancing the allocation timeout beyond 3600 seconds produced a provider
   PATCH requesting `replicas: 0` and `workersToDelete: [remote-new]`, while
   the GCS input still reported the node running.

The reproduction covered both pre-existing capacity and an ordinary pending
launch request. It used the actual Ray provider, adapter, and reconciler, with
simulated Kubernetes HTTP, GCS observations, and logical time. It did **not**
delete a Pod in kind. A shorter TTL alone cannot remove this race: GCS and the
snapshot must agree before Ray mutates instance state.

Current regression coverage is in
[`autoscaler/test_reconciliation.py`](../../ray-operator/controllers/federation/autoscaler/test_reconciliation.py)
and [`autoscaler/test_federation_autoscaler.py`](../../ray-operator/controllers/federation/autoscaler/test_federation_autoscaler.py).
The fix pauses an iteration on missing live GCS instances and rechecks before
drain, while allowing a real deletion to converge once GCS marks the node dead.

### R2: Invalid child configuration could become immutable (P1)

The original FRC validation did not always validate the final generated
RayCluster specs. For example, a head specifying both `resources.CPU` and
`rayStartParams.num-cpus` could be accepted and recorded under an immutable
configuration hash even though the RayCluster controller would reject it.
Correcting the FRC then failed the hash comparison. A member worker with
`rayStartParams.head: "true"` similarly passed FRC checks but could not be a
valid workers-only MRC.

The fix validates actual PRC and MRC projections before creating children or
recording a revision. A legacy invalid PRC can be corrected without replacing
a running cluster only when it owns no workload Pods. Admission and reconcile
share the projection rules. Tests are in
[`schema_test.go`](../../ray-operator/controllers/federation/schema_test.go)
and [`upgrade_test.go`](../../ray-operator/controllers/federation/upgrade_test.go).

### R3: A new unreachable member blocked healthy members (P1)

If an existing member's PRC target changed from 2 to 3 while a second member
with invalid credentials was added, the original inventory pass exited early.
The healthy member's MRC stayed at 2 until the bad member was removed. The fix
processes members independently after persisting each destination. Global
*new* autoscaler decisions still pause when observation is incomplete, but
confirmed targets can reach healthy members.

### R4: Credential replacement during deletion was ignored (P1)

Once FRC entered deletion, the old finalizer path used the Secret reference in
status and skipped normal credential rotation. Replacing the spec reference
with valid credentials for the **same** cluster did not unblock deletion; only
repairing the old Secret did. The fix verifies the saved cluster UID, persists
the replacement reference, and then retries cleanup. A reference to a different
cluster must never be accepted as a way to clear the finalizer.

### R5: Other lifecycle and concurrency gaps (P2)

| Old behavior | Required protection |
| --- | --- |
| First unreachable member stopped cleanup of all later members | Attempt each member and retain failed destinations for retry |
| A foreign MRC with a colliding name could block deletion of an FRC that had never owned it | Choose a stable alternate name and distinguish foreign objects from previously owned children |
| UID-only delete could remove an MRC whose owner label changed between GET and DELETE | Add a resourceVersion precondition and revalidate after conflicts |
| A PRC missing the revision annotation could be mistaken for a configuration upgrade | Reconstruct and compare the normalized accepted configuration |

The old Go reproductions used the real reconciler with fake clients; they did
not simulate all races against a live API server. Current coverage includes
real API admission, ownership changes, lost responses, and retry behavior in
[`lifecycle_regression_test.go`](../../ray-operator/controllers/federation/lifecycle_regression_test.go)
and the kind failure suite.

## API choices likely to be challenged

### Autoscaler field location

RayService embeds a full RayCluster specification under
`spec.rayClusterConfig`, so its autoscaler options live under
`spec.rayClusterConfig.autoscalerOptions`. An early FRC suggestion put the
global autoscaler settings at the FRC `spec` root because they affect all
members. That was a design suggestion, not a KubeRay requirement. The selected
API instead puts both `enableInTreeAutoscaling` and `autoscalerOptions` under
`spec.primaryCluster`, then projects them into the PRC. The sole autoscaler
still manages all groups. The old top-level proposal must not be used for new
objects.

### Replica source of truth

RayService's configuration comparison also ignores some runtime replica
fields, providing a precedent for separating templates from live scaling
state. The first post-review contract made FRC authoritative without
autoscaling. That decision was superseded: PRC now holds runtime targets in
both modes, while FRC values seed new groups. The federation controller still
distributes PRC targets to managed members.

### API and compatibility

Omitting `headGroupSpec` enables workers-only RayClusters, but the Go type
changed from a value to a pointer. This affects source compatibility for SDK
consumers, generated clients, RayJob/RayService safeguards, and webhook
validation. `managedBy` is currently a known-controller enum, not a fully
open external-controller interface. FRC is Alpha even though it uses
`ray.io/v1`; a proposal should explain its evolution plan.

### Ray adapter and operational scope

The PoC avoids rebuilding Ray images but replaces private Ray provider behavior
at runtime. That is still a Ray-side adapter, with an exact version and snapshot
protocol contract. Updating a ConfigMap does not hot-reload the running Python
process. The head ServiceAccount also inherits existing namespace-scoped
permissions; adapter intent alone does not narrow RBAC to one PRC.

A single head remains a single failure domain. A member API outage pauses new
global scaling decisions, while a local operator maintains its last accepted
target. FRC `Ready` reflects control-plane and capacity observations, not
complete data-plane health or autoscaler process health. Deleting an MRC does
not guarantee graceful Ray drain. Ray authentication/TLS, HA, network
partitions, and long-running load need separate validation.

## Review and test plan

1. Review generic RayCluster API changes independently of FRC and the adapter.
2. Check final PRC/MRC projection validation and the member inventory state
   machine, including UID/resourceVersion checks and deletion retries.
3. Test GCS ahead of Pod observations, true deletion, stale snapshots, adapter
   upgrades, leader handoff, and lost API responses.
4. Inject credential failures, a long data-plane partition, autoscaler
   process exit, and head replacement in separate kind namespaces.
5. Measure collection latency and snapshot size before claiming a supported
   member or worker scale.

The current implementation has regression coverage for the original R1–R5
counterexamples, and the [implementation report](federated-raycluster-implementation.md)
records the scope and limits of the local kind runs. A passing short demo is not
proof of HA or arbitrary Ray-version compatibility.
