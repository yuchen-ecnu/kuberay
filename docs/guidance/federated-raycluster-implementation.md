# FederatedRayCluster PoC: implementation and validation

This report records the September 2026 PoC work on
`feat/federated-raycluster`. The [design contract](federated-raycluster-contract.md)
defines the current API and failure behavior. The
[historical review](federated-raycluster-review.md) records the issues found
before the fixes.

## Autoscaler configuration

Following RayService's `spec.rayClusterConfig` convention, FRC keeps the
user-facing `enableInTreeAutoscaling` and `autoscalerOptions` fields under
`spec.primaryCluster`. The federation controller projects them into the
primary RayCluster. Member RayClusters do not run autoscalers. The earlier
proposal to place these fields at the FRC `spec` root was withdrawn; old Alpha
objects using that path need the documented migration before a CRD update.

The demo explicitly sets `spec.primaryCluster.enableInTreeAutoscaling: false`.
The YAML preview points to that field and, when present, to
`spec.primaryCluster.autoscalerOptions`. The default demo uses manual PRC scaling;
autoscaling has a separate sample manifest.

## Correctness work

| Review item | Change |
| --- | --- |
| R1: GCS ahead of Pod snapshot | Check live GCS instances before the native Ray reconciler changes instance state; pause on missing observation and recheck before drain. Tests cover pre-existing workers, pending requests, catch-up, and real deletion after GCS reports DEAD. |
| R2: Invalid configuration frozen by revision | Validate final PRC/MRC projections in admission and reconciliation before creating children or recording the revision. Permit correction of invalid legacy PRCs with no owned workload Pods. |
| R3: Faulty new member blocks healthy members | Persist destinations and reconcile members independently, aggregating errors without stopping confirmed target propagation. |
| R4: Credential replacement during deletion | Reuse the credential replacement path in finalization, verify the original cluster UID, then persist and use the new reference. |
| R5: Cleanup and concurrency | Attempt every member, record child UIDs, use UID/resourceVersion delete preconditions, choose stable names after foreign-object collisions, and recover missing primary revisions. |
| Replica ownership | Manual: user → PRC → MRC. Autoscaled: Ray → PRC → MRC. FRC replicas seed new groups in both modes; manual members remain independent. |
| Adapter compatibility | Require Ray 2.56.0 with autoscaler v2, snapshot protocol v2, and a matching adapter source digest. Reject stale or oversized observations. |

The code separates projection validation (`projection.go`), destination
inventory and lifecycle (`inventory.go`, `federated_controller.go`), revision
rules (`revision.go`, `upgrade.go`), observation (`autoscaling.go`), and Ray
integration (`autoscaler/federation_autoscaler.py`). It adds no MRC CRD.

## Validation performed

The PoC was exercised on two routed kind clusters with distinct Pod and
Service CIDRs. The tests use real Ray jobs and actors where indicated. Test
scripts are in [`ray-operator/test/federation`](../../ray-operator/test/federation/).

| Layer | Result and scope |
| --- | --- |
| Go | Operator non-`test/` packages, affected webhook and utility packages, and envtest admission tests passed. API server `pkg/` and SDK tests passed. |
| Native Ray | 30 adapter tests used the Ray 2.56 provider, InstanceManager, and complete reconciler with controlled HTTP/GCS observations and logical time. |
| Generated files | CRD, Helm CRD, SDK, and API reference generation matched; Helm lint passed. |
| Frontend | Unit tests, TypeScript check, production build, and browser tests covered YAML editing, topology, and Ray Dashboard routing. |
| Autoscaling kind run | 11 scenarios: actor-driven scale out/in across groups, bounds, exact idle worker deletion, credential outage/recovery, minimum replica updates, and cleanup. |
| Failure kind run | 7 scenarios: leader handoff, GCS/snapshot lag, adapter mismatch and rollback, 100-second network partition, sidecar exit/head replacement, member isolation, and deletion-time credential replacement. |
| Managed member kind run | 12 scenarios: independent members, network/endpoint recovery, exact downscale, Delete/Orphan, 64 MiB object transfer, and Ray Data. |
| Manual member kind run | 7 scenarios: no FRC credential, independent replica targets, worker replacement, endpoint recovery, cleanup ownership, and Ray tasks. |
| Generic `managedBy` kind run | 5 scenarios with federation disabled: external Pods remain outside local counting, suspension, and deletion. |
| Sampling budget | 1, 4, and 16 fake members with 100 Pods each; 16 members at 650 ms per read took 31.98 seconds and produced an error snapshot rather than incomplete capacity. |

The five kind suites contained **42 scenarios** in total. A later build with
the final `primaryCluster` autoscaler field placement also passed an additional
11-scenario autoscaling run. The local demo migration preserved the observed
FRC/PRC UIDs, head and worker Pod UIDs, and both group targets at the migration
checkpoint. The browser proxy test confirmed that CSS and JavaScript load from
the workspace prefix and that the Ray Dashboard opens.

The full kind suites above predate unified PRC runtime ownership in manual mode.
After that change, the federation controller package tests, dashboard unit tests,
TypeScript check, production build, and API migration script tests passed. Both
kind operators were rebuilt and deployed to the live demo. A manual PRC scale
from 2 to 3 and back propagated to the MRC and worker Pods while the FRC seed
stayed at 2. The two focused browser tests passed for PRC editing and FRC seed
editing against that demo. The full updated kind suites have not been rerun.

These are local development results; log files under `/data/tmp` were not
committed. Reproduce the main cluster suites with:

```bash
STATE_DIR=/tmp/ray-federation-test ./ray-operator/test/federation/run-kind.sh
```

The optional autoscaling and failure scripts accept the generated
`primary.kubeconfig` and `member.kubeconfig`; see the
[test commands](federated-raycluster.md#testing). The demo has its own
[setup and launch guide](../../dashboard/demo/federation/README.md).

### Limits of the evidence

- The native Ray test's hour-long timeout uses logical time; the kind network
  partition lasted 100 seconds of wall time.
- The sampling benchmark uses the real collector and a fake API store, not 16
  live Kubernetes clusters. It is not a production capacity or QPS guarantee.
- Two remote actors were lost during the long network partition. New actors
  and large object transfers worked after recovery. Head replacement also
  lost old actor state; recovery was validated on a new runtime.
- Fault-injection runs initially exposed test-harness problems: one-way
  iptables rules missed kind SNAT paths, a stale leader Lease did not identify
  a live Pod, and SIGTERM did not terminate the autoscaler container. The
  final assertions used bidirectional rules, a live leader check, and CRI
  container termination. Those failed harness runs are not product successes.
- An in-place migration of an already running *autoscaled* old Alpha object
  has not been demonstrated; the migration helper was unit-tested and the new
  field path was tested on freshly created resources.

## Suggested review order

1. Generic RayCluster changes: `managedBy`, workers-only mode, Go pointer
   compatibility, RayJob/RayService safeguards, and generated clients.
2. FRC API and controller: projection, member identity, inventory persistence,
   deletion/recovery, admission, CRD, Helm, and migration.
3. Autoscaler adapter: complete observations, private Ray interfaces,
   protocol upgrade, and failure tests.
4. Demo: YAML editor, topology, terminal, and Dashboard proxy.

## Remaining proposal decisions

This is one Ray runtime with one head. There is no GCS fault-tolerance path, so
head replacement can lose actors and objects. A standalone autoscaler container
may exit without an automatic restart; the PoC uses explicit head replacement.
The private Ray adapter requires planned upgrade or compatible rollback. A
member API outage pauses new global decisions, while healthy members can still
receive already confirmed targets. Deleting an MRC has no Ray graceful-drain
guarantee. Ray data-plane authentication/TLS, least-privilege production RBAC,
HA, and sustained load testing remain outside the PoC's validated scope.
