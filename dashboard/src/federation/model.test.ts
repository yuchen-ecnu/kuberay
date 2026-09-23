import assert from "node:assert/strict";
import { test } from "node:test";
import { stringify } from "yaml";
import {
  currentCondition,
  initialWorkers,
  federationPreview,
  groupsForCluster,
  manifest,
  parseResource,
  reconciliationIssues,
  rayClusterPreview,
  summarizePod,
  targetsForCluster,
  type ClusterView,
  type Federation,
  type Resource,
} from "./model";

import { fixture } from "./test-fixture";

const parsePreview = (source: string) =>
  federationPreview(parseResource(source))!;

test("editable manifests preserve user metadata and omit server-owned fields", () => {
  const resource = fixture();
  resource.metadata.labels = { demo: "true" };
  resource.metadata.annotations = { note: "presentation" };
  resource.metadata.finalizers = ["demo.example/finalizer"];
  resource.metadata.creationTimestamp = "2026-09-20T00:00:00Z";
  resource.metadata.managedFields = [{ manager: "kube-controller-manager" }];
  const editable = manifest(resource);
  assert.deepEqual(editable.metadata.labels, { demo: "true" });
  assert.deepEqual(editable.metadata.annotations, { note: "presentation" });
  assert.deepEqual(editable.metadata.finalizers, ["demo.example/finalizer"]);
  assert.equal(editable.metadata.uid, undefined);
  assert.equal(editable.metadata.resourceVersion, undefined);
  assert.equal(editable.metadata.creationTimestamp, undefined);
  assert.equal(editable.metadata.managedFields, undefined);
});

test("seed counts follow FRC group ownership, independently of managedBy", () => {
  const frc = parsePreview(stringify(manifest(fixture())));
  assert.equal(initialWorkers(frc), 3);
  const member = { role: "member", memberName: "member-b" } as ClusterView;
  assert.equal(groupsForCluster(frc, member)[0].groupName, "member-cpu");
  frc.spec.memberClusters[0].workerGroups![0].replicas = 7;
  assert.equal(initialWorkers(frc), 8);
  assert.deepEqual(
    groupsForCluster(frc, { ...member, memberName: "absent" }),
    [],
  );
});

test("runtime targets use RayCluster bounds and suspension; manual members use their own spec", () => {
  const frc = fixture();
  frc.spec.primaryCluster.enableInTreeAutoscaling = true;
  const member: ClusterView = {
    id: "member",
    label: "member",
    namespace: "ray-federation",
    pods: [],
    role: "member",
    memberName: "member-b",
    resource: {
      apiVersion: "ray.io/v1",
      kind: "RayCluster",
      metadata: { name: "member", namespace: "ray-federation" },
      spec: {
        workerGroupSpecs: [{ groupName: "member-cpu", replicas: 1 }],
      },
    },
  };
  const primary: Resource = {
    apiVersion: "ray.io/v1",
    kind: "RayCluster",
    metadata: { name: "primary", namespace: "ray-federation" },
    spec: {
      workerGroupSpecs: [
        {
          groupName: "member-cpu",
          managedBy: "ray.io/federated-raycluster-controller",
          replicas: 7,
          maxReplicas: 5,
        },
      ],
    },
  };
  assert.deepEqual(targetsForCluster(frc, member, primary), [
    { groupName: "member-cpu", replicas: 5, initial: false },
  ]);
  primary.spec.workerGroupSpecs[0].suspend = true;
  assert.equal(targetsForCluster(frc, member, primary)[0].replicas, 0);
  delete frc.spec.memberClusters[0].kubeconfigSecretRef;
  delete frc.spec.memberClusters[0].workerGroups;
  member.resource!.spec.workerGroupSpecs = [
    { groupName: "manual-workers", replicas: 3 },
  ];
  assert.deepEqual(targetsForCluster(frc, member), [
    { groupName: "manual-workers", replicas: 3, initial: false },
  ]);
});

test("manual members have no invented workers", () => {
  const frc = fixture();
  delete frc.spec.memberClusters[0].kubeconfigSecretRef;
  delete frc.spec.memberClusters[0].workerGroups;
  assert.equal(initialWorkers(parsePreview(stringify(manifest(frc)))), 1);
});

test("invalid YAML, duplicate keys and unsafe values are rejected as transport errors", () => {
  for (const source of [
    "a: [",
    "kind: A\nkind: B",
    "x: *missing",
    "---\na: b\n---\nc: d",
    "x".repeat(205000),
  ]) {
    assert.throws(() => parseResource(source));
  }
});

test("resource semantics are left to Kubernetes admission", () => {
  const changes = [
    (f: Federation) => {
      f.kind = "Secret";
    },
    (f: Federation) => {
      f.spec.memberClusters[0].workerGroups![0].replicas = -1;
    },
    (f: Federation) => {
      f.spec.memberClusters[0].workerGroups![0].replicas = 1.5;
    },
    (f: Federation) => {
      f.spec.memberClusters[0].workerGroups![0].groupName = "primary-cpu";
    },
    (f: Federation) => {
      f.spec.memberClusters[0].workerGroups![0].managedBy =
        "ray.io/federated-raycluster-controller";
    },
    (f: Federation) => {
      delete f.spec.memberClusters[0].kubeconfigSecretRef;
    },
    (f: Federation) => {
      f.spec.memberClusters.push(f.spec.memberClusters[0]);
    },
    (f: Federation) => {
      f.spec.primaryCluster.headGroupSpec = {};
    },
  ];
  for (const change of changes) {
    const resource = fixture();
    change(resource);
    assert.doesNotThrow(() => parseResource(stringify(manifest(resource))));
  }
  const wrongKind = manifest(fixture());
  wrongKind.kind = "Secret";
  assert.equal(federationPreview(parseResource(stringify(wrongKind))), null);

  const primary: Resource = {
    apiVersion: "ray.io/v1",
    kind: "RayCluster",
    metadata: { name: "ray-federation", namespace: "ray-federation" },
    spec: { workerGroupSpecs: [{ groupName: "member-cpu", replicas: 3 }] },
  };
  assert.equal(rayClusterPreview(primary), primary);
  primary.spec.workerGroupSpecs[0].replicas = "three";
  assert.equal(rayClusterPreview(primary), null);
});

test("pod summary excludes env credentials and distinguishes unready and terminating pods", () => {
  const pod: Resource = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name: "pod",
      namespace: "ns",
      labels: { "ray.io/node-type": "worker", "ray.io/group": "member-cpu" },
    },
    spec: {
      containers: [
        {
          name: "ray",
          image: "ray:test",
          env: [{ name: "TOKEN", value: "sensitive" }],
        },
      ],
    },
    status: {
      phase: "Running",
      conditions: [{ type: "Ready", status: "False" }],
      containerStatuses: [
        {
          restartCount: 2,
          state: {
            waiting: { reason: "CrashLoopBackOff", message: "crashed" },
          },
        },
      ],
    },
  };
  assert.equal(summarizePod(pod).ready, false);
  assert.equal(summarizePod(pod).phase, "CrashLoopBackOff");
  assert.equal(summarizePod(pod).restarts, 2);
  assert.ok(!JSON.stringify(summarizePod(pod)).includes("sensitive"));
  pod.metadata.deletionTimestamp = "now";
  pod.status!.conditions = [{ type: "Ready", status: "True" }];
  assert.equal(summarizePod(pod).phase, "Terminating");
  assert.equal(summarizePod(pod).ready, false);
});

test("conditions from old generations remain unknown", () => {
  const resource = fixture();
  resource.status = {
    conditions: [{ type: "Ready", status: "True", observedGeneration: 1 }],
  };
  assert.equal(currentCondition(resource, "Ready"), "Unknown");
  resource.status.conditions![0].observedGeneration = 2;
  assert.equal(currentCondition(resource, "Ready"), "True");
});

test("reconciliation diagnostics prefer current actionable member failures", () => {
  const resource = fixture();
  resource.status = {
    conditions: [
      {
        type: "Ready",
        status: "False",
        message: "Ready",
        observedGeneration: 2,
      },
    ],
    memberClusterStatuses: [
      {
        name: "member-c",
        conditions: [
          {
            type: "MemberControlPlaneReachable",
            status: "False",
            message: "member destination is already bound to member-b",
            observedGeneration: 2,
          },
          {
            type: "WorkersReady",
            status: "Unknown",
            message: "Current member state is unknown",
            observedGeneration: 2,
          },
        ],
      },
    ],
  };
  assert.deepEqual(reconciliationIssues(resource), [
    {
      scope: "member-c",
      type: "MemberControlPlaneReachable",
      message: "member destination is already bound to member-b",
    },
  ]);
  resource.metadata.generation = 3;
  assert.deepEqual(reconciliationIssues(resource), []);
});

test("topology reconciliation is progress rather than a blocked condition", () => {
  const resource = fixture();
  resource.status = {
    conditions: [
      {
        type: "Ready",
        status: "False",
        reason: "ReconcilingTopology",
        message: "Persisting or cleaning up member destinations",
        observedGeneration: 2,
      },
    ],
  };
  assert.deepEqual(reconciliationIssues(resource), []);
  assert.equal(currentCondition(resource, "Ready"), "False");
});

test("cyclic aliases and non-finite YAML values are diagnosed before rendering", () => {
  assert.throws(() => parseResource("spec: &cycle\n  self: *cycle\n"), /cycle/);
  assert.throws(() => parseResource("spec:\n  value: .inf\n"), /JSON/);
});
