import assert from "node:assert/strict";
import { test } from "node:test";
import { buildGraph, mergeNodePositions } from "./graph";
import { fixture } from "./test-fixture";
import type { PodView, Snapshot } from "./model";

function snapshot(): Snapshot {
  const pod = (uid: string, role: PodView["role"], group: string): PodView => ({
    uid,
    name: uid,
    role,
    group,
    ready: true,
    phase: "Running",
    ip: "10.0.0.1",
    node: "node",
    restarts: 0,
    containers: [],
    createdAt: "",
  });
  const federation = fixture();
  return {
    updatedAt: "now",
    revision: "1",
    federation,
    clusters: [
      {
        id: "primary",
        label: "primary",
        role: "primary",
        namespace: "ray-federation",
        resource: {
          apiVersion: "ray.io/v1",
          kind: "RayCluster",
          metadata: {
            name: federation.metadata.name,
            namespace: "ray-federation",
          },
          spec: {
            headGroupSpec: federation.spec.primaryCluster.headGroupSpec,
            workerGroupSpecs: [
              ...structuredClone(federation.spec.primaryCluster.workerGroups!),
              ...structuredClone(
                federation.spec.memberClusters[0].workerGroups!,
              ).map((group) => ({
                ...group,
                managedBy: "ray.io/federated-raycluster-controller",
              })),
            ],
          },
        },
        pods: [
          pod("head", "head", "headgroup"),
          pod("local", "worker", "primary-cpu"),
        ],
      },
      {
        id: "member",
        label: "member",
        role: "member",
        memberName: "member-b",
        namespace: "ray-federation",
        resource: {
          apiVersion: "ray.io/v1",
          kind: "RayCluster",
          metadata: { name: "member", namespace: "ray-federation" },
          spec: {
            workerGroupSpecs: structuredClone(
              federation.spec.memberClusters[0].workerGroups!,
            ),
          },
        },
        pods: [
          pod("remote-1", "worker", "member-cpu"),
          pod("remote-2", "worker", "member-cpu"),
        ],
      },
    ],
  };
}

test("graph groups real Pods by Kubernetes cluster and shows only their control relationships", () => {
  const live = snapshot();
  const graph = buildGraph(live, live.federation!);
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "cluster").length,
    2,
  );
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "pod").length,
    4,
  );
  assert.deepEqual(
    graph.edges.map((edge) => [edge.source, edge.target]),
    [
      ["frc", "controller"],
      ["controller", "rc:primary"],
      ["rc:primary", "pod:primary:head"],
      ["rc:primary", "pod:primary:local"],
      ["controller", "rc:member"],
      ["rc:member", "pod:member:remote-1"],
      ["rc:member", "pod:member:remote-2"],
    ],
  );
  const remote = graph.nodes.find((node) => node.data.pod?.uid === "remote-1")!;
  assert.equal(remote.parentId, "cluster:member");
  assert.ok(
    graph.edges.some(
      (edge) => edge.source === "controller" && edge.target === "rc:member",
    ),
  );
  for (const edge of graph.edges) {
    assert.ok(graph.nodes.some((node) => node.id === edge.source));
    assert.ok(graph.nodes.some((node) => node.id === edge.target));
  }
});

test("autoscaling ignores FRC seed edits and follows PRC runtime targets", () => {
  const live = snapshot();
  live.federation!.spec.primaryCluster.enableInTreeAutoscaling = true;
  const draft = structuredClone(live.federation!);
  draft.spec.memberClusters[0].workerGroups![0].replicas = 3;
  const graph = buildGraph(live, draft);
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "planned").length,
    0,
  );
  assert.deepEqual(
    graph.nodes.find((node) => node.id === "cluster:member")!.data.targets,
    [{ groupName: "member-cpu", replicas: 2, initial: false }],
  );
  // A live PRC target takes precedence even before it propagates to the MRC.
  live.clusters[0].resource!.spec.workerGroupSpecs[1].replicas = 3;
  assert.equal(
    buildGraph(live, draft).nodes.filter((node) => node.data.kind === "planned")
      .length,
    1,
  );
});

test("new groups use FRC initial replicas to preview future Pods", () => {
  const live = snapshot();
  const draft = structuredClone(live.federation!);
  draft.spec.memberClusters[0].workerGroups!.push({
    groupName: "new-workers",
    replicas: 1,
  });
  const graph = buildGraph(live, draft);
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "planned").length,
    1,
  );
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "pod").length,
    4,
  );
  assert.equal(
    live.clusters[1].pods.filter((pod) => pod.role === "worker").length,
    2,
  );
  const planned = graph.nodes.find((node) => node.data.kind === "planned")!;
  assert.deepEqual(
    graph.edges
      .filter((edge) => edge.target === planned.id)
      .map((edge) => edge.source),
    ["rc:member"],
  );
});

test("draft members outside the demo cluster registry remain visible as declared targets", () => {
  const live = snapshot();
  const draft = structuredClone(live.federation!);
  const extra = structuredClone(draft.spec.memberClusters[0]);
  extra.name = "member-c";
  extra.workerGroups![0].groupName = "member-cpu-c";
  draft.spec.memberClusters.push(extra);
  const graph = buildGraph(live, draft);
  const declared = graph.nodes.find(
    (node) => node.id === "cluster:declared:member-c",
  )!;
  assert.equal(declared.data.unmapped, true);
  assert.match(declared.data.subtitle!, /Not mapped to a demo kind cluster/);
  assert.equal(
    graph.nodes.filter(
      (node) => node.parentId === declared.id && node.data.kind === "planned",
    ).length,
    2,
  );
  assert.ok(
    graph.edges.some(
      (edge) =>
        edge.source === "controller" && edge.target === "rc:declared:member-c",
    ),
  );
});

test("stale demo registry entries disappear after member cleanup", () => {
  const live = snapshot();
  live.clusters.push({
    ...structuredClone(live.clusters[1]),
    id: "member-c",
    label: "member-c",
    memberName: "member-c",
  });
  const graph = buildGraph(live, live.federation!);
  assert.ok(!graph.nodes.some((node) => node.id.includes("member-c")));
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "pod").length,
    4,
  );
});

test("manual members have no federation management edge", () => {
  const live = snapshot();
  const draft = structuredClone(live.federation!);
  delete draft.spec.memberClusters[0].kubeconfigSecretRef;
  delete draft.spec.memberClusters[0].workerGroups;
  const graph = buildGraph(live, draft);
  assert.ok(
    !graph.edges.some(
      (edge) => edge.source === "controller" && edge.target === "rc:member",
    ),
  );
});

test("polling keeps dragged positions but updates observed Pod status", () => {
  const live = snapshot();
  const before = buildGraph(live, live.federation!);
  const moved = before.nodes.find((node) => node.data.pod?.uid === "remote-1")!;
  moved.position = { x: 90, y: 600 };
  live.clusters[1].pods[0].phase = "Terminating";
  const next = mergeNodePositions(
    buildGraph(live, live.federation!).nodes,
    before.nodes,
  );
  const result = next.find((node) => node.id === moved.id)!;
  assert.deepEqual(result.position, moved.position);
  assert.equal(result.data.pod!.phase, "Terminating");
});

test("graph rebuild keeps node identities and dragged positions", () => {
  const live = snapshot();
  const before = buildGraph(live, live.federation!);
  const moved = before.nodes.find((node) => node.id === "controller")!;
  moved.position = { x: 90, y: 220 };
  const english = buildGraph(live, live.federation!, "en");
  const next = mergeNodePositions(english.nodes, before.nodes);
  assert.deepEqual(
    next.map((node) => node.id),
    before.nodes.map((node) => node.id),
  );
  assert.deepEqual(
    english.edges.map((edge) => edge.id),
    before.edges.map((edge) => edge.id),
  );
  const controller = next.find((node) => node.id === "controller")!;
  assert.deepEqual(controller.position, moved.position);
  assert.equal(
    controller.data.subtitle,
    "Distribute desired state by groupName",
  );
  assert.ok(
    english.edges.some(
      (edge) => edge.data?.description === "Member API · Create MRC",
    ),
  );
});

test("cluster lanes align resources and Pods without overlapping nodes, including previews", () => {
  const live = snapshot();
  live.federation!.spec.primaryCluster.workerGroups![0].replicas = 6;
  const graph = buildGraph(live, live.federation!);
  const clusters = graph.nodes.filter((node) => node.data.kind === "cluster");
  for (let index = 1; index < clusters.length; index++) {
    const previous = clusters[index - 1];
    assert.ok(
      clusters[index].position.y >
        previous.position.y + Number(previous.style!.height),
    );
  }
  const resourcePositions: number[] = [];
  const podPositions: number[] = [];
  for (const cluster of clusters) {
    const children = graph.nodes.filter((node) => node.parentId === cluster.id);
    for (const child of children) {
      assert.ok(child.position.x >= 0 && child.position.y >= 0);
      assert.ok(
        child.position.x + Number(child.style!.width) <=
          Number(cluster.style!.width),
      );
      assert.ok(
        child.position.y + Number(child.style!.height) <=
          Number(cluster.style!.height),
      );
      if (child.data.kind === "raycluster")
        resourcePositions.push(cluster.position.x + child.position.x);
      if (child.data.kind === "pod" || child.data.kind === "planned")
        podPositions.push(cluster.position.x + child.position.x);
      for (const other of children) {
        if (child.id === other.id) continue;
        assert.ok(
          child.position.x + Number(child.style!.width) <= other.position.x ||
            other.position.x + Number(other.style!.width) <= child.position.x ||
            child.position.y + Number(child.style!.height) <=
              other.position.y ||
            other.position.y + Number(other.style!.height) <= child.position.y,
          `${child.id} overlaps ${other.id}`,
        );
      }
    }
  }
  assert.equal(new Set(resourcePositions).size, 1);
  assert.equal(new Set(podPositions).size, 1);
});

test("untouched lanes reflow when new group previews grow while dragged nodes keep their position", () => {
  const live = snapshot();
  const before = buildGraph(live, live.federation!);
  const controller = before.nodes.find((node) => node.id === "controller")!;
  controller.position = { x: 320, y: 180 };
  const draft = structuredClone(live.federation!);
  draft.spec.primaryCluster.workerGroups!.push({
    groupName: "new-local",
    replicas: 5,
  });
  const next = buildGraph(live, draft);
  const merged = mergeNodePositions(next.nodes, before.nodes);
  assert.deepEqual(
    merged.find((node) => node.id === "controller")!.position,
    controller.position,
  );
  for (const id of ["rc:primary", "cluster:member"]) {
    assert.notDeepEqual(
      next.nodes.find((node) => node.id === id)!.position,
      before.nodes.find((node) => node.id === id)!.position,
    );
    assert.deepEqual(
      merged.find((node) => node.id === id)!.position,
      next.nodes.find((node) => node.id === id)!.position,
    );
  }
});

test("manual scaling follows PRC targets and ignores existing FRC seed edits", () => {
  const live = snapshot();
  const draft = structuredClone(live.federation!);
  draft.spec.memberClusters[0].workerGroups![0].replicas = 3;
  let graph = buildGraph(live, draft);
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "planned").length,
    0,
  );
  assert.deepEqual(
    graph.nodes.find((node) => node.id === "cluster:member")!.data.targets,
    [{ groupName: "member-cpu", replicas: 2, initial: false }],
  );
  live.clusters[0].resource!.spec.workerGroupSpecs[1].replicas = 3;
  graph = buildGraph(live, draft);
  assert.equal(
    graph.nodes.filter((node) => node.data.kind === "planned").length,
    1,
  );
  assert.deepEqual(
    graph.nodes.find((node) => node.id === "cluster:member")!.data.targets,
    [{ groupName: "member-cpu", replicas: 3, initial: false }],
  );
});
