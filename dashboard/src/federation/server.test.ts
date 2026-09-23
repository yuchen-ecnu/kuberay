import assert from "node:assert/strict";
import { test } from "node:test";
import { manifest, toYaml, type Resource } from "./model";
import {
  createFederationService,
  DemoError,
  revision,
  validateConfig,
  type DemoConfig,
  type Kubectl,
} from "./server";
import { fixture } from "./test-fixture";

const config: DemoConfig = {
  name: "ray-federation",
  namespace: "ray-federation",
  clusters: [
    {
      id: "primary",
      label: "kind-primary",
      role: "primary",
      namespace: "ray-federation",
      kubeconfig: "/private/primary.config",
    },
    {
      id: "member",
      label: "kind-member",
      role: "member",
      memberName: "member-b",
      namespace: "ray-federation",
      kubeconfig: "/private/member.config",
    },
  ],
};

function harness() {
  let current = fixture();
  let primaryRayCluster: Resource = {
    apiVersion: "ray.io/v1",
    kind: "RayCluster",
    metadata: {
      name: config.name,
      namespace: config.namespace,
      uid: "prc-uid",
      resourceVersion: "1",
    },
    spec: {
      headGroupSpec: current.spec.primaryCluster.headGroupSpec,
      workerGroupSpecs: [
        ...current.spec.primaryCluster.workerGroups!,
        ...current.spec.memberClusters[0].workerGroups!.map((group) => ({
          ...group,
          managedBy: "ray.io/federated-raycluster-controller",
        })),
      ],
    },
  };
  const calls: { cluster: string; args: string[]; input?: any }[] = [];
  const run: Kubectl = async (cluster, args, input) => {
    calls.push({
      cluster: cluster.id,
      args,
      input: input && structuredClone(input),
    });
    if (args[0] === "replace") {
      if (!args.includes("--dry-run=server")) {
        const resource = input as Resource | undefined;
        if (resource?.kind === "RayCluster") {
          primaryRayCluster = structuredClone(input!) as Resource;
          primaryRayCluster.metadata.resourceVersion = "2";
        } else {
          current = structuredClone(input!) as typeof current;
          current.metadata.resourceVersion = "2";
        }
      }
      return JSON.stringify(input);
    }
    if (args[1] === "federatedrayclusters.ray.io")
      return JSON.stringify(current);
    if (args[1] === "rayclusters.ray.io")
      return JSON.stringify(primaryRayCluster);
    if (args[1] === "pods") return JSON.stringify({ items: [] });
    return "";
  };
  return {
    service: createFederationService(config, run),
    calls,
    run,
    get current() {
      return current;
    },
    get primaryRayCluster() {
      return primaryRayCluster;
    },
  };
}

test("snapshot exposes only configured resource scope, never kubeconfig paths", async () => {
  const h = harness();
  const result = await h.service.snapshot();
  assert.equal(result.clusters.length, 2);
  assert.ok(!JSON.stringify(result).includes("/private/"));
  const reads = h.calls.length;
  await h.service.snapshot();
  assert.equal(h.calls.length, reads, "pollers share the short snapshot cache");
  assert.ok(
    h.calls
      .filter((c) => c.args[1] === "pods")
      .every((c) =>
        c.args.includes(
          "ray.io/cluster=ray-federation,ray.io/federation-probe!=true",
        ),
      ),
  );
});

test("members sharing a namespace read their own persisted RayCluster and Pods", async () => {
  const h = harness();
  const shared = structuredClone(config);
  shared.clusters.push({
    ...shared.clusters[1],
    id: "member-c",
    memberName: "member-c",
  });
  h.current.status = {
    memberClusterStatuses: [
      {
        name: "member-b",
        namespace: config.namespace,
        rayClusterName: "ray-federation",
      },
      {
        name: "member-c",
        namespace: config.namespace,
        rayClusterName: "ray-federation-member-c",
      },
    ],
  };
  const service = createFederationService(shared, h.run);
  await service.snapshot();
  for (const [id, name] of [
    ["primary", "ray-federation"],
    ["member", "ray-federation"],
    ["member-c", "ray-federation-member-c"],
  ]) {
    const calls = h.calls.filter((c) => c.cluster === id);
    assert.equal(
      calls.find((c) => c.args[1] === "rayclusters.ray.io")?.args[2],
      name,
    );
    assert.ok(
      calls
        .find((c) => c.args[1] === "pods")
        ?.args.includes(`ray.io/cluster=${name},ray.io/federation-probe!=true`),
    );
  }
});

test("a removed configured member never aliases the legacy member RayCluster", async () => {
  const h = harness();
  const shared = structuredClone(config);
  shared.clusters.push({
    ...shared.clusters[1],
    id: "member-c",
    memberName: "member-c",
  });
  h.current.status = {
    memberClusterStatuses: [
      {
        name: "member-b",
        namespace: config.namespace,
        rayClusterName: "ray-federation",
      },
    ],
  };
  const result = await createFederationService(shared, h.run).snapshot();
  assert.deepEqual(
    result.clusters.map((cluster) => cluster.id),
    ["primary", "member"],
  );
  assert.ok(
    !h.calls.some(
      (call) =>
        call.cluster === "member-c" &&
        ["rayclusters.ray.io", "pods"].includes(call.args[1]),
    ),
  );
});

test("one unreachable member does not erase primary observations", async () => {
  const h = harness();
  const service = createFederationService(
    config,
    async (cluster, args, input) => {
      if (cluster.role === "member") throw new Error("member offline");
      return h.run(cluster, args, input);
    },
  );
  const result = await service.snapshot();
  assert.ok(result.federation);
  assert.equal(result.clusters[0].error, undefined);
  assert.match(result.clusters[1].error!, /offline/);
});

test("validation is a server dry-run and cannot write", async () => {
  const h = harness();
  await h.service.update(toYaml(h.current), revision(h.current), true);
  const writes = h.calls.filter((c) => c.args[0] === "replace");
  assert.equal(writes.length, 1);
  assert.ok(writes[0].args.includes("--dry-run=server"));
});

test("primary RayCluster edits validate and apply through the primary API", async () => {
  const h = harness();
  const draft = manifest(h.primaryRayCluster) as Resource;
  draft.spec.workerGroupSpecs[1].replicas = 3;
  const beforeFRC = structuredClone(h.current.spec);
  await h.service.update(
    toYaml(draft),
    revision(h.primaryRayCluster),
    false,
    "primary",
  );
  const writes = h.calls.filter((call) => call.args[0] === "replace");
  assert.equal(writes.length, 2);
  assert.ok(writes[0].args.includes("--dry-run=server"));
  assert.equal(writes[0].cluster, "primary");
  assert.equal(writes[1].input.kind, "RayCluster");
  assert.equal(writes[1].input.metadata.uid, "prc-uid");
  assert.equal(writes[1].input.spec.workerGroupSpecs[1].replicas, 3);
  assert.equal(h.primaryRayCluster.spec.workerGroupSpecs[1].replicas, 3);
  assert.deepEqual(h.current.spec, beforeFRC);
});

test("primary RayCluster updates use their own revision", async () => {
  const h = harness();
  await assert.rejects(
    h.service.update(
      toYaml(h.primaryRayCluster),
      revision(h.current),
      false,
      "primary",
    ),
    (error: DemoError) => error.status === 409,
  );
  assert.ok(!h.calls.some((call) => call.args[0] === "replace"));
});

test("apply validates first, preserves metadata and sends resourceVersion for concurrency", async () => {
  const h = harness();
  h.current.metadata.annotations = { "keep-me": "yes" };
  h.current.status = { conditions: [] };
  const draft = structuredClone(h.current);
  draft.spec.memberClusters[0].workerGroups![0].replicas = 3;
  await h.service.update(toYaml(draft), revision(h.current), false);
  const writes = h.calls.filter((c) => c.args[0] === "replace");
  assert.equal(writes.length, 2);
  assert.ok(writes[0].args.includes("--dry-run=server"));
  assert.ok(!writes[1].args.includes("--dry-run=server"));
  assert.equal(writes[1].input!.metadata.resourceVersion, "1");
  assert.deepEqual(writes[1].input!.metadata.annotations, { "keep-me": "yes" });
  assert.equal(writes[1].input!.status, undefined);
  assert.equal(h.current.spec.memberClusters[0].workerGroups![0].replicas, 3);
});

test("stale edits and a recreated FRC cannot overwrite a newer spec", async () => {
  const h = harness();
  await assert.rejects(
    h.service.update(toYaml(h.current), "stale", false),
    (e: DemoError) => e.status === 409,
  );
  const previous = revision(h.current);
  h.current.metadata.uid = "replacement";
  await assert.rejects(
    h.service.update(toYaml(h.current), previous, false),
    (e: DemoError) => e.status === 409,
  );
  assert.ok(!h.calls.some((c) => c.args[0] === "replace"));
});

test("status-only changes do not create false editor conflicts", () => {
  const frc = fixture();
  const previous = revision(frc);
  frc.metadata.resourceVersion = "20";
  frc.status = { conditions: [{ type: "Ready", status: "True" }] };
  assert.equal(revision(frc), previous);
  frc.spec.memberClusters[0].workerGroups![0].replicas = 4;
  assert.notEqual(revision(frc), previous);
});

test("dashboard uses the head launch port, ignores NodePort endpoints and tolerates member outages", async () => {
  const h = harness();
  const checkTarget = async (dashboardPort?: string) => {
    const service = createFederationService(
      config,
      async (cluster, args, input) => {
        if (cluster.role === "member") throw new Error("offline");
        if (args[1] === "rayclusters.ray.io")
          return JSON.stringify({
            ...h.current,
            kind: "RayCluster",
            spec: {
              headGroupSpec: {
                serviceType: "NodePort",
                rayStartParams: { "dashboard-port": dashboardPort },
              },
            },
            status: { endpoints: { dashboard: "30265" } },
          });
        if (args[1] === "pods")
          return JSON.stringify({
            items: [
              {
                apiVersion: "v1",
                kind: "Pod",
                metadata: {
                  name: "head",
                  uid: "head-uid",
                  labels: { "ray.io/node-type": "head" },
                },
                spec: {},
                status: {
                  phase: "Running",
                  conditions: [{ type: "Ready", status: "True" }],
                },
              },
            ],
          });
        return h.run(cluster, args, input);
      },
    );
    return service.dashboardTarget();
  };
  const target = await checkTarget("8266");
  assert.equal(target.cluster.id, "primary");
  assert.equal(target.podUID, "head-uid");
  assert.equal(target.port, 8266);
  assert.equal((await checkTarget()).port, 8265);
  for (const invalid of ["NaN", "0", "65536"])
    await assert.rejects(
      checkTarget(invalid),
      (error: DemoError) => error.status === 503,
    );
  await assert.rejects(h.service.dashboardTarget(), /not ready/);
});

test("API validation or last-moment concurrency failures never force an update", async () => {
  for (const failAtDryRun of [true, false]) {
    const h = harness();
    const service = createFederationService(
      config,
      async (cluster, args, input) => {
        if (
          args[0] === "replace" &&
          (failAtDryRun || !args.includes("--dry-run=server"))
        )
          throw new DemoError("Rejected", failAtDryRun ? 422 : 409);
        return h.run(cluster, args, input);
      },
    );
    await assert.rejects(
      service.update(toYaml(h.current), revision(h.current), false),
    );
    assert.equal(h.current.metadata.resourceVersion, "1");
  }
});

test("user metadata and unregistered members reach Kubernetes dry-run", async () => {
  const h = harness();
  const draft = manifest(h.current) as Resource & { demoField?: string };
  draft.metadata.labels = { presentation: "true" };
  draft.metadata.finalizers = ["demo.example/finalizer"];
  draft.spec.memberClusters.push({ name: "member-c", namespace: "other" });
  draft.demoField = "validated-by-kubernetes";
  const { stringify } = await import("yaml");
  await h.service.update(stringify(draft), revision(h.current), true);
  const validation = h.calls.find((call) => call.args[0] === "replace")!;
  assert.deepEqual(validation.input!.metadata.labels, {
    presentation: "true",
  });
  assert.deepEqual(validation.input!.metadata.finalizers, [
    "demo.example/finalizer",
  ]);
  assert.equal(validation.input!.spec.memberClusters[1].name, "member-c");
  assert.equal(
    (validation.input as Resource & { demoField?: string }).demoField,
    "validated-by-kubernetes",
  );
});

test("demo business rules never block Kubernetes admission", async () => {
  const h = harness();
  const draft = manifest(h.current) as Resource;
  draft.spec.primaryCluster.headGroupSpec = {};
  draft.spec.memberClusters[0].workerGroups![0].replicas = -1;
  draft.spec.memberClusters[0].workerGroups![0].managedBy =
    "ray.io/federated-raycluster-controller";
  await h.service.update(toYaml(draft), revision(h.current), true);
  const validation = h.calls.find((call) => call.args[0] === "replace");
  assert.ok(validation, "the semantic draft must reach kubectl");
  assert.equal(
    validation.input.spec.memberClusters[0].workerGroups[0].replicas,
    -1,
  );
});

test("deleted user metadata is not restored by the demo server", async () => {
  const h = harness();
  h.current.metadata.annotations = { obsolete: "true" };
  const draft = manifest(h.current) as Resource;
  delete draft.metadata.annotations;
  await h.service.update(toYaml(draft), revision(h.current), true);
  const validation = h.calls.find((call) => call.args[0] === "replace")!;
  assert.equal(validation.input.metadata.annotations, undefined);
  assert.equal(validation.input.metadata.uid, h.current.metadata.uid);
  assert.equal(
    validation.input.metadata.resourceVersion,
    h.current.metadata.resourceVersion,
  );
});

test("configuration rejects ambiguous cluster mapping", () => {
  assert.throws(() =>
    validateConfig({
      ...config,
      clusters: [...config.clusters, config.clusters[0]],
    }),
  );
  assert.throws(() =>
    validateConfig({ ...config, clusters: config.clusters.slice(1) }),
  );
  assert.throws(() =>
    validateConfig({
      ...config,
      clusters: [{ ...config.clusters[0], kubeconfig: "relative" }],
    }),
  );
  assert.throws(() =>
    validateConfig({
      ...config,
      clusters: [
        { ...config.clusters[0], terminalTarget: "pod/../host" },
        config.clusters[1],
      ],
    }),
  );
});
