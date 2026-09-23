import { expect, test } from "@playwright/test";
import { parse, stringify } from "yaml";
import { editorValue } from "./editor";
import { manifest, type Snapshot } from "../../src/federation/model";

test.describe.configure({ mode: "serial" });

const demoBase =
  process.env.FEDERATION_DEMO_PROXY_URL?.replace(/\/+$/, "") ?? "";
const demoPath = (path: string) => `${demoBase}${path}`;

test("live topology, generated YAML, Pod details and responsive presentation", async ({
  page,
  request,
}, testInfo) => {
  const snapshot: Snapshot = await (
    await request.get(demoPath("/api/federation"))
  ).json();
  const rayPods = snapshot.clusters.flatMap((cluster) => cluster.pods);
  const managedMembers = snapshot.federation!.spec.memberClusters.filter(
    (member) => member.kubeconfigSecretRef,
  ).length;
  const controlEdges = 2 + managedMembers + rayPods.length;
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(demoPath("/federation"));
  await expect(page.getByTestId("cluster-primary")).toBeVisible();
  await expect(page.getByTestId("cluster-member-b")).toBeVisible();
  await expect(page.getByTestId("ready-pods")).toHaveText(
    `${rayPods.length}/${rayPods.length}`,
    { timeout: 120000 },
  );
  await expect(page.getByTestId("pod-node")).toHaveCount(rayPods.length);
  await expect(page.locator(".react-flow__edge")).toHaveCount(controlEdges);
  await expect(page.locator('.react-flow__edge[data-id^="data:"]')).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("button", { name: "All edges", exact: true }),
  ).toHaveCount(0);
  await expect(page.getByText("Ray data plane", { exact: true })).toHaveCount(
    0,
  );
  const viewport = page.locator(".react-flow__viewport");
  const zoomBefore = await viewport.getAttribute("style");
  await page.getByRole("button", { name: "Zoom in" }).click();
  await expect.poll(() => viewport.getAttribute("style")).not.toBe(zoomBefore);
  await page.getByRole("button", { name: "Fit view" }).click();
  const controller = page.locator('.react-flow__node[data-id="controller"]');
  const before = await controller.getAttribute("style");
  const box = (await controller.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    box.x + box.width / 2 + 45,
    box.y + box.height / 2 + 20,
    { steps: 10 },
  );
  await page.mouse.up();
  await expect.poll(() => controller.getAttribute("style")).not.toBe(before);
  const dragged = await controller.getAttribute("style");
  const observation = page.waitForResponse((r) =>
    r.url().endsWith("/api/federation"),
  );
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await observation;
  expect(await controller.getAttribute("style")).toBe(dragged);
  await page.getByRole("button", { name: "Reset layout" }).click();
  await page.screenshot({
    path: testInfo.outputPath("topology.png"),
    fullPage: true,
  });
  await page.getByTestId("graph-frc").click();
  await page.getByRole("tab", { name: "PRC", exact: true }).click();
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await expect(page.getByLabel("YAML preview")).toContainText(
    "managedBy: ray.io/federated-raycluster-controller",
  );
  await page.getByRole("tab", { name: "MRC · member-b" }).click();
  const mrc = parse(await page.getByLabel("YAML preview").innerText());
  expect(mrc.spec.headGroupSpec).toBeUndefined();
  expect(mrc.spec.workerGroupSpecs[0].rayStartParams.address).toBeTruthy();
  expect(mrc.spec.workerGroupSpecs[0].managedBy).toBeUndefined();
  await page.getByTestId("pod-node").first().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByRole("dialog")).toContainText("Pod IP");
  await page.screenshot({ path: testInfo.outputPath("pod-details.png") });
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("presentation.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByTestId("cluster-member-b")).toBeVisible();
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);
  await page.screenshot({
    path: testInfo.outputPath("mobile.png"),
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("PRC replica edits preview and scale managed members without autoscaling", async ({
  page,
  request,
  baseURL,
}, testInfo) => {
  const snapshot = async (): Promise<Snapshot> =>
    (await request.get(demoPath("/api/federation"))).json();
  const initial = await snapshot();
  const primary = initial.clusters.find(
    (cluster) => cluster.role === "primary",
  )!;
  const original = manifest(primary.resource!);
  const group = original.spec.workerGroupSpecs.find(
    (g: { managedBy?: string }) =>
      g.managedBy === "ray.io/federated-raycluster-controller",
  );
  const replicas = group.replicas;
  const member = initial.clusters.find(
    (cluster) =>
      cluster.memberName === initial.federation!.spec.memberClusters[0].name,
  )!;
  const memberWorkers = (value: Snapshot) =>
    value.clusters
      .find((cluster) => cluster.id === member.id)!
      .pods.filter(
        (pod) =>
          pod.role === "worker" && pod.group === group.groupName && pod.ready,
      ).length;
  const originalMemberWorkers = memberWorkers(initial);
  await page.goto(demoPath("/federation"));
  await page.getByRole("tab", { name: "PRC", exact: true }).click();
  const editor = page.getByLabel("Primary RayCluster YAML editor");
  const draft = parse(await editorValue(editor));
  draft.spec.workerGroupSpecs.find(
    (g: { groupName: string }) => g.groupName === group.groupName,
  ).replicas = replicas + 1;
  try {
    await editor.fill(stringify(draft));
    await expect(page.getByTestId(`target-${group.groupName}`)).toContainText(
      `${replicas + 1}Current target`,
    );
    await expect(page.getByTestId("planned-node")).toHaveCount(1);
    expect(memberWorkers(await snapshot())).toBe(originalMemberWorkers);
    await page.getByRole("button", { name: "Apply to kind" }).click();
    await expect(page.getByRole("status")).toContainText("Applied to PRC.");
    await expect
      .poll(
        async () =>
          (await snapshot()).clusters
            .find((cluster) => cluster.role === "primary")!
            .resource!.spec.workerGroupSpecs.find(
              (g: { groupName: string }) => g.groupName === group.groupName,
            ).replicas,
      )
      .toBe(replicas + 1);
    await expect
      .poll(async () => memberWorkers(await snapshot()), { timeout: 120000 })
      .toBe(originalMemberWorkers + 1);
    expect(
      (await snapshot()).federation!.spec.memberClusters[0].workerGroups![0]
        .replicas,
    ).toBe(replicas);
    await page.screenshot({
      path: testInfo.outputPath("prc-replicas-applied.png"),
      fullPage: true,
    });
  } finally {
    const latest = await snapshot();
    const currentPrimary = latest.clusters.find(
      (cluster) => cluster.role === "primary",
    )!;
    const response = await request.post(demoPath("/api/federation"), {
      headers: { origin: baseURL! },
      data: {
        action: "apply",
        target: "primary",
        yaml: stringify(original),
        revision: currentPrimary.revision,
      },
    });
    expect(response.status(), await response.text()).toBe(200);
    await expect
      .poll(async () => memberWorkers(await snapshot()), { timeout: 120000 })
      .toBe(originalMemberWorkers);
  }
});

test("invalid drafts are preserved across polling and cannot be applied", async ({
  page,
}) => {
  await page.goto(demoPath("/federation"));
  const editor = page.getByLabel("FederatedRayCluster YAML editor");
  await expect(editor).toContainText("FederatedRayCluster");
  await editor.fill("apiVersion: [broken");
  await expect(
    page.getByRole("button", { name: "Apply to kind" }),
  ).toBeDisabled();
  await expect(
    page.getByRole("alert").filter({ hasText: "The topology shows" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Refresh", exact: true }),
  ).toBeEnabled();
  await expect.poll(() => editorValue(editor)).toBe("apiVersion: [broken");
  await page.getByRole("button", { name: "Reload latest" }).click();
  await expect(editor).toContainText("kind: FederatedRayCluster");
});

test("editing an existing FRC replica seed leaves the PRC runtime target intact", async ({
  page,
  request,
  baseURL,
}, testInfo) => {
  const snapshot = async (): Promise<Snapshot> =>
    (await request.get(demoPath("/api/federation"))).json();
  const initial = await snapshot();
  const original = manifest(initial.federation!);
  const memberSpec = initial.federation!.spec.memberClusters[0];
  const groupSpec = memberSpec.workerGroups![0];
  const groupName = groupSpec.groupName;
  const initialReplicas = groupSpec.replicas ?? 0;
  const currentTarget = initial.clusters
    .find((cluster) => cluster.role === "primary")!
    .resource!.spec.workerGroupSpecs.find(
      (group: { groupName: string }) => group.groupName === groupName,
    ).replicas;
  const memberWorkers = (value: Snapshot) =>
    value.clusters
      .find((cluster) => cluster.memberName === memberSpec.name)!
      .pods.filter(
        (pod) =>
          pod.role === "worker" &&
          pod.group === groupName &&
          pod.ready &&
          pod.phase === "Running",
      ).length;
  const originalMemberWorkers = memberWorkers(initial);
  await page.goto(demoPath("/federation"));
  const editor = page.getByLabel("FederatedRayCluster YAML editor");
  await expect(editor).toContainText("kind: FederatedRayCluster");
  const draft = parse(await editorValue(editor));
  draft.spec.memberClusters[0].workerGroups[0].replicas = initialReplicas + 1;
  try {
    await editor.fill(stringify(draft));
    await expect(page.getByTestId(`target-${groupName}`)).toContainText(
      `${currentTarget}Current target`,
    );
    await expect(page.getByTestId("planned-node")).toHaveCount(0);
    expect(memberWorkers(await snapshot())).toBe(originalMemberWorkers);
    await page
      .getByRole("button", {
        name: "Kubernetes / Operator validation",
        exact: true,
      })
      .click();
    await expect(page.getByRole("status")).toContainText("admission passed");
    expect(
      (await snapshot()).federation!.spec.memberClusters[0].workerGroups![0]
        .replicas,
    ).toBe(initialReplicas);
    await page.getByRole("button", { name: "Apply to kind" }).click();
    await expect(page.getByRole("status")).toContainText("Applied to FRC.");
    await expect
      .poll(
        async () => {
          const current = (await snapshot()).federation!;
          return current.status?.conditions?.some(
            (condition) =>
              condition.type === "Ready" &&
              condition.status === "True" &&
              condition.observedGeneration === current.metadata.generation,
          );
        },
        { timeout: 120000, intervals: [2000] },
      )
      .toBe(true);
    const observed = await snapshot();
    expect(memberWorkers(observed)).toBe(originalMemberWorkers);
    expect(
      observed.clusters
        .find((cluster) => cluster.role === "primary")!
        .resource!.spec.workerGroupSpecs.find(
          (group: { groupName: string }) => group.groupName === groupName,
        ).replicas,
    ).toBe(currentTarget);
    await expect(page.getByTestId(`target-${groupName}`)).toContainText(
      `${currentTarget}Current target`,
    );
    await expect(page.getByTestId("ready-pods")).toHaveText(/^\d+\/\d+$/);
    await page.screenshot({
      path: testInfo.outputPath("frc-seed-edit-ignored.png"),
      fullPage: true,
    });
    const stale = await request.post(demoPath("/api/federation"), {
      headers: { origin: baseURL! },
      data: {
        action: "apply",
        yaml: stringify(original),
        revision: initial.revision,
      },
    });
    expect(stale.status()).toBe(409);
  } finally {
    const latest = await snapshot();
    const response = await request.post(demoPath("/api/federation"), {
      headers: { origin: baseURL! },
      data: {
        action: "apply",
        yaml: stringify(original),
        revision: latest.revision,
      },
    });
    expect(response.status(), await response.text()).toBe(200);
    await expect
      .poll(async () => memberWorkers(await snapshot()), {
        timeout: 120000,
        intervals: [3000],
      })
      .toBe(originalMemberWorkers);
  }
});

test("API forwards requests regardless of origin while retaining format and size bounds", async ({
  request,
  baseURL,
}) => {
  const before: Snapshot = await (
    await request.get(demoPath("/api/federation"))
  ).json();
  const yaml = stringify(manifest(before.federation!));
  expect(JSON.stringify(before)).not.toMatch(
    /certificate-authority-data|client-key-data|\/private\//,
  );
  const forwarded = await request.post(demoPath("/api/federation"), {
    headers: { origin: "https://untrusted.example" },
    data: { action: "validate", yaml, revision: before.revision },
  });
  expect(forwarded.status(), await forwarded.text()).toBe(200);
  const secret = await request.post(demoPath("/api/federation"), {
    headers: { origin: baseURL! },
    data: {
      action: "apply",
      yaml: "apiVersion: v1\nkind: Secret",
      revision: before.revision,
    },
  });
  expect(secret.status()).toBe(422);
  const oversized = await request.post(demoPath("/api/federation"), {
    headers: { origin: baseURL! },
    data: {
      action: "apply",
      yaml: "x".repeat(500000),
      revision: before.revision,
    },
  });
  expect(oversized.status()).toBe(413);
});
