import { expect, test } from "@playwright/test";
import type { Snapshot } from "../../src/federation/model";

interface RayNode {
  ip: string;
  hostname: string;
  raylet: { state: string; nodeId: string };
}

test("demo opens the native Ray Dashboard with nodes from both kind clusters", async ({
  page,
  context,
  request,
}, testInfo) => {
  const snapshot: Snapshot = await (
    await request.get("/api/federation")
  ).json();
  const pods = snapshot.clusters.flatMap((cluster) =>
    cluster.pods.filter((pod) => pod.ready),
  );
  expect(pods.length).toBeGreaterThan(1);
  const errors: string[] = [];
  const failedAssets: string[] = [];
  context.on("page", (opened) => {
    opened.on("pageerror", (error) => errors.push(error.message));
    opened.on("response", (response) => {
      if (response.url().includes("/static/") && response.status() >= 400)
        failedAssets.push(response.url());
    });
  });
  await page.goto("/federation");
  const link = page.getByRole("link", { name: "Dashboard" });
  await expect(link).toHaveAttribute("href", "/ray-dashboard/");
  const opened = context.waitForEvent("page");
  await link.click();
  const dashboard = await opened;
  await expect(dashboard).toHaveTitle("Ray Dashboard");
  await dashboard.getByRole("link", { name: "Cluster", exact: true }).click();
  for (const pod of pods) {
    const row = dashboard.getByRole("row").filter({ hasText: pod.name });
    await expect(row).toContainText("ALIVE", { timeout: 60000 });
    await expect(row).toContainText(pod.ip!);
  }
  await expect(
    dashboard.getByRole("cell", { name: "ALIVE", exact: true }),
  ).toHaveCount(pods.length);
  await dashboard.screenshot({
    path: testInfo.outputPath("ray-dashboard.png"),
    fullPage: true,
  });
  const member = snapshot.clusters
    .find((cluster) => cluster.role === "member")!
    .pods.find((pod) => pod.role === "worker" && pod.ready)!;
  await dashboard
    .getByRole("row")
    .filter({ hasText: member.name })
    .getByRole("link", { name: "Log", exact: true })
    .click();
  const logResponse = dashboard.waitForResponse((response) =>
    response.url().includes("/api/v0/logs/file"),
  );
  await dashboard
    .getByRole("link", { name: "raylet.out", exact: true })
    .click();
  const log = await logResponse;
  expect(log.status()).toBe(200);
  const firstLine = (await log.text()).split("\n").find((line) => line.trim())!;
  await expect(dashboard.locator("main")).toContainText(firstLine.trim());
  await expect(
    dashboard.getByRole("link", { name: "Download log file" }),
  ).toBeVisible();
  await dashboard.screenshot({
    path: testInfo.outputPath("ray-dashboard-logs.png"),
    fullPage: true,
  });
  expect(errors).toEqual([]);
  expect(failedAssets).toEqual([]);
});

test("Dashboard proxy preserves routes, binary assets, remote logs and request forwarding", async ({
  request,
  baseURL,
}) => {
  const canonical = await request.get("/ray-dashboard?demo=1", {
    maxRedirects: 0,
  });
  expect(canonical.status()).toBe(307);
  expect(
    new URL(canonical.headers().location, baseURL + "/ray-dashboard").href,
  ).toBe(baseURL + "/ray-dashboard/?demo=1");
  const favicon = await request.get("/ray-dashboard/favicon.ico");
  expect(favicon.status()).toBe(200);
  expect(favicon.headers()["content-type"]).toContain("image/");
  expect((await favicon.body()).length).toBeGreaterThan(0);
  const head = await request.head("/ray-dashboard/favicon.ico");
  expect(head.status()).toBe(200);
  expect((await head.body()).length).toBe(0);

  const jobs = await request.get("/ray-dashboard/api/jobs/");
  expect(jobs.status()).toBe(200);
  expect(Array.isArray(await jobs.json())).toBe(true);
  const serve = await request.get("/ray-dashboard/api/serve/applications/");
  expect(serve.status()).toBe(200);
  expect(await serve.json()).toHaveProperty("applications");

  const nodes = await request.get("/ray-dashboard/nodes?view=summary");
  expect(nodes.status()).toBe(200);
  const summary: RayNode[] = (await nodes.json()).data.summary;
  const snapshot: Snapshot = await (
    await request.get("/api/federation")
  ).json();
  for (const cluster of snapshot.clusters) {
    const worker = cluster.pods.find(
      (pod) => pod.role === "worker" && pod.ready,
    )!;
    const node = summary.find(
      (item) => item.ip === worker.ip && item.raylet.state === "ALIVE",
    )!;
    expect(node).toBeTruthy();
    const logs = await request.get("/ray-dashboard/api/v0/logs", {
      params: { node_id: node.raylet.nodeId, glob: "raylet.out" },
    });
    expect(logs.status()).toBe(200);
    expect((await logs.json()).data.result.raylet).toContain("raylet.out");
    const content = await request.get("/ray-dashboard/api/v0/logs/file", {
      params: { node_id: node.raylet.nodeId, filename: "raylet.out", lines: 5 },
    });
    expect(content.status()).toBe(200);
    expect(content.headers()["content-type"]).toContain("text/plain");
    expect((await content.text()).trim().length).toBeGreaterThan(0);
  }

  const unknown = "/ray-dashboard/__demo_proxy_probe__";
  expect((await request.get(unknown)).status()).toBe(404);
  expect(
    (
      await request.post(unknown, {
        headers: { origin: baseURL! },
        data: {},
      })
    ).status(),
  ).toBe(405); // Ray rejects browser POST traffic before routing the request.
  for (const origin of [undefined, "https://untrusted.example"]) {
    const forwarded = await request.post(unknown, {
      headers: origin ? { origin } : undefined,
      data: {},
    });
    expect(forwarded.status()).toBe(405);
  }
});
