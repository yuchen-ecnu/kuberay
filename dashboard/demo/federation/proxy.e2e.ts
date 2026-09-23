import { expect, test } from "@playwright/test";

test("workspace proxy loads assets, validates YAML and opens the Ray Dashboard", async ({
  page,
  context,
  request,
}, testInfo) => {
  const base = process.env.FEDERATION_DEMO_PROXY_URL?.replace(/\/+$/, "");
  test.skip(!base, "Set FEDERATION_DEMO_PROXY_URL to the workspace proxy URL.");
  const errors: string[] = [];
  const assets: string[] = [];
  const failedAssets: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.url().includes("/_next/")) assets.push(request.url());
  });
  page.on("requestfailed", (request) => {
    if (request.url().includes("/_next/")) failedAssets.push(request.url());
  });
  page.on("response", (response) => {
    if (response.url().includes("/_next/") && !response.ok())
      failedAssets.push(`${response.status()} ${response.url()}`);
  });
  await page.goto(`${base}/federation`);
  await expect(page.getByTestId("ready-pods")).toHaveText(/^([1-9]\d*)\/\1$/);
  const rayPods = parseInt(
    (await page.getByTestId("ready-pods").textContent())!,
    10,
  );
  await expect(page.getByTestId("cluster-member-b")).toBeVisible();
  expect(assets.length).toBeGreaterThan(0);
  expect(assets.every((url) => url.startsWith(`${base}/_next/`))).toBe(true);
  expect(failedAssets).toEqual([]);
  await page
    .getByRole("button", {
      name: "Kubernetes / Operator validation",
      exact: true,
    })
    .click();
  await expect(page.getByRole("status")).toContainText("admission passed");
  await page.screenshot({
    path: testInfo.outputPath("proxy-demo.png"),
    fullPage: true,
  });

  const canonical = await request.get(`${base}/ray-dashboard`, {
    maxRedirects: 0,
  });
  expect(canonical.status()).toBe(307);
  expect(
    new URL(canonical.headers().location, `${base}/ray-dashboard`).href,
  ).toBe(`${base}/ray-dashboard/`);
  const opened = context.waitForEvent("page");
  await page.getByRole("link", { name: "Dashboard" }).click();
  const dashboard = await opened;
  await expect(dashboard).toHaveTitle("Ray Dashboard");
  await expect(dashboard).toHaveURL(new RegExp("/proxy/3000/ray-dashboard/"));
  await dashboard.getByRole("link", { name: "Cluster", exact: true }).click();
  await expect(
    dashboard.getByRole("cell", { name: "ALIVE", exact: true }),
  ).toHaveCount(rayPods);
  expect(errors).toEqual([]);
});
