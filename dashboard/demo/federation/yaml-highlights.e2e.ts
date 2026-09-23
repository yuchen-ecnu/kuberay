import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import { parse } from "yaml";
import { toYaml, type Snapshot } from "../../src/federation/model";
import { editorValue } from "./editor";

test("FRC, PRC and MRC design highlights navigate to real fields without changing YAML or downloads", async ({
  page,
  request,
}, testInfo) => {
  const base = process.env.FEDERATION_DEMO_PROXY_URL?.replace(/\/+$/, "") ?? "";
  const snapshot: Snapshot = await (
    await request.get(`${base}/api/federation`)
  ).json();
  const primary = snapshot.clusters.find(
    (cluster) => cluster.role === "primary",
  )!;
  const member = snapshot.clusters.find(
    (cluster) => cluster.role === "member",
  )!;
  const writes: string[] = [];
  page.on("request", (request) => {
    if (request.method() === "POST") writes.push(request.url());
  });
  await page.goto(`${base}/federation`);
  const editor = page.getByLabel("FederatedRayCluster YAML editor");
  await expect(editor).toContainText("FederatedRayCluster");
  const frcSource = await editorValue(editor);
  const fold = page.locator('.cm-foldGutter [title="Fold line"]').first();
  await expect(fold).toBeVisible();
  await fold.click();
  await expect(page.locator(".cm-foldPlaceholder").first()).toBeVisible();
  expect(await editorValue(editor)).toBe(frcSource);
  await page.locator(".cm-foldPlaceholder").first().click();
  await expect.poll(() => editorValue(editor)).toBe(frcSource);
  await expect(
    page.getByRole("complementary", { name: "Fields involved in this design" }),
  ).not.toBeVisible();
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  const frcDesign = page.getByRole("complementary", {
    name: "Fields involved in this design",
  });
  await frcDesign
    .getByRole("button", {
      name: "primaryCluster.enableInTreeAutoscaling",
      exact: true,
    })
    .click();
  await expect(
    page
      .getByLabel("YAML preview")
      .locator('[data-design-field="primaryCluster.enableInTreeAutoscaling"]'),
  ).toBeInViewport();
  expect(parse(await page.getByLabel("YAML preview").innerText())).toEqual(
    parse(frcSource),
  );
  await page.getByRole("tab", { name: "PRC", exact: true }).click();
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  const design = page.getByRole("complementary", {
    name: "Fields involved in this design",
  });
  await design
    .getByRole("button", { name: "managedBy", exact: true })
    .first()
    .click();
  const preview = page.getByLabel("YAML preview");
  const primaryYaml = toYaml(primary.resource!);
  const specLine = primaryYaml.split("\n").indexOf("spec:");
  const specFold = page.locator(`[data-fold-line="${specLine}"] button`);
  await expect(specFold).toHaveAttribute("aria-expanded", "true");
  await specFold.click();
  await expect(specFold).toHaveAttribute("aria-expanded", "false");
  await expect(preview).toContainText(/\d+ lines folded/);
  await expect(preview.locator('[data-design-field="managedBy"]')).toHaveCount(
    0,
  );
  await design
    .getByRole("button", { name: "managedBy", exact: true })
    .first()
    .click();
  await expect(specFold).toHaveAttribute("aria-expanded", "true");
  const managedBy = preview.locator('[data-design-field="managedBy"]');
  const managedByCount = primary.resource!.spec.workerGroupSpecs.filter(
    (group: { managedBy?: string }) => group.managedBy,
  ).length;
  await expect(managedBy).toHaveCount(managedByCount);
  await expect(managedBy.first()).toBeInViewport();
  const addedBackground = await managedBy
    .first()
    .evaluate((element) => getComputedStyle(element).backgroundColor);
  expect(addedBackground).not.toBe("rgba(0, 0, 0, 0)");
  await expect(managedBy.last()).toContainText(
    "managedBy: ray.io/federated-raycluster-controller",
  );
  expect(parse(await preview.innerText())).toEqual(parse(primaryYaml));
  await page.screenshot({
    path: testInfo.outputPath("prc-highlights.png"),
    fullPage: true,
  });

  await page.getByRole("tab", { name: `MRC · ${member.memberName}` }).click();
  await design
    .getByRole("button", { name: "rayStartParams.address", exact: true })
    .click();
  const address = preview.locator(
    '[data-design-field="rayStartParams.address"]',
  );
  await expect(address).toHaveCount(1);
  await expect(address).toBeInViewport();
  const reusedBackground = await address.evaluate(
    (element) => getComputedStyle(element).backgroundColor,
  );
  expect(reusedBackground).not.toBe(addedBackground);
  await expect(address).toContainText(
    snapshot.federation!.spec.networking.headEndpoint.address,
  );
  await expect(design).toContainText("headGroupSpec is omitted");
  await expect(
    design.getByRole("button", { name: "managedBy", exact: true }),
  ).not.toBeVisible();
  expect(parse(await preview.innerText())).toEqual(
    parse(toYaml(member.resource!)),
  );
  await design
    .getByRole("button", { name: "enableInTreeAutoscaling", exact: true })
    .click();
  await expect(
    preview.locator('[data-design-field="enableInTreeAutoscaling"]'),
  ).toBeInViewport();

  const englishDesign = page.getByRole("complementary", {
    name: "Fields involved in this design",
  });
  await expect(englishDesign).toContainText("Existing field");
  await expect(englishDesign).toContainText(
    "headGroupSpec is omitted; workers only.",
  );
  await expect(
    englishDesign.getByRole("button", {
      name: "rayStartParams.address",
      exact: true,
    }),
  ).toHaveAttribute("title", /^Existing rayStartParams.address:/);
  await page.screenshot({
    path: testInfo.outputPath("mrc-highlights.png"),
    fullPage: true,
  });
  const downloading = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download YAML" }).click();
  const download = await downloading;
  expect(await readFile((await download.path())!, "utf8")).toBe(
    toYaml(member.resource!),
  );
  expect(writes).toEqual([]);
});
