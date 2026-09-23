import { expect, test } from "@playwright/test";
import { parse, stringify } from "yaml";
import { editorValue } from "./editor";

const demoURL = process.env.FEDERATION_DEMO_PROXY_URL
  ? `${process.env.FEDERATION_DEMO_PROXY_URL.replace(/\/+$/, "")}/federation`
  : "/federation";

test("English demo preserves drafts and layout through validation and refresh", async ({
  page,
}, testInfo) => {
  const actions: string[] = [];
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      request.url().endsWith("/api/federation")
    )
      actions.push(request.postDataJSON().action);
  });
  await page.goto(demoURL);
  await expect(page.locator("html")).toHaveAttribute("lang", "en");
  await expect(page.getByTestId("ready-pods")).toHaveText(/^([1-9]\d*)\/\1$/);
  const editor = page.getByLabel("FederatedRayCluster YAML editor");
  const original = await editorValue(editor);
  const draft = original + "\n# Demo note stays as user data\n";
  await editor.fill(draft);
  await page
    .getByRole("button", {
      name: "Kubernetes / Operator validation",
      exact: true,
    })
    .click();
  await expect(page.getByRole("status")).toContainText("admission passed");
  const controller = page.locator('.react-flow__node[data-id="controller"]');
  const box = (await controller.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    box.x + box.width / 2 + 30,
    box.y + box.height / 2 + 15,
    {
      steps: 10,
    },
  );
  await page.mouse.up();
  const position = await controller.getAttribute("style");
  const viewport = await page
    .locator(".react-flow__viewport")
    .getAttribute("style");
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect.poll(() => editorValue(editor)).toBe(draft);
  await expect(controller).toHaveAttribute("style", position!);
  await expect(page.locator(".react-flow__viewport")).toHaveAttribute(
    "style",
    viewport!,
  );
  await expect(controller).toContainText(
    "Distribute desired state by groupName",
  );
  await page.screenshot({
    path: testInfo.outputPath("english-demo.png"),
    fullPage: true,
    animations: "disabled",
  });

  const invalid = parse(original);
  invalid.spec.primaryCluster.workerGroups[0].managedBy =
    "ray.io/federated-raycluster-controller";
  const invalidSource = stringify(invalid);
  await editor.fill(invalidSource);
  await page
    .getByRole("button", {
      name: "Kubernetes / Operator validation",
      exact: true,
    })
    .click();
  await expect(
    page.getByRole("status").filter({ hasText: "managedBy must be omitted" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Reload latest" }).click();
  await page.getByTestId("pod-node").first().click();
  await expect(page.getByRole("dialog")).toContainText("Namespace");
  await expect(page.getByRole("dialog")).toContainText("Requests");
  await page.getByRole("button", { name: "Close Pod details" }).click();
  await page.reload();
  await expect(page.getByRole("heading", { name: "Topology" })).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(390);
  await page.screenshot({
    path: testInfo.outputPath("english-mobile.png"),
    fullPage: true,
  });
  expect(actions).toEqual(["validate", "validate"]);
  expect(errors).toEqual([]);
});
