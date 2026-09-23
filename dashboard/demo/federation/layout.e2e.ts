import { expect, test, type Page } from "@playwright/test";
import { parse, stringify } from "yaml";
import { editorValue } from "./editor";

async function collisions(page: Page) {
  return page.evaluate(() => {
    const nodes = [
      ...document.querySelectorAll<HTMLElement>(".react-flow__node-entity"),
    ].map((node) => ({
      id: node.dataset.id!,
      box: node.getBoundingClientRect(),
    }));
    const problems: string[] = [];
    const minimap = document
      .querySelector(".react-flow__minimap")
      ?.getBoundingClientRect();
    for (const [index, node] of nodes.entries()) {
      if (
        minimap &&
        node.box.left < minimap.right &&
        node.box.right > minimap.left &&
        node.box.top < minimap.bottom &&
        node.box.bottom > minimap.top
      )
        problems.push(`minimap covers ${node.id}`);
      for (const other of nodes.slice(index + 1)) {
        if (
          node.box.left < other.box.right &&
          node.box.right > other.box.left &&
          node.box.top < other.box.bottom &&
          node.box.bottom > other.box.top
        )
          problems.push(`${node.id} overlaps ${other.id}`);
      }
    }
    for (const edge of document.querySelectorAll<SVGGElement>(
      ".react-flow__edge",
    )) {
      const path = edge.querySelector<SVGPathElement>(
        ".react-flow__edge-path",
      )!;
      const transform = path.getScreenCTM()!;
      const length = path.getTotalLength();
      for (const node of nodes) {
        // Endpoints may touch their own cards; other cards must stay clear of the path.
        if (
          edge.dataset.id!.startsWith(`control:${node.id}:`) ||
          edge.dataset.id!.endsWith(`:${node.id}`)
        )
          continue;
        for (let distance = 0; distance <= length; distance += 3) {
          const point = path
            .getPointAtLength(distance)
            .matrixTransform(transform);
          if (
            point.x > node.box.left + 1 &&
            point.x < node.box.right - 1 &&
            point.y > node.box.top + 1 &&
            point.y < node.box.bottom - 1
          ) {
            problems.push(`${edge.dataset.id} crosses ${node.id}`);
            break;
          }
        }
      }
    }
    return problems;
  });
}

test("control layout stays clear with replica previews", async ({
  page,
}, testInfo) => {
  const base = process.env.FEDERATION_DEMO_PROXY_URL?.replace(/\/+$/, "") ?? "";
  await page.goto(`${base}/federation`);
  await expect(page.getByTestId("ready-pods")).toHaveText(/^([1-9]\d*)\/\1$/);
  await expect
    .poll(() =>
      page
        .locator(".react-flow__edge-path")
        .first()
        .evaluate((element) => (element as SVGPathElement).getTotalLength()),
    )
    .toBeGreaterThan(0);
  await expect.poll(() => collisions(page)).toEqual([]);
  const primary = (await page.getByTestId("cluster-primary").boundingBox())!;
  const member = (await page.getByTestId("cluster-member-b").boundingBox())!;
  expect(member.y).toBeGreaterThan(primary.y + primary.height);
  const resources = await page.getByTestId("graph-raycluster").all();
  expect(
    Math.abs(
      (await resources[0].boundingBox())!.x -
        (await resources[1].boundingBox())!.x,
    ),
  ).toBeLessThan(1);
  await page.screenshot({
    path: testInfo.outputPath("layout-english.png"),
    fullPage: true,
  });
  const editor = page.getByLabel("FederatedRayCluster YAML editor");
  const draft = parse(await editorValue(editor));
  draft.spec.primaryCluster.workerGroups.push({
    ...structuredClone(draft.spec.primaryCluster.workerGroups[0]),
    groupName: "new-primary",
    replicas: 3,
  });
  draft.spec.memberClusters[0].workerGroups.push({
    ...structuredClone(draft.spec.memberClusters[0].workerGroups[0]),
    groupName: "new-member",
    replicas: 2,
  });
  await editor.fill(stringify(draft));
  await expect(page.getByTestId("planned-node")).toHaveCount(5);
  await expect.poll(() => collisions(page)).toEqual([]);
  await page.getByRole("button", { name: "Reload latest" }).click();
  await expect(page.getByTestId("planned-node")).toHaveCount(0);
  await expect.poll(() => collisions(page)).toEqual([]);
});
