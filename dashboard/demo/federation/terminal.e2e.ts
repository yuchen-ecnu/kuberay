import { expect, type Locator, type Page, test } from "@playwright/test";

const proxy = process.env.FEDERATION_DEMO_PROXY_URL?.replace(/\/+$/, "") ?? "";

async function command(page: Page, terminal: Locator, source: string) {
  const input = terminal
    .getByTestId("terminal-xterm")
    .locator(".xterm-helper-textarea");
  await input.focus();
  await page.keyboard.insertText(source);
  await page.keyboard.press("Enter");
}

test("interactive terminal runs shell syntax against both demo clusters", async ({
  page,
}) => {
  await page.goto(`${proxy}/federation`);
  await page.getByRole("button", { name: "Terminal", exact: true }).click();
  const terminal = page.getByRole("region", { name: "Cluster terminal" });
  await expect(terminal).toBeVisible();
  await expect(terminal.getByText("Connected", { exact: true })).toBeVisible({
    timeout: 30000,
  });

  await terminal.getByRole("button", { name: "Fullscreen terminal" }).click();
  await expect(terminal).toHaveCSS("position", "fixed");
  await command(
    page,
    terminal,
    "printf 'alpha\\nbeta\\n' | tail -n 1 && kubectl get frc ray-demo -o name",
  );
  await expect(terminal.getByTestId("terminal-xterm")).toContainText("beta");
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "federatedraycluster.ray.io/ray-demo",
  );

  await terminal
    .getByTestId("terminal-xterm")
    .locator(".xterm-helper-textarea")
    .press("Escape");
  await expect(terminal).not.toHaveCSS("position", "fixed");
  await command(page, terminal, "seq 1 30; echo __BOTTOM_SAFE__");
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "__BOTTOM_SAFE__",
  );
  await expect
    .poll(() =>
      terminal.evaluate((section) => {
        const host = section.querySelector('[data-testid="terminal-xterm"]');
        const rows = section.querySelectorAll(".xterm-rows > div");
        const lastRow = rows.item(rows.length - 1);
        if (!host || !lastRow) return 0;
        return (
          host.getBoundingClientRect().bottom -
          lastRow.getBoundingClientRect().bottom
        );
      }),
    )
    .toBeGreaterThanOrEqual(8);
  await command(
    page,
    terminal,
    "export FEDERATION_SWITCH_MARKER=primary-session; printf '<set:%s>\\n' \"$FEDERATION_SWITCH_MARKER\"",
  );
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "<set:primary-session>",
  );
  await terminal.getByRole("tab", { name: "frc-member", exact: true }).click();
  await expect(terminal.getByText("Connected", { exact: true })).toBeVisible({
    timeout: 30000,
  });
  await command(
    page,
    terminal,
    "echo member-shell > /workspace/result && cat /workspace/result && kubectl get raycluster -l ray.io/federation-name=ray-demo,ray.io/federation-member=member-b -o jsonpath='{.items[0].spec.workerGroupSpecs[0].groupName}'; echo",
  );
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "member-shell",
  );
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "member-cpu",
  );

  await terminal.getByRole("tab", { name: "frc-primary", exact: true }).click();
  await expect(terminal.getByText("Connected", { exact: true })).toBeVisible();
  await command(
    page,
    terminal,
    "printf '<after:%s>\\n' \"$FEDERATION_SWITCH_MARKER\"",
  );
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "<after:primary-session>",
  );

  const route = terminal.getByRole("link", {
    name: "Open terminal in a new tab",
  });
  await expect(route).toHaveAttribute("href", /\/terminal$/);
  await terminal.getByRole("button", { name: "Close terminal" }).click();
  await expect(terminal).not.toBeVisible();
});

test("standalone terminal route opens a full-page shell", async ({ page }) => {
  await page.goto(`${proxy}/terminal`);
  const terminal = page.getByRole("region", { name: "Cluster terminal" });
  await expect(terminal).toHaveCSS("position", "fixed");
  await expect(terminal.getByText("Connected", { exact: true })).toBeVisible({
    timeout: 30000,
  });
  await command(page, terminal, "echo standalone-route-ok");
  await expect(terminal.getByTestId("terminal-xterm")).toContainText(
    "standalone-route-ok",
  );
  await expect(
    terminal.getByRole("link", { name: "Federation Lab" }),
  ).toHaveAttribute("href", "./federation");
});
