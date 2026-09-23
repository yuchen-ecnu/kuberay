import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "*.e2e.ts",
  workers: 1,
  timeout: 240000,
  expect: { timeout: 15000 },
  outputDir: "./test-results",
  reporter: "list",
  use: {
    baseURL: process.env.FEDERATION_DEMO_URL ?? "http://127.0.0.1:3000",
    viewport: { width: 1600, height: 1200 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
      args: ["--no-sandbox"],
    },
  },
});
