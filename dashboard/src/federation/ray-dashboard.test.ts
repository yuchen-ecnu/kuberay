import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { test } from "node:test";
import {
  DashboardTunnel,
  dashboardURL,
  proxyDashboard,
  type DashboardTarget,
} from "./ray-dashboard";

const target: DashboardTarget = {
  cluster: {
    id: "primary",
    label: "kind-primary",
    namespace: "demo",
    role: "primary",
    kubeconfig: "/private/kubeconfig",
  },
  podName: "ray-head-a",
  podUID: "uid-a",
  port: 8265,
};

class FakeProcess extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  killed = false;
  exitCode: number | null = null;
  kill() {
    this.killed = true;
    this.emit("exit", 0);
    return true;
  }
  ready(port = 43210) {
    this.stdout.write(`Forwarding from 127.0.0.1:${port} -> 8265\n`);
  }
}

test("concurrent Dashboard assets share one tunnel; head replacement retires it", async () => {
  const children: FakeProcess[] = [];
  const tunnel = new DashboardTunnel(() => {
    const child = new FakeProcess();
    children.push(child);
    return child;
  });
  try {
    const a = tunnel.connect(target);
    const b = tunnel.connect(target);
    assert.equal(children.length, 1);
    children[0].ready();
    assert.deepEqual(await Promise.all([a, b]), [
      "http://127.0.0.1:43210",
      "http://127.0.0.1:43210",
    ]);
    const replaced = tunnel.connect({
      ...target,
      podName: "ray-head-b",
      podUID: "uid-b",
    });
    assert.equal(children[0].killed, true);
    assert.equal(children.length, 2);
    children[1].ready(43211);
    assert.equal(await replaced, "http://127.0.0.1:43211");
  } finally {
    tunnel.close();
  }
});

test("failed tunnels release their cached endpoint and can reconnect", async () => {
  const children: FakeProcess[] = [];
  const tunnel = new DashboardTunnel(() => {
    const child = new FakeProcess();
    children.push(child);
    return child;
  });
  try {
    const first = tunnel.connect(target);
    children[0].emit("error", new Error("contains private kubeconfig details"));
    await assert.rejects(
      first,
      (error: Error) => !error.message.includes("private"),
    );
    const retry = tunnel.connect(target);
    children[1].ready();
    await retry;
    children[1].emit("exit", 1);
    const next = tunnel.connect(target);
    children[2].ready(43212);
    assert.equal(await next, "http://127.0.0.1:43212");
  } finally {
    tunnel.close();
  }
});

test("Dashboard URL preserves query strings and rejects path escapes", () => {
  const url = dashboardURL(
    "http://127.0.0.1:43210",
    ["api", "v0", "logs", "file"],
    "?filename=worker%2Fa.log&lines=100",
  );
  assert.equal(
    url.href,
    "http://127.0.0.1:43210/api/v0/logs/file?filename=worker%2Fa.log&lines=100",
  );
  assert.equal(
    dashboardURL("http://127.0.0.1:43210", ["api", "jobs"], "", true).href,
    "http://127.0.0.1:43210/api/jobs/",
  );
  for (const part of ["..", ".", "//evil.example", "a\\b"])
    assert.throws(() => dashboardURL("http://127.0.0.1:43210", [part], ""));
});

test("proxy preserves binary bodies and status while stripping hop-by-hop and decompressed length headers", async () => {
  const bytes = new Uint8Array([0, 255, 10, 80, 3]);
  const response = await proxyDashboard(
    new Request("http://demo/ray-dashboard/favicon.ico"),
    "http://127.0.0.1:43210",
    ["favicon.ico"],
    async () =>
      new Response(bytes, {
        status: 206,
        headers: {
          "content-type": "image/x-icon",
          "content-encoding": "gzip",
          "content-length": "300",
          connection: "x-hop",
          "x-hop": "remove",
          "transfer-encoding": "chunked",
        },
      }),
  );
  assert.equal(response.status, 206);
  assert.equal(response.headers.get("content-type"), "image/x-icon");
  assert.equal(response.headers.get("content-length"), null);
  assert.equal(response.headers.get("content-encoding"), null);
  assert.equal(response.headers.get("x-hop"), null);
  assert.deepEqual(new Uint8Array(await response.arrayBuffer()), bytes);
});

test("proxy forwards request method/body and rewrites only redirects within the dashboard", async () => {
  const request = new Request(
    "http://demo/ray-dashboard/api/jobs/job-1/stop/?x=1",
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: "not-for-upstream",
      },
      body: "{}",
    },
  );
  const response = await proxyDashboard(
    request,
    "http://127.0.0.1:43210",
    ["api", "jobs", "job-1", "stop"],
    async (url, init) => {
      assert.equal(
        String(url),
        "http://127.0.0.1:43210/api/jobs/job-1/stop/?x=1",
      );
      assert.equal(init!.method, "POST");
      assert.equal(new Headers(init!.headers).get("authorization"), null);
      assert.equal(
        await new Response(init!.body as ReadableStream).text(),
        "{}",
      );
      return new Response(null, {
        status: 307,
        headers: { location: "/#/jobs" },
      });
    },
  );
  assert.equal(
    new URL(response.headers.get("location")!, request.url).href,
    "http://demo/ray-dashboard/#/jobs",
  );
  await assert.rejects(
    proxyDashboard(
      new Request("http://demo/ray-dashboard/"),
      "http://127.0.0.1:43210",
      [],
      async () =>
        new Response(null, {
          status: 302,
          headers: { location: "https://external.example" },
        }),
    ),
    /external redirect/,
  );
});

test("Dashboard redirects retain a workspace proxy prefix at every route depth", async () => {
  const base = "https://workspace.example/proxy/3000/ray-dashboard/";
  for (const suffix of ["", "logs", "api/jobs", "api/jobs/"]) {
    const request = new Request(base + suffix);
    const response = await proxyDashboard(
      request,
      "http://127.0.0.1:43210",
      suffix.split("/").filter(Boolean),
      async () =>
        new Response(null, {
          status: 307,
          headers: { location: "/#/cluster" },
        }),
    );
    assert.equal(
      new URL(response.headers.get("location")!, request.url).href,
      base + "#/cluster",
    );
  }
});

test("streaming logs are returned before EOF and upstream failures are explicit", async () => {
  let streamController!: ReadableStreamDefaultController;
  const stream = new ReadableStream({
    start(controller) {
      streamController = controller;
      controller.enqueue(new TextEncoder().encode("first line\n"));
    },
  });
  const response = await proxyDashboard(
    new Request("http://demo/ray-dashboard/api/v0/logs/file"),
    "http://127.0.0.1:43210",
    ["api", "v0", "logs", "file"],
    async () => new Response(stream),
  );
  const reader = response.body!.getReader();
  assert.equal(
    new TextDecoder().decode((await reader.read()).value),
    "first line\n",
  );
  streamController.close();
  await reader.cancel();
  await assert.rejects(
    proxyDashboard(
      new Request("http://demo/ray-dashboard/"),
      "http://127.0.0.1:43210",
      [],
      async () => {
        throw new Error("ECONNREFUSED");
      },
    ),
    /temporarily unavailable/,
  );
});
