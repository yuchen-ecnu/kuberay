import { spawn, type ChildProcess } from "node:child_process";
import type { ClusterConfig } from "./server";
import { DemoError } from "./server";

export const DASHBOARD_PATH = "/ray-dashboard";

export interface DashboardTarget {
  cluster: ClusterConfig;
  podName: string;
  podUID: string;
  port: number;
}

interface TunnelProcess
  extends Pick<
    ChildProcess,
    "stdout" | "stderr" | "kill" | "killed" | "exitCode"
  > {
  on(event: "error" | "exit", listener: () => void): void;
}
type LaunchTunnel = (target: DashboardTarget) => TunnelProcess;

const launchTunnel: LaunchTunnel = (target) =>
  spawn(
    process.env.FEDERATION_KUBECTL ?? "kubectl",
    [
      "--kubeconfig",
      target.cluster.kubeconfig,
      ...(target.cluster.context ? ["--context", target.cluster.context] : []),
      "--namespace",
      target.cluster.namespace,
      "port-forward",
      `pod/${target.podName}`,
      `:${target.port}`,
      "--address=127.0.0.1",
      "--pod-running-timeout=10s",
    ],
    { stdio: ["ignore", "pipe", "pipe"], shell: false },
  );

/** One loopback-only tunnel, shared by assets and API calls, replaced on head UID changes. */
export class DashboardTunnel {
  private current?: {
    key: string;
    child: TunnelProcess;
    ready: Promise<string>;
  };
  constructor(
    private launch: LaunchTunnel = launchTunnel,
    private startupTimeout = 12000,
  ) {}

  connect(target: DashboardTarget): Promise<string> {
    const key = `${target.cluster.id}:${target.podUID}:${target.port}`;
    if (
      this.current?.key === key &&
      !this.current.child.killed &&
      this.current.child.exitCode === null
    )
      return this.current.ready;
    this.close();
    const child = this.launch(target);
    let settled = false;
    let output = "";
    const ready = new Promise<string>((resolve, reject) => {
      const fail = (message: string) => {
        clearTimeout(timer);
        if (this.current?.child === child) this.current = undefined;
        if (!settled) {
          settled = true;
          reject(new DemoError(message, 503));
        }
      };
      const timer = setTimeout(() => {
        fail(
          "Ray Dashboard connection timed out. Check that the head Pod is ready.",
        );
        child.kill();
      }, this.startupTimeout);
      timer.unref();
      child.on("error", () =>
        fail(
          "Could not start the Dashboard connection. Check server-side kubectl configuration.",
        ),
      );
      child.on("exit", () =>
        fail("Ray Dashboard connection closed. Refresh and try again."),
      );
      child.stdout?.on("data", (data: Buffer) => {
        output = (output + data.toString()).slice(-2048);
        const match = output.match(/Forwarding from 127\.0\.0\.1:(\d+) ->/);
        if (match && !settled) {
          settled = true;
          clearTimeout(timer);
          resolve(`http://127.0.0.1:${match[1]}`);
        }
      });
      // Drain diagnostics without returning credential paths or kubeconfig details to the browser.
      child.stderr?.on("data", () => {});
    });
    this.current = { key, child, ready };
    return ready;
  }

  close() {
    const previous = this.current;
    this.current = undefined;
    previous?.child.kill();
  }
}

export const dashboardTunnel = new DashboardTunnel();
process.once("exit", () => dashboardTunnel.close());

const hopByHop = [
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
];

export function dashboardURL(
  base: string,
  segments: string[],
  query: string,
  trailingSlash = false,
) {
  if (
    segments.some(
      (segment) =>
        segment === "." || segment === ".." || /[\\/\x00]/.test(segment),
    )
  )
    throw new DemoError("Invalid Dashboard path.");
  const url = new URL(base);
  url.pathname =
    "/" +
    segments.map(encodeURIComponent).join("/") +
    (trailingSlash && segments.length ? "/" : "");
  url.search = query;
  return url;
}

/** Preserve binary assets and streaming HTTP logs, including status codes and query parameters. */
export async function proxyDashboard(
  request: Request,
  base: string,
  segments: string[],
  fetchUpstream: typeof fetch = fetch,
) {
  const original = new URL(request.url);
  const target = dashboardURL(
    base,
    segments,
    original.search,
    original.pathname.endsWith("/"),
  );
  const headers = new Headers();
  for (const name of [
    "accept",
    "content-type",
    "range",
    "if-range",
    "if-none-match",
    "if-modified-since",
  ]) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  headers.set("accept-encoding", "identity");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  const options: RequestInit & { duplex?: "half" } = {
    method: request.method,
    headers,
    redirect: "manual",
    signal: AbortSignal.any([request.signal, controller.signal]),
  };
  if (!["GET", "HEAD"].includes(request.method) && request.body) {
    options.body = request.body;
    options.duplex = "half";
  }
  let upstream: Response;
  try {
    upstream = await fetchUpstream(target, options);
  } catch {
    throw new DemoError(
      "Ray Dashboard is temporarily unavailable. Wait for the head to become ready.",
      503,
    );
  } finally {
    clearTimeout(timeout);
  }

  const responseHeaders = new Headers(upstream.headers);
  const connectionHeaders = (responseHeaders.get("connection") ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
  for (const name of [
    ...hopByHop,
    ...connectionHeaders,
    "content-length",
    "content-encoding",
  ])
    responseHeaders.delete(name);
  const location = responseHeaders.get("location");
  if (location) {
    const redirect = new URL(location, target);
    if (redirect.origin !== target.origin) {
      await upstream.body?.cancel();
      throw new DemoError(
        "Dashboard returned an unexpected external redirect.",
        502,
      );
    }
    // Relative redirects preserve any prefix stripped by a workspace reverse proxy.
    const depth = segments.length - (original.pathname.endsWith("/") ? 0 : 1);
    const root = "../".repeat(Math.max(0, depth)) || "./";
    responseHeaders.set(
      "location",
      `${root}${redirect.pathname.slice(1)}${redirect.search}${redirect.hash}`,
    );
  }
  responseHeaders.set("cache-control", "no-store");
  responseHeaders.set("x-content-type-options", "nosniff");
  return new Response(request.method === "HEAD" ? null : upstream.body, {
    status: upstream.status,
    headers: responseHeaders,
  });
}
