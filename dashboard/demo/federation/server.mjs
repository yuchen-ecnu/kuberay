#!/usr/bin/env node
/** Next.js demo server with a WebSocket-to-Kubernetes terminal bridge. */

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import next from "next";
import { WebSocket, WebSocketServer } from "ws";

const here = dirname(fileURLToPath(import.meta.url));
const dashboardRoot = resolve(here, "../..");
const bridgePath = resolve(here, "terminal_bridge.py");
const hostname = process.env.HOSTNAME || "127.0.0.1";
const port = Number(process.env.PORT || 3000);
const dev = process.env.NODE_ENV === "development";
const proxyPath = (process.env.FEDERATION_DEMO_PROXY_PATH || "").replace(
  /\/+$/,
  "",
);
const terminalPaths = new Set([
  "/api/federation/terminal",
  ...(proxyPath ? [`${proxyPath}/api/federation/terminal`] : []),
]);
const dnsName = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;

async function terminalCluster(clusterId) {
  const filename = process.env.FEDERATION_DEMO_CONFIG;
  if (!filename || !isAbsolute(filename))
    throw new Error("FEDERATION_DEMO_CONFIG is not configured");
  const config = JSON.parse(await readFile(filename, "utf8"));
  const cluster = config.clusters?.find((item) => item.id === clusterId);
  if (
    !cluster ||
    !dnsName.test(cluster.id || "") ||
    !dnsName.test(cluster.namespace || "") ||
    !isAbsolute(cluster.kubeconfig || "")
  ) {
    throw new Error("unknown terminal cluster");
  }
  const target = cluster.terminalTarget || "deployment/federation-terminal";
  const container = cluster.terminalContainer || "shell";
  if (
    !/^(?:pod|deployment)\/[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(target) ||
    !dnsName.test(container)
  ) {
    throw new Error("invalid terminal target");
  }
  return { ...cluster, terminalTarget: target, terminalContainer: container };
}

function rejectUpgrade(socket, status = "400 Bad Request") {
  socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);
}

function startTerminal(ws, cluster) {
  const command = [
    process.env.FEDERATION_KUBECTL || "kubectl",
    "--kubeconfig",
    cluster.kubeconfig,
    ...(cluster.context ? ["--context", cluster.context] : []),
    "--namespace",
    cluster.namespace,
    "exec",
    "-i",
    "-t",
    cluster.terminalTarget,
    "--container",
    cluster.terminalContainer,
    "--",
    "/usr/bin/env",
    "TERM=xterm-256color",
    "COLORTERM=truecolor",
    "/bin/bash",
    "--rcfile",
    "/etc/federation-terminal/bashrc",
    "-i",
  ];
  const bridge = spawn(
    process.env.FEDERATION_PYTHON || "python3",
    [bridgePath, "--", ...command],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  let stderr = "";
  let finished = false;

  bridge.stdout.on("data", (chunk) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(chunk, { binary: true });
  });
  bridge.stderr.on("data", (chunk) => {
    stderr = (stderr + chunk.toString()).slice(-8000);
  });
  bridge.on("error", (error) => {
    stderr = error.message;
  });
  bridge.on("close", (code) => {
    finished = true;
    if (stderr && ws.readyState === WebSocket.OPEN) {
      const diagnostic = stderr.replaceAll(cluster.kubeconfig, "[kubeconfig]");
      ws.send(Buffer.from(`\r\n\x1b[31m${diagnostic.trim()}\x1b[0m\r\n`));
    }
    if (ws.readyState === WebSocket.OPEN)
      ws.close(code === 0 ? 1000 : 1011, "shell exited");
  });
  bridge.stdin.on("error", () => {});

  ws.on("message", (payload, isBinary) => {
    if (isBinary || payload.length > 128 * 1024 || !bridge.stdin.writable)
      return;
    try {
      const message = JSON.parse(payload.toString());
      if (
        message.type === "input" &&
        typeof message.data === "string" &&
        Buffer.byteLength(message.data) <= 64 * 1024
      ) {
        bridge.stdin.write(`${JSON.stringify(message)}\n`);
      } else if (
        message.type === "resize" &&
        Number.isInteger(message.rows) &&
        Number.isInteger(message.cols) &&
        message.rows >= 2 &&
        message.rows <= 500 &&
        message.cols >= 2 &&
        message.cols <= 1000
      ) {
        bridge.stdin.write(`${JSON.stringify(message)}\n`);
      }
    } catch {
      // Ignore malformed terminal controls; the shell process is untouched.
    }
  });
  ws.on("close", () => {
    if (finished) return;
    bridge.stdin.end();
    setTimeout(() => {
      if (!finished) bridge.kill("SIGTERM");
    }, 2000).unref();
  });
  ws.on("error", () => {});
}

if (!dev) {
  const manifest = JSON.parse(
    await readFile(
      resolve(dashboardRoot, ".next/required-server-files.json"),
      "utf8",
    ),
  );
  const builtProxyPath = (manifest.config?.assetPrefix || "").replace(
    /\/+$/,
    "",
  );
  if (builtProxyPath !== proxyPath) {
    throw new Error(
      `Demo asset prefix mismatch: built with ${JSON.stringify(builtProxyPath)}, running with ${JSON.stringify(proxyPath)}. Rebuild with FEDERATION_DEMO_PROXY_PATH set.`,
    );
  }
}

const app = next({ dev, dir: dashboardRoot, hostname, port });
await app.prepare();
const requestHandler = app.getRequestHandler();
const upgradeHandler = app.getUpgradeHandler();
const server = createServer((request, response) =>
  requestHandler(request, response),
);
const terminals = new WebSocketServer({
  noServer: true,
  maxPayload: 128 * 1024,
});

server.on("upgrade", async (request, socket, head) => {
  const url = new URL(request.url || "/", "http://localhost");
  if (!terminalPaths.has(url.pathname)) {
    upgradeHandler(request, socket, head);
    return;
  }
  try {
    const cluster = await terminalCluster(
      url.searchParams.get("cluster") || "",
    );
    terminals.handleUpgrade(request, socket, head, (ws) => {
      terminals.emit("connection", ws, request, cluster);
    });
  } catch {
    rejectUpgrade(socket);
  }
});

const heartbeat = setInterval(() => {
  for (const ws of terminals.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
heartbeat.unref();
terminals.on("connection", (ws, _request, cluster) => {
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });
  startTerminal(ws, cluster);
});

server.listen(port, hostname, () => {
  console.log(`Federation demo ready on http://${hostname}:${port}`);
});
