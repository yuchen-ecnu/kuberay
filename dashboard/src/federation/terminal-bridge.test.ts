import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { resolve } from "node:path";
import { test } from "node:test";

test("PTY bridge supports shell syntax and terminal resizing", async () => {
  const child = spawn(
    "python3",
    [
      resolve("demo/federation/terminal_bridge.py"),
      "--",
      "/bin/bash",
      "--noprofile",
      "--norc",
      "-i",
    ],
    { cwd: resolve("."), stdio: ["pipe", "pipe", "pipe"] },
  );
  let output = "";
  child.stdout.on("data", (chunk) => {
    output += chunk.toString();
  });
  child.stderr.on("data", (chunk) => {
    output += chunk.toString();
  });
  child.stdin.write(
    `${JSON.stringify({ type: "resize", rows: 44, cols: 133 })}\n`,
  );
  child.stdin.write(
    `${JSON.stringify({
      type: "input",
      data: "printf '__PIPE__:%s\\n' \"$(printf 'alpha\\nbeta\\n' | tail -n 1)\"; printf '__SIZE__:'; stty size; exit\r",
    })}\n`,
  );

  const timeout = setTimeout(() => child.kill("SIGKILL"), 10000);
  const [code] = (await once(child, "close")) as [number];
  clearTimeout(timeout);
  assert.equal(code, 0, output);
  assert.match(output, /__PIPE__:beta/);
  assert.match(output, /__SIZE__:44 133/);
});
