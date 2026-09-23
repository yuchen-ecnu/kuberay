#!/usr/bin/env python3
"""Bridge newline-delimited terminal controls to a child process on a PTY."""

import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios


def resize(fd, rows, cols):
    rows = max(2, min(int(rows), 500))
    cols = max(2, min(int(cols), 1000))
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def write_all(fd, data):
    while data:
        data = data[os.write(fd, data):]


def terminate(child):
    if child.poll() is not None:
        return
    try:
        os.killpg(child.pid, signal.SIGTERM)
        child.wait(timeout=2)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--":
        raise SystemExit("usage: terminal_bridge.py -- COMMAND [ARG ...]")

    master, slave = pty.openpty()
    resize(master, os.environ.get("LINES", 30), os.environ.get("COLUMNS", 120))

    def child_setup():
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)

    child = subprocess.Popen(
        sys.argv[2:],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        close_fds=True,
        preexec_fn=child_setup,
        env={**os.environ, "TERM": "xterm-256color", "COLORTERM": "truecolor"},
    )
    os.close(slave)
    controls = bytearray()
    input_fd = sys.stdin.fileno()
    output_fd = sys.stdout.fileno()

    try:
        while True:
            readable, _, _ = select.select([master, input_fd], [], [], 0.25)
            if master in readable:
                try:
                    data = os.read(master, 65536)
                except OSError:
                    break
                if not data:
                    break
                write_all(output_fd, data)
            if input_fd in readable:
                data = os.read(input_fd, 65536)
                if not data:
                    break
                controls.extend(data)
                while b"\n" in controls:
                    line, _, remainder = controls.partition(b"\n")
                    controls = bytearray(remainder)
                    try:
                        message = json.loads(line)
                        if message.get("type") == "input":
                            write_all(master, str(message.get("data", "")).encode())
                        elif message.get("type") == "resize":
                            resize(master, message.get("rows", 30), message.get("cols", 120))
                    except (ValueError, TypeError, OSError, UnicodeError):
                        continue
            if child.poll() is not None and master not in readable:
                break
    finally:
        terminate(child)
        os.close(master)
    return child.returncode or 0


if __name__ == "__main__":
    raise SystemExit(main())
