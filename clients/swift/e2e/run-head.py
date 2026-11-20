#!/usr/bin/env python3
"""Drive Swift's bound v1 Head against the real Rust loopback-only fixture."""
import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
SCOPE = ("device-a", "example-origin", "transcript", "first", "text-v1", "local-epoch-1")
SCOPE_BYTES = b"".join(bytes([len(s)]) + s.encode() for s in SCOPE)
TOKEN = b"fixture-only"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise EOFError("short fixture response")
        result.extend(chunk)
    return bytes(result)


def authenticate_fixture(sock):
    sock.sendall(bytes([len(TOKEN)]) + TOKEN)
    require(exact(sock, 1) == b"\0", "fixture authorization")


def wait_port(server, log):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and server.poll() is None:
        port = re.search(rb"fixture session listening on 127\.0\.0\.1:(\d+)", log.read_bytes())
        if port:
            return int(port[1])
        time.sleep(0.01)
    raise RuntimeError(f"fixture not ready: {log.read_text()}")


@contextmanager
def server_fixture(rust, root, readers, writers):
    source, log = root / "source.sqlite", root / "server.log"
    subprocess.run([rust, "append", source], check=True, timeout=10, capture_output=True)
    with log.open("wb") as stream:
        with subprocess.Popen(
            [rust, "serve-caps", source, "0", TOKEN.decode(), str(readers), str(writers)],
            stdout=subprocess.DEVNULL, stderr=stream,
        ) as server:
            try:
                yield wait_port(server, log)
            finally:
                if server.poll() is None:
                    server.terminate()
                try:
                    server.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


def run_swift(swift, port, mode="head", state=None, scope=SCOPE):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        authenticate_fixture(sock)
        # Pass a real blocking connected socket, not a Python read/write
        # proxy. This isolated process has our absolute watchdog.
        sock.setblocking(True)
        argv = [swift, *scope, mode]
        if state is not None:
            argv.append(state)
        result = subprocess.run(
            argv, stdin=sock, stdout=sock, stderr=subprocess.PIPE, timeout=8,
        )
    output = result.stderr.decode().splitlines()
    require(result.returncode == 0, output)
    return output


def run(rust, swift, readers, writers, repetitions, expected):
    with tempfile.TemporaryDirectory(prefix="swift-head-e2e-") as temp:
        with server_fixture(rust, Path(temp), readers, writers) as port:
            for _ in range(repetitions):
                output = run_swift(swift, port)
                require(output == [expected, expected], output)


def run_pages(rust, swift, repetitions):
    with tempfile.TemporaryDirectory(prefix="swift-page-e2e-") as temp:
        root = Path(temp)
        state = root / "records.json"
        with server_fixture(rust, root, 0, 0) as port:
            for _ in range(repetitions):
                output = run_swift(swift, port, "deny-open", scope=("fixture-denied", *SCOPE[1:]))
                require(output == ["open-denied"], output)
                state.write_text('{"rows":[]}')
                for mode, expected in (("first", "page:2:2"), ("interrupt", "interrupted"),
                                        ("refuse", "oversized"), ("resume", "page:7:5")):
                    before = state.read_bytes()
                    output = run_swift(swift, port, mode, state)
                    require(output == [expected], output)
                    if mode in ("interrupt", "refuse"):
                        require(state.read_bytes() == before, "refusal advanced fixture storage")
                saved = json.loads(state.read_bytes())
                require(base64.b64decode(saved["scope"]) == SCOPE_BYTES, "saved exact scope")
                rows = saved["rows"]
                require([r["position"] for r in rows] == list(range(1, 8)), "positions")
                ids = [f"fact-{i}" for i in range(1, 7)] + ["bulk-fact"]
                require([r["id"] for r in rows] == ids, "record identities")
                payloads = [b"alpha", b"beta", b"gamma", b"delta", b"epsilon", b"zeta", b"p" * 4096]
                require([base64.b64decode(r["payload"]) for r in rows] == payloads, "record bytes")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=5, choices=range(1, 101), metavar="1..100")
    parser.add_argument("--rust-bin", type=Path)
    parser.add_argument("--swift-bin", type=Path)
    args = parser.parse_args()
    os.chdir(ROOT)
    if args.rust_bin is None:
        build = subprocess.run(
            ["cargo", "build", "--locked", "--features", "transport,sqlite",
             "--example", "session_records", "--message-format=json"],
            check=True, stdout=subprocess.PIPE, timeout=240,
        )
        args.rust_bin = next(
            Path(value["executable"]) for line in build.stdout.splitlines()
            if (value := json.loads(line)).get("executable") and
            value["target"]["name"] == "session_records"
        )
    if args.swift_bin is None:
        subprocess.run(["swift", "build", "--product", "SwiftHeadFixture"], check=True, timeout=120)
        bin_path = subprocess.check_output(["swift", "build", "--show-bin-path"], text=True, timeout=30)
        args.swift_bin = Path(bin_path.strip()) / "SwiftHeadFixture"
    rust, swift = args.rust_bin.resolve(), args.swift_bin.resolve()
    start = time.monotonic()
    run(rust, swift, 0, 0, args.repetitions, "head:7:0")
    run(rust, swift, 0, (1 << 64) - 1, args.repetitions, f"head:7:{(1 << 64) - 1}")
    run(rust, swift, 1, 0, args.repetitions, "missing-reader:1")
    run_pages(rust, swift, args.repetitions)
    print(json.dumps({
        "peer": "rust-session_records-capability-v1",
        "connections": 3 * args.repetitions,
        "head_operations": 6 * args.repetitions,
        "successful_swift_opens": 7 * args.repetitions,
        "refused_swift_opens": args.repetitions,
        "page_connections": 4 * args.repetitions,
        "page_operations": 4 * args.repetitions,
        "reopened_exact_records": 7 * args.repetitions,
        "elapsed_seconds": round(time.monotonic() - start, 3),
    }))


if __name__ == "__main__":
    main()
