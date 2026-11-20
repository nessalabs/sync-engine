#!/usr/bin/env python3
"""Drive Swift's bound v1 Head against the real Rust loopback-only fixture."""
import argparse
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
SCOPE = ("device-a", "example-origin", "transcript", "first", "text-v1", "local-epoch-1")
SCOPE_BYTES = b"".join(bytes([len(s)]) + s.encode() for s in SCOPE)
TOKEN = b"fixture-only"
LIMITS = struct.pack(">BIH", 1, 2048, 1)


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


def control(sock, kind):
    count, = struct.unpack(">I", exact(sock, 4))
    require(7 <= count <= 2048, "control length")
    frame = exact(sock, count)
    require(frame[:7] == b"NSC1\0\1" + bytes([kind]), "control kind")
    return frame[7:]


def bind(sock):
    sock.sendall(bytes([len(TOKEN)]) + TOKEN)
    require(exact(sock, 1) == b"\0", "fixture authorization")
    body = b"NSC1\0\1\6" + LIMITS + struct.pack(">Q", 1) + SCOPE_BYTES
    sock.sendall(struct.pack(">I", len(body)) + body)
    welcome = control(sock, 2)
    require(len(welcome) == 19 and welcome[8:15] == LIMITS, "welcome limits")
    session, = struct.unpack(">Q", welcome[:8])
    require(session != 0, "welcome session")
    bound = control(sock, 4)
    require(len(bound) == 36 + len(SCOPE_BYTES), "bound length")
    echo_session, request, handle, generation = struct.unpack(">QQIQ", bound[:28])
    require((echo_session, request) == (session, 1), "bound correlation")
    require(bound[28:-8] == SCOPE_BYTES, "bound scope")
    return session, handle, generation


def wait_port(server, log):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and server.poll() is None:
        port = re.search(rb"fixture session listening on 127\.0\.0\.1:(\d+)", log.read_bytes())
        if port:
            return int(port[1])
        time.sleep(0.01)
    raise RuntimeError(f"fixture not ready: {log.read_text()}")


def run(rust, swift, readers, writers, repetitions, expected):
    with tempfile.TemporaryDirectory(prefix="swift-head-e2e-") as temp:
        root = Path(temp)
        source, log = root / "source.sqlite", root / "server.log"
        subprocess.run([rust, "append", source], check=True, timeout=10, capture_output=True)
        with log.open("wb") as stream:
            with subprocess.Popen(
                [rust, "serve-caps", source, "0", TOKEN.decode(), str(readers), str(writers)],
                stdout=subprocess.DEVNULL, stderr=stream,
            ) as server:
                try:
                    port = wait_port(server, log)
                    for _ in range(repetitions):
                        with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
                            session, handle, generation = bind(sock)
                            # Pass a real blocking connected socket, not a Python read/write
                            # proxy. This isolated process has our absolute watchdog.
                            sock.setblocking(True)
                            result = subprocess.run(
                                [swift, str(session), str(handle), str(generation), "2048", *SCOPE],
                                stdin=sock, stdout=sock, stderr=subprocess.PIPE, timeout=8,
                            )
                            output = result.stderr.decode().splitlines()
                            require(result.returncode == 0 and output == [expected, expected], output)
                finally:
                    if server.poll() is None:
                        server.terminate()
                    try:
                        server.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()


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
    print(json.dumps({
        "peer": "rust-session_records-capability-v1",
        "connections": 3 * args.repetitions,
        "head_operations": 6 * args.repetitions,
        "elapsed_seconds": round(time.monotonic() - start, 3),
    }))


if __name__ == "__main__":
    main()
