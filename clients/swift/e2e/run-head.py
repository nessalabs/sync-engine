#!/usr/bin/env python3
"""Drive Swift's v1 record client against the real Rust loopback-only fixture."""
import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import select
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


def wait_port(server, log, label=b"fixture session"):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and server.poll() is None:
        pattern = (re.escape(label) + b" " if label else b"") + rb"listening on 127\.0\.0\.1:(\d+)"
        port = re.search(pattern, log.read_bytes())
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


def expect_marker(child, marker, deadline):
    observed = bytearray()
    fd = child.stderr.fileno()
    while len(observed) < len(marker):
        remaining = max(0, deadline - time.monotonic())
        ready, _, _ = select.select([fd], [], [], remaining)
        require(ready, "timed out waiting for Swift fixture marker")
        part = os.read(fd, len(marker) - len(observed))
        require(part, "Swift child closed before expected marker")
        observed.extend(part)
    require(observed == marker, "unexpected Swift fixture marker")


def run_overlap(rust, swift, clients):
    with tempfile.TemporaryDirectory(prefix="swift-overlap-e2e-") as temp:
        with server_fixture(rust, Path(temp), 0, 0) as port:
            release, send = os.pipe()
            children = []
            try:
                for i in range(clients):
                    scope = (("device-a" if i % 2 == 0 else "device-b"), *SCOPE[1:])
                    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
                        authenticate_fixture(sock)
                        sock.setblocking(True)
                        child = subprocess.Popen(
                            [swift, *scope, "overlap", str(release)], stdin=sock, stdout=sock,
                            stderr=subprocess.PIPE, pass_fds=(release,),
                        )
                        children.append(child)
                deadline = time.monotonic() + 5
                for child in children:
                    expect_marker(child, b"bound\n", deadline)
                require(all(child.poll() is None for child in children), "client exited before release")
                # All independent Swift owners are bound, not merely connected;
                # each is blocked on this separate fixture pipe before data I/O.
                require(os.write(send, b"\1" * clients) == clients, "short release")
                deadline = time.monotonic() + 8
                for child in children:
                    remaining = max(0, deadline - time.monotonic())
                    _, report = child.communicate(timeout=remaining)
                    require(child.returncode == 0 and report == b"overlap:7\n", report)
            finally:
                os.close(release)
                os.close(send)
                for child in children:
                    if child.poll() is None:
                        child.kill()
                for child in children:
                    try:
                        child.wait(timeout=2)
                    finally:
                        child.stderr.close()


def run_follow(rust, swift):
    with tempfile.TemporaryDirectory(prefix="swift-follow-e2e-") as temp:
        root = Path(temp)
        state = root / "records.json"
        initial = b'{"rows":[]}'
        state.write_bytes(initial)
        log = root / "server.log"
        with log.open("wb") as stream:
            with subprocess.Popen(
                [rust, "serve-cap", root / "source.sqlite", "0"],
                stdout=subprocess.DEVNULL, stderr=stream,
            ) as server:
                try:
                    port = wait_port(server, log, b"follow fixture")
                    follow_once(port, swift, state, initial)
                finally:
                    if server.poll() is None:
                        server.terminate()
                    try:
                        server.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()


def follow_once(port, swift, state, initial):
    release, send = os.pipe()
    child = None
    deadline = time.monotonic() + 8
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as data:
            token = b"dev-read-secret"
            data.sendall(bytes([len(token)]) + token)
            require(exact(data, 1) == b"\0", "follow fixture test authentication")
            data.setblocking(True)
            child = subprocess.Popen(
                [swift, *SCOPE, "follow", str(release), state], stdin=data, stdout=data,
                stderr=subprocess.PIPE, pass_fds=(release,),
            )
        expect_marker(child, b"bound\n", deadline)
        with socket.create_connection(("127.0.0.1", port), timeout=max(0, deadline - time.monotonic())) as producer:
            token = b"dev-write-secret"
            producer.sendall(bytes([len(token)]) + token)
            require(exact(producer, 1) == b"\0", "fixture producer authentication")
            producer.sendall(b"\1" + bytes(8) + b"\1\1")  # append one at exact head 0, notify once
            result = exact(producer, 16)
            require(int.from_bytes(result[:8], "big") == 1, "fixture commit head")
        expect_marker(child, b"hint:1\n", deadline)
        require(state.read_bytes() == initial, "advisory hint changed fixture progress")
        require(os.write(send, b"\1") == 1, "short release")
        _, report = child.communicate(timeout=max(0, deadline - time.monotonic()))
        require(child.returncode == 0 and report == b"follow:1:5\n", report)
        saved = json.loads(state.read_bytes())
        require(base64.b64decode(saved["scope"]) == SCOPE_BYTES, "follow exact scope")
        rows = saved["rows"]
        require(len(rows) == 1 and rows[0]["id"] == "fact-1" and rows[0]["position"] == 1, "follow exact record")
        require(base64.b64decode(rows[0]["payload"]) == b"alpha", "follow exact payload")
    finally:
        os.close(release)
        os.close(send)
        if child is not None:
            if child.poll() is None:
                child.kill()
            try:
                child.wait(timeout=2)
            finally:
                child.stderr.close()


def history_process(rust, swift, root, database, fields, state, mode):
    log = root / "history.log"
    with log.open("wb") as output:
        with subprocess.Popen(
            [rust, "serve", database], stdout=subprocess.DEVNULL, stderr=output,
        ) as server:
            try:
                port = wait_port(server, log, b"")
                with socket.create_connection(("127.0.0.1", port), timeout=3) as data:
                    token = b"history-test"
                    data.sendall(bytes([len(token)]) + token)
                    require(exact(data, 1) == b"\0", "history fixture authentication")
                    data.setblocking(True)
                    child = subprocess.run(
                        [swift, *fields, mode, state], stdin=data, stdout=data,
                        stderr=subprocess.PIPE, timeout=8,
                    )
            finally:
                if server.poll() is None:
                    server.terminate()
                try:
                    server.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
    require(child.returncode == 0, child.stderr)
    return child.stderr


def verify_history(state, scope, first):
    saved = json.loads(state.read_bytes())
    require(base64.b64decode(saved["scope"]) == scope, "history exact scope")
    coverage = (saved["historyGeneration"], saved["watermark"], saved["first"], saved["floor"])
    require(coverage == (1, 4, first, 1), "exact installed coverage and unchanged live watermark")
    rows = saved["rows"]
    require(len(rows) == 5 - first, "history exact row count")
    for index, row in enumerate(rows, start=first):
        require(row["position"] == index and row["id"] == f"event-{index}", "history exact row")
        require(base64.b64decode(row["payload"]) == bytes([index]) * 1500, "history exact payload")


def run_history(rust, swift, episodes):
    fields = ["device-a", "example-origin", "transcript", "first", "text-v1", "local-epoch-1"]
    scope = b"".join(bytes([len(f)]) + f.encode() for f in fields)
    with tempfile.TemporaryDirectory(prefix="swift-history-e2e-") as temp:
        root = Path(temp)
        database = root / "source.sqlite"
        subprocess.run([rust, "seed", database], check=True, timeout=8)
        for episode in range(episodes):
            state = root / f"{episode}.json"

            def run(mode, marker):
                report = history_process(rust, swift, root, database, fields, state, mode)
                require(report == marker, report)

            state.write_bytes(b"uninstalled")
            run("tail-interrupt", b"tail-interrupted\n")
            require(state.read_bytes() == b"uninstalled", "partial Tail produced receipt")
            run("tail", b"tail:4:3:1:3000\n")
            verify_history(state, scope, 3)
            checkpoint = state.read_bytes()
            run("older-interrupt", b"older-interrupted\n")
            require(state.read_bytes() == checkpoint, "partial Older changed installed coverage")
            run("older-restart", b"older:4:1:1:3000\n")
            verify_history(state, scope, 1)
            state.write_bytes(b"uninstalled")
            run("tail-older", b"older:4:1:1:3000\n")
            verify_history(state, scope, 1)


def build_rust_example(name):
    build = subprocess.run(
        ["cargo", "build", "--locked", "--features", "transport,sqlite",
         "--example", name, "--message-format=json"],
        check=True, stdout=subprocess.PIPE, timeout=240,
    )
    return next(
        Path(value["executable"]) for line in build.stdout.splitlines()
        if (value := json.loads(line)).get("executable") and value["target"]["name"] == name
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=5, choices=range(1, 101), metavar="1..100")
    parser.add_argument("--overlap-clients", type=int, default=4, choices=range(2, 17), metavar="2..16")
    parser.add_argument("--rust-bin", type=Path)
    parser.add_argument("--rust-follow-bin", type=Path)
    parser.add_argument("--rust-history-bin", type=Path)
    parser.add_argument("--tail-episodes", type=int, default=1, choices=range(1, 33), metavar="1..32")
    parser.add_argument("--swift-bin", type=Path)
    args = parser.parse_args()
    os.chdir(ROOT)
    if args.rust_bin is None:
        args.rust_bin = build_rust_example("session_records")
    if args.rust_follow_bin is None:
        args.rust_follow_bin = build_rust_example("session_follow")
    if args.rust_history_bin is None:
        args.rust_history_bin = build_rust_example("session_history")
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
    run_overlap(rust, swift, args.overlap_clients)
    run_follow(args.rust_follow_bin.resolve(), swift)
    run_history(args.rust_history_bin.resolve(), swift, args.tail_episodes)
    print(json.dumps({
        "peer": "rust-session_records-capability-v1",
        "connections": 3 * args.repetitions,
        "head_operations": 6 * args.repetitions,
        "successful_swift_opens": 7 * args.repetitions,
        "refused_swift_opens": args.repetitions,
        "page_connections": 4 * args.repetitions,
        "page_operations": 4 * args.repetitions,
        "reopened_exact_records": 7 * args.repetitions,
        "overlapping_bound_clients": args.overlap_clients,
        "overlap_head_operations": args.overlap_clients,
        "overlap_page_operations": 2 * args.overlap_clients,
        "overlap_exact_records": 7 * args.overlap_clients,
        "follow_connections": 1,
        "follow_hints": 1,
        "follow_head_operations": 1,
        "follow_page_operations": 1,
        "follow_exact_records": 1,
        "tail_successes": 2 * args.tail_episodes,
        "tail_interruptions": args.tail_episodes,
        "tail_exact_records": 4 * args.tail_episodes,
        "tail_exact_payload_bytes": 6000 * args.tail_episodes,
        "older_same_connection_successes": args.tail_episodes,
        "older_restart_successes": args.tail_episodes,
        "older_interruptions": args.tail_episodes,
        "older_exact_records": 4 * args.tail_episodes,
        "older_exact_payload_bytes": 6000 * args.tail_episodes,
        "elapsed_seconds": round(time.monotonic() - start, 3),
    }))


if __name__ == "__main__":
    main()
