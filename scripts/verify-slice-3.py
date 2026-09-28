#!/usr/bin/env python3
"""Process-level loopback replication checks with faults outside the Rust core."""

import argparse
import json
import socket
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "debug" / "examples" / "loopback_sync"
READ = "dev-read-secret"
WRITE = "dev-write-secret"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def recv_exact(sock, count):
    chunks = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            raise EOFError("truncated frame")
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock):
    header = recv_exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    if length > 1024 * 1024:
        raise ValueError("oversized proxy frame")
    return header + recv_exact(sock, length)


def command(*args, ok=True):
    result = subprocess.run([str(BIN), *map(str, args)], text=True, capture_output=True)
    if (result.returncode == 0) != ok:
        raise AssertionError((args, result.returncode, result.stdout, result.stderr))
    return json.loads(result.stdout) if result.stdout else None


def wait_until(predicate, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for process state")


class Proxy:
    """External framed link fault injector; never imported by the Rust crate."""

    def __init__(self, source_port, *, drop_hints=False, truncate_page=False,
                 hold_page=False, hold_subscribe=False, corrupt_page=False,
                 rtt=0, kbit=0, outage=False):
        self.source_port = source_port
        self.drop_hints = drop_hints
        self.truncate_page = truncate_page
        self.hold_page = hold_page
        self.hold_subscribe = hold_subscribe
        self.corrupt_page = corrupt_page
        self.rtt = rtt
        self.kbit = kbit
        self.outage = outage
        self.subscriptions = 0
        self.page_seen = threading.Event()
        self.release_page = threading.Event()
        self.subscribe_seen = threading.Event()
        self.release_subscribe = threading.Event()
        self.stop = threading.Event()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.handle, args=(client,), daemon=True).start()

    def send(self, sock, data):
        if self.rtt:
            time.sleep(self.rtt / 2)
        if self.kbit:
            byte_rate = self.kbit * 1000 / 8
            for offset in range(0, len(data), 256):
                chunk = data[offset:offset + 256]
                sock.sendall(chunk)
                time.sleep(len(chunk) / byte_rate)
        else:
            sock.sendall(data)

    def handle(self, client):
        with client:
            try:
                if self.outage:
                    return
                request = recv_frame(client)
                operation = request[4]
                if operation == 4:
                    self.subscriptions += 1
                with socket.create_connection(("127.0.0.1", self.source_port), 2) as server:
                    self.send(server, request)
                    response = recv_frame(server)
                    if operation == 3 and self.truncate_page:
                        self.truncate_page = False
                        client.sendall(response[:6])
                        return
                    if operation == 3 and self.corrupt_page:
                        self.corrupt_page = False
                        changed = bytearray(response)
                        # Page response includes the echoed receiver ID. A
                        # foreign echo must reach validation, never apply.
                        changed[6] = ord("x")
                        response = bytes(changed)
                    if operation == 3 and self.hold_page:
                        self.page_seen.set()
                        if not self.release_page.wait(6):
                            return
                    if operation == 4 and self.hold_subscribe:
                        self.subscribe_seen.set()
                        if not self.release_subscribe.wait(6):
                            return
                    self.send(client, response)
                    if operation == 4:
                        while not self.stop.is_set():
                            try:
                                hint = recv_frame(server)
                            except EOFError:
                                if self.drop_hints:
                                    # Keep the client socket apparently idle
                                    # so only its fallback timer can recover.
                                    while not self.stop.wait(0.05):
                                        pass
                                return
                            if not self.drop_hints:
                                self.send(client, hint)
            except (OSError, EOFError, ValueError):
                return

    def close(self):
        self.stop.set()
        self.release_page.set()
        self.release_subscribe.set()
        self.listener.close()
        self.thread.join(timeout=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profiles", action="store_true",
                        help="also run slower real-time 32/64 kbit, 0.8/1.5 s RTT profiles")
    args = parser.parse_args()
    subprocess.run(["cargo", "build", "--locked", "--features", "transport", "--example",
                    "loopback_sync"], cwd=ROOT, check=True, capture_output=True)
    with tempfile.TemporaryDirectory(prefix="nessa-sync-slice3-") as root:
        base = Path(root)
        source = base / "source.db"
        first = base / "first.db"
        second = base / "second.db"
        port = free_port()
        addr = f"127.0.0.1:{port}"
        server = subprocess.Popen([str(BIN), "serve", str(source), str(port), READ, WRITE],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        processes = []
        proxies = []
        try:
            wait_until(lambda: server.poll() is None and reachable(port))
            command("append", addr, WRITE, "fact-1", "alpha")
            command("append", addr, WRITE, "fact-2", "beta")
            a = command("sync", addr, first, "device-a", READ)
            b = command("sync", addr, second, "device-b", READ)
            assert a["checkpoint"] == b["checkpoint"] == 2
            assert a["payload_bytes"] == b["payload_bytes"] == 9
            unchanged = command("sync", addr, first, "device-a", READ)
            assert unchanged["payload_bytes"] == 0

            before = command("stats", addr, WRITE)
            command("sync", addr, base / "denied.db", "denied", "wrong-token", ok=False)
            after = command("stats", addr, WRITE)
            assert before["head_reads"] == after["head_reads"]
            assert after["refused_reads"] > before["refused_reads"]
            command("sync", addr, base / "unknown.db", "unknown-device", READ, ok=False)
            assert command("stats", addr, WRITE)["head_reads"] == after["head_reads"]
            with socket.create_connection(("127.0.0.1", port), 1) as oversized:
                oversized.sendall(struct.pack(">I", 1024 * 1024 + 1))
                assert oversized.recv(1) == b""
            assert command("stats", addr, WRITE)["page_reads"] == after["page_reads"]

            # A is offline. B follows through a proxy that discards every hint;
            # its host fallback still discovers the committed fact.
            lost = Proxy(port, drop_hints=True)
            proxies.append(lost)
            follow = subprocess.Popen([str(BIN), "follow", f"127.0.0.1:{lost.port}",
                                       str(second), "device-b", READ, "100"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            processes.append(follow)
            wait_until(lambda: command("stats", addr, WRITE)["head_reads"] > after["head_reads"])
            command("append", addr, WRITE, "fact-3", "gamma")
            wait_until(lambda: command("show", second, "device-b")["checkpoint"] == 3)
            assert lost.subscriptions == 1, "reconnect recovered instead of fallback"
            resumed = command("sync", addr, first, "device-a", READ)
            assert resumed["checkpoint"] == 3 and resumed["payload_bytes"] == 5

            # A truncated reply cannot advance the durable checkpoint; a new
            # connection resumes the same missing page without manual reset.
            broken = Proxy(port, truncate_page=True)
            proxies.append(broken)
            command("append", addr, WRITE, "fact-4", "delta")
            command("sync", f"127.0.0.1:{broken.port}", first, "device-a", READ, ok=False)
            assert command("show", first, "device-a")["checkpoint"] == 3
            retried = command("sync", addr, first, "device-a", READ)
            assert retried["checkpoint"] == 4 and retried["payload_bytes"] == 5

            wrong_echo = Proxy(port, corrupt_page=True)
            proxies.append(wrong_echo)
            command("append", addr, WRITE, "fact-5", "epsilon")
            command("sync", f"127.0.0.1:{wrong_echo.port}", first,
                    "device-a", READ, ok=False)
            assert command("show", first, "device-a")["checkpoint"] == 4

            # Hold A's reply after the source has read it. B and source append
            # must make progress before A's socket is released.
            slow = Proxy(port, hold_page=True)
            proxies.append(slow)
            blocked = subprocess.Popen([str(BIN), "sync", f"127.0.0.1:{slow.port}",
                                        str(first), "device-a", READ],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(blocked)
            assert slow.page_seen.wait(4)
            command("append", addr, WRITE, "fact-6", "zeta")
            concurrent = command("sync", addr, second, "device-b", READ)
            assert concurrent["checkpoint"] == 6 and blocked.poll() is None
            slow.release_page.set()
            output, errors = blocked.communicate(timeout=6)
            assert blocked.returncode == 0, errors
            assert json.loads(output)["checkpoint"] == 6

            command("append", addr, WRITE, "bulk-fact", "p" * 4096)
            profiles = []
            if args.profiles:
                for kbit, rtt in [(32, 0.8), (64, 1.5)]:
                    profile = Proxy(port, kbit=kbit, rtt=rtt)
                    proxies.append(profile)
                    profile_store = base / f"profile-{kbit}.db"
                    start = time.monotonic()
                    result = command("sync", f"127.0.0.1:{profile.port}", profile_store,
                                     "device-a", READ)
                    assert result["checkpoint"] == 7
                    profiles.append({"kbit_per_second": kbit, "rtt_seconds": rtt,
                                     "elapsed_seconds": round(time.monotonic() - start, 3),
                                     "protocol_bytes": result["protocol_bytes"],
                                     "payload_bytes": result["payload_bytes"]})
                    assert result["payload_bytes"] == 4126
                outage = Proxy(port, outage=True)
                proxies.append(outage)
                command("sync", f"127.0.0.1:{outage.port}", base / "outage.db",
                        "device-a", READ, ok=False)
                outage.outage = False
                restored = command("sync", f"127.0.0.1:{outage.port}",
                                   base / "outage.db", "device-a", READ)
                assert restored["checkpoint"] == 7
            # Commit while the subscription acknowledgement is held. The
            # receiver has subscribed but has not begun its first head check.
            race = Proxy(port, hold_subscribe=True)
            proxies.append(race)
            racing = subprocess.Popen([str(BIN), "sync", f"127.0.0.1:{race.port}",
                                       str(base / "racing.db"), "device-a", READ],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(racing)
            assert race.subscribe_seen.wait(4)
            command("append", addr, WRITE, "race-fact", "r")
            race.release_subscribe.set()
            race_output, race_errors = racing.communicate(timeout=6)
            assert racing.returncode == 0, race_errors
            assert json.loads(race_output)["checkpoint"] == 8
            assert command("sync", addr, first, "device-a", READ)["checkpoint"] == 8
            assert command("sync", addr, second, "device-b", READ)["checkpoint"] == 8
            final = command("show", first, "device-a")
            assert final == {"checkpoint": 8, "count": 8}
            print(json.dumps({"slice": 3, "converged": True, "final": final,
                              "offline_payload_bytes": resumed["payload_bytes"],
                              "unchanged_payload_bytes": unchanged["payload_bytes"],
                              "truncated_reply_recovered": True,
                              "all_hints_dropped_recovered": True,
                              "slow_receiver_isolated": True,
                              "subscribe_race_recovered": True,
                              "auth_precedes_source_read": True,
                              "outage_recovered": bool(args.profiles), "profiles": profiles}))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=3)
            for proxy in proxies:
                proxy.close()
            server.terminate()
            server.wait(timeout=3)


def reachable(port):
    try:
        with socket.create_connection(("127.0.0.1", port), 0.1):
            return True
    except OSError:
        return False


if __name__ == "__main__":
    main()
