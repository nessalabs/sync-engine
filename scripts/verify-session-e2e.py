#!/usr/bin/env python3
"""Real TCP session-record subset plus paired durable-cache measurements.

Default CI gates correctness, not elapsed-time thresholds. --require-complete
is the epic's separate completion gate and intentionally fails while listed
production/session coverage gaps remain. The raw relay is a real byte-forwarding
TCP hop; channel authentication here is a loopback-only plaintext token fixture.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import platform
import re
import selectors
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target/debug/examples/session_records"
LEGACY = ROOT / "target/debug/examples/loopback_sync"
READ = "dev-read-secret"
WRITE = "dev-write-secret"
GAPS = [
    "production authenticated/encrypted direct and relay channel qualification",
    "persistent hints and notification-to-applied warm follow",
    "catalogue and matching 620-entry bootstrap workload",
    "tail, older history and artifact session adapters",
    "multiple scopes per session and multi-stream resume",
    "wire cancellation versus commit and bounded credit/urgent-under-bulk scheduling",
    "uniform status metadata, piggyback heads and capability negotiation",
    "controlled multi-second outage and packet-loss performance profiles",
]


def exact(sock, count):
    chunks = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            raise EOFError("fixture channel closed")
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def frame(sock):
    header = exact(sock, 4)
    size = int.from_bytes(header, "big")
    if size > 1024 * 1024:
        raise ValueError("fixture frame exceeds hard limit")
    return header + exact(sock, size)


def tune(sock):
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(20)


class Forwarder:
    """Owned loopback fixture hop; optional frame shaper models application RTT."""
    def __init__(self, upstream, mode, framed=False, kbit=0, rtt=0):
        self.upstream, self.mode = upstream, mode
        self.framed, self.kbit, self.rtt = framed, kbit, rtt
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.sockets, self.workers = [], []
        self.requests = defaultdict(int)
        self.connections = 0
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def send(self, sock, data):
        if self.rtt and self.stop.wait(self.rtt / 2):
            raise EOFError("fixture stopping")
        if self.kbit:
            rate = self.kbit * 1000 / 8
            for offset in range(0, len(data), 256):
                chunk = data[offset:offset + 256]
                sock.sendall(chunk)
                if self.stop.wait(len(chunk) / rate):
                    raise EOFError("fixture stopping")
        else:
            sock.sendall(data)

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                self.connections += 1
                self.sockets.append(client)
            worker = threading.Thread(target=self.handle, args=(client,), daemon=True)
            self.workers.append(worker)
            worker.start()

    def handle(self, client):
        server = None
        try:
            server = socket.create_connection(("127.0.0.1", self.upstream), 5)
            tune(client)
            tune(server)
            with self.lock:
                self.sockets.append(server)
            if self.framed and self.mode == "session":
                # Host fixture authentication is outside the generic protocol;
                # include its actual preamble/ACK in bytes and exchanges.
                length = exact(client, 1)
                token = exact(client, length[0])
                with self.lock:
                    self.requests["host_auth"] += 1
                self.send(server, length + token)
                ack = exact(server, 1)
                self.send(client, ack)
                if ack != b"\0":
                    return
            down = threading.Thread(target=self.pump, args=(server, client, False), daemon=True)
            self.workers.append(down)
            down.start()
            self.pump(client, server, True)
            down.join(timeout=2)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            for sock in (client, server):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()

    def pump(self, source, target, up):
        try:
            while not self.stop.is_set():
                data = frame(source) if self.framed else source.recv(8192)
                if not data:
                    return
                if self.framed and up:
                    key = f"legacy:{data[4]}" if self.mode == "loopback" else f"{data[4:8].decode('ascii')}:{data[10]}"
                    with self.lock:
                        self.requests[key] += 1
                self.send(target, data)
        except (OSError, EOFError, ValueError, UnicodeError):
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(timeout=2)
        for worker in list(self.workers):
            worker.join(timeout=2)


def start_server(mode, source):
    command = [str(LEGACY), "serve", str(source), "0", READ, WRITE] if mode == "loopback" else [str(BIN), "serve", str(source), "0", READ]
    child = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    selector = selectors.DefaultSelector()
    selector.register(child.stderr, selectors.EVENT_READ)
    deadline = time.monotonic() + 10
    lines = []
    try:
        while time.monotonic() < deadline:
            if selector.select(0.1):
                line = child.stderr.readline()
                lines.append(line)
                found = re.search(r"127\.0\.0\.1:(\d+)", line)
                if found:
                    return child, int(found.group(1))
            if child.poll() is not None:
                raise AssertionError("fixture server exited: " + "".join(lines))
        raise AssertionError("fixture server did not start")
    except BaseException:
        child.terminate()
        child.wait(timeout=5)
        raise
    finally:
        selector.close()


def case(base, mode, path, kbit, rtt):
    base.mkdir()
    source, replica = base / "source.db", base / "replica.db"
    child, port = start_server(mode, source)
    hops = []
    try:
        if path == "relay":
            relay = Forwarder(port, mode)
            hops.append(relay)
            port = relay.port
        shaper = Forwarder(port, mode, framed=True, kbit=kbit, rtt=rtt)
        hops.append(shaper)
        result = subprocess.run([str(BIN), "bench", mode, f"127.0.0.1:{shaper.port}", str(source), str(replica), READ],
                                cwd=ROOT, text=True, capture_output=True, timeout=240)
        assert result.returncode == 0, (mode, path, kbit, result.stderr)
        report = json.loads(result.stdout)
        assert report["transport"] == mode
        for position, phase in zip([7, 14, 21], report["phases"], strict=True):
            assert phase["checkpoint"] == phase["records"] == position and phase["ordered_complete"], phase
            assert phase["payload_bytes"] == 4126 and phase["duplicate_bytes"] == 0, phase
            assert phase["total_bytes"] == phase["protocol_bytes"] + phase["payload_bytes"]
            assert phase["head_checks"] == 2
        inspected = subprocess.run([str(BIN), "inspect", str(replica)], cwd=ROOT, text=True, capture_output=True, check=True)
        assert json.loads(inspected.stdout) == {"checkpoint": 21, "records": 21, "payload_bytes": 12378, "ordered_complete": True}
        with shaper.lock:
            requests = dict(shaper.requests)
            connections = shaper.connections
        assert sum(requests.values()) == sum(p["exchanges"] for p in report["phases"]), (requests, report)
        assert connections == (21 if mode == "loopback" else 2), (connections, report)
        return {"transport": mode, "path": path, "kbit_per_second": kbit, "rtt_seconds": rtt,
                "connections": connections, "observed_requests": requests, "phases": report["phases"],
                "fresh_process_cache_inspection": True}
    finally:
        for hop in reversed(hops):
            hop.close()
        child.terminate()
        child.wait(timeout=5)
        child.stderr.close()


def summarize(cases):
    groups = defaultdict(list)
    for run in cases:
        for phase in run["phases"]:
            groups[(run["transport"], run["path"], run["kbit_per_second"], run["rtt_seconds"], phase["phase"])].append(phase)
    rows = []
    for (transport, path, kbit, rtt, phase), samples in sorted(groups.items()):
        times = [s["elapsed_ms"] for s in samples]
        fixed = {k: samples[0][k] for k in ["exchanges", "protocol_exchanges", "host_auth_exchanges", "protocol_bytes", "total_bytes", "payload_bytes", "duplicate_bytes"]}
        assert all(all(s[k] == v for k, v in fixed.items()) for s in samples), samples
        rows.append({"transport": transport, "path": path, "kbit_per_second": kbit, "rtt_seconds": rtt,
                     "phase": phase, "samples": len(times), "elapsed_ms_min": min(times),
                     "elapsed_ms_median": round(statistics.median(times), 3), "elapsed_ms_max": max(times), **fixed})
    return rows


def markdown(report):
    rows = {(r["path"], r["kbit_per_second"], r["rtt_seconds"], r["phase"], r["transport"]): r for r in report["summary"]}
    output = ["Payload per phase: 4,126 bytes; every measured row passed reopened-cache correctness checks. Timing is evidence, not a CI threshold.", "",
              "| Path / profile | Phase | L/S exchanges | L protocol / total bytes | S protocol / total bytes | L ms median [min,max] | S ms median [min,max] | n |",
              "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for path, kbit, rtt, phase in sorted({key[:4] for key in rows}):
        left, right = rows[(path,kbit,rtt,phase,"loopback")], rows[(path,kbit,rtt,phase,"session")]
        profile = f"{kbit} kbit/s, {rtt}s RTT" if kbit else "unshaped"
        def timing(row):
            return f"{row['elapsed_ms_median']} [{row['elapsed_ms_min']},{row['elapsed_ms_max']}]"
        output.append(f"| {path} / {profile} | {phase} | {left['exchanges']}/{right['exchanges']} | {left['protocol_bytes']} / {left['total_bytes']} | {right['protocol_bytes']} / {right['total_bytes']} | {timing(left)} | {timing(right)} | {left['samples']} |")
    return "\n".join(output) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", action="store_true", help="add real-time 32/64 kbit application-frame delay profiles")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 20:
        parser.error("repetitions must be 1..20")
    subprocess.run(["cargo", "build", "--locked", "--all-features", "--example", "session_records", "--example", "loopback_sync"], cwd=ROOT, check=True, capture_output=True)
    tests = subprocess.run(["cargo", "test", "--locked", "--all-features", "--test", "session_records"], cwd=ROOT, check=True, capture_output=True, text=True)
    count = re.search(r"test result: ok\. (\d+) passed", tests.stdout)
    assert count, tests.stdout
    profiles = [(0, 0)] + ([(32, 0.8), (64, 1.5)] if args.profiles else [])
    cases = []
    with tempfile.TemporaryDirectory(prefix="nessa-session-e2e-") as folder:
        for repeat in range(args.repetitions):
            for kbit, rtt in profiles:
                for path in ["direct", "relay"]:
                    for mode in ["loopback", "session"]:
                        label = f"{repeat}-{kbit}-{path}-{mode}"
                        print("measuring " + label, file=sys.stderr, flush=True)
                        run = case(Path(folder) / label, mode, path, kbit, rtt)
                        run["repetition"] = repeat + 1
                        cases.append(run)
    report = {"scope": "record-session end-to-end foundation; not full #54 completion", "completion_ready": False,
              "coverage_gaps": GAPS, "contract_tests_passed": int(count.group(1)), "environment": platform.platform(),
              "command": "python3 scripts/verify-session-e2e.py" + (" --profiles" if args.profiles else "") + f" --repetitions {args.repetitions}",
              "conditions": ["real TCP adapter/source-to-SQLite-cache paths; inspect cache again in a new process",
                  "direct path includes a zero/delayed fixture frame shaper; relay adds an opaque raw TCP forwarding hop",
                  "loopback-only plaintext token fixture: session bytes/exchanges include its separate cold/reconnect preamble+ACK",
                  "legacy sync includes one-shot subscribe; session supports explicit catch-up only, not hints/follow yet",
                  "timer includes cold TCP/auth/session setup and durable catch-up with post-pass head recheck; excludes producer appends and verification reopen",
                  "application-frame half-RTT delay per direction and 256-byte throttling; no IP/TCP/TLS overhead, packet loss, mobile power or shared duplex contention model",
                  "min/median/max of observed runs; no percentiles, speedup claim or timing pass/fail threshold"],
              "cases": cases, "summary": summarize(cases)}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    if args.markdown:
        args.markdown.write_text(markdown(report))
    print(json.dumps(report, separators=(",", ":")))
    if args.require_complete:
        print("session completion gate incomplete: " + "; ".join(GAPS), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
