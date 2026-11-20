#!/usr/bin/env python3
"""Owned real TCP cut + timed reconnect gate, not packet/IP/TLS emulation."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import selectors
import socket
import subprocess
import sys
import tempfile
import threading
import time


SCRIPT = Path(__file__).resolve()
spec = importlib.util.spec_from_file_location("session_e2e_outage_support", SCRIPT.with_name("verify-session-e2e.py"))
link = importlib.util.module_from_spec(spec)
spec.loader.exec_module(link)
PREFIX = 512
PHASES = ("seed", "interrupted", "blocked_probe", "resumed")


def shutdown(sock):
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class CutShaper(link.Forwarder):
    """Selected PAGE cut on the actual receiver TCP route, with a timed gate."""

    def __init__(self, upstream, mode, kbit, rtt):
        self.measure = threading.Condition()
        self.read_kind = {}
        self.armed = False
        self.reserved = False
        self.cut_at = None
        self.opened_at = None
        self.prefix = None
        self.rejected_at = []
        self.active_sends = 0
        self.admitted = 0
        self.forwarded = [0, 0]
        self.page_requests = 0
        super().__init__(upstream, mode, framed=True, kbit=kbit, rtt=rtt)

    def snapshot(self):
        with self.measure:
            assert self.measure.wait_for(lambda: self.active_sends == 0, timeout=20)
            with self.lock:
                seen = self.connections
            return {
                "connections_accepted": seen,
                "connections_to_upstream": self.admitted,
                "reconnect_attempts_blocked": len(self.rejected_at),
                "forwarded_up_bytes": self.forwarded[0],
                "forwarded_down_bytes": self.forwarded[1],
                "page_requests": self.page_requests,
            }

    def paced(self, sock, data, upstream):
        with self.measure:
            self.active_sends += 1
        try:
            if self.rtt and self.stop.wait(self.rtt / 2):
                raise EOFError("shaper closing")
            rate = self.kbit * 1000 / 8
            block = 256 if rate else len(data)
            for offset in range(0, len(data), block):
                leaf = memoryview(data[offset:offset + block])
                amount = len(leaf)
                while leaf:
                    sent = sock.send(leaf)
                    if sent == 0:
                        raise EOFError("shaper peer closed")
                    with self.measure:
                        self.forwarded[0 if upstream else 1] += sent
                    leaf = leaf[sent:]
                if rate and self.stop.wait(amount / rate):
                    raise EOFError("shaper closing")
        finally:
            with self.measure:
                self.active_sends -= 1
                self.measure.notify_all()

    def handle(self, client):
        server = None
        try:
            with self.measure:
                if self.cut_at is not None and self.opened_at is None:
                    self.rejected_at.append(time.monotonic())
                    return
            server = socket.create_connection(("127.0.0.1", self.upstream), 5)
            with self.measure:
                self.admitted += 1
            link.tune(client)
            link.tune(server)
            with self.lock:
                self.sockets.append(server)
            if self.mode != "loopback":
                length = link.exact(client, 1)
                token = link.exact(client, length[0])
                self.paced(server, length + token, True)
                ack = link.exact(server, 1)
                self.paced(client, ack, False)
                if ack != b"\0":
                    return
            down = threading.Thread(target=self.outage_pump, args=(server, client, False), daemon=True)
            self.workers.append(down)
            down.start()
            self.outage_pump(client, server, True)
            down.join(timeout=2)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            with self.measure:
                self.read_kind.pop(server, None)
            for sock in (client, server):
                if sock is not None:
                    shutdown(sock)
                    sock.close()

    def outage_pump(self, source, target, upstream):
        try:
            while not self.stop.is_set():
                data = link.frame(source)
                cut = False
                if upstream:
                    page = (data[4] == 3 if self.mode == "loopback"
                            else data[4:8] == b"NSR1" and data[10] == 4)
                    with self.measure:
                        self.read_kind[target] = page
                        self.page_requests += page
                else:
                    with self.measure:
                        matching = self.read_kind.get(source, False)
                        if self.armed and not self.reserved and matching:
                            self.reserved = True
                            cut = True
                if cut:
                    if len(data) <= PREFIX:
                        raise ValueError(f"selected PAGE reply is too short: {len(data)}")
                    self.paced(target, data[:PREFIX], False)
                    with self.measure:
                        self.cut_at = time.monotonic()
                        self.prefix = {"forwarded_bytes": PREFIX, "full_frame_bytes": len(data)}
                    # Physically tear down all sockets on this receiver route. The
                    # optional byte-opaque hop and source listener stay alive.
                    with self.lock:
                        sockets = list(self.sockets)
                    for sock in sockets:
                        shutdown(sock)
                    return
                self.paced(target, data, upstream)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            shutdown(source)
            shutdown(target)

    def release(self, duration):
        with self.measure:
            cut_at = self.cut_at
            rejected = list(self.rejected_at)
        assert cut_at is not None and len(rejected) == 1
        assert 0 <= rejected[0] - cut_at < duration, (rejected, cut_at, duration)
        if wait := cut_at + duration - time.monotonic():
            time.sleep(max(0, wait))
        with self.measure:
            self.opened_at = time.monotonic()
        return {
            "configured_closed_ms": duration * 1000,
            "observed_closed_ms": round((self.opened_at - cut_at) * 1000, 3),
            "probe_ms_after_cut": round((rejected[0] - cut_at) * 1000, 3),
            "selected_response_prefix": self.prefix,
        }


def barrier(child, expected):
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        if not selector.select(90):
            raise AssertionError(f"fixture stalled at {expected}")
    response = child.stdout.readline()
    if not response:
        raise AssertionError(f"fixture stopped at {expected}: {child.stderr.read()}")
    data = json.loads(response)
    assert data["phase"] == expected, data
    return data


def release(child, expected):
    child.stdin.write(expected + "\n")
    child.stdin.flush()


def case(base, mode, path, kbit, rtt, duration):
    base.mkdir()
    source, replica = base / "source.db", base / "replica.db"
    server, target = link.start_server(mode, source)
    hops = []
    child = None
    try:
        if path == "relay":
            relay = link.Forwarder(target, mode)
            hops.append(relay)
            target = relay.port
        shaper = CutShaper(target, mode, kbit, rtt)
        hops.append(shaper)
        command = [str(link.BIN), "outage", mode, f"127.0.0.1:{shaper.port}", str(source), str(replica), link.READ]
        child = subprocess.Popen(command, cwd=link.ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        previous = shaper.snapshot()
        rows = []
        outage = None
        for phase, receipt in zip(PHASES, (7, 7, 7, 21), strict=True):
            row = barrier(child, phase)
            after = shaper.snapshot()
            row["raw_route_delta"] = {key: after[key] - previous[key] for key in after}
            previous = after
            assert row["checkpoint"] == row["records"] == receipt and row["ordered_complete"], row
            assert row["reported_duplicate_bytes"] == 0, row
            rows.append(row)
            if phase == "seed":
                with shaper.measure:
                    shaper.armed = True
                release(child, "arm")
            elif phase == "interrupted":
                assert shaper.cut_at is not None, "no matching warm PAGE was cut"
                assert row["reported_payload_bytes"] == 0, row
                assert row["raw_route_delta"]["page_requests"] == 1, row
                release(child, "probe")
            elif phase == "blocked_probe":
                observed = row["raw_route_delta"]
                assert observed["connections_accepted"] == 1, observed
                assert observed["reconnect_attempts_blocked"] == 1, observed
                assert observed["connections_to_upstream"] == observed["forwarded_up_bytes"] == observed["forwarded_down_bytes"] == 0, observed
                outage = shaper.release(duration)
                assert outage["observed_closed_ms"] >= outage["configured_closed_ms"]
                release(child, "resume")
            else:
                assert row["reported_payload_bytes"] == 8252, row
        assert child.wait(timeout=5) == 0, child.stderr.read()
        inspected = subprocess.run([str(link.BIN), "inspect", str(replica)], cwd=link.ROOT, check=True, text=True, capture_output=True)
        assert json.loads(inspected.stdout) == {"checkpoint": 21, "records": 21, "payload_bytes": 12378, "ordered_complete": True}
        assert rows[0]["raw_route_delta"]["connections_to_upstream"] == (1 if mode == "session-io" else 7)
        assert rows[1]["raw_route_delta"]["connections_to_upstream"] == (0 if mode == "session-io" else 5), rows[1]
        return {
            "mode": mode, "path": path, "configured_kbit_per_second": kbit, "configured_rtt_seconds": rtt,
            "phases": rows, "outage": outage, "fresh_process_sqlite_inspection": True,
        }
    finally:
        if child is not None:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
            for pipe in (child.stdin, child.stdout, child.stderr):
                pipe.close()
        for hop in reversed(hops):
            hop.close()
        server.terminate()
        server.wait(timeout=5)
        server.stderr.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", action="store_true", help="also run both 32/64 kbit application-shaped paths")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--jobs", type=int, choices=range(1, 5), default=1,
                        help="run up to four isolated fixture pairs in parallel; not for timing comparisons")
    args = parser.parse_args()
    subprocess.run(["cargo", "build", "--locked", "--all-features", "--example", "session_records", "--example", "loopback_sync"], cwd=link.ROOT, check=True)
    profiles = [(0, 0)] + ([(32, 0.8), (64, 1.5)] if args.profiles else [])
    measured = [*link.ROOT.glob("src/**/*.rs"), *link.ROOT.glob("examples/session_records/**/*.rs"),
                link.ROOT / "examples/session_records.rs", link.ROOT / "examples/loopback_sync.rs", link.ROOT / "Cargo.toml",
                link.ROOT / "Cargo.lock", SCRIPT, Path(link.__file__)]
    fingerprint = lambda: {str(path.relative_to(link.ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(measured)}
    before = fingerprint()
    with tempfile.TemporaryDirectory(prefix="owned-record-outage-") as tmp:
        matrix = [(kbit, rtt, path, mode) for kbit, rtt in profiles
                  for path in ("direct", "relay") for mode in ("loopback", "session-io")]
        def run(row):
            kbit, rtt, path, mode = row
            label = f"{kbit}-{path}-{mode}"
            print(f"timed outage: {label}", flush=True, file=sys.stderr)
            return case(Path(tmp) / label, mode, path, kbit, rtt, 3.0)
        with ThreadPoolExecutor(max_workers=args.jobs) as workers:
            cases = list(workers.map(run, matrix))
    assert before == fingerprint()
    report = {
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=link.ROOT, text=True).strip(),
        "source_sha256": before, "environment": platform.platform(), "profiles": bool(args.profiles), "fixture_jobs": args.jobs,
        "scope": "one receiver, loopback-per-operation versus serialized session, actual TCP cut + measured 3s reconnect gate and SQLite reopen",
        "limits": ["both paths have an application-frame shaper; relay adds a byte-opaque TCP hop",
                   "half-RTT each frame with fixed 256-byte pacing; not an IP/TCP loss model, physical LAN, NAT, TLS, or deployed relay",
                   "prefix bytes forwarded to receiver route, not proof of complete decode or installed record data",
                   "fixture-host reconnect probe/resume, no production retry cadence or timing threshold"],
        "cases": cases,
    }
    rendered = json.dumps(report, separators=(",", ":"))
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
