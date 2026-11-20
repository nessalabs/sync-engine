#!/usr/bin/env python3
"""Matched history transfer on real TCP; optional bounded application-frame shaping."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("session_e2e", ROOT / "scripts/verify-session-e2e.py")
link = importlib.util.module_from_spec(spec)
sys.dont_write_bytecode = True
spec.loader.exec_module(link)


class CountingLink(link.Forwarder):
    def __init__(self, *args, **kwargs):
        self.delivered = 0
        super().__init__(*args, **kwargs)

    def send(self, sock, data):
        super().send(sock, data)
        with self.lock:
            self.delivered += len(data)


def run(binaries, directory, mode, kbit, rtt):
    directory.mkdir()
    source = directory / "source.db"
    cache = directory / "cache.db"
    history, loopback = binaries
    subprocess.run([history, "seed", source], check=True)
    command = [history, "serve", source] if mode == "session" else [
        loopback, "serve", source, "0", "history-test", "history-test-write",
    ]
    with subprocess.Popen(command, stderr=subprocess.PIPE, text=True) as server:
        proxy = None
        try:
            port = int(server.stderr.readline().strip().removeprefix("listening on 127.0.0.1:"))
            proxy = CountingLink(port, mode, framed=True, kbit=kbit, rtt=rtt)
            result = subprocess.run(
                [history, "bench", mode, f"127.0.0.1:{proxy.port}", cache],
                text=True, capture_output=True, check=False, timeout=60,
            )
            assert result.returncode == 0, result.stderr
            row = json.loads(result.stdout)
            reopened = subprocess.check_output([history, "inspect", cache], text=True, timeout=5)
            assert json.loads(reopened) == {"live_head": 4, "lower_bound": 1, "records": 4}
            # The shaper accounts after sending, which can race the receiver's
            # last read. Settle the measurement without timing it as workload.
            deadline = time.monotonic() + 2
            while True:
                with proxy.lock:
                    received = proxy.delivered
                    connections = proxy.connections
                    frames = dict(proxy.requests)
                if received == row["total_bytes"] or time.monotonic() >= deadline:
                    break
                time.sleep(.01)
            assert row["payload_bytes"] == 6000, row
            assert row["total_bytes"] == received, (row, received)
            assert connections == (1 if mode == "session" else 4), (mode, connections, frames)
            assert row["exchanges"] == sum(frames.values()), (row, frames)
            return dict(row, mode=mode, kbit_per_second=kbit, rtt_seconds=rtt, connections=connections,
                        observed_wire_bytes=received, requests=frames, reopened=True)
        finally:
            if proxy:
                proxy.close()
            if server.poll() is None:
                server.terminate()
            server.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", action="store_true")
    parser.add_argument("--repetitions", type=int, default=1)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    subprocess.run(["cargo", "build", "--locked", "--all-features", "--example",
                    "session_history", "--example", "loopback_sync"], cwd=ROOT, check=True)
    target = Path(os.getenv("CARGO_TARGET_DIR", ROOT / "target"))
    binaries = [target / "debug/examples" / name for name in ("session_history", "loopback_sync")]
    rows = []
    with tempfile.TemporaryDirectory(prefix="session-history-") as work:
        for kbit, rtt in [(0, 0)] + ([(32, .8), (64, 1.5)] if args.profiles else []):
            for _ in range(args.repetitions):
                for mode in ["loopback", "session"]:
                    rows.append(run(binaries, Path(work) / str(len(rows)), mode, kbit, rtt))
    print(json.dumps({
        "conditions": "IPv4-loopback TCP fixture; selected application frames delayed half-RTT per direction and throttled in 256-byte chunks; includes connection/authentication and durable writes, excludes reopened inspection and close; no production channel security, loss, packet overhead or latency promise.",
        "cases": rows,
    }, indent=2))


if __name__ == "__main__":
    main()
