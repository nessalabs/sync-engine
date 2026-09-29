#!/usr/bin/env python3
"""Bounded tail, durable older coverage, races and pruning over real sockets."""

import json
import socket
import struct
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "target" / "debug" / "examples" / "loopback_sync"
HISTORY = ROOT / "target" / "debug" / "examples" / "history_lab"
READ = "history-read-token"
WRITE = "history-write-token"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(binary, *args, ok=True, error=None):
    result = subprocess.run([str(binary), *map(str, args)], text=True, capture_output=True)
    if (result.returncode == 0) != ok:
        raise AssertionError((args, result.returncode, result.stdout, result.stderr))
    if error and error not in result.stderr:
        raise AssertionError((args, "missing expected error", error, result.stderr))
    return json.loads(result.stdout) if result.stdout else None


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for process boundary")


def reachable(number):
    try:
        with socket.create_connection(("127.0.0.1", number), 0.1):
            return True
    except OSError:
        return False


def exact(sock, count):
    parts = []
    while count:
        part = sock.recv(count)
        if not part:
            raise EOFError("truncated frame")
        parts.append(part)
        count -= len(part)
    return b"".join(parts)


def frame(sock):
    header = exact(sock, 4)
    length = struct.unpack(">I", header)[0]
    if length > 1024 * 1024:
        raise ValueError("oversized frame")
    return header + exact(sock, length)


class HoldReply:
    """External one-request proxy with a deterministic response barrier."""

    def __init__(self, source_port, operation):
        self.source_port = source_port
        self.operation = operation
        self.seen = threading.Event()
        self.release = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        try:
            with self.listener:
                while True:
                    client, _ = self.listener.accept()
                    with client, socket.create_connection(("127.0.0.1", self.source_port), 2) as source:
                        request = frame(client)
                        source.sendall(request)
                        response = frame(source)
                        if request[4] == self.operation:
                            self.seen.set()
                            if self.release.wait(5):
                                client.sendall(response)
                            return
                        client.sendall(response)
        except (OSError, EOFError):
            pass

    def close(self):
        self.release.set()
        self.listener.close()
        self.thread.join(timeout=1)


def start_server(source_path):
    source_port = port()
    process = subprocess.Popen([str(SERVER), "serve", str(source_path), str(source_port),
                                READ, WRITE], stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    wait_until(lambda: process.poll() is None and reachable(source_port))
    return process, source_port, f"127.0.0.1:{source_port}"


def main():
    subprocess.run(["cargo", "build", "--locked", "--features", "transport",
                    "--example", "loopback_sync", "--example", "history_lab"],
                   cwd=ROOT, check=True, capture_output=True)
    with tempfile.TemporaryDirectory(prefix="nessa-sync-slice5-") as directory:
        root = Path(directory)
        source_path = root / "source.db"
        receiver = root / "phone.db"
        race_db = root / "race.db"
        empty_db = root / "empty.db"
        server, source_port, address = start_server(source_path)
        proxies = []
        processes = []
        try:
            assert run(HISTORY, "show", receiver, "device-a")["state"] == "unloaded"
            empty = run(HISTORY, "bootstrap", address, empty_db, "device-b", READ, 1, 5)
            assert empty["state"] == "complete_empty" and empty["live_head"] == 0
            for position in range(1, 41):
                run(SERVER, "append", address, WRITE, f"fact-{position}", f"message-{position}")

            first = run(HISTORY, "bootstrap", address, receiver, "device-a", READ, 1, 5)
            assert first["state"] == "partial"
            assert (first["live_head"], first["lower_bound"]) == (40, 36)
            assert first["payload_bytes"] < 80
            assert run(HISTORY, "show", receiver, "device-a")["count"] == 5
            before = run(HISTORY, "stats", address, WRITE)
            hydrated = run(HISTORY, "hydrate", address, receiver, "device-a", READ, 25, 50, 5)
            after = run(HISTORY, "stats", address, WRITE)
            assert hydrated["pages"] == 3 and hydrated["resolved"] == 50
            assert hydrated["live_head"] == 40 and hydrated["lower_bound"] == 21
            assert after["older_reads"] - before["older_reads"] == 3
            assert run(HISTORY, "show", receiver, "device-a")["count"] == 20

            # A new process reads the committed lower bound and resumes from it.
            resumed = run(HISTORY, "hydrate", address, receiver, "device-a", READ, 16, 1, 5)
            assert resumed["lower_bound"] == 16 and resumed["live_head"] == 40

            # Hold an older reply while a new live record commits on a separate
            # connection. Historical apply must not rewind the forward head.
            held_history = HoldReply(source_port, 8)
            proxies.append(held_history)
            waiting = subprocess.Popen([str(HISTORY), "hydrate", f"127.0.0.1:{held_history.port}",
                                        str(receiver), "device-a", READ, "11", "1", "5"],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(waiting)
            assert held_history.seen.wait(4)
            run(SERVER, "append", address, WRITE, "fact-41", "message-41")
            live = run(HISTORY, "live", address, receiver, "device-a", READ)
            assert (live["live_head"], live["lower_bound"]) == (41, 16)
            held_history.release.set()
            output, errors = waiting.communicate(timeout=5)
            assert waiting.returncode == 0, errors
            assert (json.loads(output)["live_head"], json.loads(output)["lower_bound"]) == (41, 11)

            # Superseded snapshot and historical replies are rejected even if
            # the source response was valid when it was captured.
            run(HISTORY, "bootstrap", address, race_db, "device-b", READ, 1, 5)
            held_older = HoldReply(source_port, 8)
            proxies.append(held_older)
            old = subprocess.Popen([str(HISTORY), "hydrate", f"127.0.0.1:{held_older.port}",
                                    str(race_db), "device-b", READ, "32", "1", "5"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(old)
            assert held_older.seen.wait(4)
            run(HISTORY, "bootstrap", address, race_db, "device-b", READ, 2, 5)
            held_older.release.set()
            _, old_error = old.communicate(timeout=5)
            assert old.returncode != 0 and "Stale" in old_error
            assert run(HISTORY, "show", race_db, "device-b")["generation"] == 2

            held_tail = HoldReply(source_port, 7)
            proxies.append(held_tail)
            delayed = subprocess.Popen([str(HISTORY), "bootstrap", f"127.0.0.1:{held_tail.port}",
                                        str(race_db), "device-b", READ, "3", "5"],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(delayed)
            assert held_tail.seen.wait(4)
            run(SERVER, "append", address, WRITE, "fact-42", "message-42")
            assert run(HISTORY, "live", address, race_db, "device-b", READ)["live_head"] == 42
            held_tail.release.set()
            _, delayed_error = delayed.communicate(timeout=5)
            assert delayed.returncode != 0 and "Stale" in delayed_error
            assert run(HISTORY, "show", race_db, "device-b")["live_head"] == 42

            held_fenced = HoldReply(source_port, 7)
            proxies.append(held_fenced)
            fenced_reply = subprocess.Popen([str(HISTORY), "bootstrap", f"127.0.0.1:{held_fenced.port}",
                                             str(race_db), "device-b", READ, "4", "5"],
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(fenced_reply)
            assert held_fenced.seen.wait(4)
            run(HISTORY, "fence", race_db, "device-b")
            held_fenced.release.set()
            _, fenced_error = fenced_reply.communicate(timeout=5)
            assert fenced_reply.returncode != 0 and "Fenced" in fenced_error
            assert run(HISTORY, "show", race_db, "device-b")["state"] == "deleted"

            complete_db = root / "complete.db"
            run(HISTORY, "bootstrap", address, complete_db, "device-b", READ, 1, 5)
            completed = run(HISTORY, "hydrate", address, complete_db, "device-b", READ,
                            1, 1, 5)
            assert completed["lower_bound"] == 1
            assert run(HISTORY, "show", complete_db, "device-b")["state"] == "complete"

            # The source floor advances to the receiver's missing boundary;
            # there is no silent jump past the unavailable range.
            run(HISTORY, "prune", address, WRITE, 10)
            # The receiver already has 11..41; prune through 11 makes its
            # next request below the floor incompatible.
            run(HISTORY, "prune", address, WRITE, 11)
            before_gap = run(HISTORY, "show", receiver, "device-a")
            run(HISTORY, "hydrate", address, receiver, "device-a", READ, 1, 1, 5,
                ok=False, error="ResetRequired")
            assert run(HISTORY, "show", receiver, "device-a") == before_gap

            server.terminate()
            server.wait(timeout=3)
            assert run(HISTORY, "show", receiver, "device-a")["live_head"] == 41
            print(json.dumps({"slice": 5, "tail_payload_bytes": first["payload_bytes"],
                              "tail_head": first["live_head"], "tail_lower": first["lower_bound"],
                              "coalesced_waiters": hydrated["resolved"],
                              "coalesced_older_reads": after["older_reads"] - before["older_reads"],
                              "coalesced_payload_bytes": hydrated["payload_bytes"],
                              "coalesced_protocol_bytes": hydrated["protocol_bytes"],
                              "restart_lower": resumed["lower_bound"],
                              "live_after_delayed_history": 41,
                              "stale_snapshot_refused": True, "stale_older_refused": True,
                              "deletion_fence_refused": True, "pruned_gap_reset_required": True,
                              "offline_cache_kept": True}))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=3)
            for proxy in proxies:
                proxy.close()
            if server.poll() is None:
                server.terminate()
                server.wait(timeout=3)


if __name__ == "__main__":
    main()
