#!/usr/bin/env python3
"""Source-process and durable receiver lab for bounded artifact transfer."""

import argparse
import hashlib
import json
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*command, expect_ok=True):
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False, timeout=12)
    if expect_ok and result.returncode:
        raise AssertionError(f"{command}: {result.stderr}")
    if not expect_ok:
        assert result.returncode != 0, command
        return None
    return json.loads(result.stdout)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_server(binary, source, read, write, epoch="epoch-1"):
    port = free_port()
    process = subprocess.Popen([binary, "serve", source, str(port), read, write, epoch],
                               cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("source process exited")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return port, process
        except OSError:
            time.sleep(0.02)
    raise AssertionError("source did not start")


def stop_server(process):
    process.terminate()
    try:
        process.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()


class ArtifactLink:
    """Loopback-only application-wire shaper with one shared downlink budget."""

    def __init__(self, source_port, kbit, rtt, chunk_bytes, gate_head):
        self.source_port = source_port
        self.kbit = kbit
        self.rtt = rtt
        self.chunk_bytes = chunk_bytes
        self.gate_head = gate_head
        self.records = []
        self.range_attempt = 0
        self.downlink = threading.Lock()
        self.bulk_started = threading.Event()
        self.head_started = threading.Event()
        self.head_forwarded = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.workers = []
        self.receiver = threading.Thread(target=self.accept)
        self.receiver.start()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.receiver.join()
        self.listener.close()
        for worker in self.workers:
            worker.join()

    def accept(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except TimeoutError:
                continue
            worker = threading.Thread(target=self.exchange, args=(conn,))
            self.workers.append(worker)
            worker.start()

    @staticmethod
    def frame(stream):
        def exact(size):
            data = bytearray()
            while len(data) < size:
                part = stream.recv(size - len(data))
                if not part:
                    raise EOFError("short frame")
                data.extend(part)
            return bytes(data)

        prefix = exact(4)
        size = int.from_bytes(prefix, "big")
        assert 0 < size <= 1024 * 1024, size
        return prefix + exact(size)

    def transmit(self, conn, data):
        for index in range(0, len(data), 256):
            block = data[index:index + 256]
            with self.downlink:
                if self.kbit:
                    time.sleep(len(block) * 8 / (self.kbit * 1000))
                conn.sendall(block)
            # Let another ready downlink sender contend between quanta.
            time.sleep(0)

    def exchange(self, conn):
        observation = {}
        try:
            with conn:
                conn.settimeout(8)
                request = self.frame(conn)
                op = request[4]
                assert op in (2, 15, 16), op
                # Connections are sequential except for the single head injected mid-range.
                observation = {"op": op, "request_bytes": len(request), "start": time.monotonic()}
                if op == 16:
                    self.range_attempt += 1
                attempt = self.range_attempt
                urgent = op == 2 and self.bulk_started.is_set()
                if urgent:
                    self.head_started.set()
                time.sleep(self.rtt / 2)
                if self.kbit:
                    time.sleep(len(request) * 8 / (self.kbit * 1000))
                with socket.create_connection(("127.0.0.1", self.source_port), 8) as source:
                    source.sendall(request)
                    reply = self.frame(source)
                assert reply[4] == 0, reply[4]
                time.sleep(self.rtt / 2)
                # Fixture payload is the last N bytes of successful range responses.
                payload = 0
                if op == 16:
                    payload = 17 if attempt == 4 else self.chunk_bytes
                    # The range success suffix is offset:u64, budget:u64,
                    # length:u32, and bytes. Verify the measured frame layout.
                    suffix = len(reply) - payload - 20
                    offset = int.from_bytes(reply[suffix:suffix + 8], "big")
                    budget = int.from_bytes(reply[suffix + 8:suffix + 16], "big")
                    length = int.from_bytes(reply[suffix + 16:suffix + 20], "big")
                    assert length == payload and budget == self.chunk_bytes
                    observation["range_offset"] = offset
                if op == 16 and attempt in (2, 3):
                    split = payload // 2 if attempt == 2 else payload // 4
                    initial = len(reply) - payload + split
                    self.transmit(conn, reply[:initial])
                    if attempt == 2:
                        observation["response_bytes"] = initial
                        observation["payload_bytes"] = payload // 2
                    else:
                        observation["bulk_trigger"] = time.monotonic()
                        self.bulk_started.set()
                        if self.gate_head:
                            assert self.head_started.wait(4), "urgent request did not arrive"
                            final_block = len(reply) - 256
                            self.transmit(conn, reply[initial:final_block])
                            # Keep a final block outstanding for deterministic overlap.
                            assert self.head_forwarded.wait(4), "urgent response did not cross the link"
                            self.transmit(conn, reply[final_block:])
                        else:
                            # Measurement: signal the client but never wait for it.
                            self.transmit(conn, reply[initial:])
                else:
                    self.transmit(conn, reply)
                    if urgent:
                        observation["head_forwarded"] = time.monotonic()
                        self.head_forwarded.set()
                observation["last_forwarded"] = time.monotonic()
                observation.setdefault("response_bytes", len(reply))
                observation.setdefault("payload_bytes", payload)
        except Exception as error:
            observation["error"] = repr(error)
        finally:
            observation["end"] = time.monotonic()
            self.records.append(observation)


def weak_link_case(binary, root, kbit, rtt, *, gate_head=False):
    root.mkdir()
    source, cache = root / "source.db", root / "receiver.db"
    read, write = "reference-read", "reference-write"
    chunk = 8192
    size = 2 * chunk + 17
    artifact = "weak-link"
    expected = bytes((index + index // 4096 * 37) % 251 for index in range(size))
    digest = hashlib.sha256(expected).hexdigest()
    run(binary, "upsert", source, artifact, str(size), "pattern")
    port, server = start_server(binary, source, read, write)
    try:
        with ArtifactLink(port, kbit, rtt, chunk, gate_head) as link:
            step = (binary, "step", str(link.port), cache, "phone", artifact, read, str(chunk))
            show = (binary, "show", cache, "phone", artifact)
            head = (binary, "record-head", str(link.port), "phone", read)
            start = time.monotonic()
            assert run(*head) == {"head": 0}
            head_only = time.monotonic() - start
            start = time.monotonic()
            assert run(*step)["offset"] == chunk
            first = time.monotonic() - start
            start = time.monotonic()
            failed = subprocess.run(step, cwd=ROOT, capture_output=True, text=True, timeout=12)
            interruption = time.monotonic() - start
            assert failed.returncode != 0 and "Source(Unavailable)" in failed.stderr, failed.stderr
            saved = run(*show)
            assert saved["offset"] == chunk and saved["size"] == 0 and saved["sha256"] is None
            assert not saved["verified"]
            resume_start = time.monotonic()
            with subprocess.Popen(step, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE) as resumed:
                try:
                    assert link.bulk_started.wait(6), "artifact response never reached trigger"
                    start = time.monotonic()
                    assert run(*head) == {"head": 0}
                    urgent = time.monotonic() - start
                    out, err = resumed.communicate(timeout=12)
                    assert resumed.returncode == 0, err
                    assert json.loads(out)["offset"] == 2 * chunk
                finally:
                    if resumed.poll() is None:
                        resumed.kill()
            staged = run(*show)
            assert staged["offset"] == 2 * chunk and staged["size"] == 0 and not staged["verified"]
            assert run(*step)["verified"]
            resume = time.monotonic() - resume_start
            final = run(*show)
            assert final["offset"] == size and final["size"] == size and final["verified"]
            assert final["sha256"] == digest
        traffic = link.records
        assert not any("error" in item for item in traffic), traffic
        chunks = [item for item in traffic if item["op"] == 16]
        assert [item["payload_bytes"] for item in chunks] == [chunk, chunk // 2, chunk, 17]
        assert [item["range_offset"] for item in chunks] == [0, chunk, chunk, 2 * chunk]
        heads = [item for item in traffic if item["op"] == 2]
        base_head, urgent_head = heads
        assert chunks[2]["bulk_trigger"] <= urgent_head["start"]
        head_during_bulk = urgent_head["head_forwarded"] < chunks[2]["last_forwarded"]
        if gate_head:
            assert head_during_bulk
        assert base_head["request_bytes"] == urgent_head["request_bytes"]
        assert base_head["response_bytes"] == urgent_head["response_bytes"]
        transfer = [item for item in traffic if item["op"] != 2]
        total = sum(item["request_bytes"] + item["response_bytes"] for item in transfer)
        sent_payload = sum(item["payload_bytes"] for item in transfer)
        assert sent_payload == size + chunk // 2
        return {
            "kbit_per_second": kbit, "configured_rtt_seconds": rtt,
            "chunk_limit_bytes": chunk, "artifact_sha256": digest, "verified_bytes": size,
            "reopened_interrupted_offset": saved["offset"], "hidden_until_verified": True,
            "artifact_exchanges": len(transfer), "artifact_wire_bytes": total,
            "artifact_protocol_bytes": total - sent_payload, "artifact_forwarded_payload_bytes": sent_payload,
            "artifact_repeated_payload_bytes": sent_payload - size,
            "first_chunk_seconds": round(first, 6), "interrupted_attempt_seconds": round(interruption, 6),
            "resume_and_verify_seconds": round(resume, 6),
            "head_only_seconds": round(head_only, 6), "head_during_artifact_seconds": round(urgent, 6),
            "gate_head": gate_head,
            "head_request_arrived_before_bulk_end": urgent_head["start"] < chunks[2]["last_forwarded"],
            "head_reply_forwarded_before_bulk_end": head_during_bulk,
            "head_wire_bytes": urgent_head["request_bytes"] + urgent_head["response_bytes"],
        }
    finally:
        stop_server(server)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--weak-link", action="store_true", help="run matched unshaped and weak-link artifact profiles")
    args = parser.parse_args()
    subprocess.run(["cargo", "build", "--quiet", "--locked", "--features", "transport,sqlite",
                    "--example", "artifact_network"], cwd=ROOT, check=True)
    binary = ROOT / "target/debug/examples/artifact_network"
    with tempfile.TemporaryDirectory(prefix="nessa-sync-artifact-") as temp:
        if args.weak_link:
            results = [weak_link_case(binary, Path(temp) / name, kbit, rtt)
                       for name, kbit, rtt in (("unshaped", 0, 0), ("weak", 32, 0.8))]
            print(json.dumps({"profiles": results}, sort_keys=True))
            return
        source = str(Path(temp) / "source.db")
        phone = str(Path(temp) / "phone.db")
        laptop = str(Path(temp) / "laptop.db")
        read, write = "reference-read", "reference-write"
        size = 1024 * 1024 + 17
        assert run(binary, "upsert", source, "large", str(size), "a")["revision"] == 1
        run(binary, "upsert", source, "small", "100", "x")
        run(binary, "upsert", source, "delete-me", "131072", "d")
        port, server = start_server(binary, source, read, write)
        observed = []
        try:
            for expected in range(1, 4):
                step = run(binary, "step", str(port), phone, "phone", "large", read)
                observed.append(step)
                assert step["offset"] == expected * 65536 and not step["verified"]
            assert run(binary, "record-head", str(port), "phone", read)["head"] == 0
            repeated = run(binary, "repeat", str(port), "phone", "large", read)
            assert repeated["duplicate_bytes"] == 65536
            before = run(binary, "show", phone, "phone", "large")
            assert before["offset"] == 3 * 65536 and not before["verified"]
            stop_server(server)
            run(binary, "step", str(port), phone, "phone", "large", read, expect_ok=False)
            assert run(binary, "show", phone, "phone", "large")["offset"] == before["offset"]
            port, server = start_server(binary, source, read, write)
            assert run(binary, "step", str(port), phone, "phone", "large", read)["offset"] == 4 * 65536
            run(binary, "upsert", source, "large", str(size), "b")
            changed = run(binary, "step", str(port), phone, "phone", "large", read)
            observed.append(changed)
            assert changed["revision"] == 4 and changed["offset"] == 65536
            for _ in range(32):
                step = run(binary, "step", str(port), phone, "phone", "large", read)
                observed.append(step)
                if step["verified"]:
                    break
            else:
                raise AssertionError("large artifact did not finish")
            assert run(binary, "show", phone, "phone", "large")["size"] == size
            bad = run(binary, "inject-bad", str(port), laptop, "laptop", "small", read)
            assert bad["hash_mismatch"] and bad["offset"] == 0 and not bad["verified"]
            assert not run(binary, "show", laptop, "laptop", "small")["verified"]
            assert run(binary, "step", str(port), laptop, "laptop", "small", read)["verified"]
            assert run(binary, "step", str(port), laptop, "laptop", "delete-me", read)["offset"] == 65536
            run(binary, "delete", source, "delete-me")
            assert run(binary, "step", str(port), laptop, "laptop", "delete-me", read)["deleted"]
            assert run(binary, "show", laptop, "laptop", "delete-me")["deleted"]
            # A denied old epoch fences the local cache; the same epoch cannot resume.
            stop_server(server)
            port, server = start_server(binary, source, read, write, "epoch-2")
            assert run(binary, "step", str(port), phone, "phone", "large", read)["revoked"]
            assert run(binary, "show", phone, "phone", "large")["revoked"]
            assert run(binary, "record-head", str(port), "phone", read, expect_ok=False) is None
            print(json.dumps({"size": size, "chunks": len(observed),
                              "payload_bytes": sum(s["payload_bytes"] for s in observed),
                              "duplicate_bytes": repeated["duplicate_bytes"] + sum(s["duplicate_bytes"] for s in observed),
                              "protocol_bytes": sum(s["protocol_bytes"] for s in observed),
                              "restart_resumed": True, "version_restart": True,
                              "hash_mismatch_refused": True, "deletion_fenced": True,
                              "stale_grant_refused": True, "urgent_head_served": True,
                              "weak_link": weak_link_case(binary, Path(temp) / "weak", 32, 0.8, gate_head=True)},
                             sort_keys=True))
        finally:
            stop_server(server)


if __name__ == "__main__":
    main()
