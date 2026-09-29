#!/usr/bin/env python3
"""Source-process and durable receiver lab for bounded artifact transfer."""

import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*command, expect_ok=True):
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
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


def main():
    subprocess.run(["cargo", "build", "--quiet", "--locked", "--features", "transport,sqlite",
                    "--example", "artifact_network"], cwd=ROOT, check=True)
    binary = ROOT / "target/debug/examples/artifact_network"
    with tempfile.TemporaryDirectory(prefix="nessa-sync-artifact-") as temp:
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
                              "stale_grant_refused": True, "urgent_head_served": True}, sort_keys=True))
        finally:
            stop_server(server)


if __name__ == "__main__":
    main()
