#!/usr/bin/env python3
"""Separate-process catalogue wire lab with two persistent receivers."""

import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*args, expect_ok=True):
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
    if expect_ok and result.returncode:
        raise AssertionError(f"{args}: {result.stderr}")
    if not expect_ok:
        assert result.returncode != 0, args
        return None
    return json.loads(result.stdout)


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_server(port_number, process):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("source process exited")
        try:
            with socket.create_connection(("127.0.0.1", port_number), timeout=0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError("source process did not listen")


def main():
    subprocess.run(["cargo", "build", "--quiet", "--locked", "--features", "transport,sqlite", "--example", "catalogue_network", "--example", "catalogue_apps"], cwd=ROOT, check=True)
    network = ROOT / "target/debug/examples/catalogue_network"
    local = ROOT / "target/debug/examples/catalogue_apps"
    with tempfile.TemporaryDirectory(prefix="nessa-sync-catalogue-wire-") as temp:
        source = str(Path(temp) / "source.db")
        phone = str(Path(temp) / "phone.db")
        laptop = str(Path(temp) / "laptop.db")
        unused = str(Path(temp) / "unused.db")
        read_token, write_token = "reference-read", "reference-write"
        assert run(local, "transcript", source, unused, "seed", "620")["seeded"] == 620
        port_number = port()
        server = subprocess.Popen([network, "serve", source, str(port_number), read_token, write_token], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            wait_server(port_number, server)
            first = run(network, "pass", str(port_number), phone, "phone", read_token, "1")
            assert first["active"] and first["count"] == 40 and first["completed"] == 0
            measured = [first]
            for turn in range(10):
                run(local, "transcript", source, unused, "upsert", "entry-0000", f"churn-{turn}")
                one = run(network, "pass", str(port_number), phone, "phone", read_token, "1")
                assert one["active"] and one["completed"] == 0
                measured.append(one)
            run(local, "transcript", source, unused, "upsert", "entry-0000", "changed")
            source_revision = run(local, "transcript", source, unused, "delete", "entry-0619")["revision"]
            laptop_pass = run(network, "pass", str(port_number), laptop, "laptop", read_token, "40")
            phone_pass = run(network, "pass", str(port_number), phone, "phone", read_token, "40")
            assert laptop_pass["completed"] == source_revision and laptop_pass["count"] == 620
            assert phone_pass["completed"] == 620 and phone_pass["count"] == 620
            phone_catchup = run(network, "pass", str(port_number), phone, "phone", read_token, "40")
            assert phone_catchup["completed"] == source_revision and phone_catchup["count"] == 620
            measured.extend((laptop_pass, phone_pass, phone_catchup))
            assert run(network, "show", phone, "phone", "entry-0000")["payload"] == "changed"
            assert run(network, "show", laptop, "laptop", "entry-0619")["deleted"]
            before = run(network, "counters", str(port_number), write_token)
            assert before["manifest_reads"] >= 32 and before["resolve_reads"] >= 1240
            assert run(network, "show", phone, "phone", "entry-0001")["payload"] == "session-0001:hello"
            assert run(network, "counters", str(port_number), write_token) == before
            unchanged = run(network, "pass", str(port_number), phone, "phone", read_token, "40")
            assert unchanged["pages"] == 0 and unchanged["payload_bytes"] == 0
            run(network, "pass", str(port_number), phone, "phone", "wrong-token", "1", expect_ok=False)
            after = run(network, "counters", str(port_number), write_token)
            assert after["resolve_reads"] == before["resolve_reads"]
            for mode in ("drop", "truncate"):
                receiver_db = str(Path(temp) / f"fault-{mode}.db")
                initial = run(network, "pass", str(port_number), receiver_db, "phone", read_token, "1")
                assert initial["active"] and initial["count"] == 40
                fault_port = port()
                faulty = subprocess.Popen([network, "serve", source, str(fault_port), read_token, write_token, mode], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                try:
                    wait_server(fault_port, faulty)
                    run(network, "pass", str(fault_port), receiver_db, "phone", read_token, "40", expect_ok=False)
                    assert run(network, "show", receiver_db, "phone", "entry-0040")["missing"]
                    recovered = run(network, "pass", str(fault_port), receiver_db, "phone", read_token, "40")
                    assert recovered["completed"] == source_revision and recovered["count"] == 620
                finally:
                    faulty.terminate()
                    try:
                        faulty.communicate(timeout=3)
                    except subprocess.TimeoutExpired:
                        faulty.kill()
                        faulty.communicate()
            print(json.dumps({"entries": 620, "source_revision": source_revision, "receivers": 2,
                              "manifest_bytes": sum(p["manifest_bytes"] for p in measured),
                              "payload_bytes": sum(p["payload_bytes"] for p in measured),
                              "duplicate_bytes": sum(p["duplicate_bytes"] for p in measured),
                              "protocol_bytes": sum(p["protocol_bytes"] for p in measured),
                              "server_reads": after}, sort_keys=True))
        finally:
            server.terminate()
            try:
                server.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()


if __name__ == "__main__":
    main()
