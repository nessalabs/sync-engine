#!/usr/bin/env python3
"""The same process-level conformance path for two host-defined applications."""

import json
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "debug" / "examples" / "local_apps"
READ = "local-read-token"
WRITE = "local-write-token"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def call(*args, ok=True):
    result = subprocess.run([str(BIN), *map(str, args)], capture_output=True, text=True)
    if (result.returncode == 0) != ok:
        raise AssertionError((args, result.returncode, result.stdout, result.stderr))
    return json.loads(result.stdout) if result.stdout.startswith("{") else result.stdout


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.02)
    raise AssertionError("timed out waiting for example process")


def page(port_number):
    with urllib.request.urlopen(f"http://127.0.0.1:{port_number}/", timeout=2) as response:
        assert response.status == 200
        return response.read().decode("utf-8")


def stop(process):
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=3)


def source(app, path):
    source_port = port()
    process = subprocess.Popen([str(BIN), "source", app, str(path), str(source_port),
                                READ, WRITE], stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    wait_until(lambda: process.poll() is None and reachable(source_port))
    return process, f"127.0.0.1:{source_port}"


def view(app, replica, status, receiver):
    view_port = port()
    process = subprocess.Popen([str(BIN), "view", app, str(replica), str(status),
                                receiver, str(view_port)], stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    wait_until(lambda: process.poll() is None and page(view_port))
    return process, view_port


def reachable(port_number):
    try:
        with socket.create_connection(("127.0.0.1", port_number), 0.1):
            return True
    except OSError:
        return False


def run_case(app, root):
    directory = root / app
    directory.mkdir()
    source_path = directory / "source.db"
    phone_db, laptop_db = directory / "phone.db", directory / "laptop.db"
    phone_status, laptop_status = directory / "phone.status", directory / "laptop.status"
    processes = []
    source_process, address = source(app, source_path)
    processes.append(source_process)
    try:
        missing_view, missing_port = view(app, laptop_db, laptop_status, "laptop")
        processes.append(missing_view)
        assert 'data-state="not_loaded"' in page(missing_port)
        assert "Complete and empty" not in page(missing_port)

        empty = call("sync", app, address, phone_db, phone_status, "phone", READ)
        assert empty["checkpoint"] == 0 and empty["payload_bytes"] == 0
        phone_view, phone_port = view(app, phone_db, phone_status, "phone")
        processes.append(phone_view)
        assert 'data-state="complete_empty"' in page(phone_port)

        if app == "transcript":
            call("mutate", app, address, WRITE, "e1", "message", "<script>hello")
            call("mutate", app, address, WRITE, "e2", "message", "world")
            initial = "&lt;script&gt;hello"
            later = "small"
            mutation = ("message", later)
            expected_delta = 1 + 2 + len(later.encode())
        else:
            call("mutate", app, address, WRITE, "e1", "create", "t1", "alpha")
            call("mutate", app, address, WRITE, "e2", "create", "t2", "temporary")
            initial = "t1: alpha [open]"
            later = "t1: updated [open]"
            mutation = ("title", "t1", "updated")
            expected_delta = 1 + 2 + 2 + 2 + len("updated")

        first_phone = call("sync", app, address, phone_db, phone_status, "phone", READ)
        first_laptop = call("sync", app, address, laptop_db, laptop_status, "laptop", READ)
        assert first_phone["checkpoint"] == first_laptop["checkpoint"] == 2
        assert first_phone["payload_bytes"] > 0
        assert initial in page(phone_port)
        assert "<script>hello" not in page(phone_port)
        assert initial in page(missing_port)
        saved_status = directory / "phone.status.saved"
        phone_status.rename(saved_status)
        assert 'data-state="partial"' in page(phone_port)
        assert initial in page(phone_port)
        saved_status.rename(phone_status)

        # Browser navigation reads only each receiver's SQLite cache.
        before = call("stats", address, WRITE)
        for _ in range(3):
            assert initial in page(phone_port)
            assert initial in page(missing_port)
        after = call("stats", address, WRITE)
        assert before["head_reads"] == after["head_reads"]
        assert before["page_reads"] == after["page_reads"]

        stop(source_process)
        assert initial in page(phone_port)
        call("sync", app, address, phone_db, phone_status, "phone", READ, ok=False)
        assert "unavailable at last check" in page(phone_port)
        assert initial in page(phone_port)

        source_process, address = source(app, source_path)
        processes.append(source_process)
        call("mutate", app, address, WRITE, "e3", *mutation)
        changed_laptop = call("sync", app, address, laptop_db, laptop_status, "laptop", READ)
        assert changed_laptop["checkpoint"] == 3
        assert changed_laptop["payload_bytes"] == expected_delta
        assert f"<li>{later}</li>" in page(missing_port)
        assert f"<li>{later}</li>" not in page(phone_port)
        changed_phone = call("sync", app, address, phone_db, phone_status, "phone", READ)
        assert changed_phone["checkpoint"] == 3
        assert changed_phone["payload_bytes"] == expected_delta
        assert f"<li>{later}</li>" in page(phone_port)
        applied_before_idle = phone_status.read_text().split("\t")[3]
        unchanged = call("sync", app, address, phone_db, phone_status, "phone", READ)
        assert unchanged["payload_bytes"] == 0
        assert phone_status.read_text().split("\t")[3] == applied_before_idle

        if app == "tasks":
            call("mutate", app, address, WRITE, "e4", "delete", "t2")
            call("mutate", app, address, WRITE, "e5", "complete", "t1", "true")
            assert call("sync", app, address, phone_db, phone_status, "phone", READ)["checkpoint"] == 5
            task_page = page(phone_port)
            assert "t1: updated [done]" in task_page
            assert "temporary" not in task_page
            assert call("sync", app, address, laptop_db, laptop_status, "laptop", READ)["checkpoint"] == 5

        last_applied_before_failure = phone_status.read_text().split("\t")[3]
        stop(source_process)
        stop(phone_view)
        call("sync", app, address, phone_db, phone_status, "phone", READ, ok=False)
        assert phone_status.read_text().split("\t")[3] == last_applied_before_failure
        restarted_view, restarted_port = view(app, phone_db, phone_status, "phone")
        processes.append(restarted_view)
        assert later.split(" [")[0] in page(restarted_port)
        assert "Applied position" in page(restarted_port)
        assert "Last applied" in page(restarted_port)
        assert "unavailable at last check" in page(restarted_port)
        return {"app": app, "cached_navigation_remote_reads": 0,
                "one_change_payload_bytes": expected_delta,
                "unchanged_payload_bytes": unchanged["payload_bytes"],
                "phone_checkpoint": 5 if app == "tasks" else 3,
                "offline_cached_read": True, "restart_cached_read": True,
                "not_loaded_distinct_from_empty": True,
                "task_deletion_reconciled": app == "tasks"}
    finally:
        for process in processes:
            stop(process)


def main():
    subprocess.run(["cargo", "build", "--locked", "--features", "transport",
                    "--example", "local_apps"], cwd=ROOT, capture_output=True, check=True)
    with tempfile.TemporaryDirectory(prefix="nessa-sync-slice4-") as directory:
        root = Path(directory)
        results = [run_case(app, root) for app in ("transcript", "tasks")]
        print(json.dumps({"slice": 4, "conformance": results}))


if __name__ == "__main__":
    main()
