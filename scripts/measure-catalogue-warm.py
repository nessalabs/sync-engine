#!/usr/bin/env python3
"""Measure #51 warm catalogue manifests using the slice 7 application counters."""

import argparse
from collections import Counter
import importlib.util
import json
from pathlib import Path
import selectors
import socket
import sqlite3
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
spec = importlib.util.spec_from_file_location(
    "catalogue_publication_lab", HERE / "measure-catalogue-publication.py"
)
publication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publication)
READ, WRITE = "reference-read", "reference-write"
# Legacy 11–13, capability 17–19; only success branches appear in this lab.
OPERATIONS = {"legacy": (11, 12, 13), "capabilities": (17, 18, 19)}


def manifest_descriptors(request, reply):
    """Return actual wire descriptors, without counting the echoed request as entries."""
    status, operation = reply[4], request[4]
    assert operation in (12, 18) and status == 0
    # Successful manifest replies echo the exact pass and page limit.
    # The capability request adds a 10-byte version/support tuple after the
    # token; its reply adds operation + that tuple + 26-byte SourceHead.
    pass_start = 6 + request[5] + (10 if operation == 18 else 0)
    echo_start = 42 if operation == 18 else 5
    expected_echo = request[pass_start:]
    assert reply[echo_start:echo_start + len(expected_echo)] == expected_echo
    cursor = echo_start + len(expected_echo)
    assert reply[cursor] in (0, 1)  # has_more
    count = int.from_bytes(reply[cursor + 1:cursor + 3], "big")
    assert 0 <= count <= 40
    cursor += 3
    result = []
    for _ in range(count):
        start = cursor
        creation = int.from_bytes(reply[cursor:cursor + 8], "big")
        cursor += 8
        size = reply[cursor]
        cursor += 1
        identity = reply[cursor:cursor + size].decode("utf-8")
        cursor += size
        revision = int.from_bytes(reply[cursor:cursor + 8], "big")
        cursor += 8
        deleted = reply[cursor]
        cursor += 1
        assert deleted in (0, 1)
        result.append(((identity, creation, revision, deleted), cursor - start))
    assert cursor == len(reply)
    return result


class ManifestHop(publication.link.Forwarder):
    """One request/response per real TCP connection; observe before forwarding."""

    def __init__(self, source_port):
        self.total_bytes = 0
        self.pages = []
        self.errors = []
        super().__init__(source_port, "loopback", framed=True)

    def handle(self, client):
        try:
            with client, socket.create_connection(("127.0.0.1", self.upstream), 5) as source:
                publication.link.tune(client)
                publication.link.tune(source)
                request = publication.link.frame(client)
                operation = request[4]
                assert any(operation in ops for ops in OPERATIONS.values()), operation
                source.sendall(request)
                reply = publication.link.frame(source)
                assert reply[4] == 0
                with self.lock:
                    self.requests[operation] += 1
                    self.total_bytes += len(request) + len(reply)
                    if operation in (12, 18):
                        self.pages.append(manifest_descriptors(request, reply))
                client.sendall(reply)
        except (OSError, EOFError, ValueError, AssertionError, IndexError) as error:
            with self.lock:
                self.errors.append(repr(error))


def backup(source, destination):
    src, dst = sqlite3.connect(source), sqlite3.connect(destination)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def measured_pass(network, source, warm, cache, dialect, count, expected_head):
    backup(warm, cache)
    head, manifest, resolve = OPERATIONS[dialect]
    child = subprocess.Popen(
        [str(network), "serve", str(source), "0", READ, WRITE],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    hop = None
    try:
        # The owned example reports the OS-assigned loopback port after bind.
        with selectors.DefaultSelector() as readiness:
            readiness.register(child.stderr, selectors.EVENT_READ)
            assert readiness.select(5), "catalogue source did not announce readiness"
            line = child.stderr.readline()
        assert line.startswith("listening on 127.0.0.1:"), line
        hop = ManifestHop(int(line.rsplit(":", 1)[1]))
        command = "cap-pass" if dialect == "capabilities" else "pass"
        start = time.perf_counter()
        report, _ = publication.run(network, command, hop.port, cache, "phone", READ,
                                    (count + 39) // 40 + 1)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 3)
    finally:
        if hop is not None:
            hop.close()
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
        finally:
            child.stderr.close()
    assert not hop.errors, hop.errors
    original = publication.rows(warm)
    actual = publication.rows(source, True)
    assert len(original) == len(actual) == count
    assert publication.rows(cache) == actual
    assert publication.progress(cache) == (expected_head, None, None, None)
    assert report["completed"] == expected_head and not report["active"]
    assert report["duplicate_bytes"] == 0
    prior = {row[0]: row for row in original}
    changed = [row for row in actual if row[2] > count]
    emitted = [entry for page in hop.pages for entry, _ in page]
    assert emitted == [row[:4] for row in changed]
    assert report["payload_bytes"] == sum(
        len(row[4]) for row in changed if row[:4] != prior[row[0]][:4]
    )
    assert hop.requests[head] == 1 and hop.requests[manifest] == report["pages"] == len(hop.pages)
    matches = [[entry == prior[entry[0]][:4] for entry, _ in page] for page in hop.pages]
    eligible = sum(sum(size for _, size in page) for page, exact in zip(hop.pages, matches)
                   if exact and all(exact))
    exact_bytes = sum(size for page, exact in zip(hop.pages, matches)
                      for (_, size), same in zip(page, exact) if same)
    assert hop.requests[resolve] == len(changed) - sum(sum(exact) for exact in matches)
    descriptor_bytes = sum(size for page in hop.pages for _, size in page)
    # Slice 7's descriptor accounting leaves the length-prefix byte in protocol.
    assert descriptor_bytes == report["manifest_bytes"] + len(emitted)
    assert hop.total_bytes == report["protocol_bytes"] + report["manifest_bytes"] + report["payload_bytes"]
    assert all(row == prior[row[0]] for row in actual if row[2] <= count)
    return {
        "dialect": dialect, "catalogue_entries": count, "prior_completed": count,
        "completed": expected_head, "manifest_pages": len(hop.pages),
        "resolved_entries": hop.requests[resolve],
        "observed_operations": dict(hop.requests), "emitted_manifest_descriptors": len(emitted),
        "emitted_deletion_descriptors": sum(row[3] for row in emitted),
        "omitted_unchanged_rows": count - len(changed),
        "exact_cached_matches_in_emitted_descriptors": sum(sum(exact) for exact in matches),
        "all_unchanged_nonempty_manifest_pages": sum(bool(exact) and all(exact) for exact in matches),
        "observed_unchanged_descriptor_bytes_any_page": exact_bytes,
        "observed_descriptor_bytes_on_eligible_all_unchanged_pages": eligible,
        "actual_manifest_descriptor_wire_bytes": descriptor_bytes,
        "slice7_manifest_counter_bytes": report["manifest_bytes"],
        "slice7_protocol_counter_bytes": report["protocol_bytes"],
        "opaque_payload_bytes": report["payload_bytes"], "total_application_wire_bytes": hop.total_bytes,
        "reopened_rows_match_source": True, "elapsed_ms": elapsed_ms,
    }



def cached_ahead_control(apps, network, folder):
    """A valid old boundary can cover a value already newer than completed."""
    folder.mkdir()
    source, warm = folder / "source.db", folder / "warm.db"
    count = 500
    assert publication.run(apps, "transcript", source, warm, "seed", count)[0]["seeded"] == count
    partial, _ = publication.run(apps, "transcript", source, warm, "pass", 1)
    assert partial["completed"] == 0 and partial["active_boundary"] == count
    revision, _ = publication.run(apps, "transcript", source, warm, "upsert", "entry-0499", "ahead")
    assert revision["revision"] == count + 1
    finished, _ = publication.run(apps, "transcript", source, warm, "pass")
    assert finished["completed"] == count
    assert publication.rows(warm) == publication.rows(source, True)
    cases = []
    for dialect in OPERATIONS:
        case = measured_pass(network, source, warm, folder / f"{dialect}.db",
                             dialect, count, count + 1)
        assert case["emitted_manifest_descriptors"] == 1 and case["resolved_entries"] == 0
        assert case["exact_cached_matches_in_emitted_descriptors"] == 1
        assert case["all_unchanged_nonempty_manifest_pages"] == 1
        assert case["observed_descriptor_bytes_on_eligible_all_unchanged_pages"] == (
            case["actual_manifest_descriptor_wire_bytes"]
        )
        cases.append(case)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", choices=(500, 5000, 50000),
                        default=[500, 5000, 50000])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    subprocess.run(["cargo", "build", "--locked", "--all-features", "--example", "catalogue_apps",
                    "--example", "catalogue_network"], cwd=ROOT, check=True)
    apps = ROOT / "target/debug/examples/catalogue_apps"
    network = ROOT / "target/debug/examples/catalogue_network"
    cases = []
    with tempfile.TemporaryDirectory(prefix="catalogue-warm-") as scratch:
        for count in sorted(set(args.sizes)):
            folder = Path(scratch) / str(count)
            folder.mkdir()
            initial, warm = folder / "initial.db", folder / "warm.db"
            assert publication.run(apps, "transcript", initial, warm, "seed", count)[0]["seeded"] == count
            assert publication.run(apps, "transcript", initial, warm, "pass")[0]["completed"] == count
            assert publication.progress(warm) == (count, None, None, None)
            for label, stride in (("low", 100), ("high", 5)):
                source = folder / f"{label}.db"
                backup(initial, source)
                expected = count // stride
                assert publication.run(apps, "transcript", source, warm, "churn-fixture",
                                       count, stride)[0]["mutations"] == expected
                matched = []
                for dialect in OPERATIONS:
                    cache = folder / f"{label}-{dialect}.db"
                    case = measured_pass(network, source, warm, cache, dialect, count, count + expected)
                    case["churn"] = label
                    case["mutation_stride"] = stride
                    matched.append(case)
                fields = ("actual_manifest_descriptor_wire_bytes", "opaque_payload_bytes",
                          "emitted_manifest_descriptors", "observed_descriptor_bytes_on_eligible_all_unchanged_pages")
                assert all(matched[0][key] == matched[1][key] for key in fields), matched
                cases.extend(matched)
        cached_ahead = cached_ahead_control(apps, network, Path(scratch) / "cached-ahead")
    report = {
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "scope": "unshaped development loopback; no encoding or savings projection implemented",
        "cases": cases,
        "cached_ahead_control": cached_ahead,
    }
    data = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(data)
    print(data, end="")


if __name__ == "__main__":
    main()
