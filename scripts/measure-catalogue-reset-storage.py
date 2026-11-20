#!/usr/bin/env python3
"""Observe durable storage of the existing owned native catalogue-reset fixture."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
TABLES = ("catalogue_candidates", "catalogue_entries", "catalogue_progress")


def observe(cache):
    # Sample the quiescent files after the writer process has exited, before
    # opening another handle. This is not a writer-peak WAL measurement.
    wal = cache.with_name(f"{cache.name}-wal")
    files = {"main_file_bytes": cache.stat().st_size,
             "wal_file_bytes_or_null": wal.stat().st_size if wal.exists() else None}
    with closing(sqlite3.connect(cache.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        names = sorted(row[0] for row in db.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ))
        assert tuple(names) == TABLES, names
        candidate_rows = db.execute("SELECT count(*) FROM catalogue_candidates").fetchone()[0]
        live_rows, fence_rows = db.execute(
            "SELECT count(*) - coalesce(sum(deleted),0), coalesce(sum(deleted),0) FROM catalogue_entries"
        ).fetchone()
        completed, phase = db.execute(
            "SELECT completed, reset_phase FROM catalogue_progress"
        ).fetchone()
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        page_count = db.execute("PRAGMA page_count").fetchone()[0]
        freelist = db.execute("PRAGMA freelist_count").fetchone()[0]
        try:
            physical = {
                name: {"pages": pages, "cells": cells, "page_bytes": size, "payload_bytes": payload}
                for name, pages, cells, size, payload in db.execute(
                    "SELECT name,count(*),sum(ncell),sum(pgsize),sum(payload) FROM dbstat "
                    "WHERE name IN (?,?,?) GROUP BY name",
                    TABLES,
                )
            }
        except sqlite3.OperationalError as error:
            if "no such table: dbstat" not in str(error):
                raise
            physical = None
        else:
            assert tuple(sorted(physical)) == TABLES, physical
    return {
        "candidate_rows": candidate_rows, "live_rows": live_rows, "permanent_fence_rows": fence_rows,
        "completed": completed, "reset_phase": phase, "tables": names,
        "page_size": page_size, "database_logical_page_count": page_count,
        "database_logical_page_bytes": page_count * page_size, "freelist_pages": freelist,
        "dbstat_tables_or_null": physical, **files,
    }


def run(binary, command, source, cache):
    result = subprocess.run(
        [binary, command, source, cache, "owned"], cwd=ROOT, check=True,
        capture_output=True, text=True, timeout=30,
    )
    return json.loads(result.stdout)


def measure(binary):
    with tempfile.TemporaryDirectory(prefix="owned-catalogue-storage-") as directory:
        source, cache = Path(directory) / "source.db", Path(directory) / "cache.db"
        assert run(binary, "prepare", source, cache) == {"prepared": 620, "retired": 160, "head": 780}
        snapshots = {"before_reset": observe(cache)}
        report = run(binary, "start", source, cache)
        snapshots["after_start"] = observe(cache)
        journal = [snapshots["after_start"]]
        assert report["phase"] == "Enumerate"
        for _ in range(64):
            previous = report
            report = run(binary, "step", source, cache)
            current = observe(cache)
            journal.append(current)
            assert report["pending"] == current["candidate_rows"]
            assert current["candidate_rows"] <= journal[-2]["candidate_rows"]
            if previous["phase"] == "Enumerate" and report["phase"] == "Reconcile":
                snapshots["after_enumeration"] = current
            if report["done"]:
                snapshots["after_finish"] = current
                assert report["phase"] == "Idle"
                break
        else:
            raise AssertionError("owned recovery did not finish")
        phases = ("before_reset", "after_start", "after_enumeration", "after_finish")
        actual = [
            tuple(snapshots[phase][key] for key in
                  ("candidate_rows", "live_rows", "permanent_fence_rows", "completed"))
            for phase in phases
        ]
        assert actual == [(0, 620, 0, 620), (620, 0, 0, 0), (160, 460, 0, 0), (0, 460, 160, 780)], actual
        query = "SELECT entry_id,creation,revision,deleted,payload FROM {} ORDER BY entry_id"
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as src:
            expected_rows = src.execute(query.format("catalogue_source_entries")).fetchall()
        with closing(sqlite3.connect(cache.as_uri() + "?mode=ro", uri=True)) as dst:
            assert dst.execute(query.format("catalogue_entries")).fetchall() == expected_rows
        dbstat = [snap["dbstat_tables_or_null"] for snap in journal]
        assert all(data is None for data in dbstat) or all(data is not None for data in dbstat)
        return {
            "phase_snapshots": snapshots, "post_commit_samples": len(journal) + 1,
            "peak_sampled_candidate_rows": max(snap["candidate_rows"] for snap in journal),
            "peak_sampled_candidate_table_page_bytes_or_null": (
                None if dbstat[0] is None else max(data["catalogue_candidates"]["page_bytes"] for data in dbstat)
            ),
            "receiver_rows_match_source_after_reopen": True,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    build = ["cargo", "build", "--locked", "--all-features", "--example", "catalogue_reset"]
    # Ask Cargo where it placed the executable, including CARGO_TARGET_DIR overrides.
    messages = subprocess.check_output(build + ["--message-format=json"], cwd=ROOT, text=True)
    artifacts = [json.loads(line) for line in messages.splitlines()]
    binary, = [event["executable"] for event in artifacts if event.get("reason") == "compiler-artifact"
               and event.get("executable") and event["target"]["name"] == "catalogue_reset"]
    result = {
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "observer_sqlite_version": sqlite3.sqlite_version,
        "scope": "native 620-row owned fixture; quiescent post-commit storage, not transient writer peak",
        **measure(binary),
    }
    data = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(data)
    print(data, end="")


if __name__ == "__main__":
    main()
