#!/usr/bin/env python3
"""Run separate transcript processes against files this script creates itself."""

import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "target" / "debug" / "examples" / "transcript_sqlite"


def command(*args, expect_success=True, discard_reply=False):
    result = subprocess.run(
        [str(EXAMPLE), *map(str, args)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if discard_reply:
        # The caller does not learn whether the operation committed. The next
        # process must infer it solely from durable progress.
        return None
    if expect_success and result.returncode != 0:
        raise AssertionError(f"command failed: {args}: {result.stderr}")
    if not expect_success and result.returncode == 0:
        raise AssertionError(f"command unexpectedly succeeded: {args}")
    return result.stdout


def snapshot(path):
    lines = command("show", path, "device-a").splitlines()
    return lines


def main():
    subprocess.run(
        ["cargo", "build", "--locked", "--features", "sqlite", "--example", "transcript_sqlite"],
        cwd=ROOT,
        check=True,
    )
    # TemporaryDirectory owns exactly these files. No user-provided path is
    # removed, overwritten, or treated as a disposable demo database.
    with tempfile.TemporaryDirectory(prefix="nessa-sync-slice-2-") as directory:
        root = Path(directory)
        source = root / "source.db"
        replica = root / "device-a.db"
        command("append", source, "fact-1", "hello")
        command("append", source, "fact-2", "world")
        first = json.loads(command("sync", source, replica, "device-a"))
        assert (first["before"], first["after"], first["applied_records"]) == (0, 2, 2)
        assert snapshot(replica) == [
            "checkpoint\t2",
            "1\tfact-1\thello",
            "2\tfact-2\tworld",
        ]

        # The source file is absent while a fresh process reads the device.
        absent_source = root / "source-unavailable.db"
        source.rename(absent_source)
        assert snapshot(replica)[0] == "checkpoint\t2"
        absent_source.rename(source)

        command("append", source, "fact-3", "after-restart")
        with sqlite3.connect(replica) as db:
            db.execute("""
                CREATE TRIGGER fail_third_record BEFORE INSERT ON replica_records
                WHEN NEW.position = 3 BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """)
        command("sync", source, replica, "device-a", expect_success=False)
        assert snapshot(replica) == [
            "checkpoint\t2",
            "1\tfact-1\thello",
            "2\tfact-2\tworld",
        ]
        with sqlite3.connect(replica) as db:
            db.execute("DROP TRIGGER fail_third_record")
        recovered = json.loads(command("sync", source, replica, "device-a"))
        assert (recovered["before"], recovered["after"], recovered["applied_records"]) == (2, 3, 1)

        # The commit succeeds but the caller discards its reply. A new process
        # reloads the saved checkpoint and does not append duplicate rows.
        command("append", source, "fact-4", "reply-lost")
        command("sync", source, replica, "device-a", discard_reply=True)
        repeated = json.loads(command("sync", source, replica, "device-a"))
        assert (repeated["before"], repeated["after"], repeated["applied_records"]) == (4, 4, 0)
        assert (repeated["page_reads"], repeated["payload_bytes"]) == (0, 0)
        assert snapshot(replica) == [
            "checkpoint\t4",
            "1\tfact-1\thello",
            "2\tfact-2\tworld",
            "3\tfact-3\tafter-restart",
            "4\tfact-4\treply-lost",
        ]
        print(json.dumps({
            "status": "ok",
            "checkpoint": 4,
            "cached_records": 4,
            "offline_cached_read": True,
            "rollback_preserved_checkpoint": True,
            "lost_reply_duplicate_records": 0,
            "unchanged_head_payload_bytes": repeated["payload_bytes"],
        }, separators=(",", ":")))


if __name__ == "__main__":
    main()
