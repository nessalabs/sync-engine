#!/usr/bin/env python3
"""Process-level catalogue pass lab for transcript and task list hosts."""
import json
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
subprocess.run(["cargo", "build", "--locked", "--features", "sqlite", "--example", "catalogue_apps"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
BIN = ROOT / "target" / "debug" / "examples" / "catalogue_apps"


def call(app, source, receiver, command, *args):
    result = subprocess.run([str(BIN), app, str(source), str(receiver), command, *map(str, args)], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


results = []
with tempfile.TemporaryDirectory(prefix="nessa-sync-catalogue-") as directory:
    base = pathlib.Path(directory)
    for app in ("transcript", "tasks"):
        source = base / f"{app}-source.db"
        receiver = base / f"{app}-phone.db"
        assert call(app, source, receiver, "seed", 600)["seeded"] == 600
        first_page = call(app, source, receiver, "pass", 1)
        assert first_page["pages"] == 1 and first_page["completed"] == 0
        assert first_page["active_boundary"] == 600 and first_page["count"] == 40
        assert call(app, source, receiver, "show", "entry-0000")["known"]
        assert call(app, source, receiver, "upsert", "entry-0000", "changed-behind-cursor")["revision"] == 601
        assert call(app, source, receiver, "upsert", "entry-0600", "new-above-boundary")["revision"] == 602
        assert call(app, source, receiver, "delete", "entry-0599")["revision"] == 603
        completed = call(app, source, receiver, "pass")
        assert completed["completed"] == 600 and completed["active_boundary"] == 0
        assert completed["count"] == 600 and completed["pages"] == 14
        assert call(app, source, receiver, "show", "entry-0000")["payload"] != "changed-behind-cursor"
        assert call(app, source, receiver, "show", "entry-0599")["deleted"]
        assert not call(app, source, receiver, "show", "entry-0600")["known"]
        second = call(app, source, receiver, "pass")
        assert second["completed"] == 603 and second["count"] == 601
        assert second["resolve_reads"] == 2
        assert call(app, source, receiver, "show", "entry-0000")["payload"] == "changed-behind-cursor"
        assert call(app, source, receiver, "show", "entry-0600")["payload"] == "new-above-boundary"
        assert call(app, source, receiver, "show", "entry-0599")["deleted"]
        unavailable_source = base / f"{app}-source-offline.db"
        source.rename(unavailable_source)
        assert call(app, source, receiver, "show", "entry-0000")["payload"] == "changed-behind-cursor"
        unavailable_source.rename(source)
        unchanged = call(app, source, receiver, "pass")
        assert unchanged["manifest_reads"] == 0 and unchanged["payload_bytes"] == 0
        assert unchanged["completed"] == 603
        results.append({"app": app, "first_boundary": first_page["active_boundary"], "first_page_count": first_page["count"],
                        "pass_completed": completed["completed"], "second_completed": second["completed"],
                        "total_entries": second["count"], "changed_payload_reads": second["resolve_reads"],
                        "changed_payload_bytes": second["payload_bytes"], "manifest_bytes": second["manifest_bytes"],
                        "unchanged_payload_bytes": unchanged["payload_bytes"], "offline_cached_read": True})
print(json.dumps({"slice": 6, "apps": results}, separators=(",", ":")))
