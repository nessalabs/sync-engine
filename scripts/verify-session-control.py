#!/usr/bin/env python3
"""Run the bounded in-memory control lab; this is not a production session."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
result = subprocess.run(
    ["cargo", "run", "--locked", "--quiet", "--no-default-features", "--features", "transport",
     "--example", "session_control"], cwd=ROOT, check=True, text=True, capture_output=True,
)
report = json.loads(result.stdout)
assert report["control_frames"] == 4 and report["wire_bytes"] > 0, report
assert report["authorization_mode"] == "per_operation", report
assert report["grants_created"] == report["source_reads"] == report["commits"] == 0, report
assert result.stdout.count("\n") == 1, "stdout must contain only one JSON result"
print(json.dumps(report, separators=(",", ":")))
