#!/usr/bin/env python3
"""Verify the current-policy prerequisite through existing in-memory core ports."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
result = subprocess.run(
    ["cargo", "run", "--locked", "--quiet", "--no-default-features", "--features", "transport",
     "--example", "session_authorization"], cwd=ROOT, check=True, text=True, capture_output=True,
)
report = json.loads(result.stdout)
assert report == {
    "policy_calls": 3,
    "progress_loads": 1,
    "head_reads": 1,
    "page_reads": 0,
    "apply_calls": 0,
    "closed_owner_refused": True,
}, report
assert result.stdout.count("\n") == 1
print(json.dumps(report, separators=(",", ":")))
