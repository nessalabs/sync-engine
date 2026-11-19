#!/usr/bin/env python3
"""Run benign binding lifecycle checks through the landed control codec."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
result = subprocess.run(
    ["cargo", "run", "--locked", "--quiet", "--no-default-features", "--features", "transport",
     "--example", "session_lifecycle"], cwd=ROOT, check=True, text=True, capture_output=True,
)
report = json.loads(result.stdout)
assert report == {
    "accepted_bindings": 2,
    "reordered_replies": True,
    "invalidated_scope_refused": True,
    "cancelled_reply_refused": True,
    "closed_context_refused": True,
    "old_connection_refused": True,
}, report
assert result.stdout.count("\n") == 1
print(json.dumps(report, separators=(",", ":")))
