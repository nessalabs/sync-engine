#!/usr/bin/env python3
"""Verify the pinned-TLS advisory Follow fixture and report its wire counts."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

unit_command = ["cargo", "test", "--locked", "--features", "sqlite,tls", "--lib", "secure_follow::tests"]
unit_result = subprocess.run(unit_command, capture_output=True, text=True, check=False)
if unit_result.returncode:
    sys.stderr.write(unit_result.stdout + unit_result.stderr)
    raise SystemExit(unit_result.returncode)
command = ["cargo", "test", "--locked", "--features", "sqlite,tls", "--test", "session_tls", "secure_follow_", "--", "--nocapture"]
result = subprocess.run(command, capture_output=True, text=True, check=False)
if result.returncode:
    sys.stderr.write(result.stdout + result.stderr)
    raise SystemExit(result.returncode)
pattern = r"secure-follow relay=(true|false) fetch=\[(.*?)\] hint_setup_and_one_hint=\[(.*?)\]"
rows = re.findall(pattern, result.stdout + result.stderr)
if len(rows) != 2 or {row[0] for row in rows} != {"true", "false"}:
    raise SystemExit("missing direct or relay measurement")
reports = []
for relay, fetch, hints in rows:
    parse = lambda text: [tuple(map(int, m)) for m in re.findall(r"\((\d+), (\d+), (\d+)\)", text)]
    f, h = parse(fetch), parse(hints)
    if len(f) != 3 or len(h) != 2 or len(set(f)) != 1 or len(set(h)) != 1 or f[0][2] != 3 or h[0][2] != 0:
        raise SystemExit("matched fixture counters differ")
    reports.append({"path": "relay" if relay == "true" else "direct", "record_fetch": f, "hint_connection": h})
files = ["Cargo.lock", "scripts/verify-secure-follow.py", "tests/session_tls.rs", "src/replication/application/follower.rs", "src/replication/infrastructure/sqlite/source.rs", "src/replication/infrastructure/sqlite/replica.rs", "src/replication/infrastructure/session/records.rs", "src/replication/infrastructure/session/records/secure_follow.rs", "src/replication/infrastructure/session/records/follow.rs", "src/replication/infrastructure/session/channel/tls.rs"]
fingerprints = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in files}
print(json.dumps({"scope": "one pinned-TLS hint-only session plus separate TLS record session; not #54 completion", "command": command, "unit_command": unit_command, "source_sha256": fingerprints, "cases": reports}, sort_keys=True))
