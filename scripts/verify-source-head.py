#!/usr/bin/env python3
"""Verify initial native SourceHead admission; no capability-bearing transport claim."""
import json
import subprocess

for flags in [[], ["--features", "sqlite"]]:
    subprocess.run(["cargo", "test", "--locked", "--no-default-features", *flags, "--test", "source_head"], check=True, capture_output=True, text=True)
run = subprocess.run(["cargo", "run", "--locked", "--quiet", "--no-default-features", "--features", "sqlite", "--example", "source_head"], check=True, capture_output=True, text=True)
result = json.loads(run.stdout)
assert result["record_rows"] == result["catalogue_rows"] == 3
assert result["record_head_reads"] == 4 and result["record_page_reads"] == 1
assert result["catalogue_head_reads"] == 5 and result["catalogue_manifest_reads"] == result["catalogue_resolve_reads"] == 3
assert all(result[field] is True for field in ["unknown_reader_no_fetch_or_mutation", "unknown_writer_read_only_continues", "active_resume_rechecked", "reopened_cache_correct"])
assert result["transport_capabilities_implemented"] is False
assert run.stdout.count("\n") == 1
print(json.dumps(result, separators=(",", ":")))
