#!/usr/bin/env python3
"""Verify the dependency-free owner export is one clean machine-readable result."""
import json
import subprocess

result = subprocess.run(
    ["cargo", "run", "--locked", "--quiet", "--no-default-features", "--example", "wire_contract"],
    check=True, capture_output=True, text=True,
)
contract = json.loads(result.stdout)
assert set(contract) == {"id_max_utf8_bytes", "catalogue_max_entries", "catalogue_max_payload_bytes"}, contract
assert all(type(value) is int and value > 0 for value in contract.values()), contract
assert result.stdout.count("\n") == 1, "stdout must contain only one JSON result"
print(json.dumps({"wire_contract": contract, "clean_stdout": True}, separators=(",", ":")))
