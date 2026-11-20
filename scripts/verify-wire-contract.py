#!/usr/bin/env python3
"""Verify the dependency-free owner export is one clean machine-readable result."""
import json
import subprocess

result = subprocess.run(
    ["cargo", "run", "--locked", "--quiet", "--no-default-features", "--example", "wire_contract"],
    check=True, capture_output=True, text=True,
)
contract = json.loads(result.stdout)
assert set(contract) == {"record_max_payload_bytes", "id_max_utf8_bytes", "catalogue_max_entries", "catalogue_max_payload_bytes", "source_head_version", "known_reader_capabilities", "known_writer_capabilities"}, contract
assert all(type(value) is int and value >= 0 for value in contract.values()), contract
assert all(contract[name] > 0 for name in ["record_max_payload_bytes", "id_max_utf8_bytes", "catalogue_max_entries", "catalogue_max_payload_bytes"])
assert contract["source_head_version"] == 1
assert contract["known_reader_capabilities"] == contract["known_writer_capabilities"] == 0
assert result.stdout.count("\n") == 1, "stdout must contain only one JSON result"
print(json.dumps({"wire_contract": contract, "clean_stdout": True}, separators=(",", ":")))
