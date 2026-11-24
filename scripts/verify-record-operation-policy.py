#!/usr/bin/env python3
"""Run the serialized operation-policy regressions."""
import subprocess

result = subprocess.run(
    ["cargo", "test", "--locked", "--features", "transport,sqlite", "operation_policy"],
    text=True,
    capture_output=True,
    check=True,
)
expected = (
    "operation_policy_is_rechecked_for_each_response_fragment",
    "operation_policy_rejects_wrong_scope_and_unclassified_operations",
    "operation_policy_requires_bind_head_and_page_independently",
    "operation_policy_revocation_during_fragmented_page_never_applies",
)
for name in expected:
    if f"{name} ... ok" not in result.stdout:
        raise SystemExit(f"missing passing regression: {name}")
print("operation policy: four expected regressions passed")
