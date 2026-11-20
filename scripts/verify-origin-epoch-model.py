#!/usr/bin/env python3
"""Qualify the finite native receiver epoch model using pinned TLC; no downloads."""

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODEL = ROOT / "docs/state/tla/OriginEpoch.tla"
CONFIG = MODEL.with_suffix(".cfg")
PIN = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

CASES = (
    ("safe", None, None, False),
    ("stale-epoch", "UnsafeEpoch", "NoStaleEpoch", False),
    ("unstamped-result", "UnsafeStamp", "StampedApply", False),
    ("wrong-grant", "UnsafeScope", "ExactGrant", False),
    ("reopen-loses-floor", "UnsafeReopen", "DurableOriginFloor", False),
    ("split-commit", "UnsafeSplitCommit", "AtomicCheckpoint", False),
    ("authorized-apply-after-reopen", None, "NoApplyAfterReopen", True),
    ("authorized-higher-epoch-catchup", None, "NoCatchupAtHigherEpoch", True),
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    jar = args.jar.resolve()
    if sha256(jar) != PIN:
        parser.error("expected the official TLA+ v1.7.4 release JAR")

    results = []
    with tempfile.TemporaryDirectory(prefix="origin-epoch-model-") as folder:
        for name, mutation, invariant, witness in CASES:
            root = Path(folder) / name
            root.mkdir()
            spec = root / MODEL.name
            spec.write_text(MODEL.read_text(encoding="utf-8"), encoding="utf-8")
            config = CONFIG.read_text(encoding="utf-8")
            if mutation:
                config = config.replace(f"{mutation} = FALSE", f"{mutation} = TRUE")
            if witness:
                config += f"INVARIANT {invariant}\n"
            cfg = root / CONFIG.name
            cfg.write_text(config, encoding="utf-8")
            result = subprocess.run(
                ["java", "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
                 "-workers", "1", "-deadlock", "-config", str(cfg),
                 "-metadir", str(root / "states"), str(spec)],
                cwd=root, capture_output=True, text=True, timeout=60, check=False,
            )
            output = result.stdout + result.stderr
            expected = (
                result.returncode != 0 and f"Invariant {invariant} is violated" in output
                if invariant else result.returncode == 0 and "No error has been found" in output
            )
            counts = re.search(r"([\d,]+) states generated, ([\d,]+) distinct states found", output)
            if not expected or not counts:
                raise RuntimeError(f"{name}: TLC returned {result.returncode}\n{output}")
            results.append({
                "case": name,
                "generated_states": int(counts[1].replace(",", "")),
                "distinct_states": int(counts[2].replace(",", "")),
                "expected_violation": invariant,
                "reachability_witness": witness,
            })
    summary = {
        "checker": "TLC 2.19, one worker, TLA+ release v1.7.4",
        "jar_sha256": PIN,
        "model_sha256": sha256(MODEL),
        "config_sha256": sha256(CONFIG),
        "results": results,
    }
    receipt = json.dumps(summary, indent=2) + "\n"
    if args.output:
        args.output.write_text(receipt, encoding="utf-8")
    print(receipt, end="")


if __name__ == "__main__":
    main()
