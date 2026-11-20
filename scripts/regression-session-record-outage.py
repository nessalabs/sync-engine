#!/usr/bin/env python3
"""Force the seed worker to finish before the parent resumes from Popen."""
from pathlib import Path
import selectors
import subprocess
import tempfile
import traceback
from unittest.mock import patch


FIXTURE = Path(__file__).with_name("verify-session-record-outage.py").resolve()


def exercise(source, destination):
    namespace = {"__file__": str(FIXTURE), "__name__": "outage_ordering"}
    exec(compile(source, str(FIXTURE), "exec"), namespace)
    launch = subprocess.Popen

    def launch_after_seed(*args, **kwargs):
        child = launch(*args, **kwargs)
        if args[0][1] == "outage":
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(child.stdout, selectors.EVENT_READ)
                    if not selector.select(30):
                        raise RuntimeError("real outage seed did not become readable")
                # Do not consume the receipt: case() owns the child stream.
            except BaseException:
                child.kill()
                child.communicate(timeout=5)
                raise
        return child

    with patch.object(subprocess, "Popen", launch_after_seed):
        return namespace["case"](destination, "session-io", "direct", 0, 0, 3.0)


def main():
    source = FIXTURE.read_text()
    baseline = "        previous = shaper.snapshot()\n"
    spawn = next(line for line in source.splitlines(keepends=True) if "child = subprocess.Popen(" in line)
    assert source.count(baseline + spawn) == 1
    reverted = source.replace(baseline + spawn, spawn + baseline)
    with tempfile.TemporaryDirectory(prefix="record-outage-ordering-") as tmp:
        root = Path(tmp)
        try:
            exercise(reverted, root / "reverted")
        except AssertionError as error:
            frame, _ = list(traceback.walk_tb(error.__traceback__))[-1]
            assert frame.f_code.co_name == "case" and len(frame.f_locals["rows"]) == 4
            seed = frame.f_locals["rows"][0]
            assert seed["phase"] == "seed" and seed["raw_route_delta"]["connections_to_upstream"] == 0
        else:
            raise AssertionError("old seed snapshot ordering passed under a forced early seed")
        fixed = exercise(source, root / "fixed")
        assert fixed["phases"][0]["raw_route_delta"]["connections_to_upstream"] == 1
    print("Early real seed: reverted order counted 0 connections and failed; fixed order counted 1 and passed.")


if __name__ == "__main__":
    main()
