#!/usr/bin/env python3
"""Run the transport-only memory source across independent processes."""
import json
import socket
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
subprocess.run(["cargo", "build", "--locked", "--no-default-features", "--features", "transport", "--example", "generic_record_server"], cwd=ROOT, check=True)
binary = ROOT / "target" / "debug" / "examples" / "generic_record_server"
with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
server = subprocess.Popen([str(binary), "server", str(port)], cwd=ROOT)
try:
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.02)
    else:
        raise RuntimeError("server did not listen")
    result = subprocess.run([str(binary), "client", str(port)], cwd=ROOT, check=True, capture_output=True, text=True)
    observation = json.loads(result.stdout)
    assert observation == {"head": 1, "records": 1, "payload": "hello"}, observation
    print(json.dumps(observation, sort_keys=True))
finally:
    server.terminate()
    server.wait(timeout=5)
