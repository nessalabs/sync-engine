---
id: "network"
title: "Recover a connection"
kind: "guide"
status: "implemented"
summary: "The transport feature provides bounded development clients and a read-only loopback record server. Wake hints prompt checks; they do not carry authoritative progress."
parent: "sync"
sources: ["src/replication/infrastructure/loopback.rs", "src/replication/infrastructure/loopback/record_server.rs", "examples/generic_record_server.rs", "scripts/verify-record-server.py"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Recover across a connection loss

The host checks a source head on startup/reconnect and periodically according to its policy. A wake hint can accelerate that check. If the hint or connection is lost, the next explicit check starts from durable receiver progress. Do not advance progress from a notification, elapsed time or bytes sent.

The reference framed adapter enforces request/response bounds and correlates replies. `transport` alone supports a read-only generic `RecordSource` server and memory-backed separate-process lab; enabling `sqlite` adds SQLite-backed reference compositions. The server binds IPv4 loopback and uses bounded socket I/O timeouts. Example credentials are development configuration, not production device identity.

Catalogue network reads distinguish manifest/payload/protocol byte accounting. Unchanged cached entries can avoid payload bytes while still using wire metadata. Artifact transport requests exact current version/range with a 64 KiB chunk ceiling. Each path preserves typed scope/version/denial failures instead of treating network failure as empty data.

Host-owned scheduling must recover dropped hints, choose backoff and close handles. The crate does not implement pairing, an encrypted relay, remote command execution, automatic failover or a deployed service. Do not expose the development protocol as an authenticated production gateway based on these labs.
