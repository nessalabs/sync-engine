---
id: "bounds"
title: "Bound reads and memory"
kind: "guide"
status: "implemented"
summary: "Validation separates generic decoded-data limits from adapter wire and
  storage limits. Hosts should derive public constants from the pinned crate
  rather than duplicate them."
parent: "sync"
sources:
  [
    "src/replication/domain/records.rs",
    "src/replication/catalogue/domain.rs",
    "src/replication/history/application.rs",
    "src/replication/artifacts/transfer.rs",
    "examples/wire_contract.rs"
  ]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
model: { "spec": "tla/RequestEcho.tla", "config": "tla/RequestEcho.cfg" }
modelCheck:
  sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
  checker: tlc-1.7.4
  date: 2026-11-03T09:39:23.766Z
  config: tla/RequestEcho.cfg
  modelDigest: 4201d00f853a96cf5b4f3c707069af0cccb08750be3aef0d95cc30584723a3ae
  states: 5
  held:
    - ExactScope
    - ExactOffset
  broke: []
  unchecked: []
---
# Keep reads and allocations bounded

`Limits` requires positive record count, total decoded payload and per-record ceilings. `validate_page_request` checks generic range and requested budgets before source I/O; it does not check current authority, the physical head, saved checkpoint or response bytes. `validate_page` adds those response/saved-position relationships that belong to pure record validation. Adapters still enforce bounds before allocation.

Opaque `Id` validation uses the canonical `MAX_ID_BYTES` UTF-8 byte ceiling. Catalogue has owner constants `MAX_CATALOGUE_ENTRIES` and `MAX_CATALOGUE_PAYLOAD_BYTES`; artifact chunks cap at 64 KiB. The history queue caps configured waiters at 256. Limits are per operation; they are not a proof of whole-host memory or concurrency bounds.

Generate host schema values from the pinned version:

```sh
cargo run --locked --quiet --no-default-features --example wire_contract
python3 scripts/verify-wire-contract.py
```

The export names ID UTF-8 bytes and catalogue entry/decoded payload bounds. It does not invent a universal record page policy. A host can impose smaller wire/physical limits. SQLite encodings and borrowed-ID preflight have their own representation rules documented in the existing walkthrough and tested by boundary cases.

No model here proves allocation complexity, denial-of-service resistance, arbitrary numeric scale or performance. Existing validation and wire tests provide executable evidence for their named limits.

RequestEcho isolates a response with two possible scope and offset identities. Independent accepted fields must match the requested identity. Controls remove each equality separately. This is a deliberately small correlation check; it excludes other request fields, numeric budgets, parsing and actual allocation.
