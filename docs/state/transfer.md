---
id: "transfer"
title: "Verify downloaded files"
kind: "statechart"
status: "implemented"
summary: "A full staged offset is not verified availability. Publication
  rechecks the source manifest and verifies complete staged bytes under the
  current local manifest."
parent: "sync"
sources:
  [
    "src/replication/artifacts/transfer.rs",
    "src/replication/infrastructure/sqlite/artifact.rs"
  ]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
model: { "spec": "tla/ArtifactPublish.tla", "config": "tla/ArtifactPublish.cfg" }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Full",
        "section": "Full length is not verified content"
      },
    {
        "diagram": 0,
        "node": "Unknown",
        "section": "Full length is not verified content"
      }
  ]
modelCheck:
  sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
  checker: tlc-1.7.4
  date: 2026-11-03T09:39:38.858Z
  config: tla/ArtifactPublish.cfg
  modelDigest: 0afaac78c0bbd2c033cf35717df9416721f93e107cb081e03a2e7fc78a7eedae
  states: 14
  held:
    - CompleteBeforeVisible
    - HashBeforeVisible
    - ExactManifest
  broke: []
  unchecked: []
---
# Download, verify and publish bytes

A 150 KiB file takes bounded chunks of at most 64 KiB. Each append saves exact bytes and the next offset together. After the final chunk, the offset equals length but the cache is still staging. Call `publish_if_current` to recheck the authorized manifest and ask the store to verify/publish.

```mermaid
stateDiagram-v2
  [*] --> Staging: Accept exact live manifest
  Staging --> Staging: Commit one exact contiguous chunk
  Staging --> Full: Saved offset equals length
  Full --> Verified: Current manifest and full SHA-256 match
  Full --> Reset: Hash mismatch discards suspect staging
  Full --> Replaced: Source reports newer version or deletion
  Full --> Unknown: Commit response uncertain
  Unknown --> Reload: Read actual durable progress
  Reload --> Staging: More bytes needed
  Reload --> Full: Bytes complete publication unconfirmed
  Reload --> Verified: Verification already committed
  Reload --> Replaced: Current manifest changed
```

## Full length is not verified content

`transfer_one` returns after at most one chunk, leaving scheduling to the host. Exact request echo and length are validated. Repeating identical committed chunk bytes is idempotent; conflicting bytes or an offset race are refused. On `Uncertain`, reload progress instead of assuming the write failed.

`publish_if_current` queries the source again. A changed version or deletion is installed before returning VersionChanged/Deleted. Failure to obtain that current source answer cannot publish old staging. The store independently checks exact saved manifest, revocation/fence and complete contiguous length/hash in its transaction. HashMismatch clears suspect chunks and resets offset; reread before restarting.

Source recheck and local commit are separate operations. A remote update immediately afterward is possible; this is a checked version at a point in time, not a global lock preventing future changes. Host refresh and access policy remain necessary.

ArtifactPublish explores two chunks, independent hash validity and a changing local manifest. Publication records offset/hash/version evidence. Controls publish early, ignore hash or accept the wrong current version. The model does not compute SHA-256, simulate SQLite, prove remote authority freshness or cover arbitrary files.
