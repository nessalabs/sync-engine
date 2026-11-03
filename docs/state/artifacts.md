---
id: "artifacts"
title: "Check file availability"
kind: "guide"
status: "implemented"
summary: "An artifact manifest names a version and digest; local availability separately records whether verified bytes exist. Missing or unreachable does not mean deleted."
parent: "sync"
sources: ["src/replication/artifacts/mod.rs", "examples/artifact_contract.rs", "docs/design/artifact-contract.md"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Distinguish a file listing from usable bytes

A cached message can name an artifact whose bytes have never been downloaded. The manifest identifies exact scope, artifact ID, revision and either live length/SHA-256 content or a retained deletion. The UI must separately ask whether those exact bytes are locally verified. The contract distinguishes Deleted, CachedVerified, MetadataOnly, SourceUnavailable and HashMismatch; a cache-index byte-presence hint is not itself verified availability.

A source failure, missing answer or unavailable cache is not a deletion marker. Do not hide or erase a file merely because the device is disconnected. Conversely, old verified bytes are not current content after a new manifest, changed access epoch or retained deletion. Display the available/stale/unavailable state that the host can actually establish.

The contract is payload-independent: a digest proves byte equality under the algorithm, not permission to open the file or semantic safety of its content. Host code owns allowed paths, preview policy and trusted source identity. The reference cache is not a production Nessa file adapter or backup system.

Follow [transfer](transfer.md) for chunk staging and the final verification step. `artifact_contract` is a runnable contract example; `verify-artifact-transfer.py` exercises persistent reference storage and loopback transport.
