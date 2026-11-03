---
id: "recovery"
title: "Recover safely"
kind: "guide"
status: "implemented"
summary: "Unknown writes, stale plans, incompatible scopes, deletion and source unavailability need different recovery actions. None is a generic empty-success result."
parent: "sync"
sources: ["src/replication/application/follower.rs", "src/replication/history/application.rs", "src/replication/catalogue/application.rs", "src/replication/artifacts/transfer.rs"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Choose a safe recovery action

A request can fail after a store commit. Preserve its scope and read the owning store before deciding what to repeat. Do not replace all errors with a zero checkpoint or empty view.

| Outcome | What it means | Next action |
| --- | --- | --- |
| Record StoreError::Uncertain | Commit may exist while in-memory Pass did not advance | Discard Pass; begin_pass reloads durable checkpoint |
| Catalogue/transfer Uncertain | Saved cursor/offset may already advance | Reload exact progress; resume that saved state |
| Stale | Another generation/writer superseded the plan | Reload; do not force the old expected value |
| ScopeMismatch / ResetRequired / source behind checkpoint | Saved and requested identities/progress are incompatible | Inspect source, use feature's explicit compatible reset; never silently rewind |
| Denied / Unverifiable | No current authority for delivery | Stop new reads; host handles policy/cache visibility |
| History Fenced / artifact deletion | Retained deletion prevents old content installation | Keep deletion visible; do not retry stale bytes as a new identity |
| HashMismatch | Complete artifact failed digest/length verification | Reload reset staging and fetch trusted current content again |
| Source unavailable / failed payload | No successful empty/current result | Keep verified cached state with stale/error; host chooses retry |

Reset is feature-specific. Catalogue reset changes compatible scope within the same receiver/origin/stream and retains deletion markers. Tail refresh uses generation/watermark checks, not a general cross-scope reset button. Do not invent an automatic reset procedure for unsupported identity changes.

The RecordCommit, HistoryFence and ArtifactPublish pages identify the selected model boundaries. Recovery of actual storage/network failures relies on the implementation tests and correct host composition, not only finite model passes.
