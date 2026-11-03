---
id: "history"
title: "Read recent messages"
kind: "statechart"
status: "implemented"
summary: "Recent-tail installation tracks live head separately from older
  coverage. Delayed pages must still match the saved generation and deletion
  fence."
parent: "sync"
sources:
  [
    "src/replication/history/application.rs",
    "src/replication/history/domain.rs",
    "src/replication/infrastructure/sqlite/replica.rs"
  ]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
model: { "spec": "tla/HistoryFence.tla", "config": "tla/HistoryFence.cfg" }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Deleted",
        "section": "Delayed content cannot undo deletion"
      }
  ]
modelCheck:
  sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
  checker: tlc-1.7.4
  date: 2026-11-03T09:39:30.576Z
  config: tla/HistoryFence.cfg
  modelDigest: 67b3694ea93d50d6a89c0ae087e2471ed93d15d917d674d39e697ae382f97f1d
  states: 8
  held:
    - CurrentGeneration
    - NoDeletedInstall
  broke: []
  unchecked: []
---
# Open recent messages and load older ones

For a 100-message transcript, a tail can install messages 91–100 with live head 100 and lower bound 91. Loading 81–90 extends older coverage to 81 without rewinding live head. A live append can advance the head while older browsing continues.

```mermaid
stateDiagram-v2
  [*] --> Unloaded
  Unloaded --> Loading: Fetch bounded tail
  Loading --> Partial: Install verified suffix
  Loading --> Empty: Successful zero-head snapshot
  Partial --> Partial: Extend contiguous older prefix
  Partial --> Complete: Coverage reaches position one
  Partial --> Deleted: Commit permanent local deletion fence
  note right of Deleted
    A deletion fence may be committed from any state.
    Only selected transitions are shown.
  end note
  Loading --> Failed: Preserve existing verified cache
  Failed --> Loading: Host retries deliberately
```

## Delayed content cannot undo deletion

Fetching and installation are separate. The host can discard a delayed plan. SQLite tail installation rejects a saved deletion, mismatching scope, non-new generation or a watermark behind saved live head. Older installation requires the current generation, contiguous coverage and identical overlapping IDs/bytes. A permanent local deletion fence removes records and rejects later installations for that retained target. It is not a remote delete command.

`HistoryReadState` distinguishes Unloaded, Loading with optional cache, Partial, CompleteEmpty, Complete, Failed, Stale and Deleted. Source failure is never proof of an empty transcript. If the source pruned older records, expose reset-required/partial status; do not claim coverage down to one.

`HydrationQueue` combines multiple views by choosing the minimum uncovered requested position. Requests still occupy bounded waiter slots including duplicates; it is not an unbounded deduplicating subscription service. Resolve waiters only against saved lower coverage. The host can schedule urgent head work on another source handle between bounded older pages.

HistoryFence explores one fetched older plan, a generation change and a deletion before installation. Independent accepted-generation/deletion facts expose stale or resurrecting installs. It omits tail ordering, overlap bytes and gap validation; Rust tests cover those separately.
