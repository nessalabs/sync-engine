---
id: "catalogue"
title: "Refresh a changing list"
kind: "statechart"
status: "implemented"
summary: "A catalogue scans current entries in stable creation order through a
  captured revision. Page data and continuation commit together."
parent: "sync"
sources:
  [
    "src/replication/catalogue/application.rs",
    "src/replication/catalogue/domain.rs",
    "src/replication/catalogue/domain/progress.rs",
    "src/replication/infrastructure/sqlite/catalogue.rs"
  ]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
model: { "spec": "tla/CataloguePage.tla", "config": "tla/CataloguePage.cfg" }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Retry",
        "section": "A failed page does not complete the list"
      }
  ]
modelCheck:
  sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
  checker: tlc-1.7.4
  date: 2026-11-03T09:39:27.179Z
  config: tla/CataloguePage.cfg
  modelDigest: fdbb0847e9c50988975a756e897df3d8e8e3cb8082ab2d2786201937cbb03cee
  states: 5
  held:
    - PayloadBeforeCursor
    - FinalPageOnly
  broke: []
  unchecked: []
---
# Refresh a changing conversation or task list

A catalogue stores each entry's current value, not every historical edit. Begin at completed revision 10 and capture boundary 20. Manifest pages traverse stable creation keys; an entry updated during the scan can resolve to a newer authorized value. The current pass still completes boundary 20, and a later pass discovers subsequent changes. This is not a frozen full-list snapshot.

```mermaid
stateDiagram-v2
  [*] --> Begin: Authorize and load saved progress
  Begin --> Idle: Head unchanged
  Begin --> Active: Start or resume persisted pass
  Active --> Resolve: Read bounded manifest and needed values
  Resolve --> Active: Atomically commit non-final page and cursor
  Resolve --> Complete: Atomically commit final page and boundary
  Resolve --> Retry: Payload failed before apply
  Retry --> Active: Use saved pass without skipped cursor
  Complete --> Begin: Check for newer revisions
```

## A failed page does not complete the list

Cached revisions at least as new as manifest entries avoid repeated payload reads. Needed entries resolve under current authorization; the source may return a newer value or retained deletion marker. If any value read or validation fails, no page plan is applied. Keep the saved pass and cached view; show stale/error rather than an empty list. An uncertain store commit instead requires reloading progress.

Only the final page advances completed revision to the captured boundary and clears active progress. Non-final pages retain the earlier completed revision and save a continuation. The store compares the current pass/generation inside the transaction. Retained deletion markers prevent old live values from returning; they are not equivalent to physically erasing all source history.

CataloguePage models two pages, fetched values, a saved cursor and final completion. Controls commit before payload resolution and mark the boundary complete on the first page. It omits creation-key ordering, concurrent source churn, authorization and content validation; those are source/test contracts, not proved by this model.
