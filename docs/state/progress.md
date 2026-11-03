---
id: "progress"
title: "Read committed replies"
kind: "guide"
status: "implemented"
summary: "Catalogue pure helpers plan progress. Adapters own compare-and-swap
  and must return the exact confirmed replacement, not an unrelated later row."
parent: "sync"
sources:
  [
    "src/replication/catalogue/domain/progress.rs",
    "src/replication/catalogue/application.rs",
    "src/replication/infrastructure/sqlite/catalogue.rs"
  ]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
model: { "spec": "tla/CommitReceipt.tla", "config": "tla/CommitReceipt.cfg" }
modelCheck:
  sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
  checker: tlc-1.7.4
  date: 2026-11-03T09:39:34.032Z
  config: tla/CommitReceipt.cfg
  modelDigest: 189aab3f9a54df316b94495a06dce76757fe29537ce976de94a8ed7d5191e082
  states: 3
  held:
    - ExactReplacement
  broke: []
  unchecked: []
---
# Trust the exact committed replacement

Suppose caller A commits page one and another caller immediately commits page two. A's return value must describe A's confirmed page-one replacement. Returning a later row would incorrectly attribute another operation's work to A. Catalogue coordination compares the adapter response with its own pure planned replacement and refuses a mismatch.

`validate_catalogue_progress` checks parent/active scope, generation, completed revision and cursor relationships. Generation zero is only the empty inactive initial state. `catalogue_progress_after_begin`, `...after_page` and `...after_reset` calculate replacements without reading or writing a store. An adapter still needs coherent reads, transaction-time comparison, payload changes and commit evidence.

A new pass advances generation. A final page completes the boundary. Explicit reset keeps receiver/origin/stream fixed while changing compatible incarnation/schema/epoch; it clears completed/active progress, advances generation, removes old live values and retains permanent deletion markers. Generation exhaustion is a refusal, not wraparound.

The CommitReceipt model holds an intended replacement and allows the store to return that replacement or another writer's later value. An independent captured return must equal the intended one before accepted success. Its negative control removes correlation. This small model checks attribution only, not durable CAS, successful publication or the full reset algorithm.
