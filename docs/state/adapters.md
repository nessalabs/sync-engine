---
id: "adapters"
title: "Choose storage"
kind: "guide"
status: "implemented"
summary: "Default memory adapters illustrate port contracts; optional SQLite adapters test transactional persistence. A type-correct adapter must still prove atomicity and failure semantics."
parent: "sync"
sources: ["src/replication/infrastructure/mod.rs", "src/replication/infrastructure/memory.rs", "src/replication/infrastructure/sqlite/connection.rs", "src/replication/infrastructure/sqlite/replica.rs", "src/replication/infrastructure/sqlite/catalogue.rs", "src/replication/infrastructure/sqlite/artifact.rs"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Choose reference storage

The default core needs no SQLite or socket dependency. Memory adapters support deterministic examples and contract tests. The `sqlite` feature adds source/receiver, history, catalogue and artifact storage. Keep separate source/cache files; do not point demo cleanup at user-selected data.

Record apply checks saved scope/position, immutable IDs and records/checkpoint under one SQLite transaction. History installation checks current generation, overlaps and deletion inside its transaction. Catalogue saves values and continuation together and returns its exact planned progress. Artifact staging saves bytes/offset and separately verifies complete publication. These are distinct owners, not one interchangeable generic checkpoint.

SQLite opening checks database ownership/schema and configures the connection; existing unrelated files are not disposable fixtures. Commit errors map to Uncertain where the adapter cannot establish the outcome. Reload from that same durable owner before deciding what to repeat. A public port contract is an obligation for a new adapter, not proof that every implementation satisfies it.

The local tests/labs exercise rollback, reopen, competing handles and controlled failure paths. They do not establish physical power-loss durability, backup consistency, production filesystem semantics or multi-machine distributed transactions. See the exact current execution scope in [verification](verification.md).
