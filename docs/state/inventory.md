---
id: "inventory"
title: "Find feature owners"
kind: "guide"
status: "implemented"
summary: "The atlas maps implemented core, optional adapters, examples and verification. Formal coverage is selected; a source link is not a proof."
parent: "sync"
sources: ["src/replication/mod.rs", "Cargo.toml", "README.md"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Find each feature owner

| Surface | Owner | User task / evidence |
| --- | --- | --- |
| IDs, exact scope, records, limits, request/page validation | replication/domain/records.rs | [Identity](identity.md), [bounds](bounds.md); domain/request/ID tests |
| Authorized finite record catch-up and atomic store ports | application/follower.rs | [Records](records.md); RecordCommit model; replication tests |
| Tail, older pages, overlap, generation and queue | history/domain.rs, application.rs | [History](history.md); HistoryFence; history_replication tests/lab5 |
| Catalogue manifest/value validation | catalogue/domain.rs | [Catalogue](catalogue.md); entry/request/plan tests |
| Pure parent/pass progress and reset planning | catalogue/domain/progress.rs | [Progress](progress.md); catalogue_progress tests |
| Authorized catalogue coordination and return correlation | catalogue/application.rs | CataloguePage and CommitReceipt; replication tests/labs6–7 |
| Artifact identity, manifest and availability | artifacts/mod.rs | [Artifacts](artifacts.md); artifact_contract example/tests |
| Chunk validation, staging/publication coordination | artifacts/transfer.rs | [Transfer](transfer.md); ArtifactPublish and artifact_transfer tests |
| Memory source/receivers | infrastructure/memory.rs | Default no-database lab1 and contract tests |
| SQLite identity/schema, source and receiver | infrastructure/sqlite/* | [Adapters](adapters.md); reopen/rollback/competing-handle tests/lab2 |
| Framed loopback clients and generic read server | infrastructure/loopback.rs, loopback/record_server.rs | [Network](network.md); record server and wire tests/labs3,7 |
| Host transcript/task projection and freshness | examples/local_apps.rs, history_lab.rs, catalogue_apps.rs | [Host](host.md); labs4–6 |
| Network artifact composition | examples/artifact_network.rs | artifact transfer lab |
| Crate features, docs and CI classification | Cargo.toml, .github/workflows/ci.yml, scripts/ci | [Verification](verification.md) |

Model names describe narrow safety slices, not full module verification. Six finite models do not implement the wider product contract. Source payload meaning, current authorization, scheduling, cancellation, production pairing/encryption and Nessa integration stay outside this crate's reference implementation guarantees.
