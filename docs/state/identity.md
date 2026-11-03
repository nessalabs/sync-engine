---
id: "identity"
title: "Check scope and access"
kind: "guide"
status: "implemented"
summary: "Scope includes receiver, origin, stream, incarnation, schema and access epoch. Equal positions under different scopes are not interchangeable."
parent: "sync"
sources: ["src/replication/domain/records.rs", "src/replication/application/follower.rs", "src/replication/catalogue/application.rs", "src/replication/history/application.rs", "src/replication/artifacts/transfer.rs"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Keep identity and authority exact

A phone's position 90 for transcript A is not another phone's progress, nor a replacement incarnation's position 90. `Scope` contains receiver, origin, stream, incarnation, schema and access epoch. Reusing an immutable record ID with different content is a conflict, not an update.

`ScopeAuthorizer` returns Allowed with the exact current scope, Denied, or Unverifiable. A different allowed scope is refused. Unknown policy preserves uncertainty: cached data is not automatically deleted by the follower, but no new source read is admitted. The host decides cache invalidation and user-visible access.

For record begin, authorization precedes even loading saved progress, avoiding disclosure through a mismatch error. Catalogue coordination authorizes manifest and needed value reads and checks again before apply. History fetch checks policy before reading; commit independently checks generation/watermark/fences. Artifact sources must authorize the exact manifest/chunk identity. These are separate calls; none proves revocation is globally instantaneous.

On scope mismatch or a source head behind the saved checkpoint, stop normal catch-up. Inspect the source identity and use only the feature's explicit compatible reset path. Never silently set a checkpoint to zero under a new identity. The generic core has no pairing service, authenticated device directory or production credential issuer.
