# Native atomic catalogue collection (#49)

`SqliteCatalogueSource::compare_collect(expected_v2, requested)` is an explicit
source-owner mutation, alongside existing source publication. It is not a receiver
access grant or a retention-pin policy. The native source has one durable truth:
its singleton metadata and permanent per-ID evidence. Collection was introduced
with source v4 and receiver v3; source v7 includes physical proof compaction.

Physical movement of hidden rows into exact proof is described in [physical compaction](catalogue-physical-compaction.md); the retention and admission rules below remain in force.

The source validates its four immutable identity components, current writer
support and the complete expected v2 publication, including horizon, before any
effect or equal-horizon no-op. The single Immediate writer transaction requires
`previous <= requested <= head`, updates the horizon and hides exactly visible
retired markers with `revision < requested`. Equality stays served. It never
changes head, requirement masks, live values, creation/deletion revisions or
permanent IDs. A receipt counts newly hidden markers only and is returned only
after commit. An equal-horizon retry after reloading reports zero new markers.
A stale CAS requires reload; commit uncertainty requires explicit inspection,
never automatic replay. Tests include a deferred constraint failure at COMMIT,
statement rollback, competing handles and visibility before/after caller commit.

## Composition and authority

The internal transaction seam is
`collect_in_transaction(&Transaction, &CatalogueHorizonHead, u64)` returning
`Result<CatalogueCollection, CataloguePublicationError>`. Its caller must already
own the validated source identity and an Immediate source transaction; the helper
does not commit. Future catalogue retention-pin validation and horizon selection
must run in that same transaction before invoking the helper. Reading eligibility
outside it or adding another source mutation owner is insufficient. The current
public method deliberately applies explicit host-selected policy and does not
claim to enforce #48 pins.

Current authorized v2 incremental reads compare completed C, not active cursor/H,
against the floor, with C equal to it accepted. Persisted exact-scope observations
reject delayed lower metadata and fence staged commits. Full reset uses the same
incarnation, generation-bound candidate journal, retains permanent fences and
prior identity/revision floors, and obtains current owned deletion/value proofs.
Missing markers, ordinary absence and transient failures do not classify a
candidate. The fresh Finish recheck remains mandatory; a newer floor can expire
an already published completion and require a new explicit authorized generation.
An unobserved concurrent source change cannot instantly revoke an already acquired
snapshot; the protocol fences against validated observations at durable commit.

Current legacy native and wire routes refuse positive activation before data,
with no fallback. The separate [explicit v2 development wire](catalogue-horizon-wire.md) now
qualifies those native ports; legacy routes retain their fail-closed behavior. Old
already-running binaries require host upgrade fencing before activation; file
version checks cannot stop an old open writer. Receiver authority remains checked
per operation; possession of source metadata implies neither access nor progress.

## Qualification and limits

Run `cargo test --locked --all-features --test catalogue_collection`, the ordinary
repository gates, `python3 scripts/verify-catalogue-collection.py` and
`python3 scripts/verify-catalogue-collection-model.py --jar PATH_TO_PINNED_JAR`.
The native process lab uses actual API activation: 620 prior live IDs,160 deletes,
head780,159 markers hidden and one equal marker retained. Every reset step reopens
the cache; exact current rows,160 permanent receiver fences and C780 must match.
Zero-floor v1/v2 and positive legacy refusal are separate profiles. Port-call and
logical descriptor-byte counts are not network exchanges or disk fsync counts.
Allocated storage and permanent identity rows are reported without a file-size or
bounded-total-storage saving claim. The existing zero-floor direct/opaque-relay
lab supplies separate unchanged-wire evidence.

The finite two-ID collection model covers atomic selection/publication, captured
CAS offers and a concurrent newer publication, delayed replies, completed-vs-target
admission, two reset generations, crash/drop and transient classification
failure, and current authority sampled at operation admission. Semantic negative
controls must fail their intended invariants. This is bounded safety evidence,
not a liveness, SQL power-loss, pin-policy, payload-codec or instantaneous-revocation
proof. The earlier reset model remains responsible for its broader epoch/cancel
states. See [the measurements](catalogue-collection-measurement.md) and committed receipt
for exact bounds and results.

Issue #49 remains open for its final acceptance audit. Explicit development-wire
coverage is qualified separately; #48 pin policy and broader #54 production secure
catalogue transport remain separate.
