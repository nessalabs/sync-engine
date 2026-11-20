# Durable record-source requirements (#47)

**Status: native SQLite record-source ownership foundation. See the [current acceptance map](source-head-acceptance.md); #54 remains open.** No optional core feature bit is assigned. Public source APIs cannot
publish nonzero requirements until an encoder implementing those bits exists.
Catalogue publication, production mixed-version upgrades and broader transport
coverage remain separate work.

## Owner and file version

The record source's existing `source_meta` row owns immutable origin, stream,
incarnation and schema, committed head, metadata version and both requirement
masks. Receiver identity and access epoch do not own publication requirements.
There is no independent registry, metadata-only setter or receiver-side grant.
Replacing an incarnation requires a separately created source and explicit host
reset/reconciliation; this adapter cannot replace an existing file's identity.

Record-source SQLite `user_version` is now 2. Only this application ID migrates;
replica, catalogue and artifact formats keep their existing versions. A populated
version1 record file is checked for exact immutable identity and expected metadata
columns, then upgraded under one immediate transaction. Existing records, immutable
ID deduplication and history floor survive. Requirements default to zero only as
part of that recognized old-format migration. Partial schemas, future SQLite file
versions, missing rows and malformed masks are refused, never interpreted as zero.
Unknown representable SourceHead metadata versions/masks remain visible to typed
compatibility admission. Masks are exactly eight-byte big-endian BLOBs, preserving
all64 bits; bounded SQL type/length preflight precedes BLOB acquisition.

Opening uses a short immediate schema transaction, as the previous source did.
No source read transaction is held during network writes. The file migration
rejects old binaries on their next open. It does **not** fence an already-open
old writer which never checks the new fields. A host must stop/fence old writers
before upgrading or enabling future format requirements. This is not an online
rolling-upgrade or cross-gateway fencing protocol.

## Atomic append, CAS and restart

`SqliteReferenceSource::compare_append(expected, id, payload, reader, writer)`
checks the exact source identity, then starts the existing immediate append
transaction. It reads current head/version/requirements, validates current writer
compatibility, compares expected head/version/masks, validates monotonicity and
requested encoder support, then commits new requirements, head and the immutable
record together. No new head or requirement can commit without its fact.
`CapabilitySupport` remains the domain owner for reader/writer admission; the
scope-independent checks grant no authority.

The existing `append` uses the same path and preserves current requirements.
`prune_through` also rechecks current writer compatibility inside its immediate
transaction. Open-time state never authorizes a later write. Legacy mutation
methods preserve their existing `StoreError` signature and use `Failed` for
unsupported writer requirements; compare-and-append preserves typed incompatibility.

| Event | Result / durable effect |
| --- | --- |
| Fresh supported expected publication | Commit one fact, new head and requirements atomically. |
| Competing handle changes head/version/masks | `Stale`; no write; reload committed metadata. |
| Another receiver or access epoch, same source identity | Same publication owner; no bit-clearing or incarnation privilege. |
| Different origin/stream/incarnation/schema | Identity refusal; existing source is unchanged. |
| Non-monotonic masks | Transition refusal, when the current writer can interpret the source. |
| Unsupported current or requested reader/writer requirements | Typed incompatibility for compare-and-append; no dedup success or mutation. Current known masks0 therefore reject every nonzero requested mask. |
| Same immutable ID and bytes, fresh expected publication, unchanged masks | Return original position; no metadata-only update. |
| Stale retry after a lost result | `Stale`; reload actual publication, then resolve the immutable record ID. |
| SQL failure before/after metadata or record writes | Transaction rollback preserves head/requirements/facts. |
| Commit error | `Store(Uncertain)`; reload before deciding whether a retry is needed. |

The restart lab covers a discarded successful reply and reloaded committed state.
It does not inject every SQLite commit I/O error, power loss, filesystem failure,
backup restore or cross-gateway race. Durability remains SQLite's configured local
WAL/FULL contract, with process-restart evidence rather than a power-loss claim.

## Reads and compatibility

`source_head` reads head/version/masks together from one committed row. Bare `head`
remains passive binding metadata; it cannot establish compatibility. Every page,
recent tail and older page checks reader requirements in the same transaction as
its bounded row preflight and record acquisition. Unknown reader requirements or
metadata version stop before payload access. Unknown writer-only requirements
remain readable. Legacy wire routes without typed capability metadata refuse
payload through their existing fatal status instead of silently serving zero bits;
no new legacy wire status is assigned. Native history exposes typed incompatibility.

Native records are immutable and the current encoder supports only zero optional
features. A page under a previously observed view rechecks requirements; a newly
unsupported publication refuses it rather than emitting a DTO under stale metadata.
A future supported encoding change still needs proof that each read view's metadata
and every returned record are coherent. The new owner is not a cache invalidation,
authorization revocation or cancel-versus-commit mechanism. Current per-operation
authority and the deliberate fresh post-pass head recheck remain required.

Existing catalogue sources keep their separate zero-format metadata and stores;
this first durable owner does not pretend to upgrade catalogue current-value
resolution. Memory sources also keep immutable zero-format requirements.

## Evidence

```sh
cargo test --locked --no-default-features --features sqlite --test source_publication
python3 scripts/verify-source-publication.py
python3 scripts/verify-session-e2e.py --repetitions 2 --modes session-caps \
  --output record-session.json --markdown record-session.md
```

The native lab runs separate producer/reader/inspection processes, actual source
and replica files, supported catch-up, writer-only reads and cold/populated cache
reader refusals after reopen. Future nonzero rows are explicitly owned
forward-compatibility fixtures; production APIs never assign those undefined bits.
Tests additionally cover migration rollback, malformed/future schemas, full-width
masks, stale already-open new-code writer handles, all existing native payload
routes, competing writers and transactional fault boundaries.

Matched before/after session measurements use the unchanged seven-record/4,126-byte
workload over direct TCP and an opaque relay. Wire format/exchange counts do not
change; timings include source metadata reads and durable receiver apply, but exclude
producer append latency. No append-throughput or weak-link latency gain is claimed.
The PR includes actual repeated phase/short-workload numbers, conditions and limits.

[Measured transport/native publication and regression receipts](../verification/source-publication.json)
retain the exact before/after source fingerprints and verification commands.
