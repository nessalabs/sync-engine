# Durable catalogue publication (#47)

**Status: native SQLite catalogue ownership and snapshot admission. See the [current acceptance map](source-head-acceptance.md); #54 remains open.** Known optional core masks remain zero. No public encoder can publish
undefined feature bits. This supplies no session catalogue adapter, production
channel security or instantaneous receiver commit fencing.

`catalogue_source_meta` remains the sole source owner: immutable origin, catalogue
stream, incarnation and schema, committed revision, metadata version and both
requirement masks. Creation keys, latest values and retained deletions stay in the
existing entries table. Receiver identity and access epoch do not own requirements.
There is no metadata-only setter, second registry or in-place incarnation reset.

Catalogue-source file version1 migrates to version2 in one immediate transaction,
after exact identity and old-schema validation. Existing creation keys, revisions,
values and permanent deletion markers survive. Eight-byte big-endian mask BLOBs
retain all64 bits, with SQL type/length preflight before acquisition. A shared
stateless row decoder also serves the record source; their tables, application IDs,
transactions and migrations remain independent. Replica/artifact formats are not
upgraded. Partial/future schemas and malformed rows are refused, never defaulted
zero. Only recognized version1 files receive zero masks during migration.

`compare_upsert` compares fresh head/version/masks and exact immutable source
identity, validates current writer support, monotonicity and requested encoder
support, and commits current value, revision and requirements atomically. Creation
keys remain stable and deleted IDs cannot resurrect. Ordinary upsert and delete
recheck writer compatibility inside their write transaction. The equal-payload
shortcut occurs after compatibility/CAS and cannot publish metadata-only changes.
Legacy mutations retain their StoreError signature; compare-upsert retains typed
publication errors. On stale or uncertain results reload actual source state;
a discarded successful result is not permission to publish twice.

| Read or transition | Required coherence and outcome |
| --- | --- |
| Initial capability start | Fresh exact-scope metadata before store.begin or manifest; unsupported readers preserve empty/populated cache and progress. |
| Saved active pass resume | Recheck fresh metadata and head coverage before returning the same boundary/cursor/generation. |
| Manifest | Requirements and current descriptors share its short read snapshot; reject before descriptor acquisition. |
| Current-value resolution | Requirements, current descriptor and bounded payload share the resolution snapshot. The value can intentionally be newer than the captured boundary. |
| Reader requirements appear between resolutions | Refuse the next read with typed incompatibility. All staged page values remain unapplied; durable entries/cursor are unchanged after reopen. |
| Unknown writer-only requirements | Read-only admission and resolution continue. New/changed/equal upserts and deletions refuse under old writer support. |
| Newer compatible value or deletion | Preserve stable key and revision ordering; complete only the captured boundary, then explicitly recheck the fresh head. |
| Unknown version, malformed metadata, foreign identity | Typed compatibility/identity or storage refusal; no silent zero/fallback/reset. |

Catalogue bare head also checks reader compatibility, preventing legacy initial
starts from creating progress on an unsupported source. The explicit metadata port
still returns version/masks for typed native admission. A legacy active-pass start
retains its existing shortcut; manifest/resolution still enforce fresh compatibility.
Legacy wire routes have no capability metadata vocabulary: they fail closed through
the existing fatal status. This is not typed early incompatibility over that wire.
Native source errors preserve typed incompatibility through CatalogueError.

`apply_next_page` already stages resolved/unchanged entries and commits one validated
page. No new transaction/snapshot spans network waits, and no source transaction is
held until receiver apply. Current authority checks before source reads and before
apply remain separate. An already acquired compatible DTO may finish after a later
publication; requirements are format admission, not instantaneous authorization
revocation. Nonzero encoder rollout still needs per-response/client compatibility
and durable publication/apply ordering proof, including unchanged-only pages.

Old binaries are refused on their next open of version2. Already-open old writers
are not fenced by schema migration; hosts must stop/fence them before upgrade or
future nonzero use. Tests cover process restart, SQL rollback and lost successful
results, not all SQLite commit I/O errors, power loss, restore or cross-gateway races.

```sh
cargo test --locked --all-features --test catalogue_publication
python3 scripts/verify-catalogue-publication.py
python3 scripts/measure-catalogue-publication.py --repetitions 2 --native-repetitions 5
```

The native process lab has620 entries, an active40-entry cursor, writer-only reads,
and typed cold/resumed refusals with exact reopened entries/progress. The measurement
lab uses the existing plaintext development-loopback catalogue protocol over real
TCP. Its direct path includes a frame-counting hop; relay adds an opaque raw TCP
hop. It measures bootstrap, partial pass, process restart/resume, change/deletion
catch-up and explicit unchanged recheck, comparing every reopened row against source.
Actual framed RPC/byte counts split protocol overhead, manifest descriptors and
opaque payload. Native timings include process/open and seed620 or one update/delete
commit. Timings are noisy unshaped debug evidence, never CI thresholds or weak-link
claims. Matched baseline/current numbers and source fingerprints belong in the PR.

[Verification and matched measurement receipts](../verification/catalogue-publication.json)
retain exact source/harness fingerprints and regression evidence.

The later [explicit capability wire slice](catalogue-capability-wire.md) supplies
versioned development-route operations using the owned snapshot reads described
there. This document remains the native publication ownership milestone.
