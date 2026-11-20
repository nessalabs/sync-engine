# Catalogue source retirement prerequisite (#49)

The source keeps one row per entry ID in its existing immutable catalogue
identity (origin, stream, incarnation, schema). Live rows carry current payload;
retired rows retain original creation revision, final deletion revision and empty
payload permanently. Upsert, compare-upsert and repeated delete continue to check
that same row and refuse resurrection. No second identity registry is introduced.

`manifest_visible` owns serving membership, independently of identity. The partial
`catalogue_source_page` index includes exactly rows where membership is 1. A live
row must remain visible. A retired row can be represented as visible or invisible;
identity, revisions and deletion meaning do not depend on that bit. Resolution
still looks up the permanent row by ID, so a previously selected manifest can
resolve a newer deletion even if future collection removes index membership.

This prerequisite exposes no operation to clear membership. Every migrated row,
new live row and deletion made through public APIs stays visible. SQL fixtures
exercise a future invisible retired row only to verify representation and fences;
editing membership directly is not a supported collection procedure. Authorization,
owner-before-deletion privacy, finite pass boundaries, payload/cursor commits and
mandatory final head rechecks are unchanged.

## Schema decision

One native source-file change, v1/v2 to v3, adds a default-visible membership column
and replaces the existing index inside the identity-validated immediate migration
transaction. Values, creation/deletion revisions, head and capability masks are
unchanged. A failed migration rolls back the column, index and version together;
concurrent openers serialize. Reopen validates the expected column layout rather
than silently repairing a partially labelled format.

The local version bump is required to describe a new durable invariant and index;
leaving the file labelled v2 would let an old opener accept a format it does not
understand. This is one migration, not separate bumps for retirement and its
index. SourceHead/wire versions, record source and receiver schemas do not change.
Future horizon metadata may require its own justified format change; this slice
preallocates no speculative horizon fields or optional feature bit. Hosts must
stop old binaries before upgrading: a file version is not a fence against a
previously opened writer or reader.

A separate retired-ID table plus marker table would add identity duplication or
cross-table ownership checks and merged creation-key paging. Keeping the existing
row and a partial serving index preserves the single owner with fewer transitions.
It retains permanent per-ID storage: neither bounded total identity storage nor
marker storage savings are claimed while all markers are still served.

## Verification and remaining work

`cargo test --locked --all-features --test catalogue_publication` covers populated
v1/v2 migration, rollback and reopen, competing openers, unchanged served markers,
index selection, membership constraints and permanent retired identity/latest
resolution. Existing catalogue labs retain authorization/refusal, current values,
restart, finite captured targets, receiver fences and atomic cursor behavior.

`python3 scripts/measure-catalogue-retirement.py` measures real open/migration and
reopen on owned 620-ID files (160 retired), preserving every row. It reports
allocated pages and optional dbstat serving-index footprint; it never removes a
marker. It separately creates 620 IDs and retires 160 through public operations
to measure freshly encoded rows rather than relying on lazy ALTER defaults.
That separate publication timer includes 161 processes and their commits. The timer includes process start, migration/open and WAL/FULL configuration,
excluding fixture creation and inspection. Match another checkout with `--root`.
`python3 scripts/measure-catalogue-publication.py --repetitions 2` compares the same
620-entry native and direct/opaque-relay development-loopback workload; repeat
with `--client-dialect capabilities` for fresh resumed metadata admission. Timing
is evidence, not a CI performance threshold or a production secure-channel claim.

Before any collection is exposed, #49 still needs monotonic horizon publication,
compatible initial/active/mid-pass admission and typed expiry, explicit authorized
reset, rejection of delayed lower horizons, an atomic safe collection boundary,
and a model showing dropped serving markers cannot resurrect deleted content.
Receiver fences remain permanent. #48 owns retention/pin policy; broader production
transport and host integration remain #54 work. #49 and #54 remain open.

The [reviewed measurement receipt](../verification/catalogue-retirement.json)
contains matched main/candidate wire, publication, migration and physical storage
cases, source fingerprints, repository commands and invariant sensitivity proofs.
Actual numbers and conditions are also included in the pull request description.
