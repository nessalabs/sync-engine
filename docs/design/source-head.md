# Versioned SourceHead initial admission (#47 foundation)

**Status: native core initial admission only. #47 and #54 remain open.**
This slice adds shared passive metadata and explicitly opt-in application starts
for record and catalogue sources. It does not add capability-bearing network
messages, enable a new record format, or establish mid-pass compatibility fencing.
The existing transport paths and combined first-page start remain unchanged.

## One owner for version and requirements

`domain::SourceHead` retains metadata version, exact Scope, committed position or
revision, and separate reader/writer requirement masks. Version 1 is exported as
`SOURCE_HEAD_VERSION`; it is independent of NSC1/NSR1/loopback wire versions.
`wire_contract` also exports `KNOWN_READER_CAPABILITIES` and
`KNOWN_WRITER_CAPABILITIES`. Both masks are currently zero: no optional core bit
is assigned by this slice. New assignments need an implemented core feature and
reviewed ownership; hosts must not assign core bits for their payload schemas.
Opaque future advertisements remain representable so unsupported requirements can
be refused instead of silently stripped.

`CapabilitySupport` can select only implemented bits. `validate_capabilities`
checks version and required reader bits and ignores writer requirements.
`validate_writer_capabilities` requires both sets, but supplies neither permission
nor a mutation port. Unknown reader bits/version produce a typed
`Incompatible(CapabilityError)` at the new application starts. Wrong scope echoes
are typed validation failures. None of these outcomes resets, deletes or advances
a cache. A head position is a finite boundary, never an apply receipt or grant.

## Native port and admission ordering

`RecordHeadSource` and `CatalogueHeadSource` are explicit extensions of the old
source ports. There is no default conversion or blanket implementation from bare
u64 heads. Memory and SQLite reference sources explicitly advertise zero optional
requirements for their existing format. Legacy network clients do not implement
the new ports; a bare legacy head cannot masquerade as capability metadata.

| Event | New admission behavior |
| --- | --- |
| Denied/unverifiable/wrong authorization scope | Stop before reading local progress or source metadata. |
| Authorized record start | Load and validate saved progress, fetch a fresh exact-scope SourceHead, validate version/reader bits, then construct a finite Pass. No page/apply occurs in this start. |
| Authorized catalogue start | Load/validate progress, fetch/validate metadata before store.begin or returning a saved active pass. |
| Active catalogue resume | Check fresh metadata and require the current head to cover the saved active boundary; preserve the saved cursor and pass. |
| Unknown reader/version | Return typed incompatibility before page/manifest/begin/apply; saved cache and progress stay readable. |
| Unknown writer bits on a read-only consumer | Continue normally; writers must separately validate writer compatibility and current authority. |
| Head behind saved progress or active boundary | Existing behind/reset-required outcome; no silent rewind. |
| Completed pass / explicit recheck | Call the capability start again for fresh metadata. A prior Pass is not a compatibility certificate. |

`begin_pass_with_capabilities` shares the old authorization/checkpoint/boundary
construction. `begin_or_resume_with_capabilities` shares catalogue begin/CAS and
correlation logic, while deliberately adding a metadata read on active resume.
Legacy functions keep their original I/O ordering and wire cost. The ordinary
page validators, per-read authorization and atomic apply are unchanged. Hosts
must retain the deliberate fresh post-pass recheck; hints and metadata never
replace it.

## Source incarnation monotonicity

`validate_head_transition` is a pure source-publication check. For the same
origin/stream/incarnation, position cannot regress, schema cannot change, and
neither required mask can lose a bit. Receiver or access-epoch changes do not
create a new incarnation and do not permit clearing. An actual new incarnation
permits a lower head or cleared bits; it does not itself authorize resetting a
receiver. Different origins/streams and unsupported metadata versions are refused.

A host must durably compare/commit publication requirements with its source
incarnation and data format. This pure check is not a persistent publication store
or cross-process CAS. The SQLite references remain zero-requirement sources; their
schema is not migrated to persist future capability requirements in this slice.

## Evidence and remaining acceptance

```sh
cargo test --locked --all-features --test source_head
python3 scripts/verify-source-head.py
```

The runnable example uses real SQLite sources and caches, verifies exact reopened
records and catalogue entries, and reports head/page/manifest/resolve counts.
Owned fixture metadata represents unknown future requirements; it is not a
production publication owner. The lab covers empty and populated record caches,
empty and active catalogue progress, unsupported readers, writer-only evolution,
post-pass/resume rechecks, wrong scope/version and untouched progress on refusals.
Pure tests cover monotonic extension/removal, epoch/receiver changes, incarnation
replacement, schema changes and head regression.

This initial check is **not complete #47 safety**. Catalogue resolution can return
a newer current value than the captured boundary; requirements can also change
after admission. Passing a legacy Pass to step/apply is not a capability proof.
Nonzero reader feature rollout requires a verified per-response/read compatibility
contract, authoritative durable publication, and versioned transport metadata
(including combined open/first-page, catalogue, history and remaining routes).
The combined first-page path cannot discover incompatibility after payload fetch
and claim early refusal. It is not upgraded or silently used as a downgrade here.
The source-head feature must not be enabled as full production fencing until those
remaining checks are implemented. Do not close #47 from this admission milestone.

The PR includes matched baseline/current real TCP phase measurements proving
unchanged legacy wire exchanges/bytes, separately from native admission call
counts. No network performance gain or capability-bearing transport support is
claimed by a native metadata check.

[Native/transport/gate/reversion receipts](../verification/source-head-admission.json)
retain the matched measurements and reviewed source fingerprints.

The later [version1 record wire slice](source-head-wire.md) supplies an explicitly
selected record endpoint and a stronger coherent read-view contract. This original
initial-admission port alone still makes no per-read snapshot promise.
