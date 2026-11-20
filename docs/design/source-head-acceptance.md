# SourceHead capability acceptance

The requested #47 mechanism is implemented through PRs #65–#69. This map records
acceptance rather than assigning a hypothetical optional format bit. Issue status
is coordinated separately after the documentation lands. #54 remains open.

| Requested behavior | Owner and verification |
| --- | --- |
| Versioned committed position with separate reader/writer masks | `SourceHead`, `RecordHeadSource`, `CatalogueHeadSource`; `tests/source_head.rs` version, exact-scope and undefined-support tests; PR #65. |
| Unsupported reader requirement stops before page/manifest and leaves cache/checkpoint readable | Pure `validate_capabilities` in capability-aware starts; native restart tests in `tests/source_head.rs`; wire cold/resumed and reopened-cache fixtures in `verify-session-capabilities.py` and `verify-catalogue-capabilities.py`; PRs #65, #66, #69. |
| Unknown writer-only requirements do not stop a read-only consumer | Separate reader/writer validation; native, record wire and catalogue wire ordinary compatibility fixtures. Writers independently validate writer support. |
| Requirements cannot clear in one source incarnation | `validate_head_transition`; durable publication CAS within existing source transactions; PRs #67, #68. Receiver or access-epoch changes do not establish a new source incarnation. |
| Requirements survive restart and describe the same publication as the data | Existing record/catalogue metadata owners, migration, rollback/reopen and competing-handle tests; coherent record read views and catalogue manifest/current-value snapshots; no transaction spans network I/O. |
| Shared known masks | `wire_contract` exports the domain-owned version and masks; Rust equality tests and `verify-wire-contract.py` validate actual JSON. Both masks are zero until a real format feature is implemented. |
| Fresh checks on resume and after a finite pass | Capability-aware admission reloads and checks; per-response wire validation and catalogue whole-page staging preserve newer-than-boundary current values/deletions. Ordinary resume/churn/recheck labs retain exact progress. |

## Explicit boundaries

Capability-aware entry points are opt-in. The legacy bare-`u64` head/start APIs and
legacy wire operations remain stable and do not silently acquire a compatibility
guarantee or serve as a downgrade after capability refusal. Hosts enabling a new
format must select the explicit capability paths throughout the affected reads.
Native metadata-only admission does not turn a legacy page port into a coherent
snapshot. A head observation supplies neither authorization nor durable progress.

SQLite upgrades require the host to stop or fence previously open old-version
writers before migration. New writers check version and support in each mutation
transaction; the library cannot retroactively change an already-running old binary.
No new format bit, payload-schema negotiation, instantaneous receiver commit fence,
or automatic host upgrade orchestration is promised by #47.

Production channel composition, capability-bearing routes beyond the explicit
record/catalogue paths, multiplexing/priority/credit/cancel and wider outage/loss
qualification remain #54 work. The checked-in verification receipts and the design
notes for each merged slice preserve the actual measurements and limitations.
