# Version1 catalogue capability operations (#47)

**Status: explicit capability dialect on the development catalogue route. See the [current acceptance map](source-head-acceptance.md); #54 remains open.** This is not a session catalogue, secure channel, compression,
inline catalogue, multiplexing or instantaneous publication/apply fencing slice.
Known optional masks remain zero; undefined feature publication is still refused.

## Owned source snapshots

`CatalogueReadSource` explicitly extends initial `CatalogueHeadSource` admission.
There is no blanket conversion from an old source. `manifest_with_head` and
`resolve_with_head` accept implemented consumer support, validate it before data
acquisition, and return an owned publication plus data from the same snapshot.
SQLite uses the existing catalogue metadata owner and short read transactions.
The descriptor/current value can be newer than the finite pass boundary, but its
revision must be covered by the returned committed head. Requirements and payload
are never paired by making two independent source calls. Transactions and source
handles are gone before the server writes network bytes.

Native legacy methods delegate to these readers using native implemented support.
Legacy wire operations11–13 explicitly supply zero optional reader support; an
upgraded server cannot silently assume an old client implements newly known bits.
Their successful bytes remain unchanged. Required-feature refusal stays fatal in
the old vocabulary; it is not silently translated into zero requirements.

## Explicit messages, no connection mode

`CapabilityCatalogueClient` uses the existing loopback address/token connection
factory. Each operation opens one connection, as before. Selecting this client
chooses new operation numbers; no socket option, server mode or implicit legacy
probe/fallback is added. A host must know the peer supports these operations.

| Operation | Kind | Request after token | Success after status0 |
| --- | ---: | --- | --- |
| Head | 17 | u16 requested SourceHead version, u64 reader support, exact Scope | request kind/version/support echo,26-byte SourceHead metadata, exact Scope |
| Manifest | 18 | same10-byte prefix, canonical pass and max_entries | same request echo/metadata, then existing exact pass/count/continuation/descriptors |
| Current value | 19 | same10-byte prefix, canonical pass, ID and byte budget | same request echo/metadata, then exact pass/ID/budget, current descriptor, bounded opaque bytes |

Metadata is u16 version then u64 committed head, required reader mask and required
writer mask, all big-endian. Scope comes from the exact canonical echo. The version
field requests SourceHead metadata version1; future incompatible operation layouts
need distinct explicitly defined operations rather than reinterpreting these fields.
Both ends retain the existing1MiB frame limit,128-entry wire manifest limit and
512KiB wire value budget. Metadata and request echoes are inside those ceilings;
the full existing wire value budget is tested. Native catalogue limits remain
separate; this does not expand the wire's physical payload budget.

The client checks operation/version/support echo, exact scope/pass/generation,
resource echoes, compatibility and revision coverage before decoding opaque bytes.
Manifest descriptors must be within the admitted request and covered by its current
head. Resolution must cover both the captured boundary and returned current revision;
it preserves stable creation key and deletion-payload rules. A current head never
replaces the pass boundary or acknowledges durable apply.

Only new operations admit status8. Its body echoes operation/version/support, then
subcode1 plus an unsupported u16 metadata version, or subcode2 plus a nonzero missing
reader mask. Empty/unknown/trailing details, a supported-version refusal, unknown
statuses and malformed success envelopes are fatal InvalidRequest outcomes. Known
status2 remains retryable Unavailable only with its empty valid body. Current scope
or receiver denial takes precedence and suppresses all capability details. This
adds no legacy status interpretation or control/session vocabulary.

## Admission, resume and commit

Use `begin_or_resume_with_capabilities` with the new client. It authorizes before
reading local progress and rechecks fresh remote metadata before returning an active
pass, preserving its saved boundary/cursor/generation. This adds one head RPC on
resumed active passes compared with the old shortcut. The ordinary head method also
uses fresh capability metadata; explicit post-pass rechecks remain available and
required by the host's existing catch-up loop.

Every manifest and resolution validates its own current publication. Unknown writer
requirements do not stop readers. Unknown reader/version requirements produce typed
CatalogueError::Incompatible before descriptor/payload access. Whole-page staging
is unchanged: if requirements change before a later resolution, none of the staged
values or cursor changes are applied. Reopened caches remain readable and unchanged.

Per-operation authority remains the existing server check before source access and
again before writing the response. Application authorization before apply remains
separate. Tokens and MemoryAuthorizer in the lab are owned development fixtures,
not production grants or encrypted-channel qualification. Absence, identity and
permanent deletion behavior is preserved.

## Runnable evidence and limits

```sh
cargo test --locked --all-features --lib catalogue_capabilities
python3 scripts/verify-catalogue-capabilities.py --output refusals.json
python3 scripts/measure-catalogue-publication.py --client-dialect capabilities \
  --repetitions 2 --native-repetitions 5 --output capability-catalogue.json
```

The620-entry lab measures bootstrap, partial pass, process restart/resume, changed
value/deletion catch-up and unchanged recheck. Direct includes a counting TCP hop;
relay adds an opaque raw TCP hop. It compares every reopened entry and progress
against source. Actual request frames and wire bytes are observed, with manifest
and opaque payload bytes separated. Timings are unshaped debug min/median/max,
not CI thresholds or weak-link/performance-gain claims. Matched old-main legacy,
current legacy and current capability results belong in the PR.

Separate real-TCP tests cover metadata-before-opaque decode, exact operation/resource
correlation, revision coverage, bounded/unknown statuses, an ordinary unsupported
peer with no legacy retry, source acquisition guards, typed cold/resumed refusals,
writer-only newer values/deletions, a requirement change after a staged value, the
existing maximum wire budget, and explicit retry after dropped/truncated replies.

Old-writer fencing, actual nonzero encoders and coherent publication/receiver-apply
ordering still need qualification. An already acquired compatible DTO may finish
later; this is not instantaneous revocation or commit cancellation. Capability
support on persistent follow/other routes and the wider#54 session security,
priority/credit/cancel/multistream/outage requirements remain separate. No new
feature bits or downstream client implementation is introduced.

[Verification and matched wire-cost receipts](../verification/catalogue-capability-wire.json)
retain exact measured source/harness fingerprints and regression evidence.
