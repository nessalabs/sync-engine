# Version1 record capability operations (#47 / #54)

**Status: opt-in record wire slice, not complete capability rollout.** This adds
capability admission to the existing bounded, serialized record session and its
optimized first page. Catalogue/history/artifact routes, persistent follow
integration and durable requirement publication remain outside this slice. See the [current acceptance map](source-head-acceptance.md); #54 remains open. No new optional core bit is assigned; both known masks remain
zero. Hosts still own authenticated/encrypted channel composition.

## Explicit endpoint, version and no downgrade

`CapabilityRecordSession` uses the existing bounded Open handshake and returns a
normal current-policy authorizer plus `CapabilitySessionSource`. The initial
Bound is passive identity/head metadata, never evidence of compatibility or apply.
Peer endpoint support must be known by the host. There is no probe/retry as an old
read after a capability failure. An old peer may reject the operation or close;
that cannot be interpreted as zero requirements or permission to fetch payloads.

`serve_capability_record_session` is a separately selected endpoint. It permits
normal binding/authorization but refuses old Head/Page/OpenPage operations even
for zero requirements. Existing `serve_record_session` and follow endpoints retain
their own legacy behavior and refuse the new data operations. Required-feature
sources must not be routed to legacy data endpoints by a host.

NSR1 framing stays version1 with the existing negotiated 2–8KiB frame bodies,
request/session/handle/generation correlation and bounded reply assembly. New
operation numbers explicitly select SourceHead metadata version1:

| Operation | Kind | Request after existing frame header | Success response |
| --- | ---: | --- | --- |
| Metadata head | 5 | u16 metadata version; u64 supported reader mask | exact Scope, metadata version, committed position, reader mask, writer mask |
| Open target + first page | 6 | same10-byte prefix, then existing OpenPage fields | 26-byte metadata prefix, then the complete existing canonical page echo/records |
| Fixed-target page | 7 | same10-byte prefix, then existing Page fields | same26-byte metadata prefix and canonical fixed-target echo/records |

The 26-byte response prefix is u16 version plus u64 committed position, required
reader mask and required writer mask, in network byte order. Scope comes from the
complete canonical page echo and exact bound request context. Open's metadata
position equals the echoed captured target; a fixed-target page's current metadata
position must cover its target. Neither current head moves the receiver's pass
boundary or checkpoint. All26 bytes are included in reply resource ceilings.
Reader support is bounded by both the consumer's implemented declaration and the
endpoint's implemented known bits. Extra unknown writer requirements are carried
unchanged and ignored by read-only admission. Host payload schema negotiation is
separate from this protocol/format feature registry.

## Before page access and before apply

The new `RecordReadView` source port is deliberately stronger than initial
`RecordHeadSource`. A factory must provide one owned coherent requirements and
encoding view: metadata must remain valid for all records through its captured
head on that handle. Same object identity is insufficient. A host must establish
an actual snapshot or immutable-format guarantee. Native Memory/SQLite references
have permanently zero optional requirements and immutable records; append-only
growth cannot change the captured range's format. This does not prove atomic
publication of future nonzero requirements.

For every operation the server validates the request/version/bounds, checks
current scope authority, acquires a view, reads exact-scope committed metadata,
and validates reader requirements before any page call. It rechecks authority
before page access and before each emitted fragment, including refusal frames.
The view drops before network I/O. A requirement added between separate requests
is observed on the next new view and stops the next page. An already captured
coherent DTO can finish under its view; requirements are not an instantaneous
revocation or commit-cancellation mechanism.

The client correlates the bounded reply, validates metadata and the full canonical
scope/range/budget header before decoding record bodies, then uses existing page
validation and atomic apply. Ordinary `begin_pass`/`step`/`finish_pass` receive the
same typed incompatibility as `begin_pass_with_page`. The normal head port calls
metadata Head, so the deliberate authorized post-pass recheck remains fresh. No
binding head, previous requirements or authorization result is cached for speed.

| Event | Outcome |
| --- | --- |
| Unsupported metadata version or required reader bits | Typed `Incompatible` before server page access; no record payload or receiver cursor/checkpoint change. |
| Unknown writer-only mask | Read-only catch-up continues; this supplies no source-write permission. |
| Scope/policy denial or identity change | Existing metadata-free refusal takes precedence over detailed incompatibility. |
| Unsupported source preparation during initial binding | Existing metadata-free InvalidRequest setup refusal; ControlRefused vocabulary is unchanged. |
| Incompatibility after a prior pass/connection | Refuse before the next page; existing cached records remain readable after reopen. |
| Wrong metadata/echo/target, unknown status/subcode, or incompatibility after a partial success stream | Fatal protocol failure; incomplete data is never applied. |
| Source view complete / disconnect or close | Existing terminal I/O and explicit reconnect rules; completed DTO/apply cannot be recalled. |

Only kinds5–7 accept status8: one bounded incompatible detail, with offset0 and
complete total equal to its payload. Subcode1 carries an unsupported u16 version
(3bytes total); subcode2 carries a nonzero missing-reader u64 mask (9bytes total).
Other subcodes, zero missing masks, malformed details and status8 on an old
operation are fatal. A detailed error cannot replace an already fragmented success
response. A fresh denial suppresses all detail. Existing statuses1–6 remain empty;
invalid/unknown status7 remains fatal. Legacy control framing still admits only
its original six refusal reasons; capability details are not a control extension.

## Real evidence and cost

```sh
cargo test --locked --all-features --test session_capabilities
python3 scripts/verify-session-capabilities.py --repetitions 2 --output refusals.json
python3 scripts/verify-session-e2e.py --profiles --repetitions 2 \
  --modes session-io session-caps --output capability-wire.json --markdown capability-wire.md
```

The matching seven-record/4,126-payload-byte lab uses real direct TCP and an opaque
raw-TCP relay, actual application/source adapters, SQLite atomic apply and a fresh
process inspecting reopened cache. Supported peers retain 7 cold / 5 warm / 7
reconnect exchanges (19 for the short workload), including fixture authentication.
Each phase adds64 protocol bytes: +10 request/+26 response for the first page and
+10 request/+18 response for the fresh head. Short adds192bytes; idle still runs
the same authorized fallback-style check and adds64bytes. This is a compatibility
cost, not a claimed latency improvement. Min/median/max debug timings and raw
receipts are recorded in [the checked-in evidence](../verification/source-head-wire.json)
and the PR body; no timing threshold is a CI gate.

The separate unshaped refusal lab observes actual request maps and the 57-byte
status8 frame, verifies zero record payload, and reopens unchanged caches on cold
and reconnect refusals. The reconnect fixture first completes writer-only catch-up
and then monotonically adds reader requirement0→1. Static owned fixture flags do
not implement durable source publication. Tests also cover policy changes after
metadata, canonical echoes, partial responses, no downgrade, ordinary page steps,
the full source-owned 512KiB record ceiling and fresh post-pass requirements.

Durable publication and catalogue capability transport landed in later slices;
see the [current acceptance map](source-head-acceptance.md). Nonzero format
rollout and the wider secure transport/priority/credit/cancel/outage gates still
need their own implementation and qualification under #54.
