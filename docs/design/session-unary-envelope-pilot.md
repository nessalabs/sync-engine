# Unary record reply envelope pilot: decision and test plan (#54)

**Status: opt-in implementation and fixture qualification, not #54 completion.**
`UnaryHeadSession` and `serve_unary_head_session` are independently selected by
the host; neither is enabled by existing constructors. They reuse the serialized
owner, Open/Bound and host-authenticated channel. The host must know that the
peer selected this endpoint. There is no probe, retry or fallback. The fixture's
loopback token and opaque forwarding relay are test mechanisms, not production
channel authentication. No Follow, TLS, Page or receiver-apply route is added.

Kind 11 selects the pilot. Its request is empty; its success body is a big-endian
u16 envelope version 1, the existing exact six-ID Scope encoding, and the existing
26-byte SourceHead metadata. The maximum is `2 + scope_bytes + 26`, within the
existing negotiated single-head budget. Envelope version and trailing bytes are
strict. The client checks session, request, kind, handle and generation before
body decoding, then exact scope including access epoch and SourceHead compatibility.
The host's authenticated channel is the integrity boundary; the envelope is not
a separately signed proof. The result is advisory, never an authorization grant
or checkpoint. The pilot does not provide record pages or remove authorization.

Status 0 succeeds; statuses 1–6 preserve the existing typed errors with no body.
Status 8 uses the existing bounded capability-incompatibility codec for a supported
backend incompatibility; the receiver preserves its exact typed detail. Status 7,
unknown or malformed statuses and unsupported incompatibility details are fatal. Current
policy denial or uncertainty suppresses all incompatibility detail at egress.
Unsupported SourceHead metadata or required reader bits in a successful response
are returned as the existing typed incompatibility by the receiver. Current host
policy precedes source acquisition and every outgoing frame. A changed policy suppresses the captured head; the source view is dropped
before sending. Existing pre-progress authorization and store fences remain owned
by their respective callers. This is not atomic policy/commit fencing.

Run `cargo test --locked --features transport,sqlite unary_head -- --nocapture`.
The real TCP/SQLite fixture uses the same source and existing-capability Head as
baseline, directly and through an opaque relay. Both versions perform one measured
Head exchange, with 172 baseline versus 164 pilot protocol bytes and zero record
payload bytes in each path; a previously populated cache reopens at checkpoint 1
with one record. Counts exclude fixture authentication and setup and do not measure
latency, TLS bytes or throughput. Other tests exercise explicit selection mismatch,
correlation, statuses, malformed envelopes, bounds and policy withdrawal.

The remaining sections record the pre-implementation decision and future Page
qualification plan; their broader requirements are not claims about this Head-only
pilot.

## Current state at b65f4dee

* `NSR1` version 1 data frames have a 44-byte body header: magic, version, kind,
  status, session, request, handle, generation, total and offset. The negotiated
  body limit is 2–8 KiB; replies can span frames. `records.rs` rejects zero IDs,
  unexpected kind or correlation, noncontiguous fragments, invalid totals and
  unsupported statuses. A response is terminal only after complete bounded
  assembly, or a correlated refusal. The serialized owner allocates monotonically
  increasing request IDs without wrap; it owns the pending request and channel.
* Bound metadata retains the exact six-ID scope (receiver, origin, stream,
  incarnation, schema and access epoch), session-local handle and binding
  generation. A handle/generation identifies retained metadata, not a grant or a
  globally durable identity. The request's kind, scope, bounds and ID are retained
  locally. Page success already echoes the canonical request; capability operations
  additionally carry SourceHead v1 metadata. Bind's head is advisory.
* Status 0 is success; 1–6 are typed source errors, 7 is fatal invalid policy,
  and 8 is a bounded typed incompatibility only on capability operations. Unknown
  statuses and malformed details are protocol failures. A normal metadata-free
  refusal can replace an incomplete success; the staged body is discarded. A
  status-8 detail cannot follow partial success. See `session-records.md` and
  `source-head-wire.md` for exact vocabulary and limits.
* The host authenticates the channel and supplies current principal-bound policy.
  The server checks policy before source acquisition and again before each emitted
  fragment or refusal. Source handles are released before network I/O. The follower
  checks access before loading progress and before a page read; it validates the
  full page before the store's atomic scope/position compare and apply. Policy and
  commit are not one atomic operation: the existing record follower does not promise
  a fresh policy check immediately before commit. Epoch-qualified installations have
  their own independent admission/transaction fences. Neither envelope metadata nor
  an authenticated channel substitutes for any of these owners.

## Original proposed transition (historical)

| Event | Existing behavior | Required pilot behavior |
| --- | --- | --- |
| Host selects legacy | Existing operation and framing. | Identical behavior and bytes; no capability probe. |
| Host explicitly selects pilot | Not available. | Existing authenticated session owner retains one exact pending operation and its full scope, limits, request, session, handle and generation. Rejection or unsupported peer is terminal for that selected attempt; no legacy fallback. |
| Exact complete success | Correlate/assemble, then canonical decode and validate. | Authenticate the channel by the host's existing contract; correlate envelope and current retained binding before decoding operation payload. Require the selected status and exact scope/access epoch semantics, then retain existing canonical validation and store apply. |
| Exact known refusal | Return its typed source failure, no apply. | Preserve the exact typed failure and confidentiality rules; discard any staged body where the existing refusal rules permit it. Never turn refusal into empty success. |
| Wrong session/request/handle/generation/kind, retired binding, duplicate terminal or unknown status | Protocol failure; no apply. | Non-advancing; close on ambiguity. A stale response cannot rebind or revive a request. |
| Access epoch changes after capture | Policy is checked at egress; already sent bytes and later local commit are not globally atomic with policy. | Old-scope staged data cannot be admitted under a new binding. Preserve existing source checks and receiver/store fences; make no stronger instantaneous-revocation claim. |
| Disconnect, truncation or cancel before apply | Discard incomplete response; explicit reconnect/reload; cancellation has its existing owner. | Same; a reply or cancel acknowledgement is not a durable commit receipt. If commit owns the operation, observe or reload its outcome. |

The envelope must be bounded within the existing frame and per-operation response
budgets, including any added bytes, and support the existing maximum record and
fragment rules. A refusal must not disclose source identity to an unauthorized
peer. A relay is an opaque forwarder; it cannot mint authority. Distinguish the
access epoch from the independently owned writer authority epoch.

## Decision and ownership

The pilot selects channel authentication, operation kind 11 and policy-gated
refusals, as described above. The existing serialized session retains correlation;
no independent reader or source owner is added. The host owns channel authentication
and current policy; the source owns bounded capture and release. The existing
`ScopeAuthorizer` has no operation or bounds parameter, so the pilot cannot remove
the pre-progress authorization check. Coordination with the Follow owner remains
necessary for any changes to its integration. A separate Page design must prove its
own apply boundary; this Head pilot has no apply path.

## Original regression plan and future Page vectors

The following original plan includes future Page and cancellation work. Those
vectors are not all implemented by this Head-only pilot. Each later vector must
exercise the actual owner, not only a duplicate test decoder;
use controlled gates for races and verify reopened durable data/checkpoint where
applicable. Both positive and negative cases are required.

1. **Exact-success-and-legacy:** selected pilot succeeds on the exact retained
   operation and scope; capability absent produces the pre-pilot wire and result.
2. **Explicit-selection-no-downgrade:** unsupported, refused, malformed and EOF
   pilot replies issue no legacy retry and cause no durable progress; if selection fails during setup, expose no ports.
3. **Correlation-and-terminal:** individually alter session, request, handle,
   generation, kind and binding epoch; deliver a duplicate terminal or an old
   session reply after reconnect. Every mismatch is non-advancing; a current
   exact reply succeeds.
4. **Status-and-disclosure:** each supported typed refusal preserves its meaning;
   unknown status, invalid detail and malformed success fail closed. Denial and
   unverifiable policy do not expose scope/head/payload; authorized success does.
   Test permitted refusal after a partial response and forbidden detailed
   incompatibility after partial success.
5. **Bounds-and-fragments:** min/max negotiated frame, maximum admitted record,
   complete contiguous fragments and exact response ceiling succeed; over-limit,
   changed total, gaps, overlap, empty nonfinal and truncated reply do not apply.
6. **Source-egress-policy:** gate before source acquisition and between fragments;
   deny/change epoch at each gate. Assert no source read before authorization,
   no source handle held during send, no further old-scope payload after the
   checked refusal, and unchanged receiver progress. Also test the allowed ordering.
7. **Receiver-apply-and-cancel:** hold a complete reply across invalidation and
   reconnect, and gate cancel versus store apply. Stale bindings never advance;
   cancellation-wins leaves durable data unchanged, commit-wins reloads actual
   atomic state. Do not claim policy/commit atomicity that the store does not own.
8. **Authenticated-boundary:** wrong/unverified peer fails before source access;
   a valid channel with incorrect envelope/correlation still fails. Exercise the
   selected authenticated binding's replay and cross-session cases once decided.

A functional implementation also needs the existing real direct and opaque-relay
TCP/SQLite lab, exact records and reopened progress, typed failure assertions,
and matched before/after exchange, payload and protocol byte counts. Report actual
elapsed samples and conditions if measured, with no invented performance result.
Run repository gates and independent exact-head review before proposing a draft;
this document is not functional E2E or CI evidence.
