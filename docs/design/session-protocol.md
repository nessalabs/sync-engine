# Session transport: first measurement and state contract (#54)

**Status: proposed, first milestone only.** No session codec, production socket,
pairing, relay service or new authorization mechanism is implemented here.
The seven [reference slices](../implementation-plan.md) remain the implemented
baseline. [Issue #54](https://github.com/nessalabs/sync-engine/issues/54) remains
open; this note is the decision/test boundary for its first implementation slice.
The [product contract](sync-engine.md), [record walkthrough](core-walkthrough.md),
[history](tail-history.md) and [catalogue](catalogue-pass.md) retain their ownership.

## Measured loopback baseline

Run `python3 scripts/verify-slice-3.py --profiles` with Cargo on PATH. Measurements
below use source `5ea1bd2c1d5c88c1fb41d7aec0f754e7602fc561` (merged #53) plus
request-counter instrumentation in the existing proxy. The fixture has seven
records totaling 4,126 payload bytes, including one 4,096-byte record. Each
profile uses a fresh replica and completes through position seven. The initial
subscription and deliberate final head recheck are included. The new assertions
check actual request counts, rather than inferring them from elapsed time.
[Machine-readable result](../verification/session-loopback-baseline.json).

| Profile | Counted elapsed | Earlier uninstrumented elapsed | Exchanges | Protocol bytes | Payload bytes | Duplicate payload |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 kbit/s, 0.8 s configured RTT | 6.925 s | 6.932 s | 7 | 1,360 | 4,126 | 0 |
| 64 kbit/s, 1.5 s configured RTT | 11.169 s | 11.175 s | 7 | 1,360 | 4,126 | 0 |

Each counted run observes operations `{subscribe: 1, authorize: 3, head: 2,
page: 1}`. There are two explicit head checks. An exchange means one complete
request and its response, not two RTTs. The proxy delays each direction by half
the configured RTT and throttles each direction independently in 256-byte
chunks. It does not model WAN TCP connection setup, packet loss, cellular power,
shared duplex capacity, TLS or relay overhead. The page uses one request/response
pair even if TCP divides its bytes. Thus these measurements do not establish
#54's rough 12–14 RTT arithmetic, nor production latency or percentiles.
The ordinary lab also verifies recovery after a deliberately unavailable proxy;
this is not a timed multi-second outage profile.

The 620-entry catalogue and catalogue-plus-K-transcript resume estimates in #54
are **not measured yet**. Count those separately before accepting inline payloads
or batching optimizations. There is no production session comparison yet.

## Owners and state

The host owns authentication, policy, transport lifecycle, retry scheduling,
cache invalidation and command admission. The session adapter owns bounded
framing, correlation, multiplexing and cancellation of transport work. Existing
validators own semantic admission; existing stores own atomic apply/progress.
A relay forwards authenticated envelopes to an online source and grants no access.
The development loopback credential is not a production handshake.

Use only three session states: `Connecting`, `Ready`, `Closed`. A scope binding
is `Active` or `Invalid`; an in-flight request is `Receiving` or `Terminal`.
Local store commit is a separate owner and is never a session state. All maps,
queues, reassembly buffers and retired-ID windows have negotiated hard bounds.
No network wait retains a source database transaction. No background reconnect
loop is hidden in domain or application code.

| Current state / event | Decision and next state | Required contract test before implementation |
| --- | --- | --- |
| Connecting / authenticated hello succeeds | Ready; establish a fresh session ID, supported protocol version and hard limits. Bind endpoint identity to the host-authenticated channel. | Unsupported version, over-limit hello and unknown required reader capability refuse before data. |
| Connecting / failed or unverifiable authentication | Closed; no bindings or source reads. Distinguish denial from temporary unavailability. | No source factory invocation on either outcome; ordinary retry does not imply revocation. |
| Ready / bind exact scope | Host authorizes receiver before revealing source identity; return a session-local handle, binding generation, exact scope and current head. | Receiver, stream, incarnation, schema and epoch are correlated; wrong receiver receives no identity metadata. |
| Ready / issue request | Allocate a nonreused request ID under a bounded in-flight limit; retain its exact expected scope, bounds, generation and operation. | Bound overflow rejects without allocating another request; no ID wrap/reuse. |
| Receiving / valid out-of-order reply for another active request | Route to that request only; validate its full echo before returning a domain value. | A/B replies reordered across different scopes never cross-apply. |
| Receiving / wrong session, handle generation, request ID, operation, echo or unsupported status | Refuse without apply; close on framing/correlation ambiguity. Preserve saved progress. | Unknown ID, duplicate terminal reply, malformed/truncated response and incompatible status cannot look like unavailability or empty data. |
| Ready / coalesced per-scope hint | Retain advisory head/pending wake; finish the active finite pass then recheck. | Hint racing final check remains pending; hint alone never changes progress. |
| Any session state / connection closes | Closed; discard bindings and uncommitted reassembly. Reconnect with a fresh session and request namespace. | Old-session replies cannot match a new request; only durable progress is resumed. |

Hello advertises endpoint limits including the source-owned record ceiling.
Incarnation, schema, access epoch and capability requirements belong to the exact
scope binding, because streams need not share them. Reader-required unknown bits
stop data for that scope; writer-only bits must not disable a read-only receiver.
The hello/binding vocabulary reserves these distinctions without implementing
[capabilities #47](https://github.com/nessalabs/sync-engine/issues/47).
Unknown required extensions are incompatible, never silently treated as zero.

## Authorization and status boundary to resolve first

Authenticate the connection once, but authorize **every source operation and
every emitted data batch** using current host policy before source access/send.
An earlier hello or successful reply cannot authorize later work. Every reply
carries the current access outcome and epoch; correlate this before decoding data.
Authenticate/authorize the target before exposing a changed identity or deletion.
Foreign live, foreign deleted and unknown entry IDs have the same public absence
result. Unreadable ownership state is unavailable, not deletion or absence.

Known temporary-unavailable outcomes remain retryable; authentication denial,
identity changes, incompatibility and invalid/unknown status remain distinct.
Recovery classes in #54 add explicit reauthenticate/reset-scope/reset-stream/
incompatible/deleted actions without collapsing #53's existing meanings. A
pruned response must carry its floor, and retry-after is bounded advisory metadata.
Deletion cannot be inferred from an omitted entry, cancelled request or partial page.

There is a concrete port constraint: `ScopeAuthorizer::authorize(scope)` does not
receive the next operation or page bounds. `begin_pass` calls it **before even
loading saved progress**. Catalogue coordination also authorizes before apply.
Returning Allowed from a stale binding would weaken that invariant; reusing a
previous data response as fresh authorization is not acceptable. A transparent
adapter therefore cannot simply remove all authorize exchanges under today's
contract. Before claiming #54's no-separate-authorize requirement, demonstrate
an authorized-operation composition that preserves these local-read/apply gates,
or explicitly review a minimal port-contract change. Do not hide this decision
inside a cached-grant optimization. An initial session prototype may multiplex
existing authorize requests, but that is an intermediate result, not completion
of this requirement. Keep domain policy and validation unchanged.

## Small frames, cancellation and commit

Target 2–8 KiB data frames, with a separately bounded control-frame size. Negotiate
per-request credit in bytes and a connection-wide aggregate bound. Debit before
enqueue/send and return credit after bounded consumption; exhausted credit pauses
only that request. Reserve bounded capacity for control. Scheduling priority is
host commands/Stop, open transcript, catalogue, older history, then artifact data;
the core does not implement command admission. Measure both priority and starvation
before choosing a concrete scheduler. A control frame may wait for at most the
currently writing data frame plus earlier bounded control work, not an entire page.
This is a wire scheduling bound, not an end-to-end command latency guarantee.

A record can be larger than one frame. Fragments are not independently committable.
Reassembly is bounded by the admitted request/record limits, not the peer's claimed
length. A complete validated page/tail/manifest is the existing store's commit unit.
An initial adapter can fragment/reassemble one response behind today's synchronous
ports. Streaming multiple independently committable batches requires its own
explicit composition proof with the same validators and finite captured target;
fragmentation alone does not satisfy that part of #54.

| Race / event | Transport decision | Durable result and regression requirement |
| --- | --- | --- |
| Cancel before a response is eligible for local apply | Mark request terminal, discard staged fragments and remove unsent data. Send idempotent Cancel keyed to its session/request ID. | No progress changes; later frames for it never apply. |
| Cancel while the store owns an atomic commit | Do not undo or interrupt the transaction. Await its outcome, or classify it uncertain if the process loses confirmation. | Reload durable progress before retry; test both commit-wins and cancellation-wins using controlled gates. |
| Cancel acknowledgement arrives | Confirms transport work stopped, not that received data was rolled back. | Host reports actual durable outcome separately; no fabricated checkpoint receipt. |
| Drop halfway through a fragmented page | Discard the incomplete page on close. | Previous checkpoint/coverage/cursor remain; request after saved progress on reconnect. |
| Drop after commit but before any apply receipt | No delivery acknowledgement is durable-progress evidence. | Reload saved progress; repeat identical data is safe, conflicting reuse is refused. |
| Epoch/incarnation/schema changes with queued or staged data | Invalidate binding and stop queued delivery; check current policy before any further batch. Refuse old-binding data on receipt. | No silent rewind/reset. Already committed old-scope cache follows host invalidation policy. Test change-before-send and change-before-apply independently. |

Policy checks and commits are separate operations today; this does not promise
globally instantaneous revocation. Stronger atomic policy/commit fencing needs
an explicit owner and proof. Late replies cannot restore an invalid binding.
[Origin epoch fencing #50](https://github.com/nessalabs/sync-engine/issues/50) is
a separate authority field with durable host ownership, not the access epoch.
Do not claim stale-origin protection before that work exists.

## Independently reviewable slices and finish lines

| Slice | Deliverable and finish line | Dependencies / exclusions |
| --- | --- | --- |
| 0: baseline and contract (this change) | Reproducible exchange/byte counts in existing weak-link lab; proposed ordering tables and explicit port constraints. | No session implementation or claimed production readiness. |
| 1: authorization and bounded codec | Resolve the authorizer composition gate; specify concrete version/length/ID/extension rules and limits. Pure codec + controlled state tests refuse over-budget/unknown/cross-request values before allocation/apply. | No sockets or product auth owner replacement. Review the contract before adding multiplexing. |
| 2: one session, record reads | Hello/binding + correlated head/page behind existing ports where their invariants hold. Independent process lab demonstrates reconnect/drop and exact durable resume. | Preserve explicit authorize until slice 1 proves removal safe. No hidden retries. |
| 3: multiplex, credit and cancel | Interleave scopes with bounded queues and record fragments. Deterministic tests cover every ordering table above, including commit races and old epoch. | No command execution; control priority is scheduling only. |
| 4: history/catalogue/artifact adapters | Carry current head/status everywhere; batched heads and coalesced hints preserve finite passes, request echoes and deletion privacy. | Resolve #47 fields explicitly; #48 retention, #49 tombstone horizon and #50 origin fencing retain separate owners/issues. |
| 5: transport/measurement qualification | Compare direct and relay transport adapters with the same baseline and 32/64 kbit profiles, controlled multi-second outages and packet loss. Record bytes/exchanges, applied latency, first visible milestone and control latency distributions. | Production pairing/encryption integration must be qualified separately; reference credentials never become production defaults. |

Measure before choosing inline catalogue payloads, tail `have_through` reuse,
varints/lean records, compression, direct streaming or long polling. Catalogue
inline payloads need descriptor/payload byte totals for the 620-entry case;
tail reuse needs overlap and repeat-byte counts; compression needs CPU and byte
measurements with urgent frames excluded; delivery strategy needs notification-to-
apply and metered fallback costs. None is a prerequisite for the basic state
machine, and none is accepted on the small seven-record result alone.

Each implementation slice needs normal repository gates, its runnable lab,
controlled adverse-order tests and independent adversarial review. After three
reviews still finding substantive issues, stop stacking patches and simplify
states/ownership before requalification. No #54 completion or merge is claimed
by this first milestone.
