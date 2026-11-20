# Local cancellation at the follower apply boundary

Issue #54: an optional, one-shot local gate on one ordinary bounded follower
page step. This is not a wire Cancel, transport interruption, remote stop
acknowledgement, a transaction decision, or a scheduler. The existing starting
paths, Follow dispatcher, record wire protocol, and SQLite store are unchanged.

The host creates a pair with PageStepAttempt::pair, passes the single-use
attempt to step_cancellable, and keeps its independent cancellation handle.
The step uses the same current authorization, bounded page and pure validator
as ordinary step. Only a complete validated plan can try to claim apply.
Ordinary step requires no cancel state or allocation.

| Event | Local decision | Durable meaning |
| --- | --- | --- |
| Cancel before the apply claim | Cancellation wins the one-shot gate. A pre-cancelled call does not contact the source. An already-running synchronous source read can finish; discard its validated plan. | This attempt cannot call apply; it says nothing about other writers. |
| Apply claims first | Cancel returns TooLate, even if SQLite has not committed yet. Do not interrupt the store or claim it rolled back. | A successful store confirmation advances the cursor. A refusal is reported as an actual store outcome. |
| Apply confirmation becomes uncertain | Return the existing typed uncertainty, not cancellation or success. The in-memory pass remains unchanged. | Discard that pass and explicitly reload durable progress via an authorized begin_pass. Do not auto-retry. |
| Attempt finishes without apply or is dropped | Mark it finished if no earlier decision won. A later Cancel cannot claim that it prevented work. | No apply by this attempt. A real policy/source/validation error remains visible even if cancellation raced that failed read. |

The state is in-process only. A late Cancel returning TooLate is not a success
receipt, and a lost process cannot infer a durable outcome from this handle.
There is no new atomic relationship between external authorization and a store
transaction. Once admitted, a synchronous read may still block until its
existing host deadline; this API does not provide cancel-to-return liveness.

## Qualification

Run the exact fixture:

    cargo test --locked --features transport,sqlite --test session_follow local_cancel -- --nocapture

The fixture uses the existing host-authenticated development Follow session
and actual SQLite, through direct localhost TCP and a byte-opaque local TCP
relay. It exercises cancellation after receiving a complete fragmented page,
reopens to no checkpoint/rows, then performs an ordinary authorized read on
the same connection. In a separate gate Cancel loses after apply entry;
SQLite commits and reopens at position 1. A wrapper then discards confirmation
after an actual SQLite apply: the step returns uncertainty and the old
in-memory cursor stays at 1, while a fresh authorized connection and SQLite
reopen recover position 2 and finish with exactly three records.

The matching successful case reads the first 9,001-byte record on fresh
isolated receivers. Ordinary and cancellable steps each use **2 logical
exchanges, 464 application protocol bytes and 9,001 record payload bytes** on
both paths, excluding connection setup and pass start. An already-cancelled
step adds no wire traffic. The opt-in pair has one small shared allocation and
local atomic state operations; the fixture makes no CPU/latency claim. It
uses controlled local gates and a lost-confirmation wrapper, not a killed
process, TLS/production relay, or an urgent-traffic test.

Wire cancellation, bounded independent control/data progress, credit, general
multiplexing and secure duplex qualification remain separate. Issue #54 remains
open.
