# Interruptible session I/O ownership (#54 prerequisite)

**Status: liveness prerequisite, not persistent hints/follow.** The serialized
record adapter holds its client mutex across each blocking RPC. Reusing that
owner for an idle hint read would block all ports and close. This slice adds
explicit independent I/O ownership and interruption before introducing a reader
dispatcher. No hint frames, notification queue, coalescing state, scheduler or
notification-to-applied latency is implemented or claimed here.

## Owners and lifetime

`InterruptibleSessionIo` supplies a connection-specific `SessionInterrupt` handle
that does not take the reader/writer locks. Optional `DuplexSessionIo::split`
consumes the whole channel into one owned reader and one owned writer. The native
`TcpSessionIo` implementation and its halves are not Clone. The host authenticates
the channel and configures finite deadlines **before** adoption; this adapter opens
no socket and changes no socket option, security setting or policy. It supplies
no TLS or device trust. A TLS/relay host must provide its own correctly owned secure
I/O implementation; splitting a raw socket is not splitting a secure record layer.

Native interrupt handles retain an owned cloned socket, never an integer fd that
could be reused or a mutable endpoint locator. Each handle addresses the same
original connection forever. The unsplit native channel uses two descriptors;
split adds one writer descriptor. Arc handle clones allocate no further socket
descriptors. Retained handles can extend resource lifetime. On split failure or
owner disposal the host explicitly interrupts or releases them. Dropping one root
handle does not cancel other live parts, and no background reconnect is created.

This is only the I/O ownership boundary. A future reader dispatcher must capture
the lifecycle `ConnectionId` before reading, correlate exact scope/session/request/
handle/generation, bound pending replies and keep at most a coalesced advisory wake
per scope. A raw I/O half is neither a grant nor a durable-progress receipt.

## Close contract

Use `RecordSession::connect_interruptible` or `connect_open_interruptible` with
these channels. The original generic constructors remain available and retain
host-deadline-bound I/O disposal. A host needing to interrupt the connecting
handshake retains the channel's interrupt handle before moving it into a
constructor; no RecordSession exists until its binding reply passes.

`close` first sets a permanent local admission fence outside the RPC mutex. Port
admission checks it before and after acquiring the guard. It then invokes the
optional interrupt capability **before** waiting for that mutex. Once acquired,
it drops the channel and invalidates lifecycle metadata. No closing state can be
reset or reused for another connection.

| Event | Outcome |
| --- | --- |
| Close requested / new authorizer or source operation | Fail closed before waiting on the RPC mutex; no policy grant or new wire request. |
| Previously queued operation acquires its guard | Recheck the terminal fence before any operation. |
| Native interrupt with blocked read/write | Fence future I/O then request OS shutdown in both directions; accepted shutdown releases blocking socket I/O. |
| Interrupt returns error | Return the error promptly without waiting on the RPC mutex. Admission stays terminal. Host retries interruption, disposes resources, or uses its configured deadlines; blocked/already queued calls may require that cleanup. |
| Repeated or concurrent interrupt | Retry OS shutdown even if the local fence is already set. Already disconnected is success; another shutdown error is still reported. |
| Generic constructor without interrupt capability | Admission closes, but close can still wait behind an admitted RPC until host I/O completes/deadlines expire. |
| Complete authorized response races close | It may still yield a DTO, validation or commit. Interruption is not an atomic terminal-delivery fence, wire Cancel, or recall of an already accepted response. |
| Store already owns atomic apply | Let the store finish/report its actual result. No rollback or fabricated progress receipt. |
| Old handle used after explicit reconnect | It still addresses only the old socket; a new channel/lifecycle/authority check is required. |

Custom interrupt implementations must work while read/write is blocked, fence
future I/O even when interruption fails, return errors honestly and never wait on
an I/O lock they are supposed to interrupt. Native state fences reads/writes before
OS calls. Transport loss is unavailability/unverifiable access, not revocation or
cache deletion. Current-policy checks, exact echoes, validators and atomic apply
are unchanged. Diagnostic counter snapshots may still wait behind an active RPC;
they are not an admission or interrupt API.

## Real evidence and measurement boundary

The normal E2E runner adds `session-io`, using the same bounded Open and source-
captured first-page path as `session-page`, with an interruptible native channel.
Protocol exchanges, protocol/total/payload bytes and duplicate counters must match
for every cold, warm catch-up, reconnect, idle and short-workload row.

```sh
python3 scripts/verify-session-e2e.py
python3 scripts/verify-session-e2e.py --profiles --repetitions 2 \
  --modes session-page session-io --output session-io.json --markdown session-io.md
```

The [measured-source fingerprints and raw results](../verification/session-io-e2e.json)
retain actual observations. Phase/short/idle min/median/max and test conditions also
belong in the PR description. No latency gain is inferred from the ownership change;
these are noisy debug-build timings, not CI performance thresholds. Reconnect
closes the old ports before its timer starts, so phase/short intervals also exclude
inter-phase close/interruption cleanup. Shutdown liveness is qualified separately
by controlled regressions, not inferred from those timings. Idle here means
an unchanged explicit catch-up observation, not a persistent-follow subscription.
Notification-to-applied latency, repeated/coalesced hint traffic and subscription
idle traffic are **unmeasured because those paths are not implemented**.

Channel-only paired-socket tests exercise independent reader/writer progress,
interrupting an idle read, a bounded write while the owned peer does not consume,
and interruption during binding. Real TCP/SQLite record tests cover a stalled reply
on direct and opaque raw-relay paths, interruption failure with every port refusing
while the RPC is still blocked, repeated close, stale epoch binding, an old handle
versus a replacement connection, and a commit owner that completes after close.
Controlled mutex-order reversal must make the stalled-reply regression fail;
restoring the exact final source must pass. All data and sockets are owned benign
fixtures; no production authentication or exploitation proof is claimed.

The strict `--require-complete` gate still exits 2. Remaining completion work is:

- One bounded reader dispatcher, persistent advisory hints/coalescing and authorized
  warm follow, including notification-to-applied/idle measurements and lost-hint fallback.
- Production secure direct/relay channel authentication and richer capability/status/
  head metadata, without reusable stale grants or deletion-privacy changes.
- Catalogue, tail/older history and artifact session adapters, matching large catalogue
  and multi-stream resume workloads.
- Multiplexed priority/credit, wire cancel versus commit ownership, urgent traffic
  under bulk, and controlled outage/loss qualification.

#54 remains open. The Nessa integration issue #1378 remains separate.
