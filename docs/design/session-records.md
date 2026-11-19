# Serialized session record reads (#54)

This milestone implements `RecordSession` and `serve_record_session` behind the
optional `transport` feature. It connects existing `ScopeAuthorizer` and
`RecordSource` ports to one bound, serialized duplex stream. The unchanged
application validates pages and owns durable apply through `ReplicaStore`.
It is a real data path, with an intentionally small scope: one connection, one
scope and one synchronous operation at a time. Legacy loopback remains available.
Issue #54 remains open.

## Host and ownership contract

The host authenticates the channel **before** invoking the server, supplies a
current policy port tied to that authenticated principal, selects the source from
the authorized exact scope, enforces the source-owned `MAX_RECORD_BYTES` append
ceiling, and sets finite I/O deadlines. The library opens no listener and implements
no authentication service. The runnable TCP host is an IPv4-loopback-only plaintext
token fixture; neither it nor the raw forwarding hop qualifies production channel
security, device pairing, relay authentication or encryption.

The client and its two port handles share one mutex-owned connection/lifecycle.
Every part keeps that owner/channel alive; dropping one handle does not cancel
the other parts. An RPC holds this owner until a complete reply or refusal. No background read task
can relabel old replies onto a replacement owner. `close` is serialized behind an
in-flight operation and cannot preempt blocked I/O; deadlines remain necessary.
Dropping the held generic channel does not guarantee physical shutdown of a
borrowed or externally shared transport; its host must dispose it after errors or
close. No retry, reconnect, scope reset, wire cancellation or commit acknowledgment occurs
implicitly. A replacement connection requires an explicit new `RecordSession`.

Every application authorization call sends a fresh remote policy request through
the existing local lifecycle fence. The server independently invokes current policy
before acquiring any source and before every outgoing response fragment or refusal.
Bind checks current authority before opening the source and again before exposing
its identity/head. Every acquired source handle drops before response I/O. Earlier
Allowed/Bound metadata is never a reusable grant. This is not an atomic global
policy/store transaction or instantaneous revocation of bytes already sent.

## Wire and states

Hello, Welcome, Bind and Bound use the [control codec](session-protocol.md).
The server selects one binding and a body limit within the retained offer. Kind 5
`Refused` carries correlation and only a canonical typed source refusal, with no
scope/head disclosure. A refusal returns no ports. Bound's head is advisory metadata;
it is not progress and does not replace the deliberate begin-pass head recheck.

Data frames are big endian: `[body_length:u32][NSR1:4][version:u16=1]
[kind:u8][status:u8][session:u64][request:u64][handle:u32][generation:u64]
[total:u32][offset:u32][payload]`. Body header is 44 bytes. The complete body is
bounded by the negotiated 2,048–8,192-byte control budget. Request IDs start after
Bind, increase by exactly one and never wrap. The first adapter uses handle and
generation 1. Replies retain all four correlation fields and use request kind OR
128. Request payloads fit one frame; replies may fragment.

| Request | Request payload | Successful reply body |
| --- | --- | --- |
| 1 Authorize | empty | Exact six-field scope, freshly authorized. |
| 2 Head | empty | Exact scope and head u64. |
| 3 Page | after, target, max records, payload bytes, record bytes: five u64s | Existing canonical loopback record-page encoding including full request echo. |

Source status codes are shared with the canonical loopback codec: 1 Denied,
2 Unavailable, 3 Pruned, 4 InvalidRequest, 5 IdentityChanged,
6 OversizedRecord. Status 0 succeeds. Data status 7 denotes invalid host-policy
response and is fatal. Unknown statuses, versions, kinds, correlations, trailing
fields and malformed totals are fatal; they never become a retryable grant.

| State/event | Outcome |
| --- | --- |
| Connecting / current compatible Hello/Welcome and exact authorized Bind | Install passive identity; ports become available. No cache is loaded or advanced. |
| Ready / exact local scope and next serialized operation | Invoke current policy; only then acquire source. |
| Local scope mismatch | IdentityChanged before channel, policy or progress access. |
| Known temporary refusal | Return typed failure, keep connection for an explicit next operation; no automatic retry. |
| Epoch/incarnation/schema mismatch or changed Allowed echo | IdentityChanged; invalidate old channel. Reconciliation/rebind is explicit and does not reset a saved checkpoint. |
| Complete correlated fragments | Decode bounded reply; only a complete validated Page reaches the application. |
| Policy changes after first fragment | Send a same-correlated metadata-free refusal; discard the partial body. No Page or commit is produced. |
| EOF, timeout, truncation, unknown/malformed response | Drop the channel. Existing durable data/checkpoint remain unchanged. Explicit reconnect resumes from saved progress. |
| Store failure before commit | Saved progress remains old; fresh pass retries the missing range. |
| Lost commit confirmation after atomic apply | Reopen actual store and resume from saved progress; do not guess or replay committed data. |
| Explicit close / drop of the final shared handle | Invalidate the local owner / release the held channel; no wire cancel or rollback of already committed work. |

Fragment offsets must be contiguous, totals fixed and progress nonempty. Every
nonfinal fragment fills the negotiated payload capacity. Reassembly is bounded by
both the 1 MiB hard page-body cap and the operation's admitted response budget
(scope/head length or page count/ID/scope/payload ceiling). Receive-body and frame-copy scratch each
use allocations of at most 8 KiB. Page buffers, decoded records and
bounded encoding scratch can coexist; this is not a single-allocation memory claim.
A record may reach the source's 512 KiB ceiling and span many fragments, including
with the minimum 2 KiB negotiated frame. Physical pages admit at most 64 records
and 512 KiB of payload; the domain/application retain their canonical validation.

## Runnable evidence and completion boundary

Run:

```sh
python3 scripts/verify-session-e2e.py
python3 scripts/verify-session-e2e.py --profiles --repetitions 2 \
  --output session-e2e.json --markdown session-e2e.md
python3 scripts/verify-session-e2e.py --require-complete
```

CI runs the first command, with deterministic correctness/counter assertions and
no elapsed-time threshold. Integration tests use real TCP and SQLite to qualify
ordered complete records, reopened caches, exact record ceiling and minimum frame
budget, no gaps or duplicate IDs from invalid backend pages, fragmented reply
revocation, truncation/reconnect, version refusal, current policy before local
progress/source access, stale scope components, and failed/uncertain commit resume.
Codec tests additionally cover malformed correlation/status/fragment budgets.

The measured workload appends the same seven-record/4,126-byte fixture for cold
connect, warm explicit catch-up and reconnect. All 21 records and IDs are inspected
again in a fresh process. Matching legacy/session workloads run through a real TCP
frame shaper, optionally with a second opaque raw-TCP relay hop. The 32 kbit/s,
0.8s RTT and 64 kbit/s, 1.5s RTT profiles delay each application frame by half RTT
per direction and throttle 256-byte chunks. They model no packet loss, IP/TLS
headers, radio power or shared-duplex contention. Report min/median/max and sample
count; elapsed times are noisy evidence. Exchanges count request/reply operations,
not individual response fragments. Protocol/total bytes include fixture channel
authentication on cold/reconnect. The timer includes connection/session/cache open
and durable catch-up, excluding producer append and verification reopen.

The legacy workload includes its one-shot subscription; this session milestone
supports explicit catch-up only. Warm catch-up is not notification-to-applied warm
follow, and no speedup or complete feature-equivalence claim follows from these
numbers. The checked-in [raw results](../verification/session-records-e2e.json) retain
commands, environment, conditions, cases and summaries.
Actual paired results must appear in the implementation PR description as well.

The strict third command intentionally exits 2 while gaps remain: production
secure direct/relay channels, persistent hints/follow, matching catalogue bootstrap,
tail/older/artifact routes, multiple scopes and streams, wire cancellation versus
commit, bounded credit/urgent-under-bulk scheduling, richer uniform status and
capability/head metadata, and controlled multi-second outage/packet-loss profiles.
Passing the record foundation does not satisfy the full #54 finish line. Downstream
Nessa adoption remains separate in nessalabs/nessa-agent#1378.
