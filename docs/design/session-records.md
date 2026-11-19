# Serialized session record reads (#54)

This milestone implements `RecordSession` and `serve_record_session` behind the
optional `transport` feature. It connects existing `ScopeAuthorizer` and
`RecordSource` ports to one bound, serialized duplex stream. The unchanged
application validates pages and owns durable apply through `ReplicaStore`.
It is a real data path, with an intentionally small scope: one connection, one
scope and one synchronous operation at a time. Legacy loopback remains available.
Issue #54 remains open. The optional [source-captured first-page path](session-open-page.md)
adds a real bounded request that captures its target with data; it retains both
application access checks and the post-pass head recheck. Its empty-pass extra
authorize cost is measured separately.

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

## Opt-in bounded opening

`RecordSession::connect_open` sends one kind-6 Open containing the offered limits,
first request number and exact retained scope. Its maximum body is 796 bytes,
below the mandatory 2 KiB admission budget, even with all six maximum-length IDs.
The host must already authenticate the channel and know its peer supports Open;
the existing `connect` Hello/Bind path remains operational. There is no implicit
compatibility probe, retry or fallback after refusal, malformed data or EOF.

The server selects a fresh namespace and limit intersection, then emits the
existing Welcome and Bound/Refused frames. It authorizes before source acquisition
and checks again before Bound, exactly as for separate Hello/Bind. The client
validates Welcome against its retained offer, installs its first pending Bind in
the same lifecycle owner, then correlates the queued binding reply against the
retained scope, first request and selected session. Ports exist only after both
replies pass. No advisory head is reused, no application ordering changes, and all
later current-policy and head checks remain. This reduces metadata setup only;
it does not satisfy the separate open-target/head or no-authorize-exchange goals.

| Opening state/event | Decision |
| --- | --- |
| Connecting / caller selects Open with known peer support | Send bounded retained limits/request/scope; remain unbound. |
| Current Welcome within offer | Select namespace/limits; create the first local pending Bind without a second network request. |
| Exact current Bound | Return ports; advisory head still cannot advance progress. |
| Correlated Refused | Return typed refusal and no ports; no automatic mode fallback. |
| Missing/truncated/duplicate Welcome, wrong session/request/scope or unsupported reply | Refuse and dispose the channel; no progress load or grant. |
| Duplicate Bound after successful opening | It cannot be a later authorization response; close before application progress access. |
| Policy changes before source access or after initial head acquisition | Refuse before factory / before Bound respectively; no source metadata is exposed on refusal. |

Open is one logical opening exchange with two reply frames. The accounting counts
both replies' bytes. The application-frame delay model charges each reply frame,
so a saved logical exchange is **not** a measured full RTT saving. Warm explicit
catch-up uses the unchanged protocol and must retain identical counters. The
combined short workload measures each run's cold open + warm catch-up + reconnect
intervals before computing statistics. It excludes inter-phase append/verification
and is not wall-clock session lifetime; it prevents warm-only results from hiding
setup cost. The existing session mode is the matched before case and `session-open`
is the after case, with loopback retained as an additional reference.

## Wire and states

Hello, Open, Welcome, Bind and Bound use the [control codec](session-protocol.md).
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

The measured workload compares loopback, separate Hello/Bind session, and opt-in
Open session. It appends the same seven-record/4,126-byte fixture for cold
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
numbers. The previous [record baseline](../verification/session-records-e2e.json) and current
[opening comparison](../verification/session-open-e2e.json) retain
commands, environment, conditions, cases and summaries.
Actual paired results must appear in the implementation PR description as well.

The strict third command intentionally exits 2 while gaps remain: production
secure direct/relay channels, persistent hints/follow, matching catalogue bootstrap,
tail/older/artifact routes, multiple scopes and streams, wire cancellation versus
commit, bounded credit/urgent-under-bulk scheduling, richer uniform status and
capability/head metadata, and controlled multi-second outage/packet-loss profiles.
Passing the record foundation does not satisfy the full #54 finish line. Downstream
Nessa adoption remains separate in nessalabs/nessa-agent#1378.
