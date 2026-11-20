# NessaSync Swift package

Build and test from the Sync Engine repository root with Swift 6:

```sh
swift test
python3 clients/swift/e2e/run-head.py
# Optional serial recovery stress plus a 16-client overlap group:
python3 clients/swift/e2e/run-head.py --repetitions 100 --overlap-clients 16
```

The library is independent of the Nessa application, SwiftUI, app storage,
network APIs and external dependencies. A downstream SwiftPM consumer can
pin a reviewed Git revision through the repository's root manifest.

## Implemented: v1 Open, record Head and fixed-target Page

`BoundRecordClient.open` negotiates one exact record scope using the existing
[NSC1 v1 Open](../../docs/design/session-records.md#opt-in-bounded-opening), then
uses serialized NSR1 kind-5 Head and kind-7 fixed-target Page. Its caller selects
the explicit endpoint and owns dialing, channel authentication, absolute I/O
deadlines and exclusive channel transfer. The client
does not retry or probe alternate endpoints. A successful Head is advisory
metadata. A complete Page is a validated receipt, not a grant or committed
checkpoint. The receipt carries its byte-exact logical scope, excluding
transient channel numbers. The caller must atomically compare the receipt's
scope and previous cursor in its store, and reject IDs from earlier pages. The client rejects duplicate IDs
within its page; it does not own persisted records.
No deployed Nessa gateway, Nessa iOS or phone participates in these tests.

The headless E2E uses the existing Rust `session_records` binary in its
loopback-only capability mode with a temporary SQLite source. Python owns
fixture-only authentication and the process watchdog. It passes the connected
TCP socket to a Swift fixture executable without proxying any control or record
bytes; Swift directly negotiates Open/Bound and performs each operation. The library target contains
neither the fixture token nor fixture networking.

| Contract | Current automated evidence |
| --- | --- |
| v1 request and metadata/refusal byte layout | Swift and Rust consume the same seven capability vectors; Swift checks exact Head request bytes; Rust accepts the real Swift requests. |
| Exact session/request/handle/generation and six UTF-8 scope fields | Swift checks deterministic mismatches, including canonically equivalent but byte-distinct scopes; 16 independent sessions × 20 operations verify exact request numbering and positions. |
| Frame limits, truncation, shape and terminal ownership | Swift checks all truncated prefix lengths and rejects an oversized length before a body read. Head must fit one bounded frame; paged fragmentation is not part of Head. |
| Source/refusal and codec errors | Swift checks each canonical status and its explicit continuation or retirement. Live Rust/SQLite cases verify normal data, unknown writer-only bits and an incompatible required reader bit; the latter never downgrades. |
| Repeated real reads | Default 15 independent connections and 30 operations, with two Heads per connection; opt-in up to 300 / 600. Timings include harness and process work and carry no CI threshold. |
| Fixed-target Page | Exact request echo (scope, durable cursor, target and budgets), head coverage, nonempty dense records, byte-exact IDs, per-record and page budgets, and no trailing bytes. Complete fragmented Rust pages are decoded; truncated or refused pages return no receipt. |
| Reopen and retry (fixture only) | A Swift process atomically replaces its single fixture JSON checkpoint after records 1–2, a second closes TCP after the first real fragment, and a typed oversized refusal preserves the file. A fresh process resumes at 2 and exactly reopens records 1–7. Default 5 episodes / 20 Page operations; opt-in 100 / 400. No concurrent or power-loss durability claim. |
| Open and positive handoff | One scope and explicit per-operation mode; accept only bounded, correlated Welcome/Bound, enforce selected limits and record ceiling, ignore Bound's advisory head and use the same channel on request 2. Live Rust performs exact Open before every Head/Page. |
| Open refusal and channel lifetime | Real Rust returns typed Denied for an ungranted fixture receiver. Swift unit tests check all known refusals, invalid scope/limits/session/handle/generation, incompatible version/mode or ordering, every truncated response prefix and pre-body size rejection. No network fallback or partial owner. |
| Overlapping bound client lifetimes | Each independent Swift process reports real Bound over a test-side signal, then blocks until the parent releases the entire group. Every client completes Head and two Pages, verifying exact scope and seven exact records. Default 4 clients, 4 Heads, 8 Pages and 28 rows; opt-in 16, 16, 32 and 112. No claim about simultaneous server-side execution, production capacity or users. |
| Host channel trust | Fixture token is loopback-only; dialing, authentication/TLS, peer support selection and deadlines remain host-owned. |

## Verified TLS fixture (macOS)

Run from the repository root on macOS 26+ with Xcode 26's macOS SDK or newer,
Rust, and OpenSSL 3:

```sh
sh clients/swift/e2e/run-tls.sh
```

The separate fixture package under `clients/swift/e2e/tls` depends on this
checkout's public `NessaSync` product. It is not a root SwiftPM target; regular
Swift unit and headless tests still run on the macOS 15 / Swift 6.1 runner.
The Apple Network/Security channel is private to the newer-SDK executable. The Rust
test uses the existing TLS server and SQLite fixture; it owns short-lived
keys in a private temporary directory. Swift creates a process-owned P-256
identity without importing into the Keychain or changing system trust. It
requires TLS 1.3, the Sync Engine ALPN, standard CA/hostname validation and
the exact configured peer leaf before giving the channel to the unchanged
serialized client. No fallback or early application data is used.

| TLS fixture case | Evidence |
| --- | --- |
| Authorized exact peer | Real Swift Open, Head and Page against Rust; three exact IDs/positions, 3,000 exact payload bytes. |
| TLS accepted; wrong fixture scope | Correlated typed Open Denied; Rust policy ran, source opened zero times. |
| Known sibling with valid fixture chain and hostname but wrong exact leaf | Swift verifies the leaf mismatch, rejects it and transfers no channel to Open; Rust policy and source acquisition counts both remain zero. |

This qualifies direct loopback interoperability, not production endpoint
enrollment, device Keychain behavior, client-certificate timing, an actual LAN
or relay, or outage recovery. The public Swift package still takes an
authenticated host channel instead of supplying a default transport.

## Demand-driven Follow (independent client)

`RecordFollowClient.open` opts into the existing one-scope capability
[FollowOpen contract](../../docs/design/capability-follow.md) on an already
authenticated host channel. It uses the same serial frame reader for
`waitForHint`, `readHead`, and `readPage`. Calls cannot overlap; there is
no background task, additional reader or queued wake stream. One maximum
advisory head is coalesced while a Head/Page is active.

The host owns a single absolute deadline across each Open, hint wait or RPC,
including all read/write fragments; a hint never resets it. The existing
injected byte port cannot prove that zero bytes were consumed on an I/O error,
so even an idle wait timeout terminates this owner. The caller may explicitly
open a fresh connection and revalidate its saved cursor; there is no automatic
timeout retry, lease, fallback to ordinary Open, or background wake promise.

| Follow contract | Current automated evidence |
| --- | --- |
| Ordered exact setup | NSC1 v1 kind-7 FollowOpen reaches real Rust `serve-cap`. Welcome and Bound use the existing exact binding path; pre-Bound hints fail. |
| Advisory handoff | Only a bound request-1 hint with exact session/handle/generation can be consumed. Invalid, partial, foreign or terminal refusal frames retire the owner. Terminal refusal is retained rather than overwritten by a later empty read. |
| One bounded pump | Swift tests interleave two hints between actual-sized Page fragments, coalesce to the maximum, and still complete exactly one page. A terminal hint after the first fragment produces no receipt. |
| Headless Rust interop | The standard runner adds one real Follow TCP connection. After Swift reports Bound, the existing test producer commits one record and emits a hint. A fixture barrier verifies its checkpoint bytes unchanged after the hint; then one checked Head and one complete Page install one exact record (5 payload bytes). The optional serial/overlap workload does not multiply the Follow case. |

The fixture's loopback auth roles are not production credentials. TLS Follow,
asynchronous notification, idle timeout reuse, multi-scope
dispatch and product cache integration remain open.

## Bounded Tail snapshot

Call `RecordFollowClient.readTail` only on a known
[history-capable Follow endpoint](../../docs/design/session-history.md).
Tail is a bounded dense suffix ending at the coherent metadata watermark,
not a forward Page from a checkpoint. The requested history generation is
caller-owned and distinct from the binding generation. The receiver checks
the exact logical scope, echoed generation and ceilings, admitted metadata,
first/oldest coverage, unique ordered IDs and record/payload budgets before
returning a receipt. The caller owns atomic installation; no library read
changes a local store.

| Tail contract | Current automated evidence |
| --- | --- |
| Request, empty snapshot and coverage | Swift checks reject wrong caller scope or budgets without writing, canonical empty and bounded suffix are accepted, and mismatched coverage/echo/scope, oversized rows, duplicate IDs and trailing bytes are rejected. |
| Complete reply or no receipt | Swift tests cover correlated denial and interruption after a fragment. The standard runner reuses the actual Rust SQLite `session_history` example: two latest rows (3–4, 3,000 bytes), watermark 4, first 3, retained floor 1; a separate connection stops after the first full frame and does not install anything. |
| Repeatable test | Add `--tail-episodes 32` to the headless runner for 64 fragmented successful Tail reads (including 32 before an Older on the same connection) and 32 interrupted Tail reads. This does not measure many-user capacity or production I/O. |

## Bounded Older history

`RecordFollowClient.readOlder` uses the same history-capable Follow
connection to fetch a nonempty dense page ending immediately before the
caller’s exclusive installed history boundary. It validates the exact scope
and echoed generation, boundary and budgets; current metadata must cover
the requested range but is never returned as live progress. The caller owns
atomic installation and must compare scope, generation and coverage before
prepending, without advancing the live checkpoint.

| Older contract | Current automated evidence |
| --- | --- |
| Exact request and historical coverage | Swift tests reject wrong local scope or bounds before writing and reject mismatched echoes, insufficient head, bad retained floor, noncontiguous rows, duplicate IDs, oversized records and trailing bytes. A hint interleaved with a complete two-fragment reply stays advisory. |
| Real peer, interruption, restart | The Rust SQLite fixture supplies Tail positions 3–4 and Older positions 1–2 with 3,000 exact bytes per read; both same-connection Tail→Older and a new process after interruption end at live watermark 4 with history coverage now starting at 1. An incomplete read leaves saved Tail bytes unchanged. |
| Repeatable test | `--tail-episodes 32` runs 32 same-connection Older successes, 32 fresh-process Older successes, and 32 Older interruptions, separately from the forward and overlap numbers. |

The test installs a single-owner JSON fixture, not a product cache. Protected-epoch
history, TLS Follow, production resume, and history storage remain separate.

## Source-captured first page (OpenPage)

`BoundRecordClient.readOpenPage` requests the current compatible source
target and a bounded first page in one explicit [kind-6 operation](../../docs/design/source-head-wire.md).
The reply's admitted metadata and exact echo must agree on the captured target.
A nonempty page remains a scoped caller-owned receipt; its last verified record
is separate from the target. Already-current and source-behind are distinct
outcomes with no records to apply. A behind source never authorizes a rewind.
There is no implicit kind-7 retry or production storage.

| OpenPage contract | Current automated evidence |
| --- | --- |
| Exact bounded reply | Swift tests reject wrong local scope or budgets before writing, wrong kind/echo/scope, empty before target, nonempty at/below target, trailing bytes, and interrupted or typed-refused fragments. |
| Real peer | Existing Rust SQLite capability endpoint captures target 7 with first two records (9 payload bytes). A fixed-target Page and then a fresh Head on the same connection complete the pass (7 exact rows/4,126 bytes). Already-current, source-behind, oversized refusal and mid-fragment interruption leave fixture bytes unchanged. |
| Repeated fixed workload | `--open-page-episodes 32` repeats all five cases. Counts are records and payload bytes, not network bytes or production latency. |

## Remaining client acceptance

No unchecked row is covered by the Head/Page fixture. Each future slice should
have a supported pinned contract, deterministic stress tests and direct
interoperability against the Rust endpoint it uses.

| Future contract | Required E2E and stress evidence |
| --- | --- |
| Production transport lifecycle | Authenticate the actual gateway and relay; test timeouts, old-session results, explicit replacement and reconnect under real outages. |
| Production apply | Actual store compares and commits ordered records and checkpoint atomically, and a fresh authorized Head after apply. |
| Product resume, gaps, reset | Reopen a production store after uncertain commit; no duplicate or missing records; typed gap/pruned/identity change, exact reset scope and no stale application. |
| Tail/history and catalogue | Bounded tail/history ordering, lazy history, catalogue manifest pagination and horizon-aware recovery across disconnects, including previously applied/cached ranges. |
| Artifact reads | Bounded chunks, hash and size verification, partial transfer/reopen, typed unavailability or version change without partial publication. |
| Priority, cancellation, live hints | Urgent request while bulk work is active; queue and in-flight bounds; cancellation before commit vs. completion; hints never advance committed state. |
| Access and production transport | Current authorization through read and apply; metadata-free refusal and no resumed access after revocation; authenticated encrypted direct and relay paths, latency/bytes and outage recovery. |

Production LAN/relay rollout is separate from the independent client tests
and from downstream Nessa iOS wiring.
