# NessaSync Swift package

Build and test from the Sync Engine repository root with Swift 6:

```sh
swift test
python3 clients/swift/e2e/run-head.py
# Optional bounded stress run, up to 800 distinct TCP connections:
python3 clients/swift/e2e/run-head.py --repetitions 100
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
| Host channel trust | Fixture token is loopback-only; dialing, authentication/TLS, peer support selection and deadlines remain host-owned. |

## Verified TLS fixture (macOS)

Run from the repository root on macOS 15+ with Swift 6, Rust and OpenSSL 3:

```sh
sh clients/swift/e2e/run-tls.sh
```

The Apple Network/Security channel is private to the executable. The Rust
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

## Remaining client acceptance

No unchecked row is covered by the Head/Page fixture. Each future slice should
have a supported pinned contract, deterministic stress tests and direct
interoperability against the Rust endpoint it uses.

| Future contract | Required E2E and stress evidence |
| --- | --- |
| Production transport lifecycle | Authenticate the actual gateway and relay; test timeouts, old-session results, explicit replacement and reconnect under real outages. |
| Open-page and production apply | New/empty/bounded open targets, actual store compares and commits ordered records and checkpoint atomically, and a fresh authorized Head after apply. |
| Product resume, gaps, reset | Reopen a production store after uncertain commit; no duplicate or missing records; typed gap/pruned/identity change, exact reset scope and no stale application. |
| Tail/history and catalogue | Bounded tail/history ordering, lazy history, catalogue manifest pagination and horizon-aware recovery across disconnects, including previously applied/cached ranges. |
| Artifact reads | Bounded chunks, hash and size verification, partial transfer/reopen, typed unavailability or version change without partial publication. |
| Priority, cancellation, live hints | Urgent request while bulk work is active; queue and in-flight bounds; cancellation before commit vs. completion; hints never advance committed state. |
| Access and production transport | Current authorization through read and apply; metadata-free refusal and no resumed access after revocation; authenticated encrypted direct and relay paths, latency/bytes and outage recovery. |

Production LAN/relay rollout is separate from the independent client tests
and from downstream Nessa iOS wiring.
