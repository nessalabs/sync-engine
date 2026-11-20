# NessaSync Swift package

Build and test from the Sync Engine repository root with Swift 6:

```sh
swift test
python3 clients/swift/e2e/run-head.py
# Optional bounded stress run, up to 300 distinct TCP connections:
python3 clients/swift/e2e/run-head.py --repetitions 100
```

The library is independent of the Nessa application, SwiftUI, app storage,
network APIs and external dependencies. A downstream SwiftPM consumer can
pin a reviewed Git revision through the repository's root manifest.

## Implemented: already-bound v1 record Head

`BoundRecordHeadClient` drives one serialized kind-5 Head on the existing
experimental [NSR1 record endpoint](../../docs/design/session-records.md).
Its caller selects the explicit endpoint and owns authentication, Open/Bound
validation, absolute I/O deadlines and exclusive channel transfer. The client
does not retry or probe alternate endpoints. A successful Head is advisory
metadata, not a grant, a checkpoint or a durable synchronization operation.
No deployed Nessa gateway, Nessa iOS or phone participates in these tests.

The headless E2E uses the existing Rust `session_records` binary in its
loopback-only capability mode with a temporary SQLite source. Python owns
fixture-only authentication, Open/Bound and the process watchdog. It passes the
connected TCP socket to a Swift fixture executable without proxying Head bytes;
Swift writes and validates both operations itself. The library target contains
neither the fixture token nor fixture networking.

| Contract | Current automated evidence |
| --- | --- |
| v1 request and metadata/refusal byte layout | Swift and Rust consume the same seven capability vectors; Swift checks exact Head request bytes; Rust accepts the real Swift requests. |
| Exact session/request/handle/generation and six UTF-8 scope fields | Swift checks deterministic mismatches, including canonically equivalent but byte-distinct scopes; 16 independent sessions × 20 operations verify exact request numbering and positions. |
| Frame limits, truncation, shape and terminal ownership | Swift checks all truncated prefix lengths and rejects an oversized length before a body read. Head must fit one bounded frame; paged fragmentation is not part of Head. |
| Source/refusal and codec errors | Swift checks each canonical status and its explicit continuation or retirement. Live Rust/SQLite cases verify normal data, unknown writer-only bits and an incompatible required reader bit; the latter never downgrades. |
| Repeated real reads | Default 15 independent connections and 30 operations, with two Heads per connection; opt-in up to 300 / 600. Timings include harness and process work and carry no CI threshold. |
| Host setup | The fixture authenticates and correlates Open/Bound externally using established test-only Rust endpoints; these steps are not Swift API coverage. |

## Remaining client acceptance

No unchecked row is covered by the Head fixture. Each future slice should
have a supported pinned contract, deterministic stress tests and direct
interoperability against the Rust endpoint it uses.

| Future contract | Required E2E and stress evidence |
| --- | --- |
| Swift Open/Bound and transport lifecycle | Version/capability negotiation; exact binding and ownership; timeout, refusal, dropped link, old-session results and explicit replacement; bounded requests. |
| Open-page, record pages and apply | New/empty/bounded ranges, contiguous fragmented framing, exact echoed limits, partial reply discarded; ordered data and checkpoint committed atomically, and fresh authorized head after apply. |
| Resume, reopen, gaps, reset | Verified durable checkpoint after interruption and restart; no duplicate or missing records; typed gap/pruned/identity change, exact reset scope and no stale application. |
| Tail/history and catalogue | Bounded tail/history ordering, lazy history, catalogue manifest pagination and horizon-aware recovery across disconnects, including previously applied/cached ranges. |
| Artifact reads | Bounded chunks, hash and size verification, partial transfer/reopen, typed unavailability or version change without partial publication. |
| Priority, cancellation, live hints | Urgent request while bulk work is active; queue and in-flight bounds; cancellation before commit vs. completion; hints never advance committed state. |
| Access and production transport | Current authorization through read and apply; metadata-free refusal and no resumed access after revocation; authenticated encrypted direct and relay paths, latency/bytes and outage recovery. |

Production LAN/relay rollout is separate from the independent client tests
and from downstream Nessa iOS wiring.
