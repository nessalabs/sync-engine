# Source-captured target with the first page (#54)

The optional `OpenRecordSource` port and `begin_pass_with_page` use case add an
explicit first-page path. Existing `RecordSource`, `begin_pass`, `step` and
`finish_pass` remain operational. This is not a cached head or a blanket fallback
that pretends two legacy RPCs were one. The session adapter carries a real new
request and the source selects its finite target at read time.

## Authority and commit ownership

The application orders `authorize -> load -> authorize -> open_page -> validate
-> apply`. The first check protects local progress/mismatch access. The second
check remains before payload acquisition even if loading progress blocks or
changes policy. Both checks use current policy; no result is retained for another
operation. The authoritative adapter checks current policy before each underlying
head/page factory/read and before every emitted fragment. No source handle or
transaction survives a network write. The channel must already be authenticated
by its host under the [record-session contract](session-records.md).

The application validates the exact Open request echo and the source-captured
head before creating the expected canonical page range. Nonempty first pages
reuse `validate_page`, including exact scope, dense positions, unique IDs and all
byte/count ceilings. The target is a finish line, never a checkpoint. Only a
successful atomic `ReplicaStore::apply` supplies the returned Pass position.
Existing CAS, immutable-ID and uncertain-commit ownership remain with the store.
No atomic policy/commit transaction or instantaneous revocation is promised;
these are the same separate boundaries as the existing step path.

The new path captures/applies at most one page. Remaining pages use normal `step`
and `finish_pass` against that captured target. The host still calls normal
`begin_pass` after completion, preserving the deliberate fresh head recheck. A
concurrent append cannot move a captured target, and is found by that later check.

| Event | Required result |
| --- | --- |
| Initial denied/unverifiable/invalid access | No local progress load or source call. |
| Policy changes while progress loads | Second check refuses before `open_page`. |
| Policy changes after source head capture | Refuse before page factory/read; do not reveal staged target/data. |
| Policy changes during fragmented reply | Typed correlated abort discards the entire partial response; no target, Page or Pass is delivered. |
| Full exact reply with target above saved position | Validate a nonempty canonical page, atomically apply it, then return a Pass with captured target and committed position. |
| Target equals saved position | Require zero records; return a complete Pass without apply. |
| Target below saved position | `SourceBehindCheckpoint`; no page read, checkpoint reset or cache wipe. |
| Wrong echo, target with records beyond it, empty page before target, gap, duplicate ID or budget overflow | Refuse; no store apply. |
| Failed/stale/uncertain apply | Return no Pass. Explicit retry reloads actual durable progress; uncertain confirmation is never guessed. |
| Source append between capture and first page | Page stays within captured target; later post-pass head recheck discovers the append. |
| Disconnect or unsupported operation | Close/fail normally; no implicit retry, legacy fallback or grant reuse. |

## Experimental data frame

Session data kind 4 is OpenPage; reply kind is 132. It uses the existing exact
session/request/handle/generation and fragment envelope. The request payload is
four u64s: `after, max_records, max_payload_bytes, max_record_bytes`. Scope comes
from the immutable bound handle. The reply uses the existing canonical page-body
layout, explicitly echoing the full scope, after, **captured target**, all three
budgets, count and records. It does not synthesize an echo from client state.
Kind/correlation distinguish this source-selected target from the existing kind-3
fixed-target page. Zero records are representable at/below head; canonical
application validation rejects zero records before a higher target.

The source validates request ceilings before acquisition. The client caps each
frame and aggregate response by the same negotiated/operation budgets as normal
pages. Numeric, vocabulary, status and correlation failures retain their existing
typed/fatal behavior. `WireCounters.head_checks` counts source-head observations,
including captures, not only separate kind-2 RPCs. These are requested pass
observations; the advisory source head read during binding is excluded. Both source-head capture and
post-pass head recheck remain visible in counters. The client classifies received
bytes, which are not committed-progress receipts.

## Measured scope and idle tradeoff

Run `python3 scripts/verify-session-e2e.py` for all four real TCP/SQLite fixture
modes. For the matching before/after weak-link comparison:

```sh
python3 scripts/verify-session-e2e.py --profiles --repetitions 2 \
  --modes session-open session-page --output first-page.json --markdown first-page.md
```

`session-open` is the previous bounded opening plus separate head/page path;
`session-page` adds source-captured first pages using the same channel opening.
Cold, warm catch-up and reconnect each add the same seven records / 4,126 bytes.
The combined short workload sums each repetition's three timed intervals before
computing statistics. A separate idle observation transfers no new records and is
excluded from that short workload. All paths retain the post-pass head recheck,
reopen the durable cache and inspect it again in a fresh process.

For a nonempty first page the new path removes one head exchange while retaining
the two application access checks. For an empty pass it still runs both checks,
so it costs one extra authorize exchange compared with a head-only begin. This is
an explicit opt-in tradeoff; timings and bytes for idle must appear separately in
the PR alongside the delivery phases and short-workload total. No safety check is
removed for latency. Measurements use the documented plaintext loopback fixture,
real opaque TCP relay hop and application-frame delay model, not deployed secure
LAN/relay authentication. Timing thresholds are not deterministic CI gates.

[Raw first-page results](../verification/session-open-page-e2e.json) retain the
measured command, measured-source fingerprints, conditions, individual cases and
statistics. Historical loopback/opening artifacts remain intact. If only the two
matched session modes are selected, no new weak-link loopback numbers are claimed.

The strict `--require-complete` command still exits 2. #54 remains open for secure
product channels, persistent hints/follow, richer statuses/capabilities/head
metadata, catalogue/history/artifact routes, multi-scope pipelines, credit/cancel/
priority scheduling, authorized-operation composition to remove separate access
exchanges, and outage/loss qualification. This slice does not implement Nessa #1378.
