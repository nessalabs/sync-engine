# Explicit per-scope record freshness (#52)

One `RecordFreshness` owns one full Scope, one finite Pass, a coalesced check bit,
retry deadline and process-local observation times. The host injects a monotonic
`Clock` and bounded `RetryJitter`, serializes calls, supplies current authorization
and capability-aware source/store ports, and schedules `drive` at `retry_at`.
There is no thread, timer, socket or retry loop inside this owner. Existing bare
follower calls and example fixed-sleep loops are unchanged; a host using this
owner must not run a competing follower for that scope. There was no reusable
jittered retry owner in the implementation before this slice.

| Input/state | Result |
| --- | --- |
| Never + cached or fresh-intent read | Read local data; no new check. Explicit, Hint and Reconnect queue a check. |
| Always + fresh-intent read, idle | Check current authority, reload checkpoint and admit a fresh head before the local read. The returned cache may still be behind that head. |
| Always + fresh-intent read, active pass/backoff/stopped/uncertain | Typed deferral/refusal; do not call the local reader or start another pass. |
| Within(TTL) + fresh-intent read | Return local data immediately. Queue one check if unknown, failed or age strictly greater than TTL. At the exact TTL boundary reuse the observation. Never call network/authorization ports on this read path. |
| Cached-intent read | Return local data and honest observation metadata; no policy-triggered check. |
| Explicit/Hint/Reconnect | Set one pending bit; retain current target and retry deadline. Hints carry no progress or authority. |
| Drive idle with pending check | Authorized capability admission; head-only observation does not apply or advance a checkpoint. |
| Drive active pass | One canonical follower step and atomic store apply. Stamp apply time only after confirmed successful advancing commit. |
| Pass complete | Mandatory fresh head recheck, regardless of policy/TTL or pending hints. If advanced, start the next finite pass. |
| Temporary unavailable/unverifiable or definitely failed store operation | Keep safe pass/check state; bounded exponential equal-jitter backoff, one owner. No work before deadline. |
| Uncertain or competing commit | Discard pass; require explicit `resume` and authorized durable reload before further work. Clear unknown latest apply time/position. |
| Other refusal | Stop automatic work; explicit host recovery via `resume` after handling the cause. No cache deletion or implicit scope reset. |
| Scope reset | Drop observations/pass/retries; no durable mutation. New owner/scope starts passive with unknown times. |
| Clock goes backward | Invalidate timestamps, increment clock generation, rebase a pending retry without escaping backoff; preserve the finite pass. No automatic Never check. |

`last_applied_at` and `last_head_checked_at` are independent `Option<Duration>`
values in this owner's injected clock generation. They are not wall-clock dates,
not persisted and not reconstructed from a checkpoint after restart. `None` means
unknown, not zero or stale-at-startup. A failed check retains the previous successful
head timestamp but sets `check_failed`; cache availability and bounded freshness
are separate. `observed_head` is only the last admitted head, not proof of current
source state. `position` is known only after an authorized reload/confirmed apply.
All metadata describes this full scope. Clock overflow stops work explicitly.

The local reader callback is a host-owned **local-only** projection. Cached reads
must obey the host's existing cache visibility rules; freshness supplies no grant
and performs no network authority check for a cached read. Always can block on its
explicit fresh-intent head read; Within never does. Hosts wanting fully caught-up
reads must drive through the mandatory recheck and inspect returned position/head;
head validation alone is not a payload synchronization promise.

This first integration covers record follower/store ports, native SQLite and a
deterministic owned fixture. Catalogue, history, artifact and transport-specific
wake wiring remain explicit host handoffs. Existing transport/follow adapters do
not silently acquire this policy. No network latency or production scheduling
claim follows from a fake-clock measurement.

## Reproduction and measured boundary

Run `python3 scripts/verify-freshness.py` for deterministic policy contracts and
SQLite result-loss/reopen correctness; use `--repetitions 5` for repeated timings.
CI runs the same command. The [receipts](../verification/freshness.json) include
exact commands, source/harness SHA256 fingerprints and ten isolated regression
sensitivity checks with the final tree restored.

Matching debug-build native SQLite workload: cold seed seven ordered records in
four bounded pages, then 100 useful local reads of the unchanged seven rows.
Every path uses two cold head checks, including the mandatory post-pass recheck.
The fake clock advances 1 ms per read; Within expires only above 10 ms. The table
excludes cold setup and reports real `Instant` elapsed read/drive work over five
runs in fixed policy order. It does not simulate network latency.

| Read policy | Useful reads | Additional head checks | Additional pages | Elapsed ms median (min–max) |
| --- | ---: | ---: | ---: | ---: |
| Existing explicit every-read follower baseline | 100 | 100 | 0 | 21.861 (20.463–23.857) |
| Always owner | 100 | 100 | 0 | 21.352 (20.451–21.934) |
| Within 10 ms owner | 100 | 9 | 0 | 18.873 (16.231–19.935) |
| Never owner | 100 | 0 | 0 | 16.637 (16.621–21.998) |

All paths reopen with the exact seven ordered records/checkpoint and unknown
restart timestamps. Within removes 91 of 100 redundant stable-read checks in this
schedule; this count is deterministic, while local timings are noisy. The separate
real SQLite lost-result fixture commits two rows, loses the result, refuses replay,
reauthorizes/reloads after reopen, and finishes at seven rows in four total pages.
No wire format changes here; network exchanges, bytes and slow-link timings are
unmeasured for freshness policy wiring and are not inferred from native call counts.
