# Live record freshness qualification (#52)

One per-full-scope RecordFreshness outlives replaceable follow connections and
owns the only pass, pending bit, clock, jitter/backoff and observation times.
The existing SQLite store remains the sole durable progress owner.

The host supplies stable authorizer/source adapters over one optional connection.
Only the authorizer may lazily dial; on success it delegates current authorization.
The source never dials, fabricates metadata or uses a legacy path. A failed dial
is Unverifiable, entering existing backoff. Terminal outcomes require explicit
host recovery; an uncertain commit requires explicit authorized progress reload.

Cached/Within reads invoke no adapters, including while disconnected. Always
fresh intent checks first or refuses; a separate Cached intent is explicit.
One serialized host event pump drives pending work and the owner's retry deadline.
Hints/reconnect queue a check but never escape backoff or interrupt a finite pass.
Successful replacement binding is passive metadata, never a fresh observation.
Same-scope confirmed passes can finish over new ports, then freshly recheck head.
No new wire operation, timer, queue, retry owner or checkpoint is introduced.

Deterministic logical-clock contracts and real-clock timing runs are separate.
All use the real capability-follow adapter and actual SQLite source/cache over
owned direct or opaque-relay TCP fixtures. Fixture credentials are plaintext local
secrets, not deployed channel security or a product authorization implementation.

The concrete adapters in `examples/session_freshness/host.rs` retain no Allowed
result. An unavailable dial becomes Unverifiable through the authorizer, entering
the application's existing backoff; source reads cannot dial even when offline.
A binding never refreshes a head timestamp. The next authorized read must succeed.
Cache projection follows the fixture host's local visibility rules; freshness
neither grants visibility nor erases cached data on remote refusal.

`python3 scripts/verify-session-freshness.py` runs logical-clock contract episodes
for each policy on both paths, then real-clock measurements. `--profiles` adds
32 kbit/s/800ms RTT and 64 kbit/s/1.5s RTT. `--repetitions 2` repeats measurements.
Each stable episode has the same seven-record fixture and six fresh-intent reads;
250ms think delays precede reads 3/6 with 200ms TTL. Local projection, the complete
read call, inline Always check path, deferred checks, actual SQLite apply and
reconnect are separately timed. Inline check-path time subtracts local projection
from the read call and includes application bookkeeping. Think delays are excluded
from read/check times and included in the separately named stable elapsed time.

Logical-clock contracts assert no network inside Within reads, exact early retry
suppression and exponential deadlines across replacement/dial failure, stale
cache7 while source8 is unavailable, head8 observation before durable apply, and
fixed target14 finishing over new ports before target16. Repeated/grouped hints
are actual post-commit signals. A definitely failed apply preserves progress;
a lost commit result requires explicit reload, with no replay or invented apply
time. Denial/identity/protocol outcomes stop before progress loading and do not
automatically resume. New scope and process restart have unknown observations.

Observer counters distinguish attempted data RPCs, completed normal/error replies,
requests ended by terminal hints (including protocol-abort hints), and transport
unanswered requests. Client counters exclude failed-opening objects; independently
observed framed totals are primary across all attempts. Authentication preamble
and ACK are separately observed and added in total application bytes; IP/TCP/TLS
and producer-control traffic are excluded. The deliberate interruption occurs before a metadata response, at a
whole-frame boundary; this is not partial-frame/loss qualification. No new wire,
queue, timer, core state machine or checkpoint API is added. Production phone/host
integration, other resource families and wider transport gates remain separate.

A separate process reopens each final cache with a new owner and no connection;
all rows/checkpoints match and both observation times remain unknown. Cold timing
starts after opening the empty cache and seeding the source, so it includes
connection/auth/binding, catch-up/apply and the final recheck, but excludes cache
creation and source production. It is not the earlier lab's full startup timer.

Real-clock runs also interrupt one head response after the source advances from
7 to 8. They separately time the failed check, an explicit cached read of 7 with
no authorization/dial, the existing retry deadline wait, and replacement plus
recovery to 8 with a final head check. Always fresh intent refuses before its
projection callback; Within returns locally before the failed deferred check.
These are actual shaped-link timings, separate from deterministic logical-clock
assertions about exact retry boundaries.

The observer registers correlation in both directions before forwarding so a
fast reply can be attributed. A bounded condition barrier waits for all admitted
forwards to finish before final snapshots; successful bytes are published after
send, with finally cleanup on failure. Terminal scope contexts are retained even when a hint arrives
while idle; subsequently attempted requests in that context remain unanswered,
not completed. The completed/terminal-associated/transport-unanswered partition
is observed rather than assuming one RPC per terminal hint. Gated ordinary frame
contracts exercise both schedules without a network service.

For #52, the reusable policy, deterministic TTL/clock/reset contracts and native
store qualification landed in #70; the capability-aware single-follow read ports
landed in #71. This lab qualifies their composition across connection replacement,
current authorization, finite resumed passes, grouped advisory hints, existing
backoff, failed/uncertain commits and process reopen. Host scheduling and local
visibility remain host responsibilities. Catalogue/phone adoption and production
channel security remain wider #54/integration work; this record composition adds
no timer, retry or durable state owner.

## Measured receipt

The [source-fingerprinted receipt](../verification/live-freshness.json) contains all
42 final cases,39 qualification commands, nine reversion guards and independent
review. Matched Always/Never/Within costs and shaped timings are also included in
the pull request description. Reproduce with the command above; timings have no
pass/fail threshold.
