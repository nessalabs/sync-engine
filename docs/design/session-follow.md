# Persistent advisory follow over one bound record session

**Status: opt-in one-scope production adapter, with owned TCP/SQLite fixture
qualification.** This is part of #54, not its completion. The host supplies the
already authenticated duplex channel, principal-bound current policy, finite I/O
and callback deadlines, and a lossy commit/policy wake source. The fixture's
plaintext loopback secret is not deployed authentication or channel encryption.

## Opening and authority

`connect_follow` sends explicit `FollowOpen` (NSC1 kind 7), using Open's bounded
fields and per-operation authorization mode. Support must be known by the host;
there is no automatic retry/downgrade. `serve_follow_record_session` supports the
old Hello/Open paths without notifications. The old `serve_record_session` refuses
FollowOpen. Welcome and Bound retain the existing complete identity/limit checks.

The server captures the host signal version before its initial authorized head
read; after Bound is flushed it starts the signal worker with that cursor. A
commit between head capture and subscription activation therefore remains pending.
The client creates one physical reader before opening, but exposes no ports or
hints until the normal lifecycle accepts Bound. Setup uses one ownership rule:
once it decodes an on-time complete correlated Bound or Refused, the reader hands
that result to the connecting owner and stops reading after publishing it in the
reply slot. Ownership includes the bounded reader-local frame while an earlier
Welcome still occupies that slot. The owner alone performs full lifecycle validation. A validated refusal returns its exact typed error and closes;
a validated Bound publishes immutable identity and the close fence under the state
lock before releasing the reader. Bound is passive metadata, never authority for a
read. The ordinary authorize/read path remains mandatory.

There is no pre-activation hint buffer, deferred EOF state or setup-error priority
exception. Frames after Bound, including terminal hints and EOF, remain on the
channel until activation. This keeps complete result validation separate from
incomplete network reply failure.

| Setup event | Owner and precedence |
| --- | --- |
| Welcome only, then missing/partial final result or EOF | No completed outcome exists; transport/deadline failure is unavailable. Unknown/malformed Welcome remains Protocol. |
| Complete correlated Refused queued, then peer closes | Reader stays parked; connecting owner validates correlation/reason and returns the exact refusal. EOF cannot erase it. No ports exist. |
| Complete Bound queued, then EOF/hint already in the channel | Reader stays parked until full Bound validation and identity publication. Invalid scope or zero handle/generation fails Protocol and closes without reading more. |
| Validated Bound followed by normal EOF | Setup supplied passive binding only; channel becomes terminal. Future operations are unavailable; no cache progress or authority is created. |
| Validated Bound followed by terminal hint then EOF | Reader decodes the exact bound hint first and preserves its typed terminal outcome; EOF cannot replace the first terminal reason. |
| Unsolicited/wrong-correlated/unknown setup reply or pre-Bound hint | Protocol failure. No generic preservation of arbitrary frames or late grants. |
| Explicit close/drop while a final result is parked | Close wins cancellation, wakes the handoff even if physical interruption fails, and supplies no ports. |

The fixed reply deadline bounds waiting for frame arrival. Once a complete setup
outcome is decoded on time, neither local queue publication nor owner validation
uses the network-arrival deadline to overwrite it; the normal finite connecting
owner must finish or close.

NSR1 kind 64 is one unsolicited advisory frame bound to the exact selected session,
first binding request, handle and generation. Success contains exactly one u64
head. Known refusals carry no head/payload; unknown codes, invalid policy responses,
wrong context, trailing or fragmented hints terminate. A hint grants no access,
loads no cache, and never calls apply or advances a checkpoint. Scope identity
remains immutable for the lifetime of this connection.

Before each emitted hint the server checks current policy, acquires a finite
source and reads its head, releases the source, acquires the frame writer, and
checks policy again. No policy/source mutex or source handle is held across
network I/O. Every staged RPC also rechecks policy after acquiring that same
writer; a refusal replaces it with a metadata-free terminal hint before any staged
record bytes. The writer is held for a complete frame and flush. A terminal refusal
sets the stop fence before releasing that writer, so a queued RPC frame cannot
follow it. This is a send admission check, not global atomic revocation versus a
previously delivered DTO or local commit.

## Bounded ownership and lifecycle

The original serialized record owner still admits one RPC and retains all exact
request echoes, canonical record validation and per-operation authorization.
A separate sole reader handles the physical stream. It retains at most one reply
frame slot, one read frame, the consumer's current frame and one coalesced advisory
head (all frames bounded by the negotiated 2–8 KiB control budget). Fragmented
records remain bounded by their existing response/record ceilings. There is no
unbounded notification queue, general multiplexing, wire Cancel or credit system.

| State/event | Result |
| --- | --- |
| Connecting / Welcome then exact Bound | Activate one immutable scope and subscription. Partial setup never supplies ports. |
| Ready / valid hint | Merge the maximum advisory head into one pending slot; wake the host. |
| Active finite pass / more hints | Coalesce; finish the captured pass, then use the existing fresh authorized head recheck before waiting. |
| Idle first-byte read timeout | Keep listening; it is not EOF. A partial header/body timeout is terminal. Nonblocking WouldBlock retries are paced. |
| Admitted RPC / continuous hints or fragments | Its single fixed reply deadline does not reset. Replies must match the admitted kind and exact correlation, with contiguous fragments. Unsolicited/duplicate replies terminate. |
| Reply slot full / close | A permanent admission fence, condition wake and physical interrupt release both producer and consumer. |
| Denial or identity-change hint | Preserve Denied/IdentityChanged on the hint receiver and all ports; no payload or cached-state mutation. |
| Transport loss, unavailable source/policy, or invalid protocol | Terminal typed failure. Cached durable progress remains unchanged by the dispatcher. |
| Hint timeout or lost notification | Host runs the same authorized catch-up path at its chosen occasional fallback cadence. |
| Reconnect | Host explicitly creates a new channel/lifecycle, reauthorizes, reloads saved progress and catches up before waiting. Old ports stay terminal. |
| Close races completed DTO or atomic apply | Already returned data/apply may finish; close does not roll back a commit. |

`FollowSession::close` interrupts outside the record mutex and joins its reader
after accepted interruption. Interrupt failure returns promptly with the admission
fence retained; the host owns retry/disposal/deadline cleanup. Drop requests
interruption without joining. Setup/server cleanup also avoids waiting on a failed
interrupt; detached cleanup remains bounded by the host's configured I/O/callback
deadlines. Successful physical interruption cannot abort arbitrary host callbacks,
so those callbacks must return within their host-owned bounds. There is no hidden
reconnect or reuse of stale authority.

The host composes: subscribe; initial authorized catch-up; finish finite pass;
perform the deliberate fresh head recheck; consume a pending hint or wait until its
fallback deadline; repeat authorized catch-up. App-open, foreground and reconnect
also trigger catch-up. `HintSignal::notify` is called only after actual source
commit and on policy invalidation; repeated same/lower heads are deduplicated after
fresh policy validation. Loss is expected and fallback is required.

## Runnable evidence and measurement limits

```sh
cargo test --locked --all-features --test session_follow
python3 scripts/verify-session-follow.py
python3 scripts/verify-session-follow.py --profiles --repetitions 2 \
  --output session-follow.json --markdown session-follow.md
```

The lab uses the actual follow and existing polling adapters over real TCP,
directly and through an opaque raw-TCP relay. Both use the same source-generated
seven-record/4,126-payload-byte batches, first-page path, two-second wait/fallback
and SQLite cache. It asserts exact ordered records/checkpoints and byte/request
accounting, then inspects a reopened cache in a fresh process. It includes cold,
warm, reconnect, idle fallback, one deliberately dropped hint, and two distinct
coalesced updates; the latter gates reception between commits and is correctness
evidence, not a throughput benchmark. Sixteen repeated wake signals for the
same head must produce one deduplicated hint.

Producer control uses a separate unshaped owned-fixture connection and is excluded
from data-plane bytes/time. Counters include authentication and delivered protocol
bytes, exclude dropped hint bytes, and exclude TCP/IP/TLS overhead. Hints count as
one-way bytes, not invented RPC exchanges. Short workload sums cold, warm and
reconnect per run, excluding idle, producer control, cache inspection and close
cleanup. Phase elapsed ends after the post-pass head recheck. Commit-to-durable and
notification-to-durable end earlier, immediately after actual atomic SQLite apply;
notification starts at the source's hint-write call. Those timestamps use one
host wall clock, not a distributed-clock guarantee. Polling/fallback has no
notification latency. Min/median/max evidence is noisy debug-build timing, with
no performance threshold in CI. Profiles pace 256-byte chunks at 32 kbit/s with
0.8s RTT and 64 kbit/s with 1.5s RTT; this is not a general loss/outage model.

Raw measurements and paired before/after phase and short-workload tables belong
in the PR body. The general session E2E completion gate remains false: deployed
secure direct/relay channels, catalogue/history/artifact routes, large and
multi-stream resume, capability/status metadata, multiplexed urgent scheduling,
credit, wire cancellation versus commit, and broad outage/loss qualification are
still open. The source host's origin epoch fencing remains separate #50 work.

[Raw paired measurements](../verification/session-follow-e2e.json) retain all
24 runs and measured-source fingerprints. [Qualification and reverted-regression
evidence](../verification/session-follow-reversion.json) records the 29 gates,
eight deliberately reverted guards and clean restored tree.
