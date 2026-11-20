# Single-connection capability follow prerequisite (#52)

One existing FollowSession owns the reader, RPC/reply slots, coalesced hint slot,
physical interruption and close fence. `capability_parts` explicitly selects the
existing version1 capability codec on that same connection; it never synthesizes
SourceHead from a hint/legacy head or opens another session. Host peer support is
required; a refusal never triggers legacy retry.

`serve_capability_follow_record_session` supplies the existing coherent RecordReadView
handler to the existing bound server and follow writer admission. It refuses old
Head/Page/OpenPage data operations. The unchanged legacy server refuses capability
data operations. Hints remain bare advisory heads; every actual capability read
validates authoritative metadata and current policy. Typed incompatibility is an
ordinary correlated status8 response with the canonical complete bounded reason;
it is not a terminal hint. A complete canonical correlated refusal owns its
reader-local/queued frame until the RPC consumer receives the entire bounded frame
in its existing local Cursor. Network EOF cannot erase it or reapply an expired
arrival deadline. The reader then resumes, retaining subsequent terminal-hint
precedence. Explicit close still cancels the handoff. No deferred EOF or extra
reply slot is introduced. Unknown statuses, wrong kinds, partial reasons and bad
correlation remain protocol failures. Terminal hint refusal precedence is retained.

| Event | Owner and result |
| --- | --- |
| Capability head/page | Existing serialized RPC and codec; fresh server policy/read view and metadata admission. |
| Concurrent/coalesced hint | Sole reader merges one advisory slot; no progress or grant. |
| Typed incompatibility | Exact correlated capability response; no data read or durable progress. |
| Old data on capability endpoint | Typed InvalidRequest, no downgrade. |
| Hint denial/identity/close | Existing terminal fence/interrupt semantics on authorizer and capability source. |
| Complete page DTO then apply | Existing canonical follower validation and atomic store; close does not undo commit. |

The lab adds explicit `freshness` mode next to legacy push. One RecordFreshness
owner per live connection replaces its catch-up loop: request one explicit wake,
drive the finite pass and mandatory fresh final recheck through the capability
ports. No new scheduler/retry/checkpoint owner is added. Reconnect creates new
ports/owner and reloads actual SQLite progress, with unknown process-local times.
The host-chosen lab fallback explicitly forces work; this comparison does not
claim policy-triggered Never/Always/Within cached-read qualification.

Run `python3 scripts/verify-session-follow.py --capability-follow` for actual TCP,
opaque relay, grouped hints, missed-hint fallback and reopened exact-cache evidence.
Add `--profiles --repetitions 2` for shaped measurements. The plaintext fixture
secret is not production channel authentication. Control-plane producer traffic
and TCP/IP/TLS overhead remain excluded as in the existing lab. Phase elapsed
ends after the mandatory recheck; notification/wake-to-durable stops at real apply.

This prerequisite keeps #52 open for the next qualification PR: matched useful
cached reads across Never/Always/Within, independent observation times, inline
Always check/Within local-read timings and failed-check/backoff fallback on shaped
links. Broader phone/host/catalogue routes and secure-channel composition remain
outside this record slice and do not become #52 promises.

## Qualified measurements

[Receipts](../verification/capability-follow.json) retain all 38 passing commands,
eight regression-sensitivity failures/restored tests, source fingerprints, clean
round2 review and raw measurements. Narrow refusal contracts run with
`cargo test --locked --all-features --test session_capability_follow` and
`cargo test --locked --all-features --lib capability_refusal`.

Baseline is legacy push; current is capability follow driven by RecordFreshness
with explicit wakes. All paths reopen with 30 exact ordered records/checkpoint,
zero duplicates and 16,513 payload bytes. Both real data passes retain two head
captures; an already caught-up head-only check performs no page and needs no
post-apply recheck. The existing client head counter includes open-target capture;
capability page responses also carry metadata without another head RPC. These
counters do not count every server-side snapshot query or passive binding head.

Short session sums cold, warm and reconnect: 19→22 exchanges and 17,400→18,201
bytes. Each actual update costs one extra exchange and 267 bytes on this path;
empty idle check saves three exchanges and 523 bytes. No latency gain is claimed.
Unshaped timings are median [min–max] over two runs; shaped values are one
observation per mode/path. Idle elapsed includes the fixture's deliberate 2 s wait.
The producer control connection and TCP/IP/TLS overhead remain excluded.

| Phase | Head/open captures, baseline→current | Exchanges | Protocol bytes | Total bytes | Payload bytes, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| cold_connect | 2→2 | 7→8 | 1700→1967 | 5826→6093 | 4126 |
| warm_update | 2→2 | 5→6 | 1566→1833 | 5692→5959 | 4126 |
| reconnect | 2→2 | 7→8 | 1756→2023 | 5882→6149 | 4126 |
| idle_fallback | 2→1 | 5→2 | 877→354 | 877→354 | 0 |
| lost_hint_fallback | 2→2 | 5→6 | 1510→1777 | 5636→5903 | 4126 |
| coalesced_updates | 2→2 | 5→6 | 1169→1436 | 1178→1445 | 9 |

| Path/profile | Cold ms, baseline→current | Warm ms | Reconnect ms | Short-session ms | n/mode |
| --- | --- | --- | --- | --- | ---: |
| direct/unshaped | 7.364 [6.571–8.157] → 5.859 [5.643–6.076] | 6.177 [5.857–6.496] → 5.737 [5.173–6.301] | 6.968 [6.362–7.574] → 5.370 [4.658–6.083] | 20.509 [19.429–21.588] → 16.967 [15.907–18.027] | 2 |
| direct/32 kbit/s, 0.8 s RTT | 7178.081 → 8045.777 | 5653.894 → 6462.420 | 7243.898 → 8048.552 | 20075.873 → 22556.749 | 1 |
| direct/64 kbit/s, 1.5 s RTT | 11847.762 → 13383.609 | 8899.748 → 10390.395 | 11882.537 → 13381.412 | 32630.047 → 37155.416 | 1 |
| relay/unshaped | 7.054 [6.719–7.389] → 7.175 [7.081–7.270] | 5.069 [4.989–5.149] → 6.759 [5.544–7.974] | 6.012 [5.917–6.108] → 6.577 [5.943–7.212] | 18.136 [17.625–18.646] → 20.512 [19.837–21.187] | 2 |
| relay/32 kbit/s, 0.8 s RTT | 7183.810 → 8061.452 | 5665.543 → 6481.470 | 7244.306 → 8044.877 | 20093.659 → 22587.799 | 1 |
| relay/64 kbit/s, 1.5 s RTT | 11857.597 → 13384.749 | 8879.056 → 10388.820 | 11904.490 → 13410.313 | 32641.143 → 37183.882 | 1 |

| Path/profile | Warm wake→durable ms, baseline→current | Warm notification→durable ms |
| --- | --- | --- |
| direct/unshaped | 3.545 [3.083–4.007] → 3.293 [2.618–3.968] | 3.744 [3.244–4.243] → 3.755 [2.827–4.682] |
| direct/32 kbit/s, 0.8 s RTT | 3624.033 → 4428.106 | 4050.572 → 4859.201 |
| direct/64 kbit/s, 1.5 s RTT | 5125.106 → 6622.052 | 5887.127 → 7387.512 |
| relay/unshaped | 3.053 [2.823–3.284] → 4.005 [3.252–4.757] | 3.292 [3.042–3.542] → 4.210 [3.480–4.941] |
| relay/32 kbit/s, 0.8 s RTT | 3635.522 → 4450.820 | 4060.748 → 4875.841 |
| relay/64 kbit/s, 1.5 s RTT | 5116.586 → 6622.054 | 5873.881 → 7385.366 |
