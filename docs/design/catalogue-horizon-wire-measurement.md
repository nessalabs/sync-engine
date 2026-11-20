# Explicit catalogue v2 wire measurements

Issue #49 remains open. This slice reuses the existing development catalogue route and native coherent coverage/reset ports. It adds explicit operations 20/21/22 and an opt-in client, without changing v1 bytes, native v5 publication/collection/pin policy, source ownership or durable-progress ownership. The v5 owner is the separately merged #112; elapsed comparisons include that runtime difference and are not a wire-only latency attribution. The route remains plaintext with owned fixture tokens; no production channel authorization, TLS, secure relay or #54 session catalogue claim.

Measured pinned old v4 producer/client/server `8596fd3` against new v5 runtime `e29770d`. Three repetitions per direct/opaque-relay path, identical 620-entry original payloads and zero-floor workloads. The old peer is built from actual repository history, not an emulator. Normal gates, finite models and guard reversions finished before serial timing runs. Debug shared-host timing has no CI threshold or speedup claim.

| Matched phase | RPC v1 → v2 | Framed B v1 → v2 | Direct ms median (range), v1 → v2 | Relay ms median (range), v1 → v2 |
|---|---:|---:|---|---|
| bootstrap | 637 → 637 | 242,568 → 248,936 | 910.228 (854.193–918.082) → 967.742 (961.273–1138.550) | 1301.833 (1225.724–1348.409) → 1438.016 (1419.374–1675.621) |
| partial | 42 → 42 | 14,353 → 14,771 | 60.199 (58.440–65.683) → 66.864 (66.647–85.086) | 80.892 (78.892–83.102) → 106.147 (103.987–146.155) |
| restart_resume | 596 → 596 | 228,397 → 234,355 | 872.643 (842.097–930.243) → 970.160 (955.709–1011.114) | 1270.511 (1229.067–1661.222) → 1283.036 (1243.557–1316.226) |
| churn | 4 → 4 | 1,103 → 1,141 | 9.441 (8.779–11.713) → 9.534 (8.694–10.538) | 11.475 (11.022–11.540) → 11.857 (11.798–12.904) |
| unchanged_recheck | 1 → 1 | 182 → 190 | 5.090 (4.886–6.502) → 5.668 (4.408–6.009) | 6.507 (6.270–7.115) → 6.080 (5.664–6.630) |
| post_churn_recheck | 1 → 1 | 182 → 190 | 4.669 (4.238–5.323) → 4.032 (3.889–5.303) | 6.679 (5.076–10.221) → 5.388 (5.004–5.674) |

RPC counts stay unchanged. V2 adds 8 framed bytes per head exchange and 10 per manifest/value exchange for explicit horizon/read-kind evidence; it is not an exchange-reduction optimization. Manifest and opaque payload bytes are identical across matched phases. Successful bootstrap/resume/churn verifies reopened values against source. Explicit unchanged/post-churn and reset-finish checks are separate phases; finite completion is not a freshness receipt. Hosts must perform fresh admission after each finite pass before claiming caught-up state. V1 code/bytes are unchanged; visibility-only helper reuse does not reinterpret its version.

| Defensive scenario | Attempts / complete replies per run | Framed B per run | Direct ms median (range) | Relay ms median (range) |
|---|---:|---:|---|---|
| old_client_new_server_zero | 1 / 1 | 182 | 5.236 (4.628–8.352) | 10.998 (5.749–18.876) |
| equal_completed_floor | 165 / 165 | 60,308 | 288.349 (256.045–315.584) | 391.974 (374.135–394.388) |
| expired_saved-page | 1 / 1 | 296 | 4.531 (4.468–5.105) | 5.287 (4.829–5.591) |
| expired_pass | 1 / 1 | 190 | 4.767 (4.413–4.834) | 5.092 (4.565–5.667) |
| delayed_lower_head | 2 / 2 | 380 | 12.946 (12.359–18.954) | 14.855 (13.637–15.919) |
| denied_reset | 1 / 1 | 81 | 3.932 (3.401–7.310) | 4.181 (4.010–4.821) |
| old_pass_positive_refusal | 1 / 1 | 80 | 7.559 (6.536–7.617) | 6.674 (6.151–10.038) |
| old_cap-pass_positive_refusal | 1 / 1 | 90 | 6.471 (5.310–19.252) | 5.666 (5.556–11.524) |
| new_client_old_server | 1 / 0 | 85 | 3.866 (3.592–4.593) | 4.927 (4.688–4.958) |
| interrupted_payload | 3 / 2 | 958 | 8.108 (7.986–8.692) | 10.131 (8.767–37.221) |
| explicit_retry | 4 / 4 | 1,149 | 9.564 (9.501–15.506) | 12.826 (11.523–15.615) |
| value_floor_advance | 3 / 3 | 918 | 10.922 (10.510–15.390) | 12.539 (11.984–15.098) |

Equality uses durable C=620 and horizon 620 with source head 780. After one page leaves active C=620/H780, its actual durable C pin is registered at time 1/expires 10. Unforced preferred 780 stays 620 and manual collection refuses. At exact expiry 10, collect_with_retention advances h=780 and hides 159 markers; the ledger remains and manual equal-h collection still refuses. Saved-page and fresh-admission expiry preserve cache/progress/cursor/fences through fresh processes; only validated passive observation advances. The delayed-head scenario holds a genuine owned h=620 reply while another authorized process publishes/observes 780, then rejects the lower reply. Its totals include both head exchanges and its elapsed time includes producer/observer work; no recorded reply is replayed.

The actual old capability client works at zero horizon. Both old entry points refuse positive activation before data with a single terminal invalid-request reply. The new client against the actual old server sends only op 20; the unknown operation closes without a reply, remains retryable Unavailable and never probes v1. Wrong-token/epoch and unsupported-reader cases expose no descriptors/payload. Unknown writer-only requirements permit reads. Future requirement masks are ordinary owned compatibility fixtures; cutoffs and marker membership are changed only through native API.

An interrupted second value frame leaves one complete value staged and no cache/cursor mutation; explicit later retry completes correctly. The partial fragment is included in observed framed-hop bytes, separately from client complete-reply payload/protocol accounting. Attempted versus answered requests are distinct. The value-floor scenario advances coverage after one value reply, then receives typed expiry on the second; the whole page remains unapplied. Per-process duplicate counters do not claim to detect cross-process retransmission.

| Explicit reset after actual collection, including final fresh check | RPCs per run | Framed B per run | Direct ms median (range) | Relay ms median (range) |
|---|---:|---:|---|---|
| warm | 595 | 222,266 | 1014.408 (974.132–1270.461) | 1346.264 (1277.328–1681.065) |
| cold | 476 | 185,138 | 769.698 (759.172–852.985) | 1087.616 (1052.324–1434.724) |

Warm reset preserves all 620 known identity/current rows and 160 permanent receiver fences, ending at C=780 with no pending journal. Cold reset intentionally has 461 served rows (460 live plus one equality marker) and does not fabricate 159 unknown fences. Permanent source IDs remain 620 and normal later upsert of a retired ID is Fenced. Each start/step opens a fresh cache connection/process and uses the same generation-bound native journal; explicit reset requires current host/route authorization.

Validation: all 52 repository gate commands and explicit warning-free Rustdoc passed (53 recorded commands). Seven codec contracts pass, including otherwise-valid expired success, malformed 8/10, full 512 KiB plus-one boundary and exact echoes. Eight isolated semantic guard changes each fail their intended ordinary tests; restored source is clean and fingerprint-identical. Native collection/reset/origin models rerun successfully; this slice makes no new formal wire/liveness proof claim. Source/codec review was clean; lab review found no ownership issue and its wording correction is incorporated. Final independent evidence review approved the tables and fingerprints after narrowing the old capability-zero wording; exact-head hosted CI remains required. No Swift source changes; its toolchain is absent locally.

MemoryAuthorizer is an always-allowed owned host fixture. Token tests establish server-route refusal before source details/effects, not product local-cache authorization or dynamic revocation. Fixture server configuration is immutable; no instantaneous-revocation claim. Direct includes one framed counting hop, relay adds an opaque raw hop. Bytes exclude IP/TCP/TLS packet overhead; incomplete reply bytes are explicit. No source metadata transaction spans network writes. There is no automatic reset, transport retry, downgrade or source collection endpoint.

Reproduce:
```sh
git fetch --depth=1 origin 8596fd36e922af25fe1da2d2524a6e65673ee25b
cargo test --locked --all-features --lib catalogue_horizon
python3 scripts/verify-catalogue-horizon-wire.py --repetitions 3
```

Use `--baseline-root PATH` for an existing clean checkout at that exact old SHA. The default builds its own owned temporary archive. See [the contract](catalogue-horizon-wire.md) and `docs/verification/catalogue-horizon-wire.json` for full hashes, raw case counts, attempts/replies, separated bytes, timings and command receipts. Issue #49 remains open for its acceptance audit. The native pin owner is merged #48 and is preserved; secure production catalogue/session work remains #54.
