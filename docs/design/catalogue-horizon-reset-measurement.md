# Native v2 reset measurements

Issue #49 remains open. This focused slice composes explicit native v2 reset with the existing same-incarnation Enumerate/Reconcile/Finish journal. FullReset compares captured H (C stays zero), preserves pending creation/revision floors and permanent fences, and requires current full-scope authority. Cold reset CASes actual parent absence and creates its first parent/journal atomically; Some(expected) never falls through to cold creation. Every effect rechecks observed coverage, including H publication and fence release. Fresh post-commit v2 metadata remains mandatory; errors/uncertainty stop for explicit reload/supersession.

Independent review also found and corrected a missing reauthorization after an empty candidate read in the shared engine. A deterministic policy-change regression failed before the fix and now passes through both adapters for Denied and Unverifiable, with no journal movement.

Source activation/collection is **still disabled**; there is no publisher/collector API or network dialect in this PR. Only owned fixtures set a positive floor and hide retired serving membership strictly below it. Source identities and receiver fences remain permanent. No schema bump or shared source-open/configuration change is needed; the merged #92 repair and concurrent-opener checks are preserved.

Measured main baseline `e42b01a` against runtime `c747789`; three repetitions per profile/path, debug builds on the same unshaped development host. Timings are noisy evidence without a speedup/CI threshold claim.

| Native profile | Source head/manifest/value calls | Passive observations | Journal effects | Logical manifest/payload B | Work ms median (range) | Process ms median (range) |
|---|---:|---:|---:|---:|---:|---:|
| Before legacy, floor 0/all markers | 2/16/620 | — | — | 16,740/8,280 | 82.814 (80.951–93.158) | 147.570 (145.067–170.480) |
| After legacy, floor 0/all markers | 2/16/620 | 0 | 20 | 16,740/8,280 | 83.240 (81.243–84.854) | 145.235 (142.162–152.665) |
| After v2, floor 780/one served marker | 2/12/620 | 634 | 20 | 12,447/8,280 | 118.632 (112.240–118.741) | 184.323 (177.016–185.694) |

Native fixture: 620 prior cached live IDs, 160 source deletions, captured H=780. The positive fixture hides 159 markers below 780 and retains the equal marker. Both successful profiles end at C=780 with exactly 460 live rows/160 fences, all 620 reopened identity/current rows equal to source, and an empty journal. Before and after, the legacy API refuses positive fixtures with HorizonRequired and unchanged cache/progress. Source payload counters for those refused processes are unavailable and are not invented.

The zero-floor profile is matched before/after and serves all markers. The positive profile is a distinct serving condition, not a matched transport optimization. Its 634 passive observation calls are measured separately from 20 journal effects; this extra work is not hidden. Candidate-read calls are 1 (after zero) versus 5 (v2 positive). Authority checks are 658 before zero, 659 after zero (the corrected empty-read check), and 1293 on v2 positive. The uninstrumented baseline has dashes for store-call counters. All three successful profiles use 20 bounded journal steps and 98,304 allocated receiver bytes; no storage savings are claimed. Work timing excludes open; process timing includes open/effect/report. Preparation and subsequent inspection are excluded. Port calls are not RPCs or disk fsync counts.

| Wire dialect | Phase | RPCs both | Total B both | Direct median ms before → after | Relay median ms before → after |
|---|---|---:|---:|---:|---:|
| legacy | bootstrap | 637 | 212,637 | 1073.615 → 1019.571 | 1352.164 → 1321.057 |
| legacy | partial | 42 | 12,387 | 67.491 → 64.660 | 93.386 → 86.757 |
| legacy | restart_resume | 595 | 200,250 | 966.297 → 883.418 | 1318.008 → 1227.197 |
| legacy | churn | 4 | 920 | 11.484 → 10.403 | 12.329 → 12.643 |
| legacy | unchanged_recheck | 1 | 143 | 4.544 → 4.016 | 5.231 → 4.884 |
| capabilities | bootstrap | 637 | 242,568 | 1017.410 → 1007.730 | 1314.154 → 1414.790 |
| capabilities | partial | 42 | 14,353 | 65.736 → 67.662 | 88.946 → 89.495 |
| capabilities | restart_resume | 596 | 228,397 | 901.537 → 949.101 | 1239.067 → 1268.185 |
| capabilities | churn | 4 | 1,100 | 9.597 → 11.132 | 11.152 → 11.715 |
| capabilities | unchanged_recheck | 1 | 182 | 4.909 → 4.398 | 4.570 → 4.628 |

Zero-floor wire counts/bytes are identical before/after on both paths. All 120 phase observations checked exact reopened IDs, revisions, deletions, payloads and durable progress, with no duplicate payload. These are existing plaintext development-loopback routes (direct includes a counted framed hop; relay adds an opaque hop), not the production session/secure channel or a new v2 reset wire. The committed receipt includes timing ranges and separated protocol/manifest/payload bytes.

Validation: all 46 local repo gates passed, including 377 all-feature Rust tests, all other feature configurations, 8 doctests, Clippy, Rustdoc, the existing labs and finite-model runners, and Rust 1.85 checks. Thirteen new reset contracts pass. Thirteen isolated invariant removals each fail their intended ordinary regression; restored 52 targeted contracts pass with a clean guard tree. Source review round 1 finding is resolved and round 2 is clean; final evidence review approved the receipts; exact-head GitHub CI remains required. Swift is unchanged and its toolchain is absent locally; its existing macOS CI workflow remains required.

Reproduce:
```sh
cargo test --locked --all-features --test catalogue_horizon_recovery
python3 scripts/verify-catalogue-horizon-reset.py --repetitions 3
python3 scripts/verify-catalogue-horizon-reset.py --profile legacy-zero --repetitions 3
python3 scripts/verify-catalogue-horizon-reset.py --profile legacy-positive --repetitions 3
python3 scripts/measure-catalogue-publication.py --repetitions 3 --native-repetitions 3
python3 scripts/measure-catalogue-publication.py --client-dialect capabilities --repetitions 3 --native-repetitions 3
```

See `docs/design/catalogue-horizon-reset.md` and `docs/verification/catalogue-horizon-reset.json`. Remaining acceptance: one native atomic monotone horizon/marker-selection boundary, its collection-aware finite model and native restart/atomicity qualification before activation, then the explicit network dialect and mixed-version coverage. The existing reset model proves shared phase/fence safety only. Catalogue pins (#48), broader #54 work, Swift and downstream implementation remain separate.
