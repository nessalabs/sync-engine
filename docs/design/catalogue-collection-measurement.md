# Native catalogue collection measurements

Issue #49 remains open. Checked native activation atomically advances the monotone horizon and hides strict-below retired serving markers. Permanent source identities and receiver fences remain; current legacy APIs refuse positive activation before data. Source v4/receiver v3 need no schema bump; the existing source-open/WAL helper is unchanged. Successful v2 networking and #48 pin policy remain separate.

Matched baseline `8596fd3` and runtime `85a63de`; three repetitions per native profile and direct/opaque-relay wire path, debug builds on the same unshaped shared development host. No timing threshold or speedup claim. Collection-model CPU work and local repo qualification finished before these serial timing trials.

| Native reset profile | Head/manifest/value calls | Observations / journal effects / candidate reads | Logical manifest/payload B | Work ms median (range) | Process ms median (range) |
|---|---:|---:|---:|---:|---:|
| before-zero | 2/16/620 | 0/20/1 | 16,740/8,280 | 101.739 (97.748–154.050) | 192.917 (180.683–286.847) |
| after-legacy-zero | 2/16/620 | 0/20/1 | 16,740/8,280 | 146.824 (99.411–163.456) | 288.324 (179.530–386.595) |
| after-v2-zero | 2/16/620 | 638/20/1 | 16,740/8,280 | 144.193 (120.341–150.900) | 219.753 (186.223–237.321) |
| after-v2-collected | 2/12/620 | 634/20/5 | 12,447/8,280 | 123.661 (113.262–170.317) | 193.928 (185.254–257.659) |

Real activation hides 159 markers below 780, retaining the equality marker 780. API collection time median 0.647ms (range 0.415–0.711); process/open/head-check/report time 3.974ms (3.918–4.463). All 620 permanent source rows, 160 retired identities and 460 live values are unchanged. Allocated source storage stays 77,824→77,824B; freelist pages 0→1. Serving index membership 620→461. Detailed page/cell/payload accounting is in the receipt. No VACUUM, file-size or bounded-total-storage savings are claimed.

Every reset start/step uses a fresh process. All successful profiles verify exact reopened 620 current rows, 160 receiver fences, C780 and an empty pending journal. Legacy positive activation refuses HorizonRequired on all 3 trials without cache/progress mutation. Source payload counters for refused processes are unavailable. All successful profiles have 20 journal effects and 98,304 allocated receiver bytes. Port calls and logical descriptor bytes are not RPCs or fsyncs. Work timing excludes open; process timing includes open/effect/report; preparation and subsequent inspection are excluded.

| Existing zero-floor wire | Phase | RPCs both | Total B both | Direct median ms before → after | Relay median ms before → after |
|---|---|---:|---:|---:|---:|
| legacy | bootstrap | 637 | 212,637 | 1049.065 → 961.033 | 1384.393 → 1315.785 |
| legacy | partial | 42 | 12,387 | 60.794 → 66.345 | 85.273 → 91.388 |
| legacy | restart_resume | 595 | 200,250 | 959.883 → 886.891 | 1274.809 → 1191.405 |
| legacy | churn | 4 | 920 | 9.949 → 9.706 | 11.016 → 12.154 |
| legacy | unchanged_recheck | 1 | 143 | 4.146 → 4.496 | 4.669 → 4.659 |
| capabilities | bootstrap | 637 | 242,568 | 1001.622 → 951.176 | 1402.420 → 1305.877 |
| capabilities | partial | 42 | 14,353 | 63.080 → 68.131 | 87.249 → 83.912 |
| capabilities | restart_resume | 596 | 228,397 | 900.400 → 911.830 | 1293.572 → 1209.224 |
| capabilities | churn | 4 | 1,100 | 10.869 → 10.428 | 13.673 → 11.745 |
| capabilities | unchanged_recheck | 1 | 182 | 4.660 → 5.190 | 5.218 → 4.872 |

Zero-floor counts and bytes are identical before/after. All 120 wire phase observations verify exact reopened identity/revision/deletion/payload/progress. Direct includes a counted framed TCP hop; relay adds an opaque hop. These are plaintext development-loopback routes with fixture tokens, not secure production session transport or a successful v2 horizon wire. The receipt includes ranges and separated protocol/manifest/payload bytes.

Validation: all 50 local repo gates passed, including four Rust feature configurations, Rust 1.85, Clippy/Rustdoc/doctests, all existing process/TCP labs and both older finite models. Ten integration contracts plus two internal transaction contracts pass. Eight isolated invariant removals fail their intended tests; restored code passes and has identical publication-source fingerprint. Independent source review was clean; one model evidence gap was corrected, then model review including delayed Finish correlation was clean. Final independent evidence review approved these receipts; exact-head hosted CI remains required. Swift is unchanged and its toolchain is absent locally.

| Final bounded model | Distinct states | Result |
|---|---:|---|
| warm | 999,832 | exhausted, no invariant violation |
| cold | 3,424,724 | exhausted, no invariant violation |
| split-publication | 265 | expected MarkersCovered violation |
| cursor-is-not-completion | 83,677 | expected CoveredNotLive violation |
| delayed-lower | 3,468 | expected NoLowerHorizon violation |
| stale-generation | 1,430 | expected NoStaleApply violation |
| absence-is-not-proof | 136,907 | expected FenceProof violation |
| unaware-positive-read | 176 | expected LegacyBeforeData violation |
| finish-coverage | 230,199 | expected NoExpiredEffects violation |

The finite model uses two permanent IDs, one fixed full scope/incarnation, two reset generations, one queued head/data reply and one captured publisher offer (plus a concurrent newer publication). Crash/drop clears volatile state; ordinary unavailable classification stutters. It tracks delayed effects, completed-vs-target coverage, generation, monotone observed floors, permanent fences, fresh post-publication correlated Finish and current authority at operation admission. This is bounded safety evidence, not SQL power-loss, payload-codec, pin-policy, liveness or instantaneous-revocation proof. The existing reset model separately covers its epoch/cancellation states.

Reproduce:
```sh
cargo test --locked --all-features --test catalogue_collection
python3 scripts/verify-catalogue-collection.py --profile v2-collected --repetitions 3
python3 scripts/verify-catalogue-collection.py --profile v2-zero --repetitions 3
python3 scripts/verify-catalogue-collection.py --profile legacy-zero --repetitions 3
python3 scripts/verify-catalogue-collection.py --profile legacy-collected --repetitions 3
python3 scripts/verify-catalogue-collection-model.py --jar PATH_TO_PINNED_JAR
python3 scripts/measure-catalogue-publication.py --repetitions 3 --native-repetitions 3
python3 scripts/measure-catalogue-publication.py --client-dialect capabilities --repetitions 3 --native-repetitions 3
```

See [the ownership contract](catalogue-collection.md) and `docs/verification/catalogue-collection.json` for full hashes, commands, accounting and limits. Explicit v2 network coverage and mixed-version reset/restart/delayed-result qualification remain. Catalogue pins (#48) and broader #54 work remain separate.
