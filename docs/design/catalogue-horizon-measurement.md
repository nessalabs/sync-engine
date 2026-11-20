# Native horizon prerequisite measurements

Historical receipt for its named runtime. The current native activation contract
and real collection evidence are in [catalogue collection](catalogue-collection.md).

Adds a native-only #49 coverage prerequisite: explicit catalogue v2 metadata and typed expiry, durable exact-scope monotone observations, fresh initial/resume admission, per-read source snapshot checks, and a receiver transaction fence using completed C rather than H/cursor. Equality is accepted. The existing whole-page commit loop and current authorization checks remain the owners.

Positive source publication and collection are **disabled**. No horizon setter, collector, successful v2 network dialect or reset-composition claim is included. Existing generation-bound reset remains intact on the default zero horizon. V1 native and development wire routes refuse positive fixture floors before data, without downgrade. Source v4 and receiver v3 migrations preserve populated values/fences/progress; previously open old writers still require host fencing.

Measured against main `b4b780d` and candidate runtime `a8487a2`; later harness/receipt changes preserve all measured Rust/Cargo hashes. Three repetitions per phase/path, unshaped debug development host. These are plaintext loopback fixture routes, not the production session or secure transport. No latency improvement is claimed.

| Dialect | Phase | RPCs both | Total B both | Direct median ms before → after | Relay median ms before → after |
|---|---|---:|---:|---:|---:|
| legacy | bootstrap | 637 | 212,637 | 974.157 → 947.706 | 1379.591 → 1361.322 |
| legacy | partial | 42 | 12,387 | 74.076 → 61.817 | 98.143 → 90.593 |
| legacy | restart_resume | 595 | 200,250 | 894.265 → 925.907 | 1273.269 → 1217.969 |
| legacy | churn | 4 | 920 | 9.132 → 9.820 | 13.281 → 10.990 |
| legacy | unchanged_recheck | 1 | 143 | 3.877 → 4.353 | 4.915 → 4.912 |
| capabilities | bootstrap | 637 | 242,568 | 958.083 → 959.013 | 1435.045 → 1456.745 |
| capabilities | partial | 42 | 14,353 | 61.125 → 70.077 | 85.307 → 89.314 |
| capabilities | restart_resume | 596 | 228,397 | 927.874 → 900.582 | 1253.618 → 1320.673 |
| capabilities | churn | 4 | 1,100 | 10.771 → 10.284 | 11.134 → 13.668 |
| capabilities | unchanged_recheck | 1 | 182 | 4.323 → 5.387 | 4.586 → 4.887 |

RPC and byte columns are the same before and after on both direct and opaque relay. Exact reopened rows/revisions/deletions/payloads and progress matched in all 60 before and 60 after phase observations. Wire timings include process start, open, connections and commits; ranges and separate protocol/manifest/payload bytes are in the committed receipt.

| Fixture horizon | Phase | Native head/manifest/value calls | Logical manifest/payload B | Operation ms median (range) | Process ms median |
|---:|---|---:|---:|---:|---:|
| 0 | bootstrap | 2/10/620 | 16,120/9,300 | 104.739 (102.296–113.297) | 129.137 |
| 0 | partial | 1/1/64 | 1,664/0 | 11.419 (11.035–11.486) | 40.096 |
| 0 | follow | 2/2/96 | 2,496/0 | 20.824 (18.998–21.056) | 48.107 |
| 620 | bootstrap | 2/10/620 | 16,120/9,300 | 113.697 (113.402–118.623) | 139.997 |
| 620 | partial | 1/1/64 | 1,664/0 | 12.419 (11.331–12.701) | 41.783 |
| 620 | follow | 2/2/96 | 2,496/0 | 17.339 (17.091–20.361) | 45.056 |
| 621 | bootstrap | 2/10/620 | 16,120/9,300 | 114.740 (110.679–134.862) | 142.465 |
| 621 | partial | 1/1/64 | 1,664/0 | 11.666 (10.697–13.111) | 43.491 |
| 621 | refuse | 1/0/0 | 0/0 | 0.442 (0.264–0.536) | 31.861 |

Native bootstrap is 620 live IDs. After 160 deletions and an interrupted 64-entry page, equality C=620/horizon 620 finishes C=780 with 460 live/160 fences. Horizon 621 refuses with one head and zero manifest/value reads, retaining reopened C=620, cursor and 556 live/64 fences. Native calls/logical bytes are not RPC/wire counts. Operation time excludes open/post-run verification; process time includes them. Allocation is 69,632 B source/61,440 B receiver in every run; markers remain retained, with no storage-saving or isolated migration-time claim. Shared-host timing is noisy.

Validation: all 45 repository commands passed, including 344 all-feature Rust tests, the other three feature configurations, 6 doctests, Clippy, Rustdoc, seven slice labs, transport/artifact/freshness/reset/history labs, both finite-model runners and Rust 1.85 checks. Nine new horizon contracts passed. Ten isolated invariant reversions each fail their intended ordinary contract; restored horizon/publication/recovery tests pass and the guard checkout is clean. The initial disk-full build attempt was retained; only completed-task generated caches were cleared and affected gates rerun. Independent round 1 findings were resolved; round 2 source inspection was clean; final independent evidence review approved all receipts. Exact-head GitHub CI/review remain required.

Reproduce:
```sh
cargo test --locked --all-features --test catalogue_horizon
python3 scripts/verify-catalogue-horizon.py --repetitions 3
python3 scripts/measure-catalogue-publication.py --repetitions 3 --native-repetitions 3
python3 scripts/measure-catalogue-publication.py --client-dialect capabilities --repetitions 3 --native-repetitions 3
```

See `docs/design/catalogue-horizon.md` and `docs/verification/catalogue-horizon.json` for contract, ranges, hashes, commands and limits. #49 and #54 remain open: native v2 reset composition, source publication/atomic collection, network versioning and collection-aware no-resurrection proof are outstanding. No #48 integration, #51 encoding, Swift or downstream implementation is included.
