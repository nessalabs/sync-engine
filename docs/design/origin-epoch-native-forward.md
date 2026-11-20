# Claim-qualified native forward pages

This is the standalone forward-record path in [#50](https://github.com/nessalabs/sync-engine/issues/50).
It composes the [claim-qualified head producer](origin-epoch-native-head.md)
and the [protected record receiver](origin-epoch-native-receiver.md). SourceHead
v1, network frames, Nessa and Swift are unchanged.

1. The internally confirmed claim protects a bounded ordinary forward-page
   capture. The ledger guard is acquired before the source read and released
   before the result is handed off. The native page read checks reader
   capabilities, stream/head/floor bounds and payload in one source snapshot.
   Its existing maximums (256 records, 1 MiB) still apply before any host
   policy's smaller page. The guard therefore spans a bounded local read,
   not host code, receiver writes, network I/O or time in a delivery queue.
2. The returned opaque page contains the captured request, payload and the
   claim's original stamp. Canonical validation consumes it against the
   receiver's independently expected request, durable checkpoint and
   policy limits. The validated evidence retains the exact operation
   parameters, claim and record plan; there is no stamp-from-current or
   round-trip through an unqualified page.
3. The trusted host selector verifies the stable authority binding, current
   scope and actual expected operation (including its range and budgets).
   It carries the evidence to the existing protected receiver. That
   receiver checks its durable floor before ordinary record apply or
   duplicate handling, in the same mutation transaction. A saved old
   result is not made current by reopening either side.

A source claim qualification is not identity authentication, a current
receiver grant, or a replacement for an operation's correlation. A fixture
can receive opaque results directly; integrations must use their trusted
host admission port. The current source file remains ordinary and no numeric
epoch, key choice or default fallback is allowed to qualify an unguarded
page as a trusted result.

```sh
cargo test --locked --features sqlite --test epoch_forward
```

Two real source handles share one history and one claim ledger. The old
writer's first bounded page commits position 1; a second page is held
across epoch 1→2. After observing the new head, the receiver refuses the
held page without progress or payload changes, and the old producer refuses
a new page outright. The reopened receiver refuses mismatched requests
and then advances to positions 2 and 3 on matching new-epoch pages; a final
reopen confirms exact records. Tests also exercise one-record page bounds,
an oversized record and a fresh incompatible publication after the last
compatible head.

This is sequential real-handle evidence, not a timing or throughput claim.
The [tail snapshot contract](origin-epoch-native-tail.md) is separate.
This does not imply protected legacy files, older pages, catalogue,
appends, authenticated wire, source lease after delivery, separate-host
authority or product integration. No new scheduler, checkpoint owner or
restore contract is introduced.
