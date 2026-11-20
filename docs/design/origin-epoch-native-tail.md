# Claim-qualified native tail snapshots

This is the bounded native recent-tail slice of [#50](https://github.com/nessalabs/sync-engine/issues/50).
It extends the same [source claim](origin-epoch-native-head.md) and
[protected receiver](origin-epoch-native-receiver.md). It introduces no file
format, owner, legacy history path, wire change or application dependency.

1. Check the local request generation and limits using the same canonical
   check as tail validation, before reading any source records. Capture one
   existing `tail_with_capabilities` snapshot while the confirmed claim
   is guarded. Exact scope, fresh reader compatibility, source watermark,
   pruning floor and bounded payload are evaluated by the existing native
   read. Release the authority guard before transport or handing off.
2. An opaque owned result retains the original epoch, exact request, tail
   and coherently captured metadata. Its validator requires the metadata
   scope and watermark to agree and invokes the existing history validator
   with the host's independently expected request and limits. No tail
   decision raises the observed epoch. A trusted admitted head remains the
   separate way to do that.
3. The same host admission object can implement the optional tail extension,
   which verifies current scope, authority and actual pending request
   before admitting the validated snapshot. In the protected receiver's
   existing IMMEDIATE transaction, the stable binding and observed floor
   are checked before any snapshot decision. The same canonical mutation
   handles scope, reset generation, watermark, rows and both boundaries for
   ordinary and qualified tails; a same-generation repeat is still a
   typed history refusal. No unqualified `HistoryStore` implementation is
   exposed by the protected file mode.

The host still determines valid current authorization and snapshot intent.
The source and receiver do not infer a reset or new binding from an epoch,
from absence, or from a scoped observation. Existing ordinary-forward
cache does not silently become snapshot history. Reopening loads the same
durable epoch, stored scope and history progress; an uncertain transaction
requires an explicit read-back, not assuming a retry was the earlier commit.

```sh
cargo test --locked --features sqlite --test epoch_tail
```

The real fixture commits generation 1 at watermark 2 with rows `a,b`.
Absent, wrong-key, wrong-request, future-unobserved, and old-epoch results
leave that history unchanged. Observing epoch 2 alone also changes no
records. An old duplicate is refused as fenced even after reopen, before
the normal generation check. An admitted epoch-2 generation-2 tail commits
watermark 4 and lower bound 3, reopening as exact rows `c,d`; an exact
repeat is refused by ordinary generation semantics. Separate fixtures
check preflight limits and freshness of capability admission.

These are sequential local-handle correctness cases. They do not qualify
older pages, artifacts/catalogue, ordinary source files/writes, authenticated
wire, cryptographic authority, physical crashes, or Nessa/Swift integration.
