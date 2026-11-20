# Reference writer-origin epoch authority

This is the claim prerequisite for [#50](https://github.com/nessalabs/sync-engine/issues/50),
not end-to-end frame fencing. An origin epoch is a positive, monotonic writer
claim number, separate from a stream incarnation, receiver access epoch, data
revision, or checkpoint.

The host maps a gateway to a stable, trusted origin authority key. The
reference SQLite adapter can serialize competing claims by local processes
sharing one explicitly provisioned ledger. A compare with the exact previous
epoch or unclaimed state runs under a short SQLite immediate transaction.
One contender commits the next epoch; a stale contender receives the
observed epoch without a mutation. The first claim is 1; exhaustion refuses
rather than wrapping. A missing or unrecognized ledger is not re-created
on normal open.

The host controls all access to the ledger and determines who may claim a key.
The lab assumes a trusted directory and local SQLite locking. It does not prove
a lease over independent files or remote hosts. In particular, never restore
this ledger with backed-up source data. Restoring with a replacement ReplicaId
requires authentication, a trusted authority that survives the restore (for
example via an existing host re-pair or relay registration design), and a
product decision about replacement and revocation. Two arbitrary process-selected
keys are not a shared authority. A read showing the next epoch after an uncertain
commit does not identify which process claimed it; do not treat that observation
as permission to serve.

No source serves or denies traffic based on this helper, no receiver epoch
floor is saved, and no message encoding changes. Source publication checks and
atomic receiver fences on authorized heads, pages and snapshots remain required
for #50. This slice cannot establish that any existing gateway is fenced.

```sh
cargo test --locked --features sqlite --test origin_epoch --test origin_authority
fixture_dir=$(mktemp -d)
cargo run --locked --features sqlite --example origin_authority -- "$fixture_dir/authority.sqlite"
```

The disposable lab prints first epoch 1, one rejected stale compare and epoch 2
after reopen. Integration tests also race independent SQLite handles and verify
missing-file and maximum-epoch behavior. This is local correctness evidence,
not an availability or performance benchmark.
