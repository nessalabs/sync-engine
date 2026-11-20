# Claim-qualified native source head capture

This local reference result producer is related to [#50](https://github.com/nessalabs/sync-engine/issues/50).
It uses the [SQLite claim ledger](origin-epoch-authority.md); no source
file migration, wire change or new authority backend is introduced.

The trusted application supplies an existing ordinary record source handle,
a stable source-to-authority key association, and a handle to the one durable
local authority ledger. The producer acquires its epoch by a successful
compare-and-claim through that ledger; merely reading its current value
does not construct a producer. A stale or uncertain claim produces no usable
handle. An uncertain commit requires explicit host reconciliation, not
assuming the highest number belongs to this caller.

For one capture, take the ledger's short SQLite IMMEDIATE lock, compare the
current epoch, and, only when it is equal, read the ordinary scoped head
from the existing one-row source snapshot. Acquire locks in the order
authority then source; no source operation waits for the authority in this
slice. The ledger guard mutates no state. Release it before returning the
claim-stamped head, or refuse the result if release is not verified. There
is no network I/O, user callback, or receiver storage work under the guard.
A successor cannot change the shared ledger during capture, but can claim
after release, while a previously captured result is still in flight.
Therefore the stamp must be retained, and a receiver must compare it to
its durable floor before any apply.

An exact current scope still goes through the ordinary source head logic;
the producer's stamp is neither proof of requester access nor authority for
the receiver to apply anything. The host must correlate the opaque captured
head with the actual current authorized operation. `SourceHead` v1 itself
is unchanged. The producer cannot be used as the bare `RecordHeadSource`
trait and does not return its owned ordinary source handle.

This mode qualifies only this producer's head results. It does not protect
an ordinary source file or gate other handles, appends, pages, tail snapshots,
catalogue responses or transport. Independent ledgers, arbitrary mappings,
remote filesystems, and authority restored with source data are not covered.
The lab does not inject an uncertain commit or demonstrate physical crash
durability or Nessa/Swift adoption.

```sh
cargo test --locked --features sqlite --test epoch_head_source
cargo test --locked --features sqlite --lib claim_and_capture_serialize_in_the_shared_ledger
```

Two real source handles over one history and separate handles to one ledger
claim epochs 1 and 2. The first producer then refuses a fresh head; the
second returns the ordinary scope and position under epoch 2. A head captured
before handoff keeps epoch 1, not the value at delivery time. A deterministic
zero-wait contender is refused while the guard holds the shared ledger lock
and succeeds after release. These are correctness cases, not performance
or deployment measurements.
