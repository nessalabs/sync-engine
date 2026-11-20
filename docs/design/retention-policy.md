# Host retention cutoff policy

This is the host-owned policy for [#48](https://github.com/nessalabs/sync-engine/issues/48).
The SQLite record reference source can opt in; catalogue enforcement remains open.

A host registers an expiring pin for an opaque holder and exact source history:
resource family (record or catalogue), origin, resource ID and incarnation.
The host authenticates registration and refresh and supplies the last
**durably completed** position, not an in-flight reset boundary or page cursor.
A pin does not grant access, acknowledge a new position, or recover an already
pruned range. A record stream and a catalogue with the same identifier do not
share pins. A schema change that replaces history must also change incarnation;
a schema value alone is not a second retention identity.

The caller supplies a coherent committed head, existing floor, preferred
cutoff, optional forced cutoff and current time. Pins for the selected history
with `expires_at_ms > now_ms` are active. Every used position must be at or below
the committed head. Reduce the preferred cutoff by all active pins, then take
the maximum of that result, the current floor and any forced cutoff. A forced
cutoff is the minimum advancement that the host calculates when hard pressure
wins. The reference record cutoff is inclusive: `pruned_through = 40` drops
availability at positions up through 40 and still admits a receiver already
completed at 40. A lagging or expired device may still require snapshot or
reset. Catalogue collection will separately follow #49's strict marker rule.

The host owns the registry, expiration clock, byte/age-to-position conversion
and physical storage policy. A source adapter must enforce the cutoff against a coherent view of pin
registrations and source state at its mutation boundary. The pure result alone
does not move a read floor or drop markers. A catalogue adapter requires the
admitted, durable horizon in [#49](https://github.com/nessalabs/sync-engine/issues/49).

```sh
cargo test --locked --test retention_policy
cargo run --locked --example retention_policy
```

The example evaluates four independent scenarios for a phone at 40, a backup at 60, a preferred
cutoff of 90, and forced pressure at 70. It reports cutoffs 40 before expiry,
70 under pressure while both are active, 60 when just the backup remains, and
90 with no active pins. These are policy outcomes, not storage or timing
measurements.

## Opt-in SQLite record source

An explicit host registers the acknowledged durable checkpoint and provides
current Unix milliseconds and expiry. A still-live holder refreshes its exact
scope and version by durable receipt; expiry requires explicit registration
instead. The source rejects positions below its already-pruned floor or above
the committed head, refresh regressions, retired access-epoch or scope echoes
that differ from the receipt, and stale receipt revisions. A smaller clock
than the last durable policy decision is refused, including after reopen.
Observing an exact receipt's expiry persists that time before refusing it.
A receipt is not authentication or independent proof that a receiver applied
the position; the trusted host owns those facts. After an uncertain commit it
must reload, not blindly extend a registration.

The v3 record-source file keeps the ledger and retention time beside its
existing committed head/history. The migration retains records, their
immutable IDs and the prior floor, and old writer binaries must be stopped
before upgrade. The host proposes cutoffs for an observed head; one SQLite
writer transaction compares that head, checks live pins and advances only the
logical read floor. The legacy fixture-only prune is refused once a source has
registered any pin. It cannot bypass pressure or expiry policy.

Expired rows retain their version so delayed refreshes cannot become valid
again. Holder cardinality, authentication, TTL, and byte/age-to-position
conversion remain app-owned. This source does not remove physical bytes and
does not certify storage savings, cross-machine policy, or catalogue behavior.

```sh
cargo test --locked --features sqlite --test record_retention --test source_publication
fixture_dir=$(mktemp -d)
cargo run --locked --features sqlite --example record_retention -- "$fixture_dir/source.sqlite"
```

The disposable record fixture uses six committed rows. Preferred cutoff 5
becomes 2 with a live phone pin, and forced cutoff 4 supersedes the same pin.
The phone can read incrementally from 2 after the first decision. After the
second decision and reopening, its old position gets the existing typed gap.
Repeating the exact first append still returns its old position, confirming
the read-floor change did not physically remove its deduplication identity.
