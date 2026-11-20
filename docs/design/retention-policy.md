# Host retention cutoff policy

This is the pure recommendation prerequisite for [#48](https://github.com/nessalabs/sync-engine/issues/48).
It is not enabled on any source adapter.

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
and physical storage policy. A future source adapter must enforce the cutoff
against a coherent view of pin registrations and source state at its mutation
boundary. This pure result does not move the record read floor and cannot drop
catalogue markers. The catalogue adapter also requires the admitted, durable
horizon in [#49](https://github.com/nessalabs/sync-engine/issues/49).

```sh
cargo test --locked --test retention_policy
cargo run --locked --example retention_policy
```

The example evaluates four independent scenarios for a phone at 40, a backup at 60, a preferred
cutoff of 90, and forced pressure at 70. It reports cutoffs 40 before expiry,
70 under pressure while both are active, 60 when just the backup remains, and
90 with no active pins. These are policy outcomes, not storage or timing
measurements.
