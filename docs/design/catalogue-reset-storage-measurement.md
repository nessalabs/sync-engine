# Native reset candidate storage baseline (#51)

The native reset in [#77](https://github.com/nessalabs/sync-engine/pull/77)
does not persist a separate seen-ID set. It puts only prior live identities and
their known revision floors into a durable worklist, removes matching candidates
in the transaction that commits each enumerated page, and reconciles the
residue. An admitted page does not add to a growing set of seen keys. The
parent progress row owns generation and phase. This is an O(P) candidate
worklist, where P is the prior live or already pending identity count. It is
not a claim that the necessary worklist is sublinear or compressed.

Run `python3 scripts/measure-catalogue-reset-storage.py --output storage.json`.
The observer uses the unchanged owned [native example](catalogue-reset.md):
620 pre-reset live rows, 160 source deletions hidden from enumeration by
fixture-only marker membership, final head 780. It reopens the receiver
read-only after every writer-process exit and checks the exact final rows and
payloads against the source. No marker-collection API or new store is used.
The observer can report counts and SQLite file metrics without `dbstat`;
per-table metrics are then null.

| Durable boundary | Candidates | Live rows | Permanent fences | Completed | Candidate pages / bytes | Main file bytes | Freelist pages |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before reset | 0 | 620 | 0 | 620 | 1 / 4,096 | 61,440 | 0 |
| After start | 620 | 0 | 0 | 0 | 9 / 36,864 | 94,208 | 11 |
| After enumeration | 160 | 460 | 0 | 0 | 3 / 12,288 | 94,208 | 9 |
| After finish | 0 | 460 | 160 | 780 | 1 / 4,096 | 94,208 | 8 |

Observer SQLite 3.53.1 reported a 4,096-byte page size. The candidate table's
36,864 bytes were the largest observed across 21 post-process snapshots. An
empty table still occupies its root page. The main file is the quiescent file
length, distinct from candidate-table page bytes and freelist pages. It was
equivalent to SQLite's reported page count times page size at these sample
points. No receiver WAL file existed at the sample points; this does not
bound transient WAL size or SQLite's in-transaction peak. This is one
fixed fixture, not a larger-scale extrapolation or a speed measurement.

The remaining candidate data cannot be reduced to membership of creation
revisions without another source for exact keys and floors. Current
owner-authorized reconciliation requests the actual ID. The creation revision
is part of the immutable key, not a derivation of arbitrary IDs; the current
revision floor is needed separately to reject stale or conflicting evidence.
Storing only a revision range and its maximum would lose these data. A
second seen-set owner would not improve the implemented subtraction. Prefer
this measured representation unless future receipts show candidate storage
itself is a material cost. Any change to #51's original compact-encoding
proposal should explicitly record this choice; this receipt does not say a
range/bitmap codec shipped.
