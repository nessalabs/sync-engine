# Capability-coherent history reads (#54 prerequisite)

`CapabilityHistorySource` is an opt-in port for a bounded tail or older read
whose returned `SourceHead` describes the same read snapshot as the response.
A separate metadata query followed by a legacy history read cannot provide that
guarantee. The native SQLite reader validates the offered support and the
adapter's supported formats before reading the history floor or any record row.
The publication query pins the read transaction; that transaction then supplies
the floor and bounded rows. Tail watermark equals the returned head position;
the older result ends at the requested exclusive bound minus one, regardless of
a newer live head. Both return owned data and hold no SQLite transaction during
subsequent work.

| Boundary | Decision |
| --- | --- |
| Wrong source scope | Refuse before acquiring a read transaction. |
| Unsupported reader requirement or metadata version | Refuse before the history floor and record acquisition. |
| Writer-only requirement | Report it in metadata without blocking read. |
| Compatible empty source | Return head zero and the existing complete-empty tail. |
| Compatible nonempty source | Return exact request echo and bounded rows from the pinned snapshot. |
| A competing publication commits after admission | The admitted snapshot is unchanged; the next read must independently admit the new publication. |

Existing SQLite `HistorySource` methods delegate to the same path with native
reader support, discarding only the advertised head. The history store and
installation contracts are unchanged. This does not add wire routes, an
authorization mechanism, a new state owner, or production security.

Run `cargo test --locked --all-features --lib history_capability_tests` for native
read/reopen and deterministic WAL interleaving coverage. Run
`python3 scripts/verify-slice-5.py` for the existing standalone legacy history
lab, including its persistence and interruption checks.
