# Physical catalogue compaction

Source schema v7 migrates v5 and draft v6 atomically under the same source identity. The
`compact_retired` maintenance operation moves at most 256 previously hidden
retired rows, strictly below the already committed horizon, into a WITHOUT ROWID
exact proof table. Each proof retains the ID, creation revision and deletion
revision. Equality stays in the serving table. Selection uses a partial index that includes the empty-payload condition,
and insertion and removal commit in one immediate transaction. Maintenance
neither advances the horizon nor changes retention pins. Proof remains unbounded
in the number and size of retired IDs.

Current resolution consults both tables, preserving deletion evidence for a
previously accepted descriptor and reset reconciliation. The ID fence rejects
resurrection, including through an older writer's insert, which a database trigger
aborts. Unknown IDs remain unavailable. No change is made to authorization or
foreign-ID handling. A schema version is the format version for proof; missing proof tables, indexes or triggers are refused on open.

Stop **all** older source readers and writers before migration and keep them
stopped. An already-open older reader does not consult proof and can report an
unavailable value after compaction. The insert trigger is defense in depth, not a
complete binary-version fence. Transactions begun before the move observe their
SQLite snapshot; a later current reader sees the committed proof. Reopening an
old binary refuses the newer file version. Existing receiver and wire versions
retain their prior horizon admission and reset semantics.

Run `python3 scripts/measure-catalogue-compaction.py` to build an isolated fixture
and report dbstat pages for base rows, proof, serving and maintenance indexes,
plus allocated and reusable pages. The process timing includes startup and source
open. SQLite page allocation depends on workload and layout; compaction does not
promise a smaller file or bounded total proof storage.
