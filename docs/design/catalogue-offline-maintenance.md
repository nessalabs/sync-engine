# Catalogue source ownership during upgrade

```text
new file -- normal open / provision --> v7 runtime
v1..v6 -- normal open --> OfflineUpgradeRequired (file unchanged)
v1..v6 -- host stops all source users and prevents restart
       -- explicit offline maintenance / migrate --> v7 maintenance
v7     -- host owns source exclusively -- explicit offline maintenance --> v7 maintenance
v7 maintenance -- bounded compaction / close --> v7 file
v7 file -- host resumes runtime -- normal open --> v7 runtime
foreign, future, partial or wrong identity --> refusal
```

The host owns shutdown, restart prevention and exclusive access throughout
maintenance. SQLite locks do not establish that older processes have stopped.
Runtime open never upgrades an existing source, and compaction belongs only to the
explicit maintenance owner. Older file formats require an intentional offline
upgrade; source migration tests, examples and benchmarking fixtures that used
implicit open must invoke the owner after establishing their fixture's exclusive
ownership. Current runtime reads and writes use only the v7 schema.

A failed transaction leaves its schema/data unchanged; after an uncertain commit,
inspect the existing file before retrying. Do not replace or rename a live SQLite
file or WAL. Host backup and restore must preserve SQLite's consistency and source
identity; they are not provided by this maintenance API. Closing maintenance
releases its connection; it does not establish or enforce process quiescence.
