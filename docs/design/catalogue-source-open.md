# Native catalogue source journal-mode admission

Source identity validation and transactional migration run first. An invalid or
mismatched file is refused before attempting its journal-mode transition.
A valid source is returned only after SQLite confirms WAL and synchronous FULL
has been configured on the connection.

Concurrent openers can finish identity validation but race while converting
a rollback journal to WAL. SQLite may return BUSY immediately for that
transition even when a busy handler is installed. The catalogue source uses
the connection's existing five-second contention budget for this configuration
phase: individual WAL attempts do not install a nested busy wait, and only a
typed SQLite BUSY earns a pause of at most ten milliseconds and another attempt
before the deadline. A synchronous-configuration wait uses only the remaining
budget. Restore the original busy timeout before returning the handle. This is
not a deadline for filesystem I/O, nor a retry of schema migration or writes.
An expired budget, any non-BUSY error, or a result other than WAL refuses open
admission. Other SQLite roles keep their current configuration path.

Run `cargo test --locked --all-features --lib source_connection` to exercise
real competing SQLite handles, releasing the lock deterministically after
the first BUSY, and verifying typed failure with an exhausted budget and the
timeout/synchronous settings of a successful handle. The existing
`competing_openers_migrate_once_and_keep_one_identity_history` test is unchanged.
