//! Shared connection configuration and schema identity for the two file-backed
//! reference adapters.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior};

const SCHEMA_VERSION: i64 = 1;
pub(super) const SOURCE_APPLICATION_ID: i64 = 0x4e53_5353;
pub(super) const REPLICA_APPLICATION_ID: i64 = 0x4e53_5352;

/// File open, schema, or identity refusal before any replication operation.
#[derive(Debug)]
pub enum SqliteOpenError {
    /// SQLite could not open, configure, or inspect the file.
    Database(rusqlite::Error),
    /// The file has another application ID, version, or unmarked user tables.
    UnexpectedDatabase,
    /// An existing reference source has different immutable stream identity.
    SourceIdentityMismatch,
}

impl std::fmt::Display for SqliteOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(f, "SQLite: {error}"),
            Self::UnexpectedDatabase => write!(f, "unrecognized SQLite database or schema"),
            Self::SourceIdentityMismatch => write!(f, "source stream identity differs"),
        }
    }
}

impl std::error::Error for SqliteOpenError {}

impl From<rusqlite::Error> for SqliteOpenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

pub(super) fn open_connection(path: impl AsRef<Path>) -> Result<Connection, SqliteOpenError> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

pub(super) fn configure_connection(conn: &Connection) -> Result<(), SqliteOpenError> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

pub(super) fn ensure_schema<F>(
    conn: &mut Connection,
    application_id: i64,
    schema: &str,
    initialize: F,
) -> Result<(), SqliteOpenError>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<()>,
{
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let found_id: i64 = tx.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let version: i64 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if found_id == application_id && version == SCHEMA_VERSION {
        tx.commit()?;
        return Ok(());
    }
    if found_id != 0 || version != 0 {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    let user_tables: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if user_tables != 0 {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    tx.execute_batch(schema)?;
    initialize(&tx)?;
    tx.pragma_update(None, "application_id", application_id)?;
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    Ok(())
}
