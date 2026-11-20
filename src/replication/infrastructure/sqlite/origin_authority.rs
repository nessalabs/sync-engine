//! Local reference claim ledger. Stores no source records or receiver progress.

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};

use super::connection::{configure_connection, ensure_schema, SqliteOpenError};
use crate::replication::origin_epoch::{OriginAuthorityKey, OriginEpoch};

const APPLICATION_ID: i64 = 0x4e53_5347;
const SCHEMA: &str = "
CREATE TABLE origin_epochs (
    authority TEXT PRIMARY KEY NOT NULL,
    epoch BLOB NOT NULL CHECK (
        typeof(epoch) = 'blob' AND length(epoch) = 8 AND epoch != X'0000000000000000'
    )
) WITHOUT ROWID;
";
const SELECT_EPOCH: &str = "SELECT epoch FROM origin_epochs WHERE authority = ?1";

/// The reference authority could not be explicitly provisioned.
#[derive(Debug)]
pub enum OriginProvisionError {
    /// Exclusive file creation failed, including an already-existing path.
    Create(io::Error),
    /// SQLite initialization or verification failed.
    Database(SqliteOpenError),
}

impl std::fmt::Display for OriginProvisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(error) => write!(f, "cannot provision authority: {error}"),
            Self::Database(error) => write!(f, "cannot initialize authority: {error}"),
        }
    }
}

impl std::error::Error for OriginProvisionError {}

/// An authority observation or claim cannot be used as a successful claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginClaimError {
    /// Another claim won, or the caller's expected presence/absence was wrong.
    Stale {
        /// Epoch observed inside the same serialization boundary.
        actual: Option<OriginEpoch>,
    },
    /// The current valid epoch cannot be advanced without wrapping.
    Exhausted,
    /// The authority could not be read or changed before commit.
    Unavailable,
    /// The result of commit is not proven. Reload to reconcile, not blindly retry.
    Uncertain,
}

/// Atomic writer-claim reference for one trusted, shared local SQLite file.
///
/// Hosts own secure path selection, key authentication, and sharing. The file
/// must not be restored with source data. Separate databases do not coordinate.
/// Opening never creates a missing ledger. This adapter does not check a source
/// read/write, arbitrate serving, or admit any receiver frame.
pub struct SqliteOriginAuthority {
    conn: Connection,
}

impl SqliteOriginAuthority {
    /// Exclusively create and initialize a new ledger. Never call this to
    /// recover lost authority; that decision belongs to the host. Initialization
    /// failure can leave a file; normal open will still refuse incomplete setup.
    pub fn provision(path: impl AsRef<Path>) -> Result<Self, OriginProvisionError> {
        let path = path.as_ref();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(OriginProvisionError::Create)?;
        let mut conn = open_existing(path).map_err(OriginProvisionError::Database)?;
        ensure_schema(&mut conn, APPLICATION_ID, SCHEMA, |_| Ok(()))
            .map_err(OriginProvisionError::Database)?;
        Self::verify(&conn).map_err(OriginProvisionError::Database)?;
        configure_connection(&conn).map_err(OriginProvisionError::Database)?;
        Ok(Self { conn })
    }

    /// Open a provisioned ledger without creating or repairing the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteOpenError> {
        let conn = open_existing(path.as_ref())?;
        Self::verify(&conn)?;
        configure_connection(&conn)?;
        Ok(Self { conn })
    }

    fn verify(conn: &Connection) -> Result<(), SqliteOpenError> {
        let id: i64 = conn.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if id != APPLICATION_ID || version != 1 {
            return Err(SqliteOpenError::UnexpectedDatabase);
        }
        conn.prepare(SELECT_EPOCH)?;
        Ok(())
    }

    /// Observe the committed epoch for this key, not proof of claim ownership.
    pub fn current(
        &self,
        key: &OriginAuthorityKey,
    ) -> Result<Option<OriginEpoch>, OriginClaimError> {
        read(&self.conn, key)
    }

    // The only production caller performs one bounded local source-head read.
    // Acquire authority before source; never use this guard around transport,
    // or around a source operation that itself waits for authority.
    pub(super) fn with_current_claim<T>(
        &mut self,
        key: &OriginAuthorityKey,
        claimed: OriginEpoch,
        capture: impl FnOnce() -> T,
    ) -> Result<T, OriginClaimError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OriginClaimError::Unavailable)?;
        let actual = read(&tx, key)?;
        if actual != Some(claimed) {
            return Err(OriginClaimError::Stale { actual });
        }
        let result = capture();
        // This guard never mutates the ledger; fail closed if release fails.
        tx.rollback().map_err(|_| OriginClaimError::Unavailable)?;
        Ok(result)
    }

    /// Compare the exact expected epoch (or unclaimed state) and commit one
    /// successor under SQLite's writer lock. Use only for a host-authorized
    /// claim; the compare-and-set is not authentication.
    pub fn compare_and_claim(
        &mut self,
        key: &OriginAuthorityKey,
        expected: Option<OriginEpoch>,
    ) -> Result<OriginEpoch, OriginClaimError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OriginClaimError::Unavailable)?;
        let actual = read(&tx, key)?;
        if actual != expected {
            return Err(OriginClaimError::Stale { actual });
        }
        let epoch = match actual {
            Some(previous) => previous.successor().ok_or(OriginClaimError::Exhausted)?,
            None => OriginEpoch::FIRST,
        };
        tx.execute(
            "INSERT INTO origin_epochs (authority, epoch) VALUES (?1, ?2)
             ON CONFLICT(authority) DO UPDATE SET epoch = excluded.epoch",
            (key.id().as_str(), epoch.get().to_be_bytes()),
        )
        .map_err(|_| OriginClaimError::Unavailable)?;
        tx.commit().map_err(|_| OriginClaimError::Uncertain)?;
        Ok(epoch)
    }
}

fn open_existing(path: &Path) -> Result<Connection, SqliteOpenError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

fn read(
    conn: &Connection,
    key: &OriginAuthorityKey,
) -> Result<Option<OriginEpoch>, OriginClaimError> {
    let raw: Option<[u8; 8]> = conn
        .query_row(SELECT_EPOCH, [key.id().as_str()], |row| row.get(0))
        .optional()
        .map_err(|_| OriginClaimError::Unavailable)?;
    raw.map(|bytes| {
        OriginEpoch::new(u64::from_be_bytes(bytes)).ok_or(OriginClaimError::Unavailable)
    })
    .transpose()
}

#[cfg(test)]
#[path = "../../../../tests/replication/infrastructure/origin_claim_guard.rs"]
mod claim_guard_tests;
