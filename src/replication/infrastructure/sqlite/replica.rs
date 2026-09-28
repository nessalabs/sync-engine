//! SQLite receiver store. Each `apply` uses one immediate transaction so a
//! competing connection cannot advance records separately from its checkpoint.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::replication::application::{ReplicaStore, StoreError};
use crate::replication::domain::{Checkpoint, CommitPlan, Id, Record, Scope};

use super::{
    configure_connection, ensure_schema, open_connection, SqliteOpenError, REPLICA_APPLICATION_ID,
};

const MAX_READ_RECORDS: usize = 256;
const MAX_READ_BYTES: usize = 1024 * 1024;

const SCHEMA: &str = "
CREATE TABLE replica_streams (
    receiver TEXT NOT NULL,
    origin TEXT NOT NULL,
    stream TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    access_epoch TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (receiver, origin, stream)
) WITHOUT ROWID;
CREATE TABLE replica_records (
    receiver TEXT NOT NULL,
    origin TEXT NOT NULL,
    stream TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position > 0),
    record_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (receiver, origin, stream, position),
    UNIQUE (receiver, origin, stream, record_id),
    FOREIGN KEY (receiver, origin, stream)
        REFERENCES replica_streams (receiver, origin, stream)
) WITHOUT ROWID;
";

/// One independently opened SQLite receiver handle. A second handle can apply
/// a stale plan, but cannot interleave halfway through a transaction.
pub struct SqliteReplicaStore {
    conn: Connection,
}

impl SqliteReplicaStore {
    /// Opens or creates a receiver database without resetting existing data.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        ensure_schema(&mut conn, REPLICA_APPLICATION_ID, SCHEMA, |_| Ok(()))?;
        configure_connection(&conn)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { conn })
    }

    /// Reads the saved checkpoint for one receiver and source stream, even if
    /// its incarnation, schema, or access epoch differs from the supplied scope.
    pub fn checkpoint(&self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        saved_checkpoint(&self.conn, scope)
    }

    /// Reads at most one bounded cached page without opening the source. A
    /// caller can page through a transcript using the last returned position.
    /// Record and total payload lengths are checked before fetching BLOBs.
    pub fn read_after(
        &mut self,
        scope: &Scope,
        after: u64,
        max_records: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<Record>, StoreError> {
        if max_records == 0 || max_payload_bytes == 0 {
            return Err(StoreError::Failed);
        }
        let after = i64::try_from(after).map_err(|_| StoreError::Failed)?;
        let limit =
            i64::try_from(max_records.min(MAX_READ_RECORDS)).map_err(|_| StoreError::Failed)?;
        let byte_limit = max_payload_bytes.min(MAX_READ_BYTES);
        let tx = self.conn.transaction().map_err(|_| StoreError::Failed)?;
        let saved_position = match saved_checkpoint(&tx, scope)? {
            None => return Ok(Vec::new()),
            Some(saved) if saved.scope() != scope => {
                return Err(StoreError::ScopeMismatch {
                    saved: Box::new(saved.scope().clone()),
                    requested: Box::new(scope.clone()),
                });
            }
            Some(saved) => saved.position(),
        };
        if u64::try_from(after).map_err(|_| StoreError::Failed)? >= saved_position {
            return Ok(Vec::new());
        }
        let metadata: Vec<(i64, i64, i64)> = {
            let mut statement = tx
                .prepare("SELECT position, length(record_id), length(payload) FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND position > ?4 AND position <= ?5 ORDER BY position LIMIT ?6")
                .map_err(|_| StoreError::Failed)?;
            let rows = statement
                .query_map(
                    params![
                        scope.receiver().as_str(),
                        scope.origin().as_str(),
                        scope.stream().as_str(),
                        after,
                        i64::try_from(saved_position).map_err(|_| StoreError::Failed)?,
                        limit
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|_| StoreError::Failed)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| StoreError::Failed)?;
            rows
        };
        if metadata.is_empty() {
            return Err(StoreError::Failed);
        }
        let mut records = Vec::with_capacity(metadata.len());
        let mut bytes = 0usize;
        let mut expected = after;
        for (position, id_bytes, payload_bytes) in metadata {
            expected = expected.checked_add(1).ok_or(StoreError::Failed)?;
            if position != expected {
                return Err(StoreError::Failed);
            }
            let len = usize::try_from(payload_bytes).map_err(|_| StoreError::Failed)?;
            if !(1..=128).contains(&id_bytes) || len > MAX_READ_BYTES {
                return Err(StoreError::Failed);
            }
            let next = bytes.checked_add(len).ok_or(StoreError::Failed)?;
            if next > byte_limit {
                if records.is_empty() {
                    return Err(StoreError::Failed);
                }
                break;
            }
            let (raw_id, payload): (String, Vec<u8>) = tx
                .query_row(
                    "SELECT record_id, payload FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND position = ?4",
                    params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), position],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| StoreError::Failed)?;
            if payload.len() != len {
                return Err(StoreError::Failed);
            }
            records.push(Record {
                position: u64::try_from(position).map_err(|_| StoreError::Failed)?,
                id: Id::new(raw_id).map_err(|_| StoreError::Failed)?,
                scope: scope.clone(),
                payload,
            });
            bytes = next;
        }
        Ok(records)
    }

    /// Counts cached records for verification; normal apply and bounded reads
    /// never use this whole-stream query.
    pub fn count(&self, scope: &Scope) -> Result<u64, StoreError> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()],
                |row| row.get(0),
            )
            .map_err(|_| StoreError::Failed)?;
        u64::try_from(count).map_err(|_| StoreError::Failed)
    }
}

impl ReplicaStore for SqliteReplicaStore {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        let saved = self.checkpoint(scope)?;
        if let Some(checkpoint) = &saved {
            if checkpoint.scope() != scope {
                return Err(StoreError::ScopeMismatch {
                    saved: Box::new(checkpoint.scope().clone()),
                    requested: Box::new(scope.clone()),
                });
            }
        }
        Ok(saved)
    }

    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StoreError::Failed)?;
        let scope = plan.expected().scope();
        let saved = saved_checkpoint(&tx, scope)?;
        let current = saved.unwrap_or_else(|| Checkpoint::new(scope.clone(), 0));
        if current.scope() != scope {
            return Err(StoreError::ScopeMismatch {
                saved: Box::new(current.scope().clone()),
                requested: Box::new(scope.clone()),
            });
        }
        if current.position() >= plan.next().position() {
            if exact_saved_slice(&tx, &plan)? {
                return Ok(());
            }
            if has_conflicting_id(&tx, &plan)? {
                return Err(StoreError::ConflictingRecord);
            }
            return Err(StoreError::Stale);
        }
        if current != *plan.expected() {
            return Err(StoreError::Stale);
        }
        if has_conflicting_id(&tx, &plan)? {
            return Err(StoreError::ConflictingRecord);
        }
        let next = i64::try_from(plan.next().position()).map_err(|_| StoreError::Failed)?;
        if current.position() == 0 {
            tx.execute(
                "INSERT OR IGNORE INTO replica_streams (receiver, origin, stream, incarnation, schema_id, access_epoch, position) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str()],
            )
            .map_err(|_| StoreError::Failed)?;
        }
        for record in plan.records() {
            let position = i64::try_from(record.position).map_err(|_| StoreError::Failed)?;
            tx.execute(
                "INSERT INTO replica_records (receiver, origin, stream, position, record_id, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), position, record.id.as_str(), record.payload],
            )
            .map_err(|_| StoreError::Failed)?;
        }
        let updated = tx
            .execute(
                "UPDATE replica_streams SET position = ?1 WHERE receiver = ?2 AND origin = ?3 AND stream = ?4 AND incarnation = ?5 AND schema_id = ?6 AND access_epoch = ?7 AND position = ?8",
                params![next, scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str(), i64::try_from(plan.expected().position()).map_err(|_| StoreError::Failed)?],
            )
            .map_err(|_| StoreError::Failed)?;
        if updated != 1 {
            return Err(StoreError::Stale);
        }
        tx.commit().map_err(|_| StoreError::Uncertain)
    }
}

fn saved_checkpoint(conn: &Connection, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
    let saved: Option<(String, String, String, i64)> = conn
        .query_row(
            "SELECT incarnation, schema_id, access_epoch, position FROM replica_streams WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|_| StoreError::Failed)?;
    saved
        .map(|(incarnation, schema, access_epoch, position)| {
            let stored_scope = Scope::new(
                scope.receiver().clone(),
                scope.origin().clone(),
                scope.stream().clone(),
                Id::new(incarnation).map_err(|_| StoreError::Failed)?,
                Id::new(schema).map_err(|_| StoreError::Failed)?,
                Id::new(access_epoch).map_err(|_| StoreError::Failed)?,
            );
            Ok(Checkpoint::new(
                stored_scope,
                u64::try_from(position).map_err(|_| StoreError::Failed)?,
            ))
        })
        .transpose()
}

fn exact_saved_slice(conn: &Connection, plan: &CommitPlan) -> Result<bool, StoreError> {
    let scope = plan.expected().scope();
    for record in plan.records() {
        let position = i64::try_from(record.position).map_err(|_| StoreError::Stale)?;
        let saved: Option<(String, i64)> = conn
            .query_row(
                "SELECT record_id, length(payload) FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND position = ?4",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), position],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StoreError::Failed)?;
        if saved.as_ref().is_none_or(|(id, len)| {
            id != record.id.as_str() || usize::try_from(*len).ok() != Some(record.payload.len())
        }) {
            return Ok(false);
        }
        let payload: Vec<u8> = conn
            .query_row(
                "SELECT payload FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND position = ?4",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), position],
                |row| row.get(0),
            )
            .map_err(|_| StoreError::Failed)?;
        if payload != record.payload {
            return Ok(false);
        }
    }
    Ok(true)
}

fn has_conflicting_id(conn: &Connection, plan: &CommitPlan) -> Result<bool, StoreError> {
    let scope = plan.expected().scope();
    for record in plan.records() {
        let saved: Option<(i64, i64)> = conn
            .query_row(
                "SELECT position, length(payload) FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND record_id = ?4",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), record.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StoreError::Failed)?;
        if let Some((position, len)) = saved {
            if u64::try_from(position).ok() != Some(record.position)
                || usize::try_from(len).ok() != Some(record.payload.len())
            {
                return Ok(true);
            }
            let payload: Vec<u8> = conn
                .query_row(
                    "SELECT payload FROM replica_records WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND record_id = ?4",
                    params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), record.id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| StoreError::Failed)?;
            if payload != record.payload {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
