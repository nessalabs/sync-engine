//! Host-registered logical history retention under the source writer transaction.

use super::*;
use crate::replication::domain::{
    Checkpoint, RetentionError, RetentionKind, RetentionPin, RetentionPosition, RetentionScope,
    RetentionWindow,
};

const READ_HISTORY: &str =
    "SELECT pruned_through, retention_now_ms FROM source_history WHERE singleton = 1";
const READ_PIN: &str = "SELECT revision, receiver, access_epoch, completed, expires_at_ms
    FROM source_retention_pins WHERE holder = ?1";

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    tx.execute_batch(
        "ALTER TABLE source_history ADD COLUMN retention_now_ms INTEGER NOT NULL DEFAULT 0
             CHECK (retention_now_ms >= 0);
         CREATE TABLE source_retention_pins (
             holder TEXT PRIMARY KEY NOT NULL,
             receiver TEXT NOT NULL,
             access_epoch TEXT NOT NULL,
             revision INTEGER NOT NULL CHECK (revision > 0),
             completed INTEGER NOT NULL CHECK (completed >= 0),
             expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms > 0)
         ) WITHOUT ROWID;",
    )
}

pub(super) fn verify(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    history(conn)?;
    conn.prepare(READ_PIN)?;
    Ok(())
}

/// Exact durable pin version. This identifies a refresh target, not authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordPinReceipt {
    holder: Id,
    scope: Scope,
    revision: i64,
}

/// Host retention operation was refused or its SQLite result is not known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordRetentionError {
    /// The checkpoint or requested scope does not identify this exact source/pin.
    ScopeMismatch,
    /// The position is outside the committed and still readable interval.
    PositionOutsideHistory,
    /// The same holder already has a nonexpired registration.
    AlreadyRegistered,
    /// This receipt no longer names the current durable pin.
    StaleReceipt,
    /// The current receipt expired. A new explicit registration is required.
    Expired,
    /// Refresh would rewind the holder's durable acknowledged checkpoint.
    PositionRegressed,
    /// Lease expiry was not after the supplied time, or time is out of range.
    InvalidTime,
    /// Supplied time is earlier than the source's durable retention time.
    ClockRegressed,
    /// Source head changed after the host computed its proposed cutoffs.
    HeadChanged,
    /// The requested pure retention bounds are invalid.
    Policy(RetentionError),
    /// The holder's pin revision cannot advance.
    RevisionExhausted,
    /// SQLite failed or commit certainty was lost. Reload before retrying.
    Store(StoreError),
}

use RecordRetentionError as Error;

struct StoredPin {
    revision: i64,
    receiver: String,
    access_epoch: String,
    completed: u64,
    expires_at_ms: i64,
}

impl StoredPin {
    fn matches(&self, scope: &Scope) -> bool {
        self.receiver == scope.receiver().as_str()
            && self.access_epoch == scope.access_epoch().as_str()
    }
}

fn history(conn: &rusqlite::Connection) -> rusqlite::Result<(u64, i64)> {
    conn.query_row(READ_HISTORY, [], |row| {
        Ok((sqlite_u64(row, 0)?, row.get(1)?))
    })
}

fn pin(conn: &rusqlite::Connection, holder: &Id) -> Result<Option<StoredPin>, Error> {
    conn.query_row(READ_PIN, [holder.as_str()], |row| {
        Ok(StoredPin {
            revision: row.get(0)?,
            receiver: row.get(1)?,
            access_epoch: row.get(2)?,
            completed: sqlite_u64(row, 3)?,
            expires_at_ms: row.get(4)?,
        })
    })
    .optional()
    .map_err(failed)
}

fn sqlite_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    value.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
}

fn sqlite_i64(position: u64) -> Result<i64, Error> {
    position
        .try_into()
        .map_err(|_| Error::PositionOutsideHistory)
}

fn failed(_: rusqlite::Error) -> Error {
    Error::Store(StoreError::Failed)
}

fn commit(tx: rusqlite::Transaction<'_>) -> Result<(), Error> {
    tx.commit().map_err(|_| Error::Store(StoreError::Uncertain))
}

fn writable_head(tx: &rusqlite::Transaction<'_>) -> Result<u64, Error> {
    let publication = publication::read(tx).map_err(failed)?;
    publication
        .writer()
        .map_err(|_| Error::Store(StoreError::Failed))?;
    Ok(publication.position)
}

fn time_ms(value: u64) -> Result<i64, Error> {
    value.try_into().map_err(|_| Error::InvalidTime)
}

fn tick(tx: &rusqlite::Transaction<'_>, now: i64) -> Result<u64, Error> {
    let (floor, last) = history(tx).map_err(failed)?;
    if now < last {
        return Err(Error::ClockRegressed);
    }
    if now > last {
        tx.execute(
            "UPDATE source_history SET retention_now_ms = ?1 WHERE singleton = 1",
            [now],
        )
        .map_err(failed)?;
    }
    Ok(floor)
}

fn write_floor(tx: &rusqlite::Transaction<'_>, position: u64) -> Result<(), Error> {
    tx.execute(
        "UPDATE source_history SET pruned_through = ?1 WHERE singleton = 1",
        [sqlite_i64(position)?],
    )
    .map_err(failed)?;
    Ok(())
}

impl SqliteReferenceSource {
    /// Explicitly register a pin or replace an expired registration.
    ///
    /// The trusted host authenticates the holder, validates current access, and
    /// supplies an acknowledged durable checkpoint and Unix milliseconds.
    /// An active holder registration cannot be overwritten; refresh it by receipt.
    pub fn register_retention_pin(
        &mut self,
        holder: &Id,
        checkpoint: &Checkpoint,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RecordPinReceipt, Error> {
        self.update_pin(holder, checkpoint, None, now_ms, expires_at_ms)
    }

    /// Advance a still-live pin using its exact current durable receipt.
    ///
    /// A stale receipt, expired lease, mismatched grant, or a regressed
    /// acknowledgement cannot refresh the registration. Authorization is
    /// always revalidated by the caller, not implied by the receipt.
    pub fn refresh_retention_pin(
        &mut self,
        receipt: &RecordPinReceipt,
        checkpoint: &Checkpoint,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RecordPinReceipt, Error> {
        if checkpoint.scope() != &receipt.scope {
            return Err(Error::ScopeMismatch);
        }
        self.update_pin(
            &receipt.holder,
            checkpoint,
            Some(receipt.revision),
            now_ms,
            expires_at_ms,
        )
    }

    /// Reload the exact durable receipt for a host-authorized holder and scope.
    ///
    /// Returned versions include expired registrations and prove neither
    /// liveness nor ownership of a prior uncertain update.
    pub fn retention_pin_receipt(
        &self,
        holder: &Id,
        scope: &Scope,
    ) -> Result<Option<RecordPinReceipt>, Error> {
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        match pin(&self.conn, holder)? {
            Some(saved) if saved.matches(scope) => Ok(Some(RecordPinReceipt {
                holder: holder.clone(),
                scope: scope.clone(),
                revision: saved.revision,
            })),
            Some(_) => Err(Error::ScopeMismatch),
            None => Ok(None),
        }
    }

    fn update_pin(
        &mut self,
        holder: &Id,
        checkpoint: &Checkpoint,
        expected: Option<i64>,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RecordPinReceipt, Error> {
        let scope = checkpoint.scope();
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        let (now, expires) = (time_ms(now_ms)?, time_ms(expires_at_ms)?);
        if expires <= now {
            return Err(Error::InvalidTime);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let head = writable_head(&tx)?;
        let floor = tick(&tx, now)?;
        let position = checkpoint.position();
        let saved = pin(&tx, holder)?;
        match (expected, &saved) {
            (Some(revision), Some(saved)) if revision == saved.revision => {
                if !saved.matches(scope) {
                    return Err(Error::ScopeMismatch);
                }
                if saved.expires_at_ms <= now {
                    // Record the observed expiry so a clock rollback cannot
                    // make this unchanged receipt live after a reopen.
                    commit(tx)?;
                    return Err(Error::Expired);
                }
                if position < saved.completed {
                    return Err(Error::PositionRegressed);
                }
            }
            (Some(_), _) => return Err(Error::StaleReceipt),
            (None, Some(saved)) if saved.expires_at_ms > now => {
                return Err(Error::AlreadyRegistered);
            }
            (None, _) => {}
        }
        if !(floor..=head).contains(&position) {
            return Err(Error::PositionOutsideHistory);
        }
        let revision = saved
            .map_or(0, |saved| saved.revision)
            .checked_add(1)
            .ok_or(Error::RevisionExhausted)?;
        tx.execute(
            "INSERT INTO source_retention_pins
                (holder, receiver, access_epoch, revision, completed, expires_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(holder) DO UPDATE SET receiver=excluded.receiver,
                access_epoch=excluded.access_epoch, revision=excluded.revision,
                completed=excluded.completed, expires_at_ms=excluded.expires_at_ms",
            params![
                holder.as_str(),
                scope.receiver().as_str(),
                scope.access_epoch().as_str(),
                revision,
                sqlite_i64(position)?,
                expires,
            ],
        )
        .map_err(failed)?;
        commit(tx)?;
        Ok(RecordPinReceipt {
            holder: holder.clone(),
            scope: scope.clone(),
            revision,
        })
    }

    /// Advance the logical record floor from a host-selected retention policy.
    ///
    /// Compare the observed head that informed the host's cutoffs, then check
    /// active registrations and publish the new floor in the same transaction.
    /// Forced pressure may override pins. No physical records are deleted.
    pub fn prune_with_retention(
        &mut self,
        scope: &Scope,
        expected_head: RetentionPosition,
        preferred: RetentionPosition,
        forced: Option<RetentionPosition>,
        now_ms: u64,
    ) -> Result<RetentionPosition, Error> {
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        let now = time_ms(now_ms)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let head = RetentionPosition::new(writable_head(&tx)?);
        if head != expected_head {
            return Err(Error::HeadChanged);
        }
        let floor = tick(&tx, now)?;
        let window = RetentionWindow::new(head, RetentionPosition::new(floor), preferred, forced)
            .map_err(Error::Policy)?;
        let retention_scope = RetentionScope::from_scope(RetentionKind::Records, scope);
        let pins = {
            let mut query = tx
                .prepare("SELECT holder, completed, expires_at_ms FROM source_retention_pins WHERE expires_at_ms > ?1")
                .map_err(failed)?;
            let rows = query
                .query_map([now], |row| {
                    let holder: String = row.get(0)?;
                    let position = sqlite_u64(row, 1)?;
                    let expires = sqlite_u64(row, 2)?;
                    Ok((holder, position, expires))
                })
                .map_err(failed)?;
            rows.map(|row| {
                let (holder, completed, expires) = row.map_err(failed)?;
                Ok(RetentionPin::for_completed(
                    Id::new(holder).map_err(|_| Error::Store(StoreError::Failed))?,
                    retention_scope.clone(),
                    RetentionPosition::new(completed),
                    expires,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?
        };
        let position = window
            .cutoff(&retention_scope, now_ms, &pins)
            .map_err(Error::Policy)?;
        if position.get() != floor {
            write_floor(&tx, position.get())?;
        }
        commit(tx)?;
        Ok(position)
    }

    /// Legacy fixture-only logical prune for a source that has never had a pin.
    ///
    /// After registration, explicit host policy and time are required; this
    /// entry point cannot bypass the pin ledger, even when all pins expired.
    pub fn prune_through(&mut self, position: u64) -> Result<(), StoreError> {
        fn bare(source: &mut SqliteReferenceSource, position: u64) -> Result<(), Error> {
            let tx = source
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failed)?;
            let head = writable_head(&tx)?;
            let (floor, _) = history(&tx).map_err(failed)?;
            let registered: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_retention_pins)",
                    [],
                    |row| row.get(0),
                )
                .map_err(failed)?;
            if registered || !(floor..=head).contains(&position) {
                return Err(Error::PositionOutsideHistory);
            }
            write_floor(&tx, position)?;
            commit(tx)
        }
        bare(self, position).map_err(|error| match error {
            Error::Store(error) => error,
            _ => StoreError::Failed,
        })
    }
}
