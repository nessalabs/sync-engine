//! Host-registered logical catalogue retention under the source writer transaction.

use super::*;
use crate::replication::domain::{
    RetentionError, RetentionKind, RetentionPin, RetentionPosition, RetentionScope, RetentionWindow,
};

const READ_HISTORY: &str =
    "SELECT horizon, retention_now_ms FROM catalogue_source_meta WHERE singleton = 1";
const READ_PIN: &str = "SELECT revision, receiver, access_epoch, completed, expires_at_ms
    FROM catalogue_retention_pins WHERE holder = ?1";

pub(super) fn migrate(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    tx.execute_batch(
        "ALTER TABLE catalogue_source_meta ADD COLUMN retention_now_ms INTEGER NOT NULL DEFAULT 0
             CHECK (retention_now_ms >= 0);
         CREATE TABLE catalogue_retention_pins (
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
pub struct CataloguePinReceipt {
    holder: Id,
    scope: Scope,
    revision: i64,
}

/// Host retention operation was refused or its SQLite result is not known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogueRetentionError {
    /// The progress or requested scope does not identify this exact source/pin.
    ScopeMismatch,
    /// The completed revision is outside the committed catalogue coverage.
    PositionOutsideHistory,
    /// The same holder already has a nonexpired registration.
    AlreadyRegistered,
    /// This receipt no longer names the current durable pin.
    StaleReceipt,
    /// The current receipt expired. A new explicit registration is required.
    Expired,
    /// Refresh would rewind the holder's durable acknowledged progress.
    PositionRegressed,
    /// Lease expiry was not after the supplied time, or time is out of range.
    InvalidTime,
    /// Supplied time is earlier than the source's durable retention time.
    ClockRegressed,
    /// Supplied receiver progress is internally contradictory.
    InvalidProgress,
    /// The exact catalogue publication or collection was refused.
    Publication(CataloguePublicationError),
    /// The requested pure retention bounds are invalid.
    Policy(RetentionError),
    /// The holder's pin revision cannot advance.
    RevisionExhausted,
    /// SQLite failed or commit certainty was lost. Reload before retrying.
    Store(CatalogueStoreError),
}

use CatalogueRetentionError as Error;

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
    Error::Store(CatalogueStoreError::Failed)
}

fn commit(tx: rusqlite::Transaction<'_>) -> Result<(), Error> {
    tx.commit()
        .map_err(|_| Error::Store(CatalogueStoreError::Uncertain))
}

fn writable_snapshot(
    tx: &rusqlite::Transaction<'_>,
    scope: &Scope,
) -> Result<CatalogueHorizonHead, Error> {
    let (publication, horizon) = publication::read_snapshot(tx).map_err(failed)?;
    publication
        .writer()
        .map_err(|e| Error::Publication(CataloguePublicationError::Incompatible(e)))?;
    Ok(CatalogueHorizonHead::new(publication.head(scope), horizon))
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
            "UPDATE catalogue_source_meta SET retention_now_ms = ?1 WHERE singleton = 1",
            [now],
        )
        .map_err(failed)?;
    }
    Ok(floor)
}

pub(super) fn has_registration(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM catalogue_retention_pins)",
        [],
        |row| row.get(0),
    )
}

impl SqliteCatalogueSource {
    /// Explicitly register a pin or replace an expired registration.
    ///
    /// The trusted host authenticates the holder, validates current access, and
    /// supplies actual durable receiver progress and Unix milliseconds. Only
    /// `completed` is acknowledged: active boundaries/cursors grant nothing.
    /// An active holder registration cannot be overwritten; refresh it by receipt.
    pub fn register_catalogue_retention_pin(
        &mut self,
        holder: &Id,
        progress: &CatalogueProgress,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<CataloguePinReceipt, Error> {
        self.update_pin(holder, progress, None, now_ms, expires_at_ms)
    }

    /// Advance a still-live pin using its exact current durable receipt.
    ///
    /// A stale receipt, expired lease, mismatched grant, or a regressed
    /// acknowledgement cannot refresh the registration. Authorization is
    /// always revalidated by the caller, not implied by the receipt.
    pub fn refresh_catalogue_retention_pin(
        &mut self,
        receipt: &CataloguePinReceipt,
        progress: &CatalogueProgress,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<CataloguePinReceipt, Error> {
        if progress.scope != receipt.scope {
            return Err(Error::ScopeMismatch);
        }
        self.update_pin(
            &receipt.holder,
            progress,
            Some(receipt.revision),
            now_ms,
            expires_at_ms,
        )
    }

    /// Reload the exact durable receipt for a host-authorized holder and scope.
    ///
    /// Returned versions include expired registrations and prove neither
    /// liveness nor ownership of a prior uncertain update.
    pub fn catalogue_retention_pin_receipt(
        &self,
        holder: &Id,
        scope: &Scope,
    ) -> Result<Option<CataloguePinReceipt>, Error> {
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        match pin(&self.conn, holder)? {
            Some(saved) if saved.matches(scope) => Ok(Some(CataloguePinReceipt {
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
        progress: &CatalogueProgress,
        expected: Option<i64>,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<CataloguePinReceipt, Error> {
        let scope = &progress.scope;
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        validate_catalogue_progress(progress).map_err(|_| Error::InvalidProgress)?;
        let (now, expires) = (time_ms(now_ms)?, time_ms(expires_at_ms)?);
        if expires <= now {
            return Err(Error::InvalidTime);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let head = writable_snapshot(&tx, scope)?.source.position();
        let floor = tick(&tx, now)?;
        let position = progress.completed;
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
            "INSERT INTO catalogue_retention_pins
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
        Ok(CataloguePinReceipt {
            holder: holder.clone(),
            scope: scope.clone(),
            revision,
        })
    }

    /// Advance the logical catalogue horizon using active durable pins.
    ///
    /// The host owns time and cutoffs. Forced pressure may override pins, but
    /// does not delete identities, reclaim bytes, or mutate receiver progress.
    pub fn collect_with_retention(
        &mut self,
        expected: &CatalogueHorizonHead,
        preferred: RetentionPosition,
        forced: Option<RetentionPosition>,
        now_ms: u64,
    ) -> Result<CatalogueCollection, Error> {
        let scope = expected.source.scope();
        if !self.matches(scope) {
            return Err(Error::ScopeMismatch);
        }
        let now = time_ms(now_ms)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        let current = writable_snapshot(&tx, scope)?;
        if &current != expected {
            return Err(Error::Publication(CataloguePublicationError::Stale));
        }
        let floor = tick(&tx, now)?;
        let window = RetentionWindow::new(
            RetentionPosition::new(current.source.position()),
            RetentionPosition::new(floor),
            preferred,
            forced,
        )
        .map_err(Error::Policy)?;
        let retention_scope = RetentionScope::from_scope(RetentionKind::Catalogue, scope);
        let pins = {
            let mut query = tx
                .prepare("SELECT holder, completed, expires_at_ms FROM catalogue_retention_pins WHERE expires_at_ms > ?1")
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
                    Id::new(holder).map_err(|_| Error::Store(CatalogueStoreError::Failed))?,
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
        let receipt = publication::collect_in_transaction(&tx, expected, position.get())
            .map_err(Error::Publication)?;
        commit(tx)?;
        Ok(receipt)
    }
}
