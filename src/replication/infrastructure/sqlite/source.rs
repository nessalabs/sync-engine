//! File-backed, append-only reference source for the restartable transcript lab.
//! This is an illustrative `RecordSource`, not Nessa's canonical event owner.

use std::path::Path;

use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::replication::application::{RecordSource, SourceError, StoreError};
use crate::replication::domain::{Id, Page, PageRequest, Record, Scope};
use crate::replication::history::{
    HistorySource, HistorySourceError, OlderPage, OlderRequest, TailRequest, TailSnapshot,
};

use super::{
    configure_connection, ensure_schema, open_connection, SqliteOpenError, SOURCE_APPLICATION_ID,
};

const MAX_PAGE_RECORDS: usize = 256;
const MAX_PAGE_BYTES: usize = 1024 * 1024;

const SCHEMA: &str = "
CREATE TABLE source_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    origin TEXT NOT NULL,
    stream TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    head INTEGER NOT NULL CHECK (head >= 0)
);
CREATE TABLE source_records (
    position INTEGER PRIMARY KEY CHECK (position > 0),
    record_id TEXT NOT NULL UNIQUE,
    payload BLOB NOT NULL
);
CREATE TABLE source_history (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    pruned_through INTEGER NOT NULL CHECK (pruned_through >= 0)
);
";

/// A SQLite reference source with immutable stream identity and indexed records.
/// Each handle owns one connection; callers may open competing handles to the
/// same file. Append commits before a new head can be advertised.
pub struct SqliteReferenceSource {
    conn: rusqlite::Connection,
    origin: Id,
    stream: Id,
    incarnation: Id,
    schema: Id,
    head_reads: u64,
    page_reads: u64,
    payload_bytes: u64,
    tail_reads: u64,
    older_reads: u64,
    history_payload_bytes: u64,
}

impl SqliteReferenceSource {
    /// Opens or creates a source file. Existing identity is never reset or
    /// silently replaced, and no user-selected path is deleted.
    pub fn open(
        path: impl AsRef<Path>,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
    ) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        ensure_schema(&mut conn, SOURCE_APPLICATION_ID, SCHEMA, |tx| {
            tx.execute(
                "INSERT INTO source_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
                params![
                    origin.as_str(),
                    stream.as_str(),
                    incarnation.as_str(),
                    schema.as_str()
                ],
            )?;
            Ok(())
        })?;
        let saved: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT origin, stream, incarnation, schema_id FROM source_meta WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        match saved {
            Some((o, s, i, k))
                if o == origin.as_str()
                    && s == stream.as_str()
                    && i == incarnation.as_str()
                    && k == schema.as_str() => {}
            Some(_) => return Err(SqliteOpenError::SourceIdentityMismatch),
            None => return Err(SqliteOpenError::UnexpectedDatabase),
        }
        configure_connection(&conn)?;
        // Additive reference-source metadata for files created by slice 2.
        // Physical fact rows remain available for immutable ID deduplication;
        // the floor is the historical read contract.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS source_history (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), pruned_through INTEGER NOT NULL CHECK (pruned_through >= 0));
             INSERT OR IGNORE INTO source_history VALUES (1, 0);",
        )?;
        Ok(Self {
            conn,
            origin,
            stream,
            incarnation,
            schema,
            head_reads: 0,
            page_reads: 0,
            payload_bytes: 0,
            tail_reads: 0,
            older_reads: 0,
            history_payload_bytes: 0,
        })
    }

    /// Appends one committed fact. Repeating the same ID and bytes returns its
    /// original position; different bytes under that ID are refused. Payloads
    /// above the reference adapter's one-megabyte record cap are refused.
    pub fn append(&mut self, id: &Id, payload: &[u8]) -> Result<u64, StoreError> {
        if payload.len() > MAX_PAGE_BYTES {
            return Err(StoreError::Failed);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StoreError::Failed)?;
        let existing: Option<(i64, i64)> = tx
            .query_row(
                "SELECT position, length(payload) FROM source_records WHERE record_id = ?1",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| StoreError::Failed)?;
        if let Some((position, length)) = existing {
            if usize::try_from(length).ok() != Some(payload.len()) {
                return Err(StoreError::ConflictingRecord);
            }
            let saved: Vec<u8> = tx
                .query_row(
                    "SELECT payload FROM source_records WHERE record_id = ?1",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| StoreError::Failed)?;
            return if saved == payload {
                u64::try_from(position).map_err(|_| StoreError::Failed)
            } else {
                Err(StoreError::ConflictingRecord)
            };
        }
        let head: i64 = tx
            .query_row(
                "SELECT head FROM source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| StoreError::Failed)?;
        let next = head.checked_add(1).ok_or(StoreError::Failed)?;
        tx.execute(
            "INSERT INTO source_records (position, record_id, payload) VALUES (?1, ?2, ?3)",
            params![next, id.as_str(), payload],
        )
        .map_err(|_| StoreError::Failed)?;
        tx.execute(
            "UPDATE source_meta SET head = ?1 WHERE singleton = 1",
            [next],
        )
        .map_err(|_| StoreError::Failed)?;
        tx.commit().map_err(|_| StoreError::Uncertain)?;
        u64::try_from(next).map_err(|_| StoreError::Failed)
    }

    /// Number of head reads issued through the source port by this handle.
    pub fn head_reads(&self) -> u64 {
        self.head_reads
    }

    /// Number of page reads issued through the source port by this handle.
    pub fn page_reads(&self) -> u64 {
        self.page_reads
    }

    /// Payload bytes returned through the source port by this handle.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Number of bounded recent-tail reads on this handle.
    pub fn tail_reads(&self) -> u64 {
        self.tail_reads
    }

    /// Number of bounded older-history reads on this handle.
    pub fn older_reads(&self) -> u64 {
        self.older_reads
    }

    /// History record payload bytes returned on this handle.
    pub fn history_payload_bytes(&self) -> u64 {
        self.history_payload_bytes
    }

    /// Advances the reference source's historical read floor without deleting
    /// physical records. This preserves immutable record-ID deduplication while
    /// exercising a pruned-range contract. The floor cannot move backwards or
    /// past the committed head.
    pub fn prune_through(&mut self, position: u64) -> Result<(), StoreError> {
        let position = i64::try_from(position).map_err(|_| StoreError::Failed)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StoreError::Failed)?;
        let head: i64 = tx
            .query_row(
                "SELECT head FROM source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| StoreError::Failed)?;
        let floor: i64 = tx
            .query_row(
                "SELECT pruned_through FROM source_history WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| StoreError::Failed)?;
        if position < floor || position > head {
            return Err(StoreError::Failed);
        }
        tx.execute(
            "UPDATE source_history SET pruned_through = ?1 WHERE singleton = 1",
            [position],
        )
        .map_err(|_| StoreError::Failed)?;
        tx.commit().map_err(|_| StoreError::Uncertain)
    }

    fn matches(&self, scope: &Scope) -> bool {
        scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
    }
}

impl RecordSource for SqliteReferenceSource {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.head_reads += 1;
        if !self.matches(scope) {
            return Err(SourceError::IdentityChanged);
        }
        let head: i64 = self
            .conn
            .query_row(
                "SELECT head FROM source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| SourceError::Unavailable)?;
        u64::try_from(head).map_err(|_| SourceError::Unavailable)
    }

    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.page_reads += 1;
        if !self.matches(&request.scope) {
            return Err(SourceError::IdentityChanged);
        }
        if request.max_records == 0
            || request.max_payload_bytes == 0
            || request.max_record_bytes == 0
            || request.after >= request.target
        {
            return Err(SourceError::InvalidRequest);
        }
        let after = i64::try_from(request.after).map_err(|_| SourceError::InvalidRequest)?;
        let target = i64::try_from(request.target).map_err(|_| SourceError::InvalidRequest)?;
        let limit = i64::try_from(request.max_records.min(MAX_PAGE_RECORDS))
            .map_err(|_| SourceError::InvalidRequest)?;
        let byte_limit = request.max_payload_bytes.min(MAX_PAGE_BYTES);
        let record_limit = request.max_record_bytes.min(MAX_PAGE_BYTES);
        // Hold one read snapshot across length preflight and BLOB fetch. No
        // payload is fetched until its declared size fits the page budget.
        let tx = self
            .conn
            .transaction()
            .map_err(|_| SourceError::Unavailable)?;
        let head: i64 = tx
            .query_row(
                "SELECT head FROM source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| SourceError::Unavailable)?;
        let floor: i64 = tx
            .query_row(
                "SELECT pruned_through FROM source_history WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| SourceError::Unavailable)?;
        if after < floor {
            return Err(SourceError::Pruned);
        }
        if target > head {
            return Err(SourceError::InvalidRequest);
        }
        let metadata: Vec<(i64, i64, i64)> = {
            let mut statement = tx
                .prepare("SELECT position, length(record_id), length(payload) FROM source_records WHERE position > ?1 AND position <= ?2 ORDER BY position LIMIT ?3")
                .map_err(|_| SourceError::Unavailable)?;
            let rows = statement
                .query_map(params![after, target, limit], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|_| SourceError::Unavailable)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| SourceError::Unavailable)?;
            rows
        };
        if metadata.is_empty() {
            return Err(SourceError::Pruned);
        }
        let mut records = Vec::with_capacity(metadata.len());
        let mut bytes = 0usize;
        let mut expected = after;
        for (position, id_bytes, payload_bytes) in metadata {
            expected = expected.checked_add(1).ok_or(SourceError::Unavailable)?;
            if position != expected {
                return Err(SourceError::Pruned);
            }
            let len = usize::try_from(payload_bytes).map_err(|_| SourceError::Unavailable)?;
            if !(1..=128).contains(&id_bytes) {
                return Err(SourceError::Unavailable);
            }
            let next = bytes.checked_add(len).ok_or(SourceError::OversizedRecord)?;
            if len > record_limit || next > byte_limit {
                if records.is_empty() {
                    return Err(SourceError::OversizedRecord);
                }
                break;
            }
            let (raw_id, payload): (String, Vec<u8>) = tx
                .query_row(
                    "SELECT record_id, payload FROM source_records WHERE position = ?1",
                    [position],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| SourceError::Unavailable)?;
            if payload.len() != len {
                return Err(SourceError::Unavailable);
            }
            records.push(Record {
                position: u64::try_from(position).map_err(|_| SourceError::Unavailable)?,
                id: Id::new(raw_id).map_err(|_| SourceError::Unavailable)?,
                scope: request.scope.clone(),
                payload,
            });
            bytes = next;
        }
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        Ok(Page {
            request: request.clone(),
            records,
        })
    }
}

fn history_meta(tx: &rusqlite::Transaction<'_>) -> Result<(u64, u64), HistorySourceError> {
    let head: i64 = tx
        .query_row(
            "SELECT head FROM source_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| HistorySourceError::Unavailable)?;
    let floor: i64 = tx
        .query_row(
            "SELECT pruned_through FROM source_history WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| HistorySourceError::Unavailable)?;
    Ok((
        u64::try_from(head).map_err(|_| HistorySourceError::Unavailable)?,
        u64::try_from(floor).map_err(|_| HistorySourceError::Unavailable)?,
    ))
}

fn read_backward(
    tx: &rusqlite::Transaction<'_>,
    scope: &Scope,
    before: u64,
    oldest: u64,
    max_records: usize,
    max_payload_bytes: usize,
    max_record_bytes: usize,
) -> Result<Vec<Record>, HistorySourceError> {
    if max_records == 0
        || max_records > MAX_PAGE_RECORDS
        || max_payload_bytes == 0
        || max_payload_bytes > MAX_PAGE_BYTES
        || max_record_bytes == 0
        || max_record_bytes > MAX_PAGE_BYTES
        || before <= oldest
    {
        return Err(HistorySourceError::InvalidRequest);
    }
    let metadata: Vec<(i64, i64, i64)> = {
        let mut statement = tx
            .prepare("SELECT position, length(record_id), length(payload) FROM source_records WHERE position >= ?1 AND position < ?2 ORDER BY position DESC LIMIT ?3")
            .map_err(|_| HistorySourceError::Unavailable)?;
        let rows = statement
            .query_map(
                params![
                    i64::try_from(oldest).map_err(|_| HistorySourceError::InvalidRequest)?,
                    i64::try_from(before).map_err(|_| HistorySourceError::InvalidRequest)?,
                    i64::try_from(max_records).map_err(|_| HistorySourceError::InvalidRequest)?
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| HistorySourceError::Unavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| HistorySourceError::Unavailable)?;
        rows
    };
    let mut expected = before
        .checked_sub(1)
        .ok_or(HistorySourceError::InvalidRequest)?;
    let mut bytes = 0_usize;
    let mut records = Vec::new();
    for (position, id_bytes, payload_bytes) in metadata {
        if u64::try_from(position).ok() != Some(expected) {
            return Err(HistorySourceError::ResetRequired);
        }
        if !(1..=128).contains(&id_bytes) {
            return Err(HistorySourceError::Unavailable);
        }
        let len = usize::try_from(payload_bytes).map_err(|_| HistorySourceError::Unavailable)?;
        let next = bytes
            .checked_add(len)
            .ok_or(HistorySourceError::OversizedRecord)?;
        if len > max_record_bytes || next > max_payload_bytes {
            if records.is_empty() {
                return Err(HistorySourceError::OversizedRecord);
            }
            break;
        }
        let (raw_id, payload): (String, Vec<u8>) = tx
            .query_row(
                "SELECT record_id, payload FROM source_records WHERE position = ?1",
                [position],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| HistorySourceError::Unavailable)?;
        if payload.len() != len {
            return Err(HistorySourceError::Unavailable);
        }
        records.push(Record {
            position: expected,
            id: Id::new(raw_id).map_err(|_| HistorySourceError::Unavailable)?,
            scope: scope.clone(),
            payload,
        });
        bytes = next;
        expected = expected.saturating_sub(1);
    }
    if records.is_empty() {
        return Err(HistorySourceError::ResetRequired);
    }
    records.reverse();
    Ok(records)
}

impl HistorySource for SqliteReferenceSource {
    fn tail(&mut self, request: &TailRequest) -> Result<TailSnapshot, HistorySourceError> {
        self.tail_reads += 1;
        if !self.matches(&request.scope) {
            return Err(HistorySourceError::IdentityChanged);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let (head, floor) = history_meta(&tx)?;
        if head == 0 {
            return Ok(TailSnapshot {
                request: request.clone(),
                watermark: 0,
                first: 1,
                oldest_available: 1,
                records: Vec::new(),
            });
        }
        let oldest = floor
            .checked_add(1)
            .ok_or(HistorySourceError::Unavailable)?;
        if oldest > head {
            return Err(HistorySourceError::ResetRequired);
        }
        let records = read_backward(
            &tx,
            &request.scope,
            head.checked_add(1).ok_or(HistorySourceError::Unavailable)?,
            oldest,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        )?;
        let bytes: usize = records.iter().map(|record| record.payload.len()).sum();
        self.history_payload_bytes = self.history_payload_bytes.saturating_add(bytes as u64);
        Ok(TailSnapshot {
            request: request.clone(),
            watermark: head,
            first: records[0].position,
            oldest_available: oldest,
            records,
        })
    }

    fn older(&mut self, request: &OlderRequest) -> Result<OlderPage, HistorySourceError> {
        self.older_reads += 1;
        if !self.matches(&request.scope) {
            return Err(HistorySourceError::IdentityChanged);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let (head, floor) = history_meta(&tx)?;
        if request.before > head.saturating_add(1) {
            return Err(HistorySourceError::InvalidRequest);
        }
        let oldest = floor
            .checked_add(1)
            .ok_or(HistorySourceError::Unavailable)?;
        if request.before <= oldest {
            return Err(HistorySourceError::ResetRequired);
        }
        let records = read_backward(
            &tx,
            &request.scope,
            request.before,
            oldest,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        )?;
        let bytes: usize = records.iter().map(|record| record.payload.len()).sum();
        self.history_payload_bytes = self.history_payload_bytes.saturating_add(bytes as u64);
        Ok(OlderPage {
            request: request.clone(),
            oldest_available: oldest,
            records,
        })
    }
}
