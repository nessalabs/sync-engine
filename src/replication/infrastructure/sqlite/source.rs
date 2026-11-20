//! File-backed, append-only reference source for the restartable transcript lab.
//! This is an illustrative `RecordSource`, not Nessa's canonical event owner.

use std::path::Path;

use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::replication::application::{
    RecordHeadSource, RecordReadView, RecordSource, SourceError, StoreError, MAX_RECORD_BYTES,
};
use crate::replication::domain::{
    validate_capabilities, validate_page_request, CapabilitySupport, Id, Limits, Page, PageRequest,
    Record, Scope, SourceHead,
};
use crate::replication::history::{
    CapabilityHistorySource, HistorySource, HistorySourceError, OlderPage, OlderRequest,
    TailRequest, TailSnapshot,
};

use super::{
    configure_connection, open_connection, read_record_row, SqliteOpenError, MAX_STORED_ID_BYTES,
    SOURCE_APPLICATION_ID,
};

pub(super) const FORWARD_RECORD_METADATA_SQL: &str = "SELECT position, octet_length(record_id) <= ?4, length(payload) FROM source_records WHERE position > ?1 AND position <= ?2 ORDER BY position LIMIT ?3";

pub(super) const HISTORY_RECORD_METADATA_SQL: &str = "SELECT position, octet_length(record_id) <= ?4, length(payload) FROM source_records WHERE position >= ?1 AND position < ?2 ORDER BY position DESC LIMIT ?3";

mod publication;
mod retention;
pub use publication::SourcePublicationError;
pub use retention::{RecordPinReceipt, RecordRetentionError};

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
    /// silently replaced, and no user-selected path is deleted. Recognized version1
    /// and version2 record files migrate atomically to version3. Hosts must stop
    /// old writers before upgrade (already-open old binaries are not fenced).
    pub fn open(
        path: impl AsRef<Path>,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
    ) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        publication::initialize(&mut conn, [&origin, &stream, &incarnation, &schema])?;
        configure_connection(&conn)?;
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
    /// above [`MAX_RECORD_BYTES`] are refused. Fresh unsupported writer
    /// requirements return Failed before dedup or mutation; compare_append
    /// preserves typed publication errors.
    pub fn append(&mut self, id: &Id, payload: &[u8]) -> Result<u64, StoreError> {
        self.append_publication(id, payload, None)
            .map_err(|error| match error {
                SourcePublicationError::Store(error) => error,
                _ => StoreError::Failed,
            })
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
        publication::read(&self.conn)
            .map(|value| value.position)
            .map_err(|_| SourceError::Unavailable)
    }

    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.page_reads += 1;
        if !self.matches(&request.scope) {
            return Err(SourceError::IdentityChanged);
        }
        // Generic admissibility has one domain owner. These adapters preserve
        // their existing representable request budgets; physical limits below
        // may return a smaller page without imposing a receiver policy.
        let limits = Limits::new(usize::MAX, usize::MAX, usize::MAX)
            .map_err(|_| SourceError::InvalidRequest)?;
        validate_page_request(request, limits).map_err(|_| SourceError::InvalidRequest)?;
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
        publication::read(&tx)
            .map_err(|_| SourceError::Unavailable)?
            .reader()
            .map_err(SourceError::Incompatible)?;
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
        let metadata: Vec<(i64, bool, i64)> = {
            let mut statement = tx
                .prepare(FORWARD_RECORD_METADATA_SQL)
                .map_err(|_| SourceError::Unavailable)?;
            let rows = statement
                .query_map(
                    params![
                        after,
                        target,
                        limit,
                        i64::try_from(MAX_STORED_ID_BYTES).map_err(|_| SourceError::Unavailable)?
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
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
        for (position, storage_fits, payload_bytes) in metadata {
            expected = expected.checked_add(1).ok_or(SourceError::Unavailable)?;
            if position != expected {
                return Err(SourceError::Pruned);
            }
            let len = usize::try_from(payload_bytes).map_err(|_| SourceError::Unavailable)?;
            if !storage_fits {
                return Err(SourceError::Unavailable);
            }
            let next = bytes.checked_add(len).ok_or(SourceError::OversizedRecord)?;
            if len > record_limit || next > byte_limit {
                if records.is_empty() {
                    return Err(SourceError::OversizedRecord);
                }
                break;
            }
            let (id, payload) = tx
                .query_row(
                    "SELECT record_id, payload FROM source_records WHERE position = ?1",
                    [position],
                    read_record_row,
                )
                .map_err(|_| SourceError::Unavailable)?
                .map_err(|_| SourceError::Unavailable)?;
            if payload.len() != len {
                return Err(SourceError::Unavailable);
            }
            records.push(Record {
                position: u64::try_from(position).map_err(|_| SourceError::Unavailable)?,
                id,
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

// The publication SELECT pins the transaction snapshot before floor or rows
// are acquired. The metadata and payload must not come from separate reads.
fn history_meta(
    tx: &rusqlite::Transaction<'_>,
    scope: &Scope,
    supported: CapabilitySupport,
) -> Result<(SourceHead, u64), HistorySourceError> {
    let publication = publication::read(tx).map_err(|_| HistorySourceError::Unavailable)?;
    let head = publication.head(scope);
    validate_capabilities(&head, supported).map_err(HistorySourceError::Incompatible)?;
    publication
        .reader()
        .map_err(HistorySourceError::Incompatible)?;
    let floor: i64 = tx
        .query_row(
            "SELECT pruned_through FROM source_history WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| HistorySourceError::Unavailable)?;
    Ok((
        head,
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
    let metadata: Vec<(i64, bool, i64)> = {
        let mut statement = tx
            .prepare(HISTORY_RECORD_METADATA_SQL)
            .map_err(|_| HistorySourceError::Unavailable)?;
        let rows = statement
            .query_map(
                params![
                    i64::try_from(oldest).map_err(|_| HistorySourceError::InvalidRequest)?,
                    i64::try_from(before).map_err(|_| HistorySourceError::InvalidRequest)?,
                    i64::try_from(max_records).map_err(|_| HistorySourceError::InvalidRequest)?,
                    i64::try_from(MAX_STORED_ID_BYTES)
                        .map_err(|_| HistorySourceError::Unavailable)?
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
    for (position, storage_fits, payload_bytes) in metadata {
        if u64::try_from(position).ok() != Some(expected) {
            return Err(HistorySourceError::ResetRequired);
        }
        if !storage_fits {
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
        let (id, payload) = tx
            .query_row(
                "SELECT record_id, payload FROM source_records WHERE position = ?1",
                [position],
                read_record_row,
            )
            .map_err(|_| HistorySourceError::Unavailable)?
            .map_err(|_| HistorySourceError::Unavailable)?;
        if payload.len() != len {
            return Err(HistorySourceError::Unavailable);
        }
        records.push(Record {
            position: expected,
            id,
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
        self.tail_with_capabilities(request, CapabilitySupport::current())
            .map(|(_, tail)| tail)
    }

    fn older(&mut self, request: &OlderRequest) -> Result<OlderPage, HistorySourceError> {
        self.older_with_capabilities(request, CapabilitySupport::current())
            .map(|(_, page)| page)
    }
}

impl CapabilityHistorySource for SqliteReferenceSource {
    fn tail_with_capabilities(
        &mut self,
        request: &TailRequest,
        supported: CapabilitySupport,
    ) -> Result<(SourceHead, TailSnapshot), HistorySourceError> {
        self.tail_reads += 1;
        if !self.matches(&request.scope) {
            return Err(HistorySourceError::IdentityChanged);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let (metadata, floor) = history_meta(&tx, &request.scope, supported)?;
        let head = metadata.position();
        if head == 0 {
            return Ok((
                metadata,
                TailSnapshot {
                    request: request.clone(),
                    watermark: 0,
                    first: 1,
                    oldest_available: 1,
                    records: Vec::new(),
                },
            ));
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
        Ok((
            metadata,
            TailSnapshot {
                request: request.clone(),
                watermark: head,
                first: records[0].position,
                oldest_available: oldest,
                records,
            },
        ))
    }

    fn older_with_capabilities(
        &mut self,
        request: &OlderRequest,
        supported: CapabilitySupport,
    ) -> Result<(SourceHead, OlderPage), HistorySourceError> {
        self.older_reads += 1;
        if !self.matches(&request.scope) {
            return Err(HistorySourceError::IdentityChanged);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let (metadata, floor) = history_meta(&tx, &request.scope, supported)?;
        let head = metadata.position();
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
        Ok((
            metadata,
            OlderPage {
                request: request.clone(),
                oldest_available: oldest,
                records,
            },
        ))
    }
}

impl RecordHeadSource for SqliteReferenceSource {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        self.head_reads += 1;
        if !self.matches(scope) {
            return Err(SourceError::IdentityChanged);
        }
        publication::read(&self.conn)
            .map(|value| value.head(scope))
            .map_err(|_| SourceError::Unavailable)
    }
}

// Native records are immutable and this encoder implements only zero optional
// features. Every page checks fresh requirements in its row-read snapshot; a
// concurrent unsupported publication refuses the page rather than returning a
// DTO under stale metadata. Future supported format changes need snapshot proof.
impl RecordReadView for SqliteReferenceSource {
    fn view_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        RecordHeadSource::source_head(self, scope)
    }
}

#[cfg(test)]
mod history_capability_tests;
