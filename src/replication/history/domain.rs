//! Pure identity, bound and order checks for transcript tail and older pages.

use std::collections::HashSet;

use crate::replication::domain::{Limits, Record, Scope};

/// One bounded recent-tail request under an explicit reset generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailRequest {
    /// Exact receiver/source/schema/authorization scope.
    pub scope: Scope,
    /// Host-owned monotone reset attempt number, greater than zero.
    pub generation: u64,
    /// Maximum returned record count.
    pub max_records: usize,
    /// Maximum total record payload bytes.
    pub max_payload_bytes: usize,
    /// Maximum single record payload bytes.
    pub max_record_bytes: usize,
}

/// Source response captured from one committed read snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailSnapshot {
    /// Exact request echoed by the source, never synthesized by the receiver.
    pub request: TailRequest,
    /// Committed source head captured with the tail.
    pub watermark: u64,
    /// First position in the returned suffix; one for an empty source.
    pub first: u64,
    /// First position the source currently promises it can serve.
    pub oldest_available: u64,
    /// Contiguous records from `first` through `watermark`.
    pub records: Vec<Record>,
}

/// One bounded historical request ending immediately before `before`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OlderRequest {
    /// Exact receiver/source/schema/authorization scope.
    pub scope: Scope,
    /// Snapshot generation whose history is being filled.
    pub generation: u64,
    /// Exclusive upper position; must be greater than one.
    pub before: u64,
    /// Maximum returned record count.
    pub max_records: usize,
    /// Maximum total record payload bytes.
    pub max_payload_bytes: usize,
    /// Maximum single record payload bytes.
    pub max_record_bytes: usize,
}

/// Source response for a contiguous historical range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OlderPage {
    /// Exact request echoed by the source.
    pub request: OlderRequest,
    /// First position still historically readable at source.
    pub oldest_available: u64,
    /// Ascending records ending at `request.before - 1`.
    pub records: Vec<Record>,
}

/// Durable progress. `live_head` and `lower_bound` never substitute for one another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryProgress {
    /// Exact saved scope.
    pub scope: Scope,
    /// Installed reset generation.
    pub generation: u64,
    /// Last contiguous live record applied.
    pub live_head: u64,
    /// First saved position of the contiguous tail; one for an empty stream.
    pub lower_bound: u64,
    /// Last known source pruning floor.
    pub oldest_available: u64,
    /// Permanent local deletion fence.
    pub deleted: bool,
}

/// Pure response validation failure; no store effect follows this error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryValidationError {
    /// Request generation or bounds were invalid.
    InvalidRequest,
    /// Response echoed a different request.
    WrongRequest,
    /// Source watermark or historical floor was inconsistent.
    InvalidBoundary,
    /// Page exceeded count or decoded byte limits.
    BoundsExceeded,
    /// Position had a gap, duplicate, or wrong endpoint.
    Noncontiguous,
    /// Record carried a different scope or duplicate immutable ID.
    WrongRecord,
}

/// A tail accepted by pure validation and ready for atomic installation.
pub struct TailPlan(TailSnapshot);

impl TailPlan {
    /// Validated snapshot for a store to commit atomically.
    pub fn snapshot(&self) -> &TailSnapshot {
        &self.0
    }

    /// Consumes the plan when transferring ownership to a store.
    pub fn into_snapshot(self) -> TailSnapshot {
        self.0
    }
}

/// An older page accepted by pure validation and ready for overlap checks.
pub struct OlderPlan(OlderPage);

impl OlderPlan {
    /// Validated page for a store to compare and commit atomically.
    pub fn page(&self) -> &OlderPage {
        &self.0
    }

    /// Consumes the plan when transferring ownership to a store.
    pub fn into_page(self) -> OlderPage {
        self.0
    }
}

fn request_bounds(
    generation: u64,
    records: usize,
    payload_bytes: usize,
    record_bytes: usize,
    limits: Limits,
) -> Result<(), HistoryValidationError> {
    if generation == 0
        || records == 0
        || records > limits.max_records()
        || payload_bytes == 0
        || payload_bytes > limits.max_payload_bytes()
        || record_bytes == 0
        || record_bytes > limits.max_record_bytes()
    {
        return Err(HistoryValidationError::InvalidRequest);
    }
    Ok(())
}

fn validate_records(
    scope: &Scope,
    records: &[Record],
    start: u64,
    max_records: usize,
    max_payload_bytes: usize,
    max_record_bytes: usize,
) -> Result<(), HistoryValidationError> {
    if records.len() > max_records {
        return Err(HistoryValidationError::BoundsExceeded);
    }
    let mut expected = start;
    let mut bytes = 0_usize;
    let mut ids = HashSet::new();
    for record in records {
        if record.scope != *scope || !ids.insert(&record.id) {
            return Err(HistoryValidationError::WrongRecord);
        }
        if record.position != expected {
            return Err(HistoryValidationError::Noncontiguous);
        }
        if record.payload.len() > max_record_bytes {
            return Err(HistoryValidationError::BoundsExceeded);
        }
        bytes = bytes
            .checked_add(record.payload.len())
            .ok_or(HistoryValidationError::BoundsExceeded)?;
        if bytes > max_payload_bytes {
            return Err(HistoryValidationError::BoundsExceeded);
        }
        expected = expected
            .checked_add(1)
            .ok_or(HistoryValidationError::Noncontiguous)?;
    }
    Ok(())
}

/// Checks one exact bounded suffix before a store may install it.
pub fn validate_tail(
    request: &TailRequest,
    snapshot: TailSnapshot,
    limits: Limits,
) -> Result<TailPlan, HistoryValidationError> {
    request_bounds(
        request.generation,
        request.max_records,
        request.max_payload_bytes,
        request.max_record_bytes,
        limits,
    )?;
    if snapshot.request != *request {
        return Err(HistoryValidationError::WrongRequest);
    }
    if snapshot.watermark == 0 {
        if snapshot.first != 1 || snapshot.oldest_available != 1 || !snapshot.records.is_empty() {
            return Err(HistoryValidationError::InvalidBoundary);
        }
    } else {
        if snapshot.first == 0
            || snapshot.first > snapshot.watermark
            || snapshot.oldest_available == 0
            || snapshot.oldest_available > snapshot.first
            || snapshot.records.is_empty()
            || u64::try_from(snapshot.records.len())
                .ok()
                .and_then(|count| snapshot.first.checked_add(count - 1))
                != Some(snapshot.watermark)
        {
            return Err(HistoryValidationError::InvalidBoundary);
        }
        validate_records(
            &request.scope,
            &snapshot.records,
            snapshot.first,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        )?;
    }
    Ok(TailPlan(snapshot))
}

/// Checks a historical page ending immediately before the requested boundary.
pub fn validate_older(
    request: &OlderRequest,
    page: OlderPage,
    limits: Limits,
) -> Result<OlderPlan, HistoryValidationError> {
    request_bounds(
        request.generation,
        request.max_records,
        request.max_payload_bytes,
        request.max_record_bytes,
        limits,
    )?;
    if request.before <= 1 {
        return Err(HistoryValidationError::InvalidRequest);
    }
    if page.request != *request {
        return Err(HistoryValidationError::WrongRequest);
    }
    let Some(first) = page.records.first().map(|record| record.position) else {
        return Err(HistoryValidationError::InvalidBoundary);
    };
    if page.oldest_available == 0
        || page.oldest_available > first
        || u64::try_from(page.records.len())
            .ok()
            .and_then(|count| first.checked_add(count))
            != Some(request.before)
    {
        return Err(HistoryValidationError::InvalidBoundary);
    }
    validate_records(
        &request.scope,
        &page.records,
        first,
        request.max_records,
        request.max_payload_bytes,
        request.max_record_bytes,
    )?;
    Ok(OlderPlan(page))
}
