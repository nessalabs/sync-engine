//! Pure, immutable record and checkpoint contract. No I/O or adapter imports.

use std::collections::HashSet;

/// A validated opaque identifier, at most 128 UTF-8 bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(String);

impl Id {
    /// Constructs an ID. Empty, whitespace-only, and oversized IDs are refused.
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 128 {
            return Err(ValidationError::InvalidId);
        }
        Ok(Self(value))
    }

    /// Returns the opaque host-selected ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact receiving and source stream identity, including authorization epoch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scope {
    /// Receiver whose local checkpoint this is.
    receiver: Id,
    /// Authoritative origin.
    origin: Id,
    /// Stream selected by the host.
    stream: Id,
    /// Source incarnation; changes require an explicit reset outside this slice.
    incarnation: Id,
    /// Required payload schema identity; interpretation remains host-owned.
    schema: Id,
    /// Current host authorization epoch.
    access_epoch: Id,
}

impl Scope {
    /// Creates an exact scope from validated host-selected identities.
    pub fn new(
        receiver: Id,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
        access_epoch: Id,
    ) -> Self {
        Self {
            receiver,
            origin,
            stream,
            incarnation,
            schema,
            access_epoch,
        }
    }
    /// Receiving device identity.
    pub fn receiver(&self) -> &Id {
        &self.receiver
    }
    /// Authoritative origin identity.
    pub fn origin(&self) -> &Id {
        &self.origin
    }
    /// Stream identity.
    pub fn stream(&self) -> &Id {
        &self.stream
    }
    /// Source incarnation identity.
    pub fn incarnation(&self) -> &Id {
        &self.incarnation
    }
    /// Required schema identity.
    pub fn schema(&self) -> &Id {
        &self.schema
    }
    /// Current access epoch.
    pub fn access_epoch(&self) -> &Id {
        &self.access_epoch
    }
}

/// Untrusted source envelope in a dense stream. `validate_page` turns these DTOs
/// into a commit plan only after checking identity, order, and bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// Source position, starting at one.
    pub position: u64,
    /// Immutable fact identity, checked for conflicting reuse by the store.
    pub id: Id,
    /// Exact source identity and scope carried with the record.
    pub scope: Scope,
    /// Opaque host data.
    pub payload: Vec<u8>,
}

/// Last position atomically saved with all preceding records on one receiver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    scope: Scope,
    position: u64,
}

impl Checkpoint {
    /// Constructs a checkpoint from an exact scope and last applied position.
    pub fn new(scope: Scope, position: u64) -> Self {
        Self { scope, position }
    }
    /// Exact scope of this local progress.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Last applied position; zero means empty.
    pub fn position(&self) -> u64 {
        self.position
    }
}

/// Limits applied to every fetched page before a store effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum records in a page, greater than zero.
    max_records: usize,
    /// Maximum total payload bytes in a page, greater than zero.
    max_payload_bytes: usize,
    /// Maximum bytes in one decoded record, greater than zero.
    max_record_bytes: usize,
}

impl Limits {
    /// Constructs nonzero bounds. The effective single-record limit is the
    /// smaller of `max_record_bytes` and `max_payload_bytes`. A transport
    /// adapter must additionally bound wire frames.
    pub fn new(
        max_records: usize,
        max_payload_bytes: usize,
        max_record_bytes: usize,
    ) -> Result<Self, ValidationError> {
        if max_records == 0 || max_payload_bytes == 0 || max_record_bytes == 0 {
            return Err(ValidationError::InvalidLimits);
        }
        Ok(Self {
            max_records,
            max_payload_bytes,
            max_record_bytes,
        })
    }
    /// Maximum records allowed in one page.
    pub fn max_records(&self) -> usize {
        self.max_records
    }
    /// Maximum total decoded payload bytes in one page.
    pub fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
    /// Maximum decoded bytes in one record.
    pub fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }
}

/// Exact request for one contiguous page within a captured target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    /// Scope to read.
    pub scope: Scope,
    /// Position just before the requested range.
    pub after: u64,
    /// Fixed end of this pass.
    pub target: u64,
    /// Maximum number of records requested.
    pub max_records: usize,
    /// Maximum decoded payload bytes requested.
    pub max_payload_bytes: usize,
    /// Maximum bytes allowed in any one returned record.
    pub max_record_bytes: usize,
}

/// Bounded response, correlated to one exact request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    /// Echo of the exact request; transport adapters must not synthesize it from local state.
    pub request: PageRequest,
    /// Ordered records returned by the source.
    pub records: Vec<Record>,
}

/// Validated, immutable unit passed to the atomic store port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPlan {
    /// Expected prior progress for compare-and-swap.
    expected: Checkpoint,
    /// New progress committed with the records.
    next: Checkpoint,
    /// Validated records.
    records: Vec<Record>,
}

impl CommitPlan {
    /// Prior checkpoint the store must compare atomically.
    pub fn expected(&self) -> &Checkpoint {
        &self.expected
    }
    /// New checkpoint saved with the records.
    pub fn next(&self) -> &Checkpoint {
        &self.next
    }
    /// Validated records in this plan.
    pub fn records(&self) -> &[Record] {
        &self.records
    }
    /// Consumes the plan after the store has checked its current state.
    pub fn into_parts(self) -> (Checkpoint, Checkpoint, Vec<Record>) {
        (self.expected, self.next, self.records)
    }
}

/// Pure validation refusal. No store call is allowed after one of these errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationError {
    /// Opaque identity was empty or too long.
    InvalidId,
    /// A configured bound was zero.
    InvalidLimits,
    /// Request did not start at the saved position or ended before it.
    InvalidRange,
    /// Response echoed a different request, scope, epoch, or receiver.
    WrongRequest,
    /// Returned page had no forward progress before the target.
    EmptyPage,
    /// Count or byte bound was exceeded.
    BoundsExceeded,
    /// A single record cannot fit the declared page bound.
    OversizedRecord,
    /// A record had zero position, a gap, duplicate position, or exceeded the target.
    Noncontiguous,
    /// A record carried a foreign scope or duplicate fact ID within the page.
    WrongRecord,
    /// Numeric position overflowed.
    PositionOverflow,
}

/// Validates correlation, density, and bounds, then builds an atomic commit plan.
/// No I/O occurs. The store must still compare `expected` and check record-ID reuse.
pub fn validate_page(
    expected: &Checkpoint,
    request: &PageRequest,
    page: Page,
    limits: Limits,
) -> Result<CommitPlan, ValidationError> {
    if request.scope != expected.scope
        || request.after != expected.position
        || request.target <= request.after
        || request.max_records == 0
        || request.max_records > limits.max_records
        || request.max_payload_bytes == 0
        || request.max_payload_bytes > limits.max_payload_bytes
        || request.max_record_bytes == 0
        || request.max_record_bytes > limits.max_record_bytes
    {
        return Err(ValidationError::InvalidRange);
    }
    if page.request != *request {
        return Err(ValidationError::WrongRequest);
    }
    if page.records.is_empty() {
        return Err(ValidationError::EmptyPage);
    }
    if page.records.len() > request.max_records {
        return Err(ValidationError::BoundsExceeded);
    }
    let mut position = request.after;
    let mut bytes = 0usize;
    let mut ids = HashSet::new();
    for record in &page.records {
        if record.scope != request.scope || !ids.insert(&record.id) {
            return Err(ValidationError::WrongRecord);
        }
        if record.payload.len() > request.max_record_bytes
            || (position == request.after && record.payload.len() > request.max_payload_bytes)
        {
            return Err(ValidationError::OversizedRecord);
        }
        bytes = bytes
            .checked_add(record.payload.len())
            .ok_or(ValidationError::BoundsExceeded)?;
        if bytes > request.max_payload_bytes {
            return Err(ValidationError::BoundsExceeded);
        }
        position = position
            .checked_add(1)
            .ok_or(ValidationError::PositionOverflow)?;
        if record.position != position || position > request.target {
            return Err(ValidationError::Noncontiguous);
        }
    }
    Ok(CommitPlan {
        expected: expected.clone(),
        next: Checkpoint {
            scope: expected.scope.clone(),
            position,
        },
        records: page.records,
    })
}
