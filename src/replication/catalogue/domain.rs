//! Pure catalogue identity, page, and pass validation.

use crate::replication::domain::{Id, Scope};

/// Maximum manifest entries in one catalogue page.
pub const MAX_CATALOGUE_ENTRIES: usize = 256;
/// Maximum combined resolved payload bytes in one catalogue page.
pub const MAX_CATALOGUE_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Immutable key used to page current entries in creation order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryKey {
    /// Revision at which this identity was first created.
    pub creation: u64,
    /// Stable opaque entry identity.
    pub id: Id,
}

/// Small current-value descriptor; no summary payload is retained on the source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Stable entry key.
    pub key: EntryKey,
    /// Current committed revision, possibly newer than the pass boundary.
    pub revision: u64,
    /// Whether this entry currently has a retained deletion marker.
    pub deleted: bool,
}

/// Latest resolved authorized entry or retained deletion marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedEntry {
    /// Descriptor captured by the payload read.
    pub manifest: ManifestEntry,
    /// Opaque current host value; empty for a deletion marker.
    pub payload: Vec<u8>,
}

/// Exact durable pass, including the last committed page cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CataloguePass {
    /// Exact receiving scope and access epoch.
    pub scope: Scope,
    /// Last fully completed source revision before this pass.
    pub completed: u64,
    /// Fixed source head captured when the pass began.
    pub boundary: u64,
    /// Last committed stable key, if any.
    pub cursor: Option<EntryKey>,
    /// Monotonic receiver generation; delayed responses from older passes fail.
    pub generation: u64,
}

/// Durable receiver progress and optional in-progress pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogueProgress {
    /// Exact scope used to create the current cache.
    pub scope: Scope,
    /// Last fully completed revision.
    pub completed: u64,
    /// Latest pass/reset generation.
    pub generation: u64,
    /// Unfinished pass, if any.
    pub active: Option<CataloguePass>,
}

/// Bounded metadata request after the committed cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRequest {
    /// Exact saved pass.
    pub pass: CataloguePass,
    /// Maximum entries returned in this page.
    pub max_entries: usize,
}

/// Source page correlated to one exact request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestPage {
    /// Exact echoed request.
    pub request: ManifestRequest,
    /// Stable-key ordered entries.
    pub entries: Vec<ManifestEntry>,
    /// Whether another eligible entry was present after this page.
    pub has_more: bool,
}

/// Validated entries and continuation to save in a single store transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CataloguePagePlan {
    /// Pass the store must compare atomically.
    pub pass: CataloguePass,
    /// Exact validated manifest whose entries this plan resolves.
    pub manifest: ManifestPage,
    /// Last key included by this page; none is valid only for an empty final page.
    pub next_cursor: Option<EntryKey>,
    /// Whether this page completes the captured pass.
    pub final_page: bool,
    /// Latest resolved payloads and deletion markers.
    pub entries: Vec<ResolvedEntry>,
    /// Metadata-only entries already stored at this revision or newer.
    pub unchanged: Vec<ManifestEntry>,
}

/// Pure validation refusal, before any store effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueValidationError {
    /// Invalid pass or request bounds.
    InvalidRequest,
    /// Response did not echo the exact pass/request.
    WrongRequest,
    /// Entries are out of order, beyond the boundary, or repeat an ID.
    InvalidOrder,
    /// A page claims more entries without making cursor progress.
    NoProgress,
    /// Resolved payload changed identity or moved backward in revision.
    WrongPayload,
    /// Payload or entry count exceeded the caller's bounds.
    BoundsExceeded,
}

/// Validates a bounded page before resolving any payloads.
pub fn validate_manifest(
    request: &ManifestRequest,
    page: &ManifestPage,
    max_entries: usize,
) -> Result<(), CatalogueValidationError> {
    if request.max_entries == 0
        || request.max_entries > MAX_CATALOGUE_ENTRIES
        || request.max_entries > max_entries
        || request.pass.boundary <= request.pass.completed
        || request.pass.generation == 0
    {
        return Err(CatalogueValidationError::InvalidRequest);
    }
    if &page.request != request {
        return Err(CatalogueValidationError::WrongRequest);
    }
    if page.entries.len() > request.max_entries {
        return Err(CatalogueValidationError::BoundsExceeded);
    }
    if page.has_more && page.entries.is_empty() {
        return Err(CatalogueValidationError::NoProgress);
    }
    let mut previous = request.pass.cursor.as_ref();
    for entry in &page.entries {
        if entry.key.creation == 0
            || entry.key.creation > request.pass.boundary
            || entry.revision < entry.key.creation
            || entry.revision <= request.pass.completed
            || previous.is_some_and(|key| entry.key <= *key)
        {
            return Err(CatalogueValidationError::InvalidOrder);
        }
        previous = Some(&entry.key);
    }
    Ok(())
}

/// Checks a current payload against the manifest identity and revision.
pub fn validate_resolved(
    manifest: &ManifestEntry,
    resolved: &ResolvedEntry,
    max_payload_bytes: usize,
) -> Result<(), CatalogueValidationError> {
    if resolved.manifest.key != manifest.key
        || resolved.manifest.revision < manifest.revision
        || (resolved.manifest.deleted && !resolved.payload.is_empty())
    {
        return Err(CatalogueValidationError::WrongPayload);
    }
    if resolved.payload.len() > max_payload_bytes {
        return Err(CatalogueValidationError::BoundsExceeded);
    }
    Ok(())
}
