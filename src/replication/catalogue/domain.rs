//! Pure catalogue identity, page, and pass validation.

use crate::replication::domain::{Id, Scope};
use std::collections::HashSet;

mod progress;
pub use progress::*;

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
    /// Continuation after this page: its last key, or the previous cursor for an
    /// empty final page. None is valid when an empty final page has no prior cursor.
    pub next_cursor: Option<EntryKey>,
    /// Whether this page completes the captured pass.
    pub final_page: bool,
    /// Latest resolved payloads and deletion markers.
    pub entries: Vec<ResolvedEntry>,
    /// Metadata-only entries already stored at this revision or newer.
    pub unchanged: Vec<ManifestEntry>,
}

impl CataloguePagePlan {
    /// Constructs correlated continuation fields and validates this page plan.
    ///
    /// This pure operation performs no I/O. Payloads remain opaque and all
    /// input vectors move into the returned plan. Store adapters still validate
    /// the public DTO before effects and compare durable pass/cache evidence
    /// inside their transaction; construction establishes no authority or commit.
    /// An empty final page retains the request's previous cursor.
    ///
    /// # Errors
    /// Returns [`CatalogueValidationError`] for contradictory coverage,
    /// correlations, ordering or the published catalogue page ceilings.
    pub fn new(
        manifest: ManifestPage,
        entries: Vec<ResolvedEntry>,
        unchanged: Vec<ManifestEntry>,
    ) -> Result<Self, CatalogueValidationError> {
        let plan = Self {
            pass: manifest.request.pass.clone(),
            next_cursor: page_cursor(&manifest),
            final_page: !manifest.has_more,
            manifest,
            entries,
            unchanged,
        };
        validate_catalogue_page_plan(&plan)?;
        Ok(plan)
    }
}

fn page_cursor(manifest: &ManifestPage) -> Option<EntryKey> {
    manifest.entries.last().map_or_else(
        || manifest.request.pass.cursor.clone(),
        |entry| Some(entry.key.clone()),
    )
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
    /// Later live content contradicts an earlier retained deletion descriptor.
    DeletionFence,
}

/// Checks the existing finite-pass relationships before a source effect.
///
/// This pure borrowed check performs no I/O or mutation. It establishes an
/// advancing captured boundary, nonzero receiver generation and a positive cursor
/// creation within the boundary when present. It does not authorize the scope,
/// read durable progress or validate ordering relative to manifest entries.
/// Immutable requests may be checked concurrently.
///
/// # Errors
/// Returns [`CatalogueValidationError::InvalidRequest`] when the boundary does
/// not advance completed progress, the generation is zero or cursor creation is
/// zero or exceeds the captured boundary.
pub fn validate_catalogue_pass(pass: &CataloguePass) -> Result<(), CatalogueValidationError> {
    if pass.boundary <= pass.completed
        || pass.generation == 0
        || pass
            .cursor
            .as_ref()
            .is_some_and(|key| !key_is_in_boundary(key, pass.boundary))
    {
        return Err(CatalogueValidationError::InvalidRequest);
    }
    Ok(())
}

/// Checks manifest request bounds before a host performs source I/O.
///
/// The request is borrowed and unchanged; this pure operation allocates no
/// payload or progress and performs no effects. It is safe to call concurrently
/// with independent or shared immutable requests. `max_entries` is the caller's
/// entry ceiling; the published [`MAX_CATALOGUE_ENTRIES`] ceiling also applies.
/// Authorization, source identity, durable pass correlation and response cursor
/// ordering remain with their respective owners.
///
/// # Example
/// ```
/// use nessa_sync::replication::{
///     catalogue::{validate_manifest_request, CataloguePass, ManifestRequest},
///     domain::{Id, Scope},
/// };
/// let id = Id::new("opaque").unwrap();
/// let request = ManifestRequest {
///     pass: CataloguePass {
///         scope: Scope::new(id.clone(), id.clone(), id.clone(), id.clone(), id.clone(), id),
///         completed: 0,
///         boundary: 1,
///         cursor: None,
///         generation: 1,
///     },
///     max_entries: 1,
/// };
/// assert_eq!(validate_manifest_request(&request, 1), Ok(()));
/// ```
///
/// # Errors
/// Returns [`CatalogueValidationError::InvalidRequest`] for zero entry count,
/// either exceeded entry ceiling or a refusal from [`validate_catalogue_pass`].
pub fn validate_manifest_request(
    request: &ManifestRequest,
    max_entries: usize,
) -> Result<(), CatalogueValidationError> {
    if request.max_entries == 0
        || request.max_entries > MAX_CATALOGUE_ENTRIES
        || request.max_entries > max_entries
    {
        return Err(CatalogueValidationError::InvalidRequest);
    }
    validate_catalogue_pass(&request.pass)
}

/// Checks the numerical fields of one current catalogue descriptor.
///
/// This pure check borrows the descriptor and leaves it unchanged. Its [`Id`]
/// already carries validated identity syntax. It establishes no page boundary,
/// revision comparison, payload meaning, authorization or durable progress;
/// those checks retain their respective owners. Independent or shared immutable
/// descriptors may be checked concurrently, without resource ownership changes.
/// `individual_entry_accepts_full_numeric_range` covers the accepted range and
/// unchanged input; `individual_entry_refuses_invalid_numeric_evidence` covers
/// refusal. Manifest validation consumes this same owner.
///
/// # Errors
/// Returns [`CatalogueValidationError::InvalidOrder`] when creation is zero or
/// the current revision precedes creation.
pub fn validate_manifest_entry(entry: &ManifestEntry) -> Result<(), CatalogueValidationError> {
    if !creation_is_positive(entry.key.creation) || entry.revision < entry.key.creation {
        return Err(CatalogueValidationError::InvalidOrder);
    }
    Ok(())
}

/// Validates a bounded page before resolving any payloads.
pub fn validate_manifest(
    request: &ManifestRequest,
    page: &ManifestPage,
    max_entries: usize,
) -> Result<(), CatalogueValidationError> {
    validate_manifest_request(request, max_entries)?;
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
    let mut identities = HashSet::with_capacity(page.entries.len());
    for entry in &page.entries {
        validate_manifest_entry(entry)?;
        if !key_is_in_boundary(&entry.key, request.pass.boundary)
            || entry.revision <= request.pass.completed
            || previous.is_some_and(|key| entry.key <= *key)
            || !identities.insert(&entry.key.id)
        {
            return Err(CatalogueValidationError::InvalidOrder);
        }
        previous = Some(&entry.key);
    }
    Ok(())
}

fn key_is_in_boundary(key: &EntryKey, boundary: u64) -> bool {
    creation_is_positive(key.creation) && key.creation <= boundary
}

fn creation_is_positive(creation: u64) -> bool {
    creation > 0
}

/// Checks that two descriptors for one key describe a compatible revision order.
///
/// This pure constant-work operation borrows both values and performs no I/O or
/// allocation. It validates their relationship, not individual page ranges,
/// payload bytes, cache presence or authority. Equal revisions retain the same
/// deletion meaning; later revisions cannot restore an earlier deleted identity.
/// Stores choose the earlier/later order from their coherent revision evidence.
///
/// # Errors
/// Returns [`CatalogueValidationError::WrongPayload`] for differing stable keys,
/// backward revisions or changed deletion meaning at the same revision. Returns
/// [`CatalogueValidationError::DeletionFence`] for live content after deletion.
pub fn validate_catalogue_revision_transition(
    earlier: &ManifestEntry,
    later: &ManifestEntry,
) -> Result<(), CatalogueValidationError> {
    if later.key != earlier.key
        || later.revision < earlier.revision
        || (later.revision == earlier.revision && later.deleted != earlier.deleted)
    {
        return Err(CatalogueValidationError::WrongPayload);
    }
    if earlier.deleted && !later.deleted {
        return Err(CatalogueValidationError::DeletionFence);
    }
    Ok(())
}

/// Checks a current payload against the manifest identity and revision.
pub fn validate_resolved(
    manifest: &ManifestEntry,
    resolved: &ResolvedEntry,
    max_payload_bytes: usize,
) -> Result<(), CatalogueValidationError> {
    validate_catalogue_revision_transition(manifest, &resolved.manifest)
        .map_err(|_| CatalogueValidationError::WrongPayload)?;
    if resolved.manifest.deleted && !resolved.payload.is_empty() {
        return Err(CatalogueValidationError::WrongPayload);
    }
    if resolved.payload.len() > max_payload_bytes {
        return Err(CatalogueValidationError::BoundsExceeded);
    }
    Ok(())
}

/// Validates the cross-field structure of one public catalogue commit plan.
///
/// This pure, bounded operation checks the manifest/pass correlation, stable
/// continuation, final-page meaning, exact resolved/unchanged coverage and the
/// published entry/payload ceilings. It reads no storage, grants no authority,
/// and does not validate host payload schemas. A store calls it before effects,
/// then checks durable scope/pass generation, unchanged cache revisions and
/// deletion fences in its own atomic transaction. The public DTO is mutable;
/// a prior successful validation is not evidence for a subsequently edited plan.
///
/// # Errors
/// Returns [`CatalogueValidationError::WrongRequest`] for contradicted
/// correlation/continuation, [`CatalogueValidationError::WrongPayload`] for
/// contradicted entry coverage, or the manifest/budget validation error.
pub fn validate_catalogue_page_plan(
    plan: &CataloguePagePlan,
) -> Result<(), CatalogueValidationError> {
    if plan.manifest.request.pass != plan.pass
        || plan.final_page == plan.manifest.has_more
        || plan.next_cursor != page_cursor(&plan.manifest)
    {
        return Err(CatalogueValidationError::WrongRequest);
    }
    validate_manifest(
        &plan.manifest.request,
        &plan.manifest,
        MAX_CATALOGUE_ENTRIES,
    )?;
    if plan.entries.len().checked_add(plan.unchanged.len()) != Some(plan.manifest.entries.len()) {
        return Err(CatalogueValidationError::WrongPayload);
    }
    for manifest in &plan.manifest.entries {
        let resolved = plan
            .entries
            .iter()
            .find(|entry| entry.manifest.key.id == manifest.key.id);
        let unchanged = plan
            .unchanged
            .iter()
            .find(|entry| entry.key.id == manifest.key.id);
        if resolved.is_some() == unchanged.is_some() {
            return Err(CatalogueValidationError::WrongPayload);
        }
        if let Some(value) = resolved {
            validate_resolved(manifest, value, MAX_CATALOGUE_PAYLOAD_BYTES)?;
        } else if unchanged != Some(manifest) {
            return Err(CatalogueValidationError::WrongPayload);
        }
    }
    let total = plan.entries.iter().try_fold(0usize, |sum, entry| {
        sum.checked_add(entry.payload.len())
            .ok_or(CatalogueValidationError::BoundsExceeded)
    })?;
    if total > MAX_CATALOGUE_PAYLOAD_BYTES {
        return Err(CatalogueValidationError::BoundsExceeded);
    }
    Ok(())
}
