//! Explicit catalogue v2 coverage; passive observations never advance progress.
use super::*;
use crate::replication::domain::{validate_capabilities, CapabilitySupport, Scope, SourceHead};
/// Catalogue coverage contract version, independent of SourceHead v1.
pub const CATALOGUE_HORIZON_VERSION: u16 = 2;
/// Untrusted exact-scope v2 publication, never authority or a checkpoint receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogueHorizonHead {
    /// Explicit contract version; v1 must never be reinterpreted as horizon zero.
    pub version: u16,
    /// Coherent source-owned revision and format requirements.
    pub source: SourceHead,
    /// Lowest completed C whose incremental deletion coverage is guaranteed.
    pub horizon: u64,
}
impl CatalogueHorizonHead {
    /// Construct this explicit version from a coherent source snapshot.
    pub fn new(source: SourceHead, horizon: u64) -> Self {
        Self {
            version: CATALOGUE_HORIZON_VERSION,
            source,
            horizon,
        }
    }
}
/// Admission kind; full reset must finish the durable candidate journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogueReadKind {
    /// Compare pass.completed with the horizon, never boundary/cursor.
    Incremental,
    /// Explicit compatible full scan with completed zero and per-ID reconciliation.
    FullReset,
}
/// Select the comparison point without treating a reset's zero C as coverage.
pub(crate) fn coverage_base(
    kind: CatalogueReadKind,
    completed: u64,
    boundary: u64,
) -> Result<u64, CatalogueSourceError> {
    match kind {
        CatalogueReadKind::Incremental => Ok(completed),
        CatalogueReadKind::FullReset if completed == 0 => Ok(boundary),
        CatalogueReadKind::FullReset => Err(CatalogueSourceError::InvalidRequest),
    }
}
/// Coherent descriptors plus v2 coverage metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HorizonManifestRead {
    /// Exact source snapshot describing coverage and returned descriptors.
    pub head: CatalogueHorizonHead,
    /// Correlated finite manifest.
    pub page: ManifestPage,
}
/// Coherent current value plus v2 coverage metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HorizonValueRead {
    /// Exact source snapshot covering the returned value revision.
    pub head: CatalogueHorizonHead,
    /// Current owned live value or retained deletion proof.
    pub value: ResolvedEntry,
}
/// Explicit coverage-aware source ports. No default implementation or v1 fallback.
pub trait CatalogueHorizonSource {
    /// Read an explicit v2 head under current source identity.
    fn horizon_head(
        &mut self,
        scope: &Scope,
        supported: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError>;
    /// Validate coverage before acquiring descriptors in the same snapshot.
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        supported: CapabilitySupport,
        kind: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError>;
    /// Validate coverage before acquiring payload in the same snapshot.
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &crate::replication::domain::Id,
        budget: usize,
        supported: CapabilitySupport,
        kind: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError>;
}
/// Passive exact-scope coverage observations under the receiver store owner.
/// Observation commits may survive a refused pass, but never create progress.
/// Implementations must compare the actual completed C against the durable
/// observation atomically inside inherited `begin` and `apply_page` effects.
/// A preceding observation call alone cannot fence competing handles. Explicit
/// reset reconciliation remains a distinct generation/phase contract.
pub trait CatalogueHorizonStore: CatalogueStore {
    /// Compare/update a durable observation. A lower horizon changes nothing.
    fn observe_horizon(&mut self, scope: &Scope, horizon: u64) -> Result<(), CatalogueStoreError>;
    /// Reload a durable floor independently of completed/active progress.
    fn observed_horizon(&mut self, scope: &Scope) -> Result<Option<u64>, CatalogueStoreError>;
}
/// Validate known metadata before its horizon can be observed or used.
/// Scope/capabilities are separate from authority; caller must authorize first.
pub fn validate_catalogue_horizon(
    head: &CatalogueHorizonHead,
    scope: &Scope,
    supported: CapabilitySupport,
) -> Result<(), CatalogueError> {
    if head.version != CATALOGUE_HORIZON_VERSION {
        return Err(CatalogueError::Source(
            CatalogueSourceError::HorizonVersion(head.version),
        ));
    }
    if head.source.scope() != scope || head.horizon > head.source.position() {
        return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
    }
    validate_capabilities(&head.source, supported).map_err(CatalogueError::Incompatible)
}
/// Compare actual durable C; equality is valid. No mutation or cursor inference.
pub fn validate_catalogue_coverage(
    completed: u64,
    head: &CatalogueHorizonHead,
) -> Result<(), CatalogueSourceError> {
    if completed < head.horizon {
        Err(CatalogueSourceError::HorizonExpired {
            completed,
            head: Box::new(head.clone()),
        })
    } else {
        Ok(())
    }
}

/// Fresh v2 admission on every initial/resumed pass. Accepted observations may
/// persist even when coverage expires; no projection or progress changes then.
pub fn begin_or_resume_with_horizon<
    A: crate::replication::application::ScopeAuthorizer,
    S: CatalogueHorizonSource,
    D: CatalogueHorizonStore,
>(
    scope: &Scope,
    supported: CapabilitySupport,
    authorizer: &mut A,
    source: &mut S,
    store: &mut D,
) -> Result<Option<CataloguePass>, CatalogueError> {
    application::authorize(scope, authorizer)?;
    let saved = store.progress(scope).map_err(CatalogueError::Store)?;
    if let Some(p) = &saved {
        validate_catalogue_progress(p)
            .map_err(CatalogueStoreError::from)
            .map_err(CatalogueError::Store)?;
        if p.scope != *scope {
            return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
        }
    }
    let head = source
        .horizon_head(scope, supported)
        .map_err(CatalogueError::from)?;
    observe(&head, scope, supported, authorizer, store)?;
    validate_catalogue_coverage(saved.as_ref().map_or(0, |p| p.completed), &head)
        .map_err(CatalogueError::from)?;
    if let Some(pass) = saved.as_ref().and_then(|p| p.active.as_ref()) {
        if head.source.position() < pass.boundary {
            return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
        }
        return Ok(Some(pass.clone()));
    }
    let completed = saved.as_ref().map_or(0, |p| p.completed);
    if head.source.position() < completed {
        return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
    }
    if head.source.position() == completed && saved.is_some() {
        return Ok(None);
    }
    application::authorize(scope, authorizer)?;
    application::begin_checked(scope, saved, head.source.position(), store).map(|p| p.active)
}
pub(super) fn observe<
    A: crate::replication::application::ScopeAuthorizer,
    D: CatalogueHorizonStore,
>(
    h: &CatalogueHorizonHead,
    s: &Scope,
    supported: CapabilitySupport,
    a: &mut A,
    d: &mut D,
) -> Result<(), CatalogueError> {
    validate_catalogue_horizon(h, s, supported)?;
    application::authorize(s, a)?;
    d.observe_horizon(s, h.horizon)
        .map_err(CatalogueError::Store)
}
struct HorizonReads<'a, S> {
    source: &'a mut S,
    supported: CapabilitySupport,
}
impl<
        A: crate::replication::application::ScopeAuthorizer,
        D: CatalogueHorizonStore,
        S: CatalogueHorizonSource,
    > application::PageReads<A, D> for HorizonReads<'_, S>
{
    fn manifest(
        &mut self,
        r: &ManifestRequest,
        a: &mut A,
        d: &mut D,
    ) -> Result<ManifestPage, CatalogueError> {
        let read = coverage_result(
            self.source
                .horizon_manifest(r, self.supported, CatalogueReadKind::Incremental),
            &r.pass,
            self.supported,
            CatalogueReadKind::Incremental,
            a,
            d,
        )?;
        validate_manifest(r, &read.page, MAX_CATALOGUE_ENTRIES)
            .map_err(CatalogueError::Validation)?;
        if read.head.source.position() < r.pass.boundary
            || read
                .page
                .entries
                .iter()
                .any(|e| e.revision > read.head.source.position())
        {
            return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
        }
        observe(&read.head, &r.pass.scope, self.supported, a, d)?;
        validate_catalogue_coverage(r.pass.completed, &read.head).map_err(CatalogueError::from)?;
        Ok(read.page)
    }
    fn value(
        &mut self,
        p: &CataloguePass,
        id: &crate::replication::domain::Id,
        b: usize,
        a: &mut A,
        d: &mut D,
    ) -> Result<ResolvedEntry, CatalogueError> {
        let read = coverage_result(
            self.source
                .horizon_value(p, id, b, self.supported, CatalogueReadKind::Incremental),
            p,
            self.supported,
            CatalogueReadKind::Incremental,
            a,
            d,
        )?;
        if read.value.manifest.key.id != *id
            || read.head.source.position() < p.boundary
            || read.value.manifest.revision > read.head.source.position()
        {
            return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
        }
        observe(&read.head, &p.scope, self.supported, a, d)?;
        validate_catalogue_coverage(p.completed, &read.head).map_err(CatalogueError::from)?;
        Ok(read.value)
    }
}
/// Stage a complete bounded v2 page through the existing page-commit owner.
/// Each owned snapshot checks C, and the store rechecks its observed horizon in
/// the commit transaction. A hint or head never advances durable completion.
pub fn apply_next_page_with_horizon<
    A: crate::replication::application::ScopeAuthorizer,
    S: CatalogueHorizonSource,
    D: CatalogueHorizonStore,
>(
    pass: &CataloguePass,
    max_entries: usize,
    max_payload_bytes: usize,
    supported: CapabilitySupport,
    authorizer: &mut A,
    source: &mut S,
    store: &mut D,
) -> Result<CatalogueProgress, CatalogueError> {
    application::apply_page_using(
        pass,
        max_entries,
        max_payload_bytes,
        authorizer,
        &mut HorizonReads { source, supported },
        store,
    )
}

pub(super) fn coverage_result<
    T,
    A: crate::replication::application::ScopeAuthorizer,
    D: CatalogueHorizonStore,
>(
    result: Result<T, CatalogueSourceError>,
    p: &CataloguePass,
    supported: CapabilitySupport,
    kind: CatalogueReadKind,
    a: &mut A,
    d: &mut D,
) -> Result<T, CatalogueError> {
    match result {
        Err(CatalogueSourceError::HorizonExpired { completed, head }) => {
            if completed != p.completed
                || head.horizon <= coverage_base(kind, p.completed, p.boundary)?
                || head.source.position() < p.boundary
            {
                return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
            }
            observe(&head, &p.scope, supported, a, d)?;
            Err(CatalogueError::Source(
                CatalogueSourceError::HorizonExpired { completed, head },
            ))
        }
        other => other.map_err(CatalogueError::from),
    }
}

mod recovery;
pub use recovery::*;
