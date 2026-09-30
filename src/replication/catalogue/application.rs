//! Authorized catalogue reads and atomic page-commit ports.

use crate::replication::application::{Access, ScopeAuthorizer};
use crate::replication::domain::{Id, Scope};

use super::{
    validate_manifest, validate_resolved, CataloguePagePlan, CataloguePass, CatalogueProgress,
    CatalogueValidationError, ManifestPage, ManifestRequest, ResolvedEntry, MAX_CATALOGUE_ENTRIES,
    MAX_CATALOGUE_PAYLOAD_BYTES,
};

/// Source refusal before receiver progress changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueSourceError {
    /// Source cannot currently answer or resolve an entry.
    Unavailable,
    /// Exact source identity or access scope changed.
    IdentityChanged,
    /// Request exceeds configured bounds.
    InvalidRequest,
    /// One current value exceeds the payload limit.
    OversizedEntry,
}

/// Durable receiver refusal; rejected transactions leave progress unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueStoreError {
    /// Store failed without a confirmed commit.
    Failed,
    /// Commit outcome is uncertain; reload progress before retry.
    Uncertain,
    /// Pass or reset generation was superseded.
    Stale,
    /// Existing scope/incarnation/epoch needs explicit reset.
    ResetRequired,
    /// Permanent entry deletion fence rejected content.
    Fenced,
    /// Same revision carried different content or identity.
    Conflict,
}

/// End-to-end refusal with source and store errors preserved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueError {
    /// Current policy denied delivery.
    Denied,
    /// Current policy cannot be checked.
    Unverifiable,
    /// Policy returned a different scope or epoch.
    WrongAccessScope,
    /// Source read failed.
    Source(CatalogueSourceError),
    /// Response validation failed.
    Validation(CatalogueValidationError),
    /// Local store refused the transition.
    Store(CatalogueStoreError),
}

/// One current-value source, with no per-receiver payload history.
pub trait CatalogueSource {
    /// Reads the current committed revision for this exact scope.
    fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError>;
    /// Returns one bounded manifest page under a short source read snapshot.
    fn manifest(&mut self, request: &ManifestRequest)
        -> Result<ManifestPage, CatalogueSourceError>;
    /// Resolves the latest authorized value or retained deletion marker.
    fn resolve(
        &mut self,
        pass: &CataloguePass,
        id: &Id,
        max_payload_bytes: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError>;
}

/// Atomic receiver progress and latest-value store.
pub trait CatalogueStore {
    /// Returns saved progress for the exact receiver and catalogue name.
    fn progress(&mut self, scope: &Scope)
        -> Result<Option<CatalogueProgress>, CatalogueStoreError>;
    /// Starts a pass only if current progress still equals `expected`.
    fn begin(
        &mut self,
        scope: &Scope,
        expected: Option<CatalogueProgress>,
        boundary: u64,
    ) -> Result<CatalogueProgress, CatalogueStoreError>;
    /// Returns current cached entry revision, including a deletion marker.
    fn cached_revision(
        &mut self,
        scope: &Scope,
        id: &Id,
    ) -> Result<Option<u64>, CatalogueStoreError>;
    /// Saves resolved entries and continuation in one transaction.
    fn apply_page(
        &mut self,
        plan: CataloguePagePlan,
    ) -> Result<CatalogueProgress, CatalogueStoreError>;
    /// Explicitly replaces an incompatible scope, retaining permanent deletion
    /// markers but erasing previously authorized live values and pass state.
    fn reset(
        &mut self,
        scope: &Scope,
        expected: CatalogueProgress,
    ) -> Result<CatalogueProgress, CatalogueStoreError>;
}

fn authorize<A: ScopeAuthorizer>(scope: &Scope, authorizer: &mut A) -> Result<(), CatalogueError> {
    match authorizer.authorize(scope) {
        Access::Allowed(current) if current == *scope => Ok(()),
        Access::Allowed(_) => Err(CatalogueError::WrongAccessScope),
        Access::Denied => Err(CatalogueError::Denied),
        Access::Unverifiable => Err(CatalogueError::Unverifiable),
    }
}

/// Starts a finite pass, or returns the saved active pass after a restart.
/// An unchanged head produces no new pass and no payload request.
pub fn begin_or_resume<A, S, D>(
    scope: &Scope,
    authorizer: &mut A,
    source: &mut S,
    store: &mut D,
) -> Result<Option<CataloguePass>, CatalogueError>
where
    A: ScopeAuthorizer,
    S: CatalogueSource,
    D: CatalogueStore,
{
    authorize(scope, authorizer)?;
    let saved = store.progress(scope).map_err(CatalogueError::Store)?;
    if let Some(progress) = &saved {
        if progress.scope != *scope {
            return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
        }
        if let Some(pass) = &progress.active {
            return Ok(Some(pass.clone()));
        }
    }
    let head = source.head(scope).map_err(CatalogueError::Source)?;
    let completed = saved.as_ref().map_or(0, |p| p.completed);
    if head < completed {
        return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
    }
    if head == completed && saved.is_none() {
        authorize(scope, authorizer)?;
        store.begin(scope, None, 0).map_err(CatalogueError::Store)?;
        return Ok(None);
    }
    if head == completed {
        return Ok(None);
    }
    authorize(scope, authorizer)?;
    store
        .begin(scope, saved, head)
        .map_err(CatalogueError::Store)
        .map(|progress| progress.active)
}

/// Resolves and commits exactly one bounded page. A failed payload read does
/// not advance the cursor. The caller repeats until `active` becomes `None`,
/// then calls `begin_or_resume` to check for newer revisions.
pub fn apply_next_page<A, S, D>(
    pass: &CataloguePass,
    max_entries: usize,
    max_payload_bytes: usize,
    authorizer: &mut A,
    source: &mut S,
    store: &mut D,
) -> Result<CatalogueProgress, CatalogueError>
where
    A: ScopeAuthorizer,
    S: CatalogueSource,
    D: CatalogueStore,
{
    if max_entries == 0
        || max_entries > MAX_CATALOGUE_ENTRIES
        || max_payload_bytes == 0
        || max_payload_bytes > MAX_CATALOGUE_PAYLOAD_BYTES
    {
        return Err(CatalogueError::Validation(
            CatalogueValidationError::InvalidRequest,
        ));
    }
    authorize(&pass.scope, authorizer)?;
    let request = ManifestRequest {
        pass: pass.clone(),
        max_entries,
    };
    let page = source.manifest(&request).map_err(CatalogueError::Source)?;
    validate_manifest(&request, &page, MAX_CATALOGUE_ENTRIES)
        .map_err(CatalogueError::Validation)?;
    let mut resolved = Vec::new();
    let mut unchanged = Vec::new();
    let mut total_bytes = 0usize;
    for entry in &page.entries {
        let cached = store
            .cached_revision(&pass.scope, &entry.key.id)
            .map_err(CatalogueError::Store)?;
        if cached.is_some_and(|revision| revision >= entry.revision) {
            unchanged.push(entry.clone());
            continue;
        }
        authorize(&pass.scope, authorizer)?;
        let remaining = max_payload_bytes.saturating_sub(total_bytes);
        if remaining == 0 {
            return Err(CatalogueError::Validation(
                CatalogueValidationError::BoundsExceeded,
            ));
        }
        let value = source
            .resolve(pass, &entry.key.id, remaining)
            .map_err(CatalogueError::Source)?;
        validate_resolved(entry, &value, max_payload_bytes).map_err(CatalogueError::Validation)?;
        total_bytes =
            total_bytes
                .checked_add(value.payload.len())
                .ok_or(CatalogueError::Validation(
                    CatalogueValidationError::BoundsExceeded,
                ))?;
        if total_bytes > max_payload_bytes {
            return Err(CatalogueError::Validation(
                CatalogueValidationError::BoundsExceeded,
            ));
        }
        resolved.push(value);
    }
    authorize(&pass.scope, authorizer)?;
    let plan =
        CataloguePagePlan::new(page, resolved, unchanged).map_err(CatalogueError::Validation)?;
    store.apply_page(plan).map_err(CatalogueError::Store)
}

/// Explicitly resets an incompatible cache under the new currently authorized
/// scope. The store erases old live values and advances the reset generation.
pub fn reset_catalogue<A: ScopeAuthorizer, D: CatalogueStore>(
    scope: &Scope,
    expected: CatalogueProgress,
    authorizer: &mut A,
    store: &mut D,
) -> Result<CatalogueProgress, CatalogueError> {
    authorize(scope, authorizer)?;
    store.reset(scope, expected).map_err(CatalogueError::Store)
}
