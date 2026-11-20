//! Explicit same-scope reset with a finite durable candidate worklist.
use super::application::authorize;
use super::*;
use crate::replication::{
    application::ScopeAuthorizer,
    domain::{validate_capabilities, CapabilitySupport, Scope, SourceHead},
};
/// Durable reset phase. Normal catalogue operations cannot finish a reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// Read a complete finite manifest at captured boundary.
    Enumerate,
    /// Resolve remaining prior live IDs using current owned evidence.
    Reconcile,
    /// Publish the boundary, then freshly check head before releasing recovery.
    Finish,
}
/// Exact generation-bound recovery receipt; adapters compare the entire value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogueRecovery {
    /// Existing sole durable catalogue progress owner.
    pub progress: CatalogueProgress,
    /// Captured finite reset boundary; observations never advance it.
    pub boundary: u64,
    /// Current durable phase.
    pub phase: RecoveryPhase,
}
/// Recovery-only ports under the existing catalogue store transaction owner.
/// V2 composition requires every mutation to recheck the observed horizon in
/// that transaction: captured H must cover it while rebuilding, and actual C
/// must cover it before releasing published Finish. Refusal preserves the journal.
pub trait CatalogueRecoveryStore: CatalogueStore {
    /// Reload exact current recovery. Uncertain outcomes require this reload.
    fn recovery(&mut self, scope: &Scope)
        -> Result<Option<CatalogueRecovery>, CatalogueStoreError>;
    /// Compare progress, capture candidate floors, erase live payloads and advance
    /// generation atomically. Existing pending candidates survive a superseding reset.
    fn start_recovery(
        &mut self,
        scope: &Scope,
        expected: CatalogueProgress,
        boundary: u64,
    ) -> Result<CatalogueRecovery, CatalogueStoreError>;
    /// Atomically CAS parent absence and create the first generation/phase.
    /// No ordinary checkpoint may be created first. Unsupported adapters refuse;
    /// callers must not fall back from a stale existing-parent comparison.
    fn start_fresh_recovery(
        &mut self,
        _scope: &Scope,
        _boundary: u64,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        Err(CatalogueStoreError::ResetRequired)
    }
    /// Commit a complete resolved manifest page with its recovery continuation.
    fn recovery_page(
        &mut self,
        expected: &CatalogueRecovery,
        plan: CataloguePagePlan,
    ) -> Result<CatalogueRecovery, CatalogueStoreError>;
    /// Read a bounded prefix of prior-live identity/revision floors.
    fn recovery_candidates(
        &mut self,
        expected: &CatalogueRecovery,
        limit: usize,
    ) -> Result<Vec<ManifestEntry>, CatalogueStoreError>;
    /// Classify a complete bounded batch and remove it atomically. Missing source
    /// rows are never supplied as deletion/absence evidence.
    fn recovery_values(
        &mut self,
        expected: &CatalogueRecovery,
        values: Vec<ResolvedEntry>,
    ) -> Result<CatalogueRecovery, CatalogueStoreError>;
    /// Reconcile with no remaining candidates enters Finish. Finish publishes H
    /// but retains the fence until a fresh post-commit head check succeeds.
    fn advance_recovery(
        &mut self,
        expected: &CatalogueRecovery,
    ) -> Result<CatalogueRecovery, CatalogueStoreError>;
    /// Release Finish only after its boundary is durable and a fresh check succeeds.
    fn finish_recovery(&mut self, expected: &CatalogueRecovery) -> Result<(), CatalogueStoreError>;
}
/// Result of one bounded host-driven recovery step. No timer/retry owner is added.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryStep {
    /// Durable work remains; the returned state is the exact commit receipt.
    Pending(CatalogueRecovery),
    /// Recovery released after a fresh head check. A higher head needs a new pass.
    Finished(SourceHead),
}
pub(crate) fn recovery_valid(r: &CatalogueRecovery) -> Result<(), CatalogueError> {
    validate_catalogue_progress(&r.progress)
        .map_err(CatalogueStoreError::from)
        .map_err(CatalogueError::Store)?;
    let p = &r.progress;
    let ok = p.generation > 0
        && match r.phase {
            RecoveryPhase::Enumerate => {
                p.completed == 0 && p.active.as_ref().is_some_and(|a| a.boundary == r.boundary)
            }
            RecoveryPhase::Reconcile => p.completed == 0 && p.active.is_none(),
            RecoveryPhase::Finish => {
                p.active.is_none() && (p.completed == 0 || p.completed == r.boundary)
            }
        };
    if !ok {
        return Err(CatalogueError::Store(CatalogueStoreError::Conflict));
    }
    Ok(())
}
fn admit(
    head: &SourceHead,
    scope: &Scope,
    support: CapabilitySupport,
    boundary: u64,
) -> Result<(), CatalogueError> {
    if head.scope() != scope || head.position() < boundary {
        return Err(CatalogueError::Source(
            CatalogueSourceError::IdentityChanged,
        ));
    }
    validate_capabilities(head, support).map_err(CatalogueError::Incompatible)
}
/// Start an explicit same-scope reset after current authorization and fresh
/// capability admission. No absent/missing response is authoritative.
pub fn start_catalogue_recovery<
    A: ScopeAuthorizer,
    S: CatalogueHeadSource,
    D: CatalogueRecoveryStore,
>(
    scope: &Scope,
    expected: CatalogueProgress,
    support: CapabilitySupport,
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<CatalogueRecovery, CatalogueError> {
    validate_catalogue_progress(&expected)
        .map_err(CatalogueStoreError::from)
        .map_err(CatalogueError::Store)?;
    if expected.scope != *scope {
        return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
    }
    authorize(scope, a)?;
    let h = s.source_head(scope)?;
    admit(&h, scope, support, expected.completed)?;
    authorize(scope, a)?;
    let planned = recovery_start(&expected, h.position()).map_err(CatalogueError::Store)?;
    let r = d
        .start_recovery(scope, expected, h.position())
        .map_err(CatalogueError::Store)?;
    confirm(planned, r)
}
/// One bounded authorized step, using coherent native publication/value reads.
/// Whole batches stage before store effects. Errors stop this call; callers own
/// retry and must explicitly reload after uncertain results. Finish observes head
/// only after the captured boundary has committed, including after restart.
pub fn step_catalogue_recovery<
    A: ScopeAuthorizer,
    S: CatalogueReadSource,
    D: CatalogueRecoveryStore,
>(
    r: &CatalogueRecovery,
    support: CapabilitySupport,
    limit: usize,
    max_bytes: usize,
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<RecoveryStep, CatalogueError> {
    step_using(
        r,
        support,
        limit,
        max_bytes,
        a,
        &mut LegacyRecoveryReads(s),
        d,
    )
}
pub(super) trait RecoveryReads<A, D> {
    fn finish_head(
        &mut self,
        scope: &Scope,
        support: CapabilitySupport,
        minimum: u64,
        a: &mut A,
        d: &mut D,
    ) -> Result<SourceHead, CatalogueError>;
    fn manifest(
        &mut self,
        r: &ManifestRequest,
        support: CapabilitySupport,
        a: &mut A,
        d: &mut D,
    ) -> Result<CatalogueManifestRead, CatalogueError>;
    fn value(
        &mut self,
        p: &CataloguePass,
        id: &crate::replication::domain::Id,
        budget: usize,
        support: CapabilitySupport,
        a: &mut A,
        d: &mut D,
    ) -> Result<CatalogueValueRead, CatalogueError>;
}
struct LegacyRecoveryReads<'a, S>(&'a mut S);
impl<A, D, S: CatalogueReadSource> RecoveryReads<A, D> for LegacyRecoveryReads<'_, S> {
    fn finish_head(
        &mut self,
        s: &Scope,
        _: CapabilitySupport,
        _: u64,
        _: &mut A,
        _: &mut D,
    ) -> Result<SourceHead, CatalogueError> {
        Ok(self.0.source_head(s)?)
    }
    fn manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        _: &mut A,
        _: &mut D,
    ) -> Result<CatalogueManifestRead, CatalogueError> {
        Ok(self.0.manifest_with_head(r, c)?)
    }
    fn value(
        &mut self,
        p: &CataloguePass,
        id: &crate::replication::domain::Id,
        b: usize,
        c: CapabilitySupport,
        _: &mut A,
        _: &mut D,
    ) -> Result<CatalogueValueRead, CatalogueError> {
        Ok(self.0.resolve_with_head(p, id, b, c)?)
    }
}
pub(super) fn step_using<A: ScopeAuthorizer, S: RecoveryReads<A, D>, D: CatalogueRecoveryStore>(
    r: &CatalogueRecovery,
    support: CapabilitySupport,
    limit: usize,
    max_bytes: usize,
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<RecoveryStep, CatalogueError> {
    recovery_valid(r)?;
    if limit == 0
        || limit > MAX_CATALOGUE_ENTRIES
        || max_bytes == 0
        || max_bytes > MAX_CATALOGUE_PAYLOAD_BYTES
    {
        return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
    }
    let scope = &r.progress.scope;
    authorize(scope, a)?;
    match r.phase {
        RecoveryPhase::Enumerate => {
            let pass = r.progress.active.as_ref().unwrap();
            let request = ManifestRequest {
                pass: pass.clone(),
                max_entries: limit,
            };
            let read = s.manifest(&request, support, a, d)?;
            admit(&read.head, scope, support, r.boundary)?;
            validate_manifest(&request, &read.page, MAX_CATALOGUE_ENTRIES)
                .map_err(CatalogueError::Validation)?;
            if read
                .page
                .entries
                .iter()
                .any(|e| e.revision > read.head.position())
            {
                return Err(CatalogueError::Source(CatalogueSourceError::Unavailable));
            }
            let mut values = Vec::new();
            let mut bytes = 0;
            for floor in &read.page.entries {
                authorize(scope, a)?;
                let v = s.value(
                    pass,
                    &floor.key.id,
                    max_bytes.saturating_sub(bytes),
                    support,
                    a,
                    d,
                )?;
                admit(&v.head, scope, support, r.boundary)?;
                validate_resolved(floor, &v.value, max_bytes)
                    .map_err(CatalogueError::Validation)?;
                if v.value.manifest.revision > v.head.position() {
                    return Err(CatalogueError::Source(CatalogueSourceError::Unavailable));
                }
                bytes += v.value.payload.len();
                if bytes > max_bytes {
                    return Err(CatalogueError::Source(CatalogueSourceError::OversizedEntry));
                }
                values.push(v.value);
            }
            authorize(scope, a)?;
            let plan = CataloguePagePlan::new(read.page, values, vec![])
                .map_err(CatalogueError::Validation)?;
            let planned = recovery_page(r, &plan).map_err(CatalogueError::Store)?;
            Ok(RecoveryStep::Pending(confirm(
                planned,
                d.recovery_page(r, plan).map_err(CatalogueError::Store)?,
            )?))
        }
        RecoveryPhase::Reconcile => {
            let floors = d
                .recovery_candidates(r, limit)
                .map_err(CatalogueError::Store)?;
            if floors.len() > limit
                || floors.iter().any(|f| {
                    f.deleted
                        || f.key.creation > r.boundary
                        || f.revision > r.boundary
                        || validate_manifest_entry(f).is_err()
                })
            {
                return Err(CatalogueError::Store(CatalogueStoreError::Conflict));
            }
            if floors
                .iter()
                .map(|f| &f.key.id)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != floors.len()
            {
                return Err(CatalogueError::Store(CatalogueStoreError::Conflict));
            }
            if floors.is_empty() {
                authorize(scope, a)?;
                return Ok(RecoveryStep::Pending(confirm(
                    recovery_advance(r).map_err(CatalogueError::Store)?,
                    d.advance_recovery(r).map_err(CatalogueError::Store)?,
                )?));
            }
            let pass = CataloguePass {
                scope: scope.clone(),
                completed: 0,
                boundary: r.boundary,
                cursor: None,
                generation: r.progress.generation,
            };
            let mut values = Vec::new();
            let mut bytes = 0;
            for floor in floors {
                authorize(scope, a)?;
                let read = s.value(
                    &pass,
                    &floor.key.id,
                    max_bytes.saturating_sub(bytes),
                    support,
                    a,
                    d,
                )?;
                admit(&read.head, scope, support, r.boundary)?;
                validate_resolved(&floor, &read.value, max_bytes)
                    .map_err(CatalogueError::Validation)?;
                if read.value.manifest.revision > read.head.position() {
                    return Err(CatalogueError::Source(CatalogueSourceError::Unavailable));
                }
                bytes += read.value.payload.len();
                if bytes > max_bytes {
                    return Err(CatalogueError::Source(CatalogueSourceError::OversizedEntry));
                }
                values.push(read.value);
            }
            authorize(scope, a)?;
            Ok(RecoveryStep::Pending(confirm(
                r.clone(),
                d.recovery_values(r, values)
                    .map_err(CatalogueError::Store)?,
            )?))
        }
        RecoveryPhase::Finish => {
            if r.progress.completed != r.boundary {
                return Ok(RecoveryStep::Pending(confirm(
                    recovery_advance(r).map_err(CatalogueError::Store)?,
                    d.advance_recovery(r).map_err(CatalogueError::Store)?,
                )?));
            }
            let head = s.finish_head(scope, support, r.progress.completed, a, d)?;
            admit(&head, scope, support, r.boundary)?;
            authorize(scope, a)?;
            d.finish_recovery(r).map_err(CatalogueError::Store)?;
            Ok(RecoveryStep::Finished(head))
        }
    }
}

pub(crate) fn recovery_fresh_start(
    scope: &Scope,
    boundary: u64,
) -> Result<CatalogueRecovery, CatalogueStoreError> {
    recovery_start(
        &CatalogueProgress {
            scope: scope.clone(),
            completed: 0,
            generation: 0,
            active: None,
        },
        boundary,
    )
}
pub(crate) fn recovery_start(
    p: &CatalogueProgress,
    h: u64,
) -> Result<CatalogueRecovery, CatalogueStoreError> {
    validate_catalogue_progress(p).map_err(CatalogueStoreError::from)?;
    if h < p.completed {
        return Err(CatalogueStoreError::ResetRequired);
    }
    let generation = p
        .generation
        .checked_add(1)
        .ok_or(CatalogueStoreError::Conflict)?;
    let scope = p.scope.clone();
    Ok(CatalogueRecovery {
        progress: CatalogueProgress {
            scope: scope.clone(),
            completed: 0,
            generation,
            active: (h > 0).then_some(CataloguePass {
                scope,
                completed: 0,
                boundary: h,
                cursor: None,
                generation,
            }),
        },
        boundary: h,
        phase: if h == 0 {
            RecoveryPhase::Reconcile
        } else {
            RecoveryPhase::Enumerate
        },
    })
}
pub(crate) fn recovery_page(
    r: &CatalogueRecovery,
    plan: &CataloguePagePlan,
) -> Result<CatalogueRecovery, CatalogueStoreError> {
    recovery_valid(r).map_err(|_| CatalogueStoreError::Conflict)?;
    if !plan.unchanged.is_empty() {
        return Err(CatalogueStoreError::Conflict);
    }
    if r.phase != RecoveryPhase::Enumerate || r.progress.active.as_ref() != Some(&plan.pass) {
        return Err(CatalogueStoreError::Stale);
    }
    let mut p = catalogue_progress_after_page(plan).map_err(CatalogueStoreError::from)?;
    p.completed = 0;
    Ok(CatalogueRecovery {
        progress: p,
        boundary: r.boundary,
        phase: if plan.final_page {
            RecoveryPhase::Reconcile
        } else {
            RecoveryPhase::Enumerate
        },
    })
}
pub(crate) fn recovery_advance(
    r: &CatalogueRecovery,
) -> Result<CatalogueRecovery, CatalogueStoreError> {
    recovery_valid(r).map_err(|_| CatalogueStoreError::Conflict)?;
    let mut next = r.clone();
    match r.phase {
        RecoveryPhase::Reconcile => next.phase = RecoveryPhase::Finish,
        RecoveryPhase::Finish if r.progress.completed != r.boundary => {
            next.progress.completed = r.boundary
        }
        _ => return Err(CatalogueStoreError::Stale),
    };
    Ok(next)
}

pub(super) fn confirm(
    planned: CatalogueRecovery,
    returned: CatalogueRecovery,
) -> Result<CatalogueRecovery, CatalogueError> {
    if planned != returned {
        return Err(CatalogueError::Store(CatalogueStoreError::Conflict));
    }
    Ok(returned)
}
