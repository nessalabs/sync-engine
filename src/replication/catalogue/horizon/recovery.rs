//! V2 read policy over the existing finite recovery transition owner.
use super::*;
use crate::replication::{
    application::ScopeAuthorizer,
    catalogue::recovery::{self, RecoveryReads},
};
/// One bounded v2 recovery receipt. No transport or progress owner is retained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HorizonRecoveryStep {
    /// Committed phase/cursor; the host explicitly drives or reloads the next step.
    Pending(CatalogueRecovery),
    /// Recovery released after a fresh compatible v2 post-commit head check.
    Finished(CatalogueHorizonHead),
}
/// Start an explicitly authorized same-scope v2 reset, including a fresh receiver.
/// `None` must CAS actual parent absence in the same transaction as the journal;
/// it never creates an ordinary zero checkpoint first. Existing work uses Some.
pub fn start_catalogue_recovery_with_horizon<
    A: ScopeAuthorizer,
    S: CatalogueHorizonSource,
    D: CatalogueRecoveryStore + CatalogueHorizonStore,
>(
    scope: &Scope,
    expected: Option<CatalogueProgress>,
    support: CapabilitySupport,
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<CatalogueRecovery, CatalogueError> {
    if let Some(p) = &expected {
        validate_catalogue_progress(p)
            .map_err(CatalogueStoreError::from)
            .map_err(CatalogueError::Store)?;
        if p.scope != *scope {
            return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
        }
    }
    application::authorize(scope, a)?;
    let h = s.horizon_head(scope, support)?;
    validate_catalogue_horizon(&h, scope, support)?;
    if h.source.position() < expected.as_ref().map_or(0, |p| p.completed) {
        return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
    }
    observe(&h, scope, support, a, d)?;
    let planned = match &expected {
        Some(p) => recovery::recovery_start(p, h.source.position()),
        None => recovery::recovery_fresh_start(scope, h.source.position()),
    }
    .map_err(CatalogueError::Store)?;
    application::authorize(scope, a)?;
    let returned = match expected {
        Some(p) => d.start_recovery(scope, p, h.source.position()),
        None => d.start_fresh_recovery(scope, h.source.position()),
    }
    .map_err(CatalogueError::Store)?;
    recovery::confirm(planned, returned)
}
struct Reads<'a, S> {
    source: &'a mut S,
    last_head: Option<CatalogueHorizonHead>,
}
fn reset_observation<A: ScopeAuthorizer, D: CatalogueHorizonStore>(
    h: &CatalogueHorizonHead,
    p: &CataloguePass,
    c: CapabilitySupport,
    a: &mut A,
    d: &mut D,
) -> Result<(), CatalogueError> {
    validate_catalogue_horizon(h, &p.scope, c)?;
    if h.source.position() < p.boundary {
        return Err(CatalogueError::Source(
            CatalogueSourceError::IdentityChanged,
        ));
    }
    observe(h, &p.scope, c, a, d)?;
    if h.horizon > p.boundary {
        return Err(CatalogueError::Store(CatalogueStoreError::ResetRequired));
    }
    Ok(())
}
impl<A: ScopeAuthorizer, D: CatalogueHorizonStore, S: CatalogueHorizonSource> RecoveryReads<A, D>
    for Reads<'_, S>
{
    fn finish_head(
        &mut self,
        scope: &Scope,
        c: CapabilitySupport,
        completed: u64,
        a: &mut A,
        d: &mut D,
    ) -> Result<SourceHead, CatalogueError> {
        let h = self.source.horizon_head(scope, c)?;
        validate_catalogue_horizon(&h, scope, c)?;
        if h.source.position() < completed {
            return Err(CatalogueError::Source(
                CatalogueSourceError::IdentityChanged,
            ));
        }
        observe(&h, scope, c, a, d)?;
        validate_catalogue_coverage(completed, &h)?;
        let component = h.source.clone();
        self.last_head = Some(h);
        Ok(component)
    }
    fn manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        a: &mut A,
        d: &mut D,
    ) -> Result<CatalogueManifestRead, CatalogueError> {
        let read = coverage_result(
            self.source
                .horizon_manifest(r, c, CatalogueReadKind::FullReset),
            &r.pass,
            c,
            CatalogueReadKind::FullReset,
            a,
            d,
        )?;
        validate_manifest(r, &read.page, MAX_CATALOGUE_ENTRIES)
            .map_err(CatalogueError::Validation)?;
        if read
            .page
            .entries
            .iter()
            .any(|v| v.revision > read.head.source.position())
        {
            return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
        }
        reset_observation(&read.head, &r.pass, c, a, d)?;
        Ok(CatalogueManifestRead {
            head: read.head.source,
            page: read.page,
        })
    }
    fn value(
        &mut self,
        p: &CataloguePass,
        id: &crate::replication::domain::Id,
        b: usize,
        c: CapabilitySupport,
        a: &mut A,
        d: &mut D,
    ) -> Result<CatalogueValueRead, CatalogueError> {
        let read = coverage_result(
            self.source
                .horizon_value(p, id, b, c, CatalogueReadKind::FullReset),
            p,
            c,
            CatalogueReadKind::FullReset,
            a,
            d,
        )?;
        if read.value.manifest.key.id != *id
            || read.value.manifest.revision > read.head.source.position()
        {
            return Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest));
        }
        reset_observation(&read.head, p, c, a, d)?;
        Ok(CatalogueValueRead {
            head: read.head.source,
            value: read.value,
        })
    }
}
/// Drive the existing Enumerate/Reconcile/Finish engine through explicit v2 reads.
/// Source grants are not retained; every bounded call checks current authority.
/// Failure/uncertainty stops this call. The host reloads and explicitly resumes or
/// supersedes an expired boundary; no automatic reset or replay is introduced.
pub fn step_catalogue_recovery_with_horizon<
    A: ScopeAuthorizer,
    S: CatalogueHorizonSource,
    D: CatalogueRecoveryStore + CatalogueHorizonStore,
>(
    r: &CatalogueRecovery,
    support: CapabilitySupport,
    limit: usize,
    max_bytes: usize,
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<HorizonRecoveryStep, CatalogueError> {
    let mut reads = Reads {
        source: s,
        last_head: None,
    };
    match recovery::step_using(r, support, limit, max_bytes, a, &mut reads, d)? {
        RecoveryStep::Pending(r) => Ok(HorizonRecoveryStep::Pending(r)),
        RecoveryStep::Finished(h) => match reads.last_head {
            Some(v) if v.source == h => Ok(HorizonRecoveryStep::Finished(v)),
            _ => Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest)),
        },
    }
}
