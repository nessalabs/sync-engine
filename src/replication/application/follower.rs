//! Synchronous, explicitly driven replication. A host calls `begin_pass` on
//! startup, reconnect, or a wake hint, and drives `step` until completion.
//! No timer, subscription, or retry is created here. Each source read is preceded
//! by current authorization; every page is validated before atomic store apply.

use crate::replication::domain::{
    validate_capabilities, validate_open_page, validate_open_page_request, validate_page,
    CapabilityError, CapabilitySupport, Checkpoint, CommitPlan, Limits, OpenPageRequest,
    OpenedPage, Page, PageRequest, Scope, SourceHead, ValidationError,
};

mod cancellation;
pub use cancellation::{
    CancellablePageStep, PageStepAttempt, PageStepCancel, PageStepCancelResult,
};

/// Current host policy decision for this exact receiver and stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    /// Authorized under the supplied current scope and epoch.
    Allowed(Scope),
    /// Authenticated denial. Host owns any cache invalidation policy.
    Denied,
    /// Source scope or identity no longer matches.
    IdentityChanged,
    /// Policy reply was malformed or used an unsupported status.
    InvalidResponse,
    /// Policy could not be checked; keep cached data but deliver nothing.
    Unverifiable,
}

/// Host authorization port. It must check current policy before every source read.
pub trait ScopeAuthorizer {
    /// Checks the exact requested scope; `Allowed` must echo its current identity.
    fn authorize(&mut self, scope: &Scope) -> Access;
}

/// Maximum payload accepted by the reference record source and its wire adapter.
pub const MAX_RECORD_BYTES: usize = 512 * 1024;

/// Typed source failure; source adapters must enforce the request bounds before allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceError {
    /// Source metadata version or reader requirements are incompatible.
    Incompatible(CapabilityError),
    /// Authentication or receiver policy refused this request.
    Denied,
    /// Source is temporarily unavailable.
    Unavailable,
    /// History is pruned; a later compatible reset is required.
    Pruned,
    /// Request was outside the source's current bounds or had invalid limits.
    InvalidRequest,
    /// Source identity changed; no silent checkpoint reset is allowed.
    IdentityChanged,
    /// Next record cannot fit the requested byte budget.
    OversizedRecord,
}

/// Bounded, committed source reads. Source payloads are untrusted until validation.
pub trait RecordSource {
    /// Reads the current committed head for the exact authorized scope.
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError>;
    /// Reads at most one bounded page within the captured target.
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError>;
}

/// Explicit metadata port for initial capability admission. There is no blanket
/// conversion from a legacy bare head, and no transport support is implied.
/// Returned metadata must describe this exact authorized scope. Subsequent
/// reads still require their own compatibility contract; a Pass is no proof.
///
/// A bare-head implementation cannot silently acquire this contract:
/// ```compile_fail
/// use nessa_sync::replication::application::{RecordSource, RecordHeadSource, SourceError};
/// use nessa_sync::replication::domain::{Scope, Page, PageRequest};
/// struct Legacy;
/// impl RecordSource for Legacy {
///     fn head(&mut self, _: &Scope) -> Result<u64, SourceError> { Ok(0) }
///     fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> { Err(SourceError::Unavailable) }
/// }
/// fn requires_metadata(_: impl RecordHeadSource) {}
/// requires_metadata(Legacy);
/// ```
pub trait RecordHeadSource: RecordSource {
    /// Read current committed head and requirements, before any payload request.
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError>;
}

/// One owned, capability-coherent record view for a server read operation.
/// Requirements/encoding returned by view_head must remain valid for every
/// page through its captured head on this handle. The factory must establish
/// that snapshot or immutable-format guarantee; object identity alone is not
/// evidence. This is stronger than initial-admission RecordHeadSource.
/// Current authorization remains the server's separate responsibility.
///
/// Initial metadata admission alone does not establish a coherent read view:
/// ```compile_fail
/// use nessa_sync::replication::application::{RecordSource, RecordHeadSource, RecordReadView, SourceError};
/// use nessa_sync::replication::domain::{Scope, Page, PageRequest, SourceHead};
/// struct InitialOnly;
/// impl RecordSource for InitialOnly {
///     fn head(&mut self, _: &Scope) -> Result<u64, SourceError> { Ok(0) }
///     fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> { Err(SourceError::Unavailable) }
/// }
/// impl RecordHeadSource for InitialOnly {
///     fn source_head(&mut self, s: &Scope) -> Result<SourceHead, SourceError> { Ok(SourceHead::new(s.clone(),0,0,0)) }
/// }
/// fn requires_view(_: impl RecordReadView) {}
/// requires_view(InitialOnly);
/// ```
pub trait RecordReadView: RecordSource {
    /// Read committed metadata for this exact scope and coherent encoding view.
    fn view_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError>;
}

/// Optional first-page port. The authoritative adapter captures one committed
/// head and reads only through it, checking current policy before each underlying
/// read and emitted batch. Responses are untrusted until canonical validation.
pub trait OpenRecordSource: RecordSource {
    /// Read one bounded first page, echoing the full request and captured target.
    /// A target at/below `after` has no records; it cannot reset saved progress.
    fn open_page(&mut self, request: &OpenPageRequest) -> Result<OpenedPage, SourceError>;
}

/// Atomic replica-store refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// Storage was unavailable or transaction failed; no effect was committed.
    Failed,
    /// Commit may have happened; caller must reload progress before retrying.
    Uncertain,
    /// Competing writer moved progress before this plan committed.
    Stale,
    /// The saved receiver/stream exists under a different incarnation, schema, or access epoch.
    ScopeMismatch {
        /// Scope already saved for this receiver and stream.
        saved: Box<Scope>,
        /// Scope requested by the current attempt.
        requested: Box<Scope>,
    },
    /// A record ID was previously committed with a different meaning.
    ConflictingRecord,
    /// This receiver/source stream has a durable deletion fence.
    Fenced,
}

/// Replica store. `apply` must compare scope and position inside one atomic operation,
/// check immutable ID reuse, and save records with the new checkpoint together.
pub trait ReplicaStore {
    /// Loads durable progress for this receiver. `None` starts at position zero.
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError>;
    /// Atomically applies a validated plan or leaves records and progress unchanged.
    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError>;
}

/// Use-case failure with the owner of the refusal preserved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// Source metadata version or required reader features are incompatible.
    Incompatible(CapabilityError),
    /// Authorization returned an unsupported or malformed reply.
    InvalidAccessResponse,
    /// Policy denied delivery.
    Denied,
    /// Current policy could not be verified.
    Unverifiable,
    /// Authorization returned a different scope or epoch.
    WrongAccessScope,
    /// Source read failed.
    Source(SourceError),
    /// Store read or apply failed.
    Store(StoreError),
    /// Pure validation refused the response.
    Validation(ValidationError),
    /// Source head is behind durable local progress.
    SourceBehindCheckpoint,
}

impl From<SourceError> for SyncError {
    fn from(error: SourceError) -> Self {
        match error {
            SourceError::Incompatible(error) => Self::Incompatible(error),
            other => Self::Source(other),
        }
    }
}

/// One finite pass, with a target captured from one authorized head read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pass {
    scope: Scope,
    target: u64,
    next: u64,
}

impl Pass {
    /// Captured finish line; source writes cannot move it.
    pub fn target(&self) -> u64 {
        self.target
    }
    /// Last position applied by this pass.
    pub fn position(&self) -> u64 {
        self.next
    }
    /// Whether this captured pass has finished.
    pub fn is_complete(&self) -> bool {
        self.next == self.target
    }
}

/// Starts or rechecks a pass from durable progress. Current authorization is
/// checked before local progress or the source head, so a revoked or unverifiable
/// grant cannot disclose a saved scope through a mismatch error. Calling this
/// again explicitly recovers lost hints and uncertain commits.
pub fn begin_pass<A: ScopeAuthorizer, S: RecordSource, R: ReplicaStore>(
    scope: &Scope,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
) -> Result<Pass, SyncError> {
    begin_from_head(scope, authorizer, store, || {
        source.head(scope).map_err(SyncError::from)
    })
}

/// Initial admission through an explicitly versioned metadata port. Refuse
/// unsupported reader requirements before page fetch or apply; ignore writer
/// requirements. This is not mid-pass compatibility fencing. Keep fresh explicit
/// post-pass rechecks, and do not use legacy/combined-page starts as a downgrade.
pub fn begin_pass_with_capabilities<A: ScopeAuthorizer, S: RecordHeadSource, R: ReplicaStore>(
    scope: &Scope,
    supported: CapabilitySupport,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
) -> Result<Pass, SyncError> {
    begin_from_head(scope, authorizer, store, || {
        let head = source.source_head(scope).map_err(SyncError::from)?;
        if head.scope() != scope {
            return Err(SyncError::Validation(ValidationError::WrongRequest));
        }
        validate_capabilities(&head, supported).map_err(SyncError::Incompatible)?;
        Ok(head.position())
    })
}
fn begin_from_head<A: ScopeAuthorizer, R: ReplicaStore>(
    scope: &Scope,
    authorizer: &mut A,
    store: &mut R,
    read_head: impl FnOnce() -> Result<u64, SyncError>,
) -> Result<Pass, SyncError> {
    check_access(scope, authorizer)?;
    let checkpoint = store.load(scope).map_err(SyncError::Store)?;
    let position = match checkpoint {
        None => 0,
        Some(saved) if saved.scope() == scope => saved.position(),
        Some(saved) => {
            return Err(SyncError::Store(StoreError::ScopeMismatch {
                saved: Box::new(saved.scope().clone()),
                requested: Box::new(scope.clone()),
            }))
        }
    };
    let target = read_head()?;
    if target < position {
        return Err(SyncError::SourceBehindCheckpoint);
    }
    Ok(Pass {
        scope: scope.clone(),
        target,
        next: position,
    })
}

/// Opt-in pass start that captures a target with the first page. Check current
/// authority before local progress and again before the combined source read,
/// then validate and atomically apply at most one page. Empty/behind-head replies
/// cannot advance progress. Errors return no pass; after an uncertain commit,
/// reload actual durable progress on a fresh explicit attempt. Remaining pages
/// use `step`; hosts must retain their normal post-pass `begin_pass` head recheck.
/// This preserves the existing non-atomic policy/commit boundary. Empty requests
/// still pay both access checks; no grant or advisory head is cached for speed.
pub fn begin_pass_with_page<A: ScopeAuthorizer, S: OpenRecordSource, R: ReplicaStore>(
    scope: &Scope,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
) -> Result<Pass, SyncError> {
    check_access(scope, authorizer)?;
    let saved = store.load(scope).map_err(SyncError::Store)?;
    let position = match saved {
        None => 0,
        Some(saved) if saved.scope() == scope => saved.position(),
        Some(saved) => {
            return Err(SyncError::Store(StoreError::ScopeMismatch {
                saved: Box::new(saved.scope().clone()),
                requested: Box::new(scope.clone()),
            }))
        }
    };
    let request = OpenPageRequest {
        scope: scope.clone(),
        after: position,
        max_records: limits.max_records(),
        max_payload_bytes: limits.max_payload_bytes(),
        max_record_bytes: limits.max_record_bytes(),
    };
    validate_open_page_request(&request, limits).map_err(SyncError::Validation)?;
    check_access(scope, authorizer)?;
    let opened = source.open_page(&request).map_err(SyncError::from)?;
    let (target, plan) = validate_open_page(
        &Checkpoint::new(scope.clone(), position),
        &request,
        opened,
        limits,
    )
    .map_err(|error| {
        if error == ValidationError::HeadBehindProgress {
            SyncError::SourceBehindCheckpoint
        } else {
            SyncError::Validation(error)
        }
    })?;
    let mut next = position;
    if let Some(plan) = plan {
        let committed = plan.next().position();
        store.apply(plan).map_err(SyncError::Store)?;
        next = committed;
    }
    Ok(Pass {
        scope: scope.clone(),
        target,
        next,
    })
}

/// Applies at most one page and returns whether the captured pass is complete.
/// The host may choose a different validated page limit on each step; the
/// captured target and saved progress remain fixed by the pass and store.
/// A failure leaves the pass's in-memory position unchanged. After an uncertain
/// commit, discard the pass and call `begin_pass` to reload durable progress.
pub fn step<A: ScopeAuthorizer, S: RecordSource, R: ReplicaStore>(
    pass: &mut Pass,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
) -> Result<bool, SyncError> {
    if pass.is_complete() {
        return Ok(true);
    }
    let plan = prepare_step(pass, limits, authorizer, source)?;
    apply_step(pass, plan, store)
}

/// Run one ordinary follower page step with an explicit one-shot local apply
/// gate. This does not cancel the synchronous source read or send a wire
/// message. A late complete response is discarded if cancellation wins.
///
/// Once apply is admitted, the store owns the outcome. Cancellation cannot
/// interrupt it or turn an uncertain result into a rollback. After uncertainty,
/// discard the pass and reload durable progress through authorized
/// [begin_pass]. Actual policy/source/validation errors remain typed even if a
/// cancellation races the failing read.
pub fn step_cancellable<A: ScopeAuthorizer, S: RecordSource, R: ReplicaStore>(
    pass: &mut Pass,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
    attempt: PageStepAttempt,
) -> Result<CancellablePageStep, SyncError> {
    if attempt.cancelled() {
        return Ok(CancellablePageStep::CancelledBeforeApply);
    }
    if pass.is_complete() {
        return Ok(if attempt.finish_without_apply() {
            CancellablePageStep::Finished { complete: true }
        } else {
            CancellablePageStep::CancelledBeforeApply
        });
    }
    let plan = prepare_step(pass, limits, authorizer, source)?;
    if !attempt.claim() {
        return Ok(CancellablePageStep::CancelledBeforeApply);
    }
    let complete = apply_step(pass, plan, store)?;
    Ok(CancellablePageStep::Finished { complete })
}

fn prepare_step<A: ScopeAuthorizer, S: RecordSource>(
    pass: &Pass,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
) -> Result<CommitPlan, SyncError> {
    check_access(&pass.scope, authorizer)?;
    let request = PageRequest {
        scope: pass.scope.clone(),
        after: pass.next,
        target: pass.target,
        max_records: limits.max_records(),
        max_payload_bytes: limits.max_payload_bytes(),
        max_record_bytes: limits.max_record_bytes(),
    };
    let page = source.page(&request).map_err(SyncError::from)?;
    let expected = Checkpoint::new(pass.scope.clone(), pass.next);
    validate_page(&expected, &request, page, limits).map_err(SyncError::Validation)
}

fn apply_step<R: ReplicaStore>(
    pass: &mut Pass,
    plan: CommitPlan,
    store: &mut R,
) -> Result<bool, SyncError> {
    let next = plan.next().position();
    store.apply(plan).map_err(SyncError::Store)?;
    pass.next = next;
    Ok(pass.is_complete())
}

/// Runs exactly the captured pass through its target. A later explicit `begin_pass`
/// checks for churn; this function does not continuously chase a moving head.
pub fn finish_pass<A: ScopeAuthorizer, S: RecordSource, R: ReplicaStore>(
    pass: &mut Pass,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
    store: &mut R,
) -> Result<(), SyncError> {
    while !step(pass, limits, authorizer, source, store)? {}
    Ok(())
}

fn check_access<A: ScopeAuthorizer>(scope: &Scope, authorizer: &mut A) -> Result<(), SyncError> {
    match authorizer.authorize(scope) {
        Access::Allowed(current) if current == *scope => Ok(()),
        Access::Allowed(_) => Err(SyncError::WrongAccessScope),
        Access::Denied => Err(SyncError::Denied),
        Access::IdentityChanged => Err(SyncError::WrongAccessScope),
        Access::InvalidResponse => Err(SyncError::InvalidAccessResponse),
        Access::Unverifiable => Err(SyncError::Unverifiable),
    }
}
