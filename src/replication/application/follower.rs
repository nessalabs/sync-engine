//! Synchronous, explicitly driven replication. A host calls `begin_pass` on
//! startup, reconnect, or a wake hint, and drives `step` until completion.
//! No timer, subscription, or retry is created here. Each source read is preceded
//! by current authorization; every page is validated before atomic store apply.

use crate::replication::domain::{
    validate_page, Checkpoint, CommitPlan, Limits, Page, PageRequest, Scope, ValidationError,
};

/// Current host policy decision for this exact receiver and stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    /// Authorized under the supplied current scope and epoch.
    Allowed(Scope),
    /// Authenticated denial. Host owns any cache invalidation policy.
    Denied,
    /// Policy could not be checked; keep cached data but deliver nothing.
    Unverifiable,
}

/// Host authorization port. It must check current policy before every source read.
pub trait ScopeAuthorizer {
    /// Checks the exact requested scope; `Allowed` must echo its current identity.
    fn authorize(&mut self, scope: &Scope) -> Access;
}

/// Typed source failure; source adapters must enforce the request bounds before allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceError {
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
    let target = source.head(scope).map_err(SyncError::Source)?;
    if target < position {
        return Err(SyncError::SourceBehindCheckpoint);
    }
    Ok(Pass {
        scope: scope.clone(),
        target,
        next: position,
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
    check_access(&pass.scope, authorizer)?;
    let request = PageRequest {
        scope: pass.scope.clone(),
        after: pass.next,
        target: pass.target,
        max_records: limits.max_records(),
        max_payload_bytes: limits.max_payload_bytes(),
        max_record_bytes: limits.max_record_bytes(),
    };
    let page = source.page(&request).map_err(SyncError::Source)?;
    let expected = Checkpoint::new(pass.scope.clone(), pass.next);
    let plan = validate_page(&expected, &request, page, limits).map_err(SyncError::Validation)?;
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
        Access::Unverifiable => Err(SyncError::Unverifiable),
    }
}
