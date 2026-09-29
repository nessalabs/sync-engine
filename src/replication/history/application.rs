//! Authorized tail/older reads and atomic store ports. Scheduling stays with
//! the host; fetching and installation can be separated to test delayed replies.

use crate::replication::application::{Access, ScopeAuthorizer};
use crate::replication::domain::{Limits, Scope};

use super::{
    validate_older, validate_tail, HistoryProgress, HistoryValidationError, OlderPage, OlderPlan,
    OlderRequest, TailPlan, TailRequest, TailSnapshot,
};

/// Reference source outcome for bounded history reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistorySourceError {
    /// Source cannot currently answer.
    Unavailable,
    /// Requested history was pruned; an explicit compatible reset is needed.
    ResetRequired,
    /// Scope/source incarnation no longer matches.
    IdentityChanged,
    /// Request exceeded source bounds.
    InvalidRequest,
    /// A single record cannot fit the requested byte budget.
    OversizedRecord,
}

/// Durable history-store outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryStoreError {
    /// Store transaction or read failed without a confirmed commit.
    Failed,
    /// Commit outcome is unknown; reload progress before retry.
    Uncertain,
    /// Response generation, watermark, or boundary was superseded.
    Stale,
    /// Existing source identity/schema/epoch requires an explicit compatible reset.
    ResetRequired,
    /// Permanent deletion fence prevents installation.
    Fenced,
    /// Overlap reused a position or immutable record ID with different meaning.
    Conflict,
    /// Page cannot extend current contiguous history without a gap.
    Gap,
}

/// Authorization, source, validation, or store refusal with owner preserved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryError {
    /// Current policy denied delivery.
    Denied,
    /// Current policy could not be verified.
    Unverifiable,
    /// Authorization echoed another scope/epoch.
    WrongAccessScope,
    /// Bounded source read failed.
    Source(HistorySourceError),
    /// Pure response validation failed.
    Validation(HistoryValidationError),
    /// Durable installation failed.
    Store(HistoryStoreError),
}

/// Host-facing loading state. A view keeps verified cached progress while a
/// fetch is pending, failed, or stale; absence is never inferred as emptiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryReadState {
    /// No compatible snapshot or verified empty result has been installed.
    Unloaded,
    /// A bounded source request is in progress; cached progress may still show.
    Loading(Option<HistoryProgress>),
    /// Recent content exists while older positions remain uncovered.
    Partial(HistoryProgress),
    /// A successful snapshot confirmed a zero-head source.
    CompleteEmpty(HistoryProgress),
    /// All history down to position one is locally covered.
    Complete(HistoryProgress),
    /// Latest request failed; verified cached progress remains available.
    Failed(Option<HistoryProgress>, HistoryError),
    /// Source is unavailable and the verified cache may be behind.
    Stale(HistoryProgress),
    /// A permanent local deletion fence removed cached content.
    Deleted(HistoryProgress),
}

impl HistoryReadState {
    /// Classifies durable progress without consulting the network.
    pub fn from_progress(progress: Option<HistoryProgress>) -> Self {
        match progress {
            None => Self::Unloaded,
            Some(progress) if progress.deleted => Self::Deleted(progress),
            Some(progress) if progress.live_head == 0 => Self::CompleteEmpty(progress),
            Some(progress) if progress.lower_bound == 1 => Self::Complete(progress),
            Some(progress) => Self::Partial(progress),
        }
    }
}

/// A source that can capture a recent suffix and bounded older pages.
pub trait HistorySource {
    /// Captures one bounded committed suffix and source pruning floor.
    fn tail(&mut self, request: &TailRequest) -> Result<TailSnapshot, HistorySourceError>;
    /// Reads one bounded contiguous older page, ending before `before`.
    fn older(&mut self, request: &OlderRequest) -> Result<OlderPage, HistorySourceError>;
}

/// Atomic tail/history store. The live follower's `ReplicaStore` port is
/// implemented by the same durable adapter but remains a separate contract.
pub trait HistoryStore {
    /// Loads saved progress, including the deletion fence if present.
    fn history_progress(
        &mut self,
        scope: &Scope,
    ) -> Result<Option<HistoryProgress>, HistoryStoreError>;
    /// Atomically installs a validated suffix and its two boundaries.
    fn install_tail(&mut self, plan: TailPlan) -> Result<HistoryProgress, HistoryStoreError>;
    /// Atomically checks overlap and adds only a missing older prefix.
    fn install_older(&mut self, plan: OlderPlan) -> Result<HistoryProgress, HistoryStoreError>;
    /// Atomically fences and removes cached records for this identity.
    fn fence_deletion(&mut self, scope: &Scope) -> Result<(), HistoryStoreError>;
}

fn check_access<A: ScopeAuthorizer>(scope: &Scope, authorizer: &mut A) -> Result<(), HistoryError> {
    match authorizer.authorize(scope) {
        Access::Allowed(current) if current == *scope => Ok(()),
        Access::Allowed(_) => Err(HistoryError::WrongAccessScope),
        Access::Denied => Err(HistoryError::Denied),
        Access::Unverifiable => Err(HistoryError::Unverifiable),
    }
}

/// Fetches and validates a suffix without installing it. The host can cancel a
/// delayed plan; the store rechecks generation, watermark and fences at commit.
pub fn fetch_tail<A: ScopeAuthorizer, S: HistorySource>(
    request: &TailRequest,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
) -> Result<TailPlan, HistoryError> {
    check_access(&request.scope, authorizer)?;
    let snapshot = source.tail(request).map_err(HistoryError::Source)?;
    validate_tail(request, snapshot, limits).map_err(HistoryError::Validation)
}

/// Fetches and validates older content without installing it. The atomic
/// store checks current lower coverage and overlap when this plan is applied.
pub fn fetch_older<A: ScopeAuthorizer, S: HistorySource>(
    request: &OlderRequest,
    limits: Limits,
    authorizer: &mut A,
    source: &mut S,
) -> Result<OlderPlan, HistoryError> {
    check_access(&request.scope, authorizer)?;
    let page = source.older(request).map_err(HistoryError::Source)?;
    validate_older(request, page, limits).map_err(HistoryError::Validation)
}

const MAX_HYDRATION_WAITERS: usize = 256;

/// Small host-owned request combiner. Callers enqueue view needs before the
/// next bounded fetch; the loader asks for the minimum uncovered lower target.
/// Urgent head checks can run on an independent source handle between pages.
#[derive(Debug)]
pub struct HydrationQueue {
    max_waiters: usize,
    needs: Vec<u64>,
}

impl HydrationQueue {
    /// Creates a queue with a finite number of simultaneous view requests.
    pub fn new(max_waiters: usize) -> Result<Self, HistoryValidationError> {
        if max_waiters == 0 || max_waiters > MAX_HYDRATION_WAITERS {
            return Err(HistoryValidationError::InvalidRequest);
        }
        Ok(Self {
            max_waiters,
            needs: Vec::new(),
        })
    }

    /// Adds a requested first visible position. Repeated requests coalesce.
    pub fn request(&mut self, lower_target: u64) -> Result<(), HistoryValidationError> {
        if lower_target == 0 || self.needs.len() >= self.max_waiters {
            return Err(HistoryValidationError::InvalidRequest);
        }
        self.needs.push(lower_target);
        Ok(())
    }

    /// Minimum still-uncovered requested position, or `None` when all are met.
    pub fn next_target(&self, current_lower: u64) -> Option<u64> {
        self.needs
            .iter()
            .copied()
            .filter(|target| *target < current_lower)
            .min()
    }

    /// Removes requests covered by an atomically saved lower boundary.
    pub fn resolve(&mut self, current_lower: u64) -> usize {
        let before = self.needs.len();
        self.needs.retain(|target| *target < current_lower);
        before - self.needs.len()
    }

    /// Number of waiting view requests, including duplicates.
    pub fn waiters(&self) -> usize {
        self.needs.len()
    }
}
