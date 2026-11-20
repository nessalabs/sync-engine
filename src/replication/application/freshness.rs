//! Opt-in, explicitly driven per-scope cache freshness. See the freshness design
//! for host cache visibility, timer ownership and process-local timestamp limits.

use std::time::Duration;

use super::{
    begin_pass_with_capabilities, step, Pass, RecordHeadSource, ReplicaStore, ScopeAuthorizer,
    SourceError, StoreError, SyncError,
};
use crate::replication::domain::{CapabilitySupport, Limits, Scope};

/// Host monotonic clock; timestamps are local to this owner, not wall-clock dates.
pub trait Clock {
    /// Current elapsed time. Backward observations invalidate freshness times.
    fn now(&self) -> Duration;
}

/// Host randomness port. The owner clamps samples to its equal-jitter interval.
pub trait RetryJitter {
    /// Select a delay for this exponential window; valid range is [ceiling/2, ceiling].
    fn delay(&mut self, ceiling: Duration) -> Duration;
}

/// Application cache-read policy. No variant starts a background task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessPolicy {
    /// Passive until an explicit, hint or reconnect wake.
    Never,
    /// A read explicitly asking for freshness first checks a head.
    Always,
    /// Such a read queues an expired check and immediately returns local data.
    Within(Duration),
}

/// Whether this read asks the policy to consider a head check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadIntent {
    /// Local navigation without a freshness request.
    Cached,
    /// Apply the configured freshness policy.
    Fresh,
}

/// Advisory reasons to queue one check; none carries progress or authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessWake {
    /// An explicit user/host request, including foreground fallback.
    Explicit,
    /// A possibly repeated or lost transport hint.
    Hint,
    /// The host has re-established its transport.
    Reconnect,
}

/// Exponential retry limits; host timers must use the owner's returned deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    initial: Duration,
    maximum: Duration,
}
impl RetryPolicy {
    /// Nonzero initial window and a maximum at least as large are required.
    pub fn new(initial: Duration, maximum: Duration) -> Result<Self, FreshnessError> {
        if initial.is_zero() || maximum < initial {
            return Err(FreshnessError::InvalidRetry);
        }
        Ok(Self { initial, maximum })
    }
}

/// Current coordination state; none is a grant or a claim of live source freshness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessPhase {
    /// No pass; a pending check may be waiting for host drive.
    Idle,
    /// Finishing one captured target.
    Applying,
    /// Must freshly recheck after a completed pass, even under Never.
    Recheck,
    /// Commit outcome or competing progress requires explicit authorized reload.
    ReloadRequired,
    /// A terminal refusal needs explicit host recovery.
    Stopped,
}

/// Independent process-local observations returned with a cached value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessMetadata {
    /// Exact identity to which every observation belongs.
    pub scope: Scope,
    /// Changes when the injected clock regresses; timestamps cannot span generations.
    pub clock_generation: u64,
    /// Latest confirmed advancing commit observed by this owner; unknown after restart/reload uncertainty.
    pub last_applied_at: Option<Duration>,
    /// Latest successfully admitted head, independent of apply and retained on a failed check.
    pub last_head_checked_at: Option<Duration>,
    /// Last admitted head, not proof that the source has not changed.
    pub observed_head: Option<u64>,
    /// Last authorized loaded or confirmed applied position; never derived from a head.
    pub position: Option<u64>,
    /// Previous attempt failed; the last successful observation may be stale.
    pub check_failed: bool,
    /// One coalesced check is pending or a mandatory recheck remains.
    pub check_pending: bool,
    /// Earliest retry time in the current clock generation.
    pub retry_at: Option<Duration>,
    /// Finite-pass/recovery state.
    pub phase: FreshnessPhase,
}

/// Host-projected local value accompanied by independent freshness observations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedRead<T> {
    /// Local-only host value, possibly behind the last observed head.
    pub value: T,
    /// Honest metadata; no timestamp is fabricated from durable progress.
    pub freshness: FreshnessMetadata,
}

/// Refusal to check or deliver an explicitly fresh read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FreshnessError {
    /// Current finite pass or retry deadline defers a fresh check.
    Deferred,
    /// Reload is necessary after uncertain or competing progress.
    ReloadRequired,
    /// Host must explicitly recover from a terminal failure.
    Stopped,
    /// Invalid retry limits.
    InvalidRetry,
    /// The clock cannot represent a retry deadline.
    ClockRange,
    /// Original authority/source/store refusal, preserved for the host.
    Sync(SyncError),
}

/// One operation performed by a host drive call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessWork {
    /// Passive or waiting for a retry deadline; no ports called.
    Waiting,
    /// Authorized checkpoint reload and fresh head admission, with no durable progress.
    Checked,
    /// One canonical page atomically applied.
    Applied,
}

/// Sole freshness/check/backoff/pass owner for one full scope. Calls are synchronous
/// and exclusive; the host serializes events and supplies local-only read callbacks.
/// Do not run a competing follower against its scope/store progress.
pub struct RecordFreshness<C, J> {
    clock: C,
    jitter: J,
    policy: FreshnessPolicy,
    support: CapabilitySupport,
    retry: RetryPolicy,
    window: Duration,
    last_now: Duration,
    meta: FreshnessMetadata,
    pass: Option<Pass>,
    pending: bool,
}

impl<C: Clock, J: RetryJitter> RecordFreshness<C, J> {
    /// Starts passive with unknown observation times even when the cache is populated.
    pub fn new(
        scope: Scope,
        policy: FreshnessPolicy,
        support: CapabilitySupport,
        clock: C,
        jitter: J,
        retry: RetryPolicy,
    ) -> Self {
        let now = clock.now();
        Self {
            clock,
            jitter,
            policy,
            support,
            retry,
            window: retry.initial,
            last_now: now,
            meta: Self::empty(scope, 0),
            pass: None,
            pending: false,
        }
    }

    fn empty(scope: Scope, clock_generation: u64) -> FreshnessMetadata {
        FreshnessMetadata {
            scope,
            clock_generation,
            last_applied_at: None,
            last_head_checked_at: None,
            observed_head: None,
            position: None,
            check_failed: false,
            check_pending: false,
            retry_at: None,
            phase: FreshnessPhase::Idle,
        }
    }

    fn now(&mut self) -> Result<Duration, FreshnessError> {
        let now = self.clock.now();
        if now < self.last_now {
            self.meta.clock_generation = self.meta.clock_generation.saturating_add(1);
            self.meta.last_applied_at = None;
            self.meta.last_head_checked_at = None;
            if let Some(deadline) = self.meta.retry_at {
                let remaining = deadline.saturating_sub(self.last_now);
                self.meta.retry_at = Some(now.checked_add(remaining).ok_or_else(|| {
                    self.meta.phase = FreshnessPhase::Stopped;
                    FreshnessError::ClockRange
                })?);
            }
        }
        self.last_now = now;
        Ok(now)
    }

    /// Returns metadata without network or local-store access; detects clock regression.
    pub fn metadata(&mut self) -> Result<FreshnessMetadata, FreshnessError> {
        self.now()?;
        let mut meta = self.meta.clone();
        meta.check_pending = self.pending || meta.phase == FreshnessPhase::Recheck;
        Ok(meta)
    }

    /// Coalesces ordinary wakes. It cannot escape a retry deadline or recover a refusal.
    pub fn request(&mut self, _wake: FreshnessWake) {
        self.pending = true;
    }

    /// Explicit host recovery after handling a refusal/uncertain outcome. The next
    /// drive reauthorizes and reloads durable progress; it does not replay an old pass.
    /// A prior retry deadline remains in force.
    pub fn resume(&mut self) {
        if matches!(
            self.meta.phase,
            FreshnessPhase::ReloadRequired | FreshnessPhase::Stopped
        ) {
            self.pass = None;
            self.meta.phase = FreshnessPhase::Idle;
        }
        self.pending = true;
    }

    /// Explicitly replace the full scope and clear only process-local state. The
    /// store/host owns any required durable reset and visibility fencing.
    pub fn reset_scope(&mut self, scope: Scope) {
        self.meta = Self::empty(scope, self.meta.clock_generation.saturating_add(1));
        self.pass = None;
        self.pending = false;
        self.window = self.retry.initial;
        self.last_now = self.clock.now();
    }

    fn ready(&self, now: Duration) -> Result<bool, FreshnessError> {
        match self.meta.phase {
            FreshnessPhase::ReloadRequired => return Err(FreshnessError::ReloadRequired),
            FreshnessPhase::Stopped => return Err(FreshnessError::Stopped),
            _ => {}
        }
        Ok(self.meta.retry_at.is_none_or(|deadline| now >= deadline))
    }

    fn failed(&mut self, error: SyncError) -> FreshnessError {
        self.meta.check_failed = true;
        match &error {
            SyncError::Store(StoreError::Uncertain | StoreError::Stale) => {
                self.pass = None;
                self.meta.position = None;
                self.meta.last_applied_at = None;
                self.meta.phase = FreshnessPhase::ReloadRequired;
            }
            SyncError::Unverifiable
            | SyncError::Source(SourceError::Unavailable)
            | SyncError::Store(StoreError::Failed) => {
                let ceiling = self.window;
                let minimum = (ceiling / 2).max(Duration::from_nanos(1));
                let delay = self.jitter.delay(ceiling).clamp(minimum, ceiling);
                let now = match self.now() {
                    Ok(now) => now,
                    Err(error) => return error,
                };
                let Some(deadline) = now.checked_add(delay) else {
                    self.meta.phase = FreshnessPhase::Stopped;
                    return FreshnessError::ClockRange;
                };
                self.meta.retry_at = Some(deadline);
                self.window = ceiling.saturating_mul(2).min(self.retry.maximum);
            }
            _ => {
                self.pass = None;
                self.meta.phase = FreshnessPhase::Stopped;
            }
        }
        FreshnessError::Sync(error)
    }

    /// Performs at most one authorized head or one canonical page operation.
    /// Pass completion always schedules a fresh head recheck; TTL cannot suppress it.
    pub fn drive<A: ScopeAuthorizer, S: RecordHeadSource, R: ReplicaStore>(
        &mut self,
        limits: Limits,
        authorizer: &mut A,
        source: &mut S,
        store: &mut R,
    ) -> Result<FreshnessWork, FreshnessError> {
        let now = self.now()?;
        if !self.ready(now)? {
            return Ok(FreshnessWork::Waiting);
        }
        if self.meta.phase == FreshnessPhase::Applying {
            let pass = self.pass.as_mut().expect("applying owns one finite pass");
            let before = pass.position();
            match step(pass, limits, authorizer, source, store) {
                Ok(complete) => {
                    self.meta.position = Some(pass.position());
                    if pass.position() > before {
                        self.meta.last_applied_at = Some(self.now()?);
                    }
                    self.meta.retry_at = None;
                    self.window = self.retry.initial;
                    if complete {
                        self.pass = None;
                        self.meta.phase = FreshnessPhase::Recheck;
                    }
                    Ok(FreshnessWork::Applied)
                }
                Err(error) => Err(self.failed(error)),
            }
        } else if self.pending || self.meta.phase == FreshnessPhase::Recheck {
            match begin_pass_with_capabilities(
                &self.meta.scope,
                self.support,
                authorizer,
                source,
                store,
            ) {
                Ok(pass) => {
                    self.meta.last_head_checked_at = Some(self.now()?);
                    self.meta.observed_head = Some(pass.target());
                    self.meta.position = Some(pass.position());
                    self.meta.check_failed = false;
                    self.meta.retry_at = None;
                    self.window = self.retry.initial;
                    self.pending = false;
                    if pass.is_complete() {
                        self.pass = None;
                        self.meta.phase = FreshnessPhase::Idle;
                    } else {
                        self.pass = Some(pass);
                        self.meta.phase = FreshnessPhase::Applying;
                    }
                    Ok(FreshnessWork::Checked)
                }
                Err(error) => Err(self.failed(error)),
            }
        } else {
            Ok(FreshnessWork::Waiting)
        }
    }

    /// Reads a host-owned local projection and returns independent observations.
    /// The callback must perform local-only work under the host's cache visibility
    /// rules. Within/Cached/Never paths call neither source nor authorization.
    /// Always+Fresh checks first, or refuses/defer without invoking the reader.
    /// Successful head admission can still return a cache behind the observed head.
    #[allow(clippy::too_many_arguments)]
    pub fn read<T, A: ScopeAuthorizer, S: RecordHeadSource, R: ReplicaStore>(
        &mut self,
        intent: ReadIntent,
        limits: Limits,
        authorizer: &mut A,
        source: &mut S,
        store: &mut R,
        local: impl FnOnce(&mut R, &Scope) -> Result<T, StoreError>,
    ) -> Result<CachedRead<T>, FreshnessError> {
        let now = self.now()?;
        if intent == ReadIntent::Fresh {
            match self.policy {
                FreshnessPolicy::Never => {}
                FreshnessPolicy::Within(ttl) => {
                    if self.meta.check_failed
                        || self
                            .meta
                            .last_head_checked_at
                            .is_none_or(|at| now.saturating_sub(at) > ttl)
                    {
                        self.pending = true;
                    }
                }
                FreshnessPolicy::Always => {
                    self.pending = true;
                    if !self.ready(now)? || self.meta.phase == FreshnessPhase::Applying {
                        return Err(FreshnessError::Deferred);
                    }
                    self.drive(limits, authorizer, source, store)?;
                }
            }
        }
        let value = local(store, &self.meta.scope)
            .map_err(|error| FreshnessError::Sync(SyncError::Store(error)))?;
        Ok(CachedRead {
            value,
            freshness: self.metadata()?,
        })
    }
}
