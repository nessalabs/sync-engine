//! Claim-qualified local head, forward-page and tail capture. Source writer and wire are unchanged.

use super::super::origin_authority::{OriginClaimError, SqliteOriginAuthority};
use super::{
    RecordHeadSource, RecordSource, Scope, SourceError, SourceHead, SqliteReferenceSource,
};
use crate::replication::domain::{
    validate_page, CapabilitySupport, Checkpoint, CommitPlan, Limits, Page, PageRequest,
    ValidationError,
};
use crate::replication::history::{
    validate_tail, validate_tail_request, CapabilityHistorySource, HistorySourceError,
    HistoryValidationError, TailPlan, TailRequest, TailSnapshot,
};
use crate::replication::origin_epoch::{OriginAuthorityKey, OriginEpoch};

#[derive(Clone, Debug)]
struct Stamp {
    key: OriginAuthorityKey,
    epoch: OriginEpoch,
}

/// A native head observed while its origin writer claim was current.
///
/// This is source-side capture evidence, neither a claim that the epoch is
/// still current nor receiver access admission. A trusted host must bind it
/// to the requested operation and current authorization before using it.
#[derive(Debug)]
pub struct ClaimedNativeHead {
    stamp: Stamp,
    head: SourceHead,
}

impl ClaimedNativeHead {
    /// Extract the exact captured head and its associated authority stamp.
    pub fn into_parts(self) -> (OriginAuthorityKey, OriginEpoch, SourceHead) {
        (self.stamp.key, self.stamp.epoch, self.head)
    }
}

/// One native forward page captured under the source's confirmed writer claim.
///
/// The page and its stamp stay together until canonical request and checkpoint
/// validation. This type does not authenticate requester access.
pub struct ClaimedNativePage {
    stamp: Stamp,
    page: Page,
}

impl ClaimedNativePage {
    /// Validate against the receiver's expected request, checkpoint and
    /// policy limits. Preserve the original stamp and exact operation echo.
    pub fn validate(
        self,
        checkpoint: &Checkpoint,
        request: &PageRequest,
        limits: Limits,
    ) -> Result<ClaimedNativePlan, ValidationError> {
        let plan = validate_page(checkpoint, request, self.page, limits)?;
        Ok(ClaimedNativePlan {
            stamp: self.stamp,
            request: request.clone(),
            plan,
        })
    }
}

/// A claim-qualified exact request and validated forward mutation plan.
///
/// A trusted host still checks that the operation was expected and the
/// requesting scope is currently authorized before it admits this evidence.
pub struct ClaimedNativePlan {
    stamp: Stamp,
    request: PageRequest,
    plan: CommitPlan,
}

impl ClaimedNativePlan {
    /// Borrow the exact correlated request for host admission.
    pub fn request(&self) -> &PageRequest {
        &self.request
    }

    /// Extract the immutable authority stamp, correlated request and plan for
    /// admission by the trusted host.
    pub fn into_parts(self) -> (OriginAuthorityKey, OriginEpoch, PageRequest, CommitPlan) {
        (self.stamp.key, self.stamp.epoch, self.request, self.plan)
    }
}

/// One opaque native tail snapshot and metadata captured under a confirmed claim.
pub struct ClaimedNativeTail {
    stamp: Stamp,
    metadata: SourceHead,
    snapshot: TailSnapshot,
}

impl ClaimedNativeTail {
    /// Validate the exact request, snapshot bounds and metadata from the same
    /// capture. This does not independently authenticate requester access or
    /// advance the receiver's observed epoch.
    pub fn validate(
        self,
        request: &TailRequest,
        limits: Limits,
    ) -> Result<ClaimedNativeTailPlan, HistoryValidationError> {
        if self.metadata.scope() != &request.scope {
            return Err(HistoryValidationError::WrongRequest);
        }
        if self.metadata.position() != self.snapshot.watermark {
            return Err(HistoryValidationError::InvalidBoundary);
        }
        let plan = validate_tail(request, self.snapshot, limits)?;
        Ok(ClaimedNativeTailPlan {
            stamp: self.stamp,
            plan,
        })
    }
}

/// One exact validated tail operation, retaining its original writer claim.
pub struct ClaimedNativeTailPlan {
    stamp: Stamp,
    plan: TailPlan,
}

impl ClaimedNativeTailPlan {
    /// Borrow the validated exact request for trusted host admission.
    pub fn request(&self) -> &TailRequest {
        &self.plan.snapshot().request
    }

    /// Extract the immutable claim and exact tail plan for the trusted host.
    pub fn into_parts(self) -> (OriginAuthorityKey, OriginEpoch, TailPlan) {
        (self.stamp.key, self.stamp.epoch, self.plan)
    }
}

/// Refusal from a claim-qualified native source capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpochSourceError {
    /// The claim is stale or the local authority could not be consulted.
    Authority(OriginClaimError),
    /// The ordinary source declined the exact request or its local read.
    Source(SourceError),
    /// The ordinary native tail route refused scope, compatibility or storage.
    History(HistorySourceError),
    /// The requested tail generation or bounds are not admissible.
    TailRequest(HistoryValidationError),
}

/// Compatibility name for head capture refusals.
pub type EpochHeadError = EpochSourceError;

/// Local claim-qualified head, forward-page and tail producer over an ordinary source.
///
/// The caller gives this mode an existing source handle and trusted shared
/// authority ledger. It does not expose the owned handle or implement legacy
/// `RecordHeadSource`. This is a protected **result producer**, not a protected
/// source file mode: ordinary handles, appends and older pages remain unchanged
/// and produce no claim-qualified result.
///
/// A producer cannot be substituted for a legacy bare-head reader:
/// ```compile_fail
/// use nessa_sync::replication::application::RecordHeadSource;
/// use nessa_sync::replication::infrastructure::SqliteEpochHeadSource;
/// fn accept_bare(_: &mut impl RecordHeadSource) {}
/// fn cannot_drop_stamp(heads: &mut SqliteEpochHeadSource) {
///     accept_bare(heads);
/// }
/// ```
pub struct SqliteEpochRecordSource {
    source: SqliteReferenceSource,
    authority: SqliteOriginAuthority,
    stamp: Stamp,
}

/// Compatibility name for the original claim-qualified head producer.
pub type SqliteEpochHeadSource = SqliteEpochRecordSource;

impl SqliteEpochRecordSource {
    /// Compare and acquire one claim through the injected durable ledger.
    ///
    /// The trusted host selects the source-to-key association and shared
    /// ledger. An uncertain commit returns no usable producer; reading
    /// the new epoch does not prove that this handle acquired it.
    pub fn claim(
        source: SqliteReferenceSource,
        mut authority: SqliteOriginAuthority,
        key: OriginAuthorityKey,
        expected: Option<OriginEpoch>,
    ) -> Result<Self, OriginClaimError> {
        let epoch = authority.compare_and_claim(&key, expected)?;
        Ok(Self {
            source,
            authority,
            stamp: Stamp { key, epoch },
        })
    }

    /// Return the handle's immutable claimed epoch, not its current authority.
    pub fn epoch(&self) -> OriginEpoch {
        self.stamp.epoch
    }

    /// Capture one exact ordinary head under the current authority.
    ///
    /// One short SQLite authority writer lock serializes this local snapshot
    /// with later claim changes. The lock is released before returning, so an
    /// already captured value can still be delayed beyond a later handoff.
    pub fn capture_head(&mut self, scope: &Scope) -> Result<ClaimedNativeHead, EpochHeadError> {
        let head =
            self.capture(|source| source.source_head(scope).map_err(EpochSourceError::Source))?;
        Ok(ClaimedNativeHead {
            stamp: self.stamp.clone(),
            head,
        })
    }

    /// Capture one bounded native page under the current writer claim.
    ///
    /// The existing ordinary page operation checks fresh reader capabilities,
    /// budget and payload in one source snapshot. The authority guard covers
    /// that local operation; it is released before returning the opaque result.
    pub fn capture_page(
        &mut self,
        request: &PageRequest,
    ) -> Result<ClaimedNativePage, EpochSourceError> {
        let page = self.capture(|source| source.page(request).map_err(EpochSourceError::Source))?;
        Ok(ClaimedNativePage {
            stamp: self.stamp.clone(),
            page,
        })
    }

    /// Capture one bounded native tail and its coherent metadata under the claim.
    ///
    /// Validate the requested operation before acquiring the claim guard. The
    /// native source checks fresh reader compatibility in the tail snapshot;
    /// the guard is released before returning any opaque value.
    pub fn capture_tail(
        &mut self,
        request: &TailRequest,
        limits: Limits,
    ) -> Result<ClaimedNativeTail, EpochSourceError> {
        validate_tail_request(request, limits).map_err(EpochSourceError::TailRequest)?;
        let (metadata, snapshot) = self.capture(|source| {
            source
                .tail_with_capabilities(request, CapabilitySupport::current())
                .map_err(EpochSourceError::History)
        })?;
        Ok(ClaimedNativeTail {
            stamp: self.stamp.clone(),
            metadata,
            snapshot,
        })
    }

    fn capture<T>(
        &mut self,
        read: impl FnOnce(&mut SqliteReferenceSource) -> Result<T, EpochSourceError>,
    ) -> Result<T, EpochSourceError> {
        let source = &mut self.source;
        self.authority
            .with_current_claim(&self.stamp.key, self.stamp.epoch, || read(source))
            .map_err(EpochSourceError::Authority)?
    }
}
