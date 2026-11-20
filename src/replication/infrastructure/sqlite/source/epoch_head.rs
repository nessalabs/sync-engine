//! Claim-qualified local head capture. Source writer and wire are unchanged.

use super::super::origin_authority::{OriginClaimError, SqliteOriginAuthority};
use super::{RecordHeadSource, Scope, SourceError, SourceHead, SqliteReferenceSource};
use crate::replication::origin_epoch::{OriginAuthorityKey, OriginEpoch};

/// A native head observed while its origin writer claim was current.
///
/// This is source-side capture evidence, neither a claim that the epoch is
/// still current nor receiver access admission. A trusted host must bind it
/// to the requested operation and current authorization before using it.
#[derive(Debug)]
pub struct ClaimedNativeHead {
    key: OriginAuthorityKey,
    epoch: OriginEpoch,
    head: SourceHead,
}

impl ClaimedNativeHead {
    /// Extract the exact captured head and its associated authority stamp.
    pub fn into_parts(self) -> (OriginAuthorityKey, OriginEpoch, SourceHead) {
        (self.key, self.epoch, self.head)
    }
}

/// Refusal from one claim-qualified head capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpochHeadError {
    /// The claim is stale or the local authority could not be consulted.
    Authority(OriginClaimError),
    /// The ordinary source declined the exact requested scope or head read.
    Source(SourceError),
}

/// Local claim-qualified native head producer over an ordinary reference source.
///
/// The caller gives this mode an existing source handle and trusted shared
/// authority ledger. It does not expose the owned handle or implement legacy
/// `RecordHeadSource`. This is a protected **result producer**, not a protected
/// source file mode: other ordinary source handles, appends, pages and
/// snapshots remain unchanged and produce no claim-qualified result.
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
pub struct SqliteEpochHeadSource {
    source: SqliteReferenceSource,
    authority: SqliteOriginAuthority,
    key: OriginAuthorityKey,
    epoch: OriginEpoch,
}

impl SqliteEpochHeadSource {
    /// Compare and acquire one claim through the injected durable ledger.
    ///
    /// The trusted host selects the source-to-key association and shared
    /// ledger. An uncertain commit returns no usable head producer; reading
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
            key,
            epoch,
        })
    }

    /// Return the handle's immutable claimed epoch, not its current authority.
    pub fn epoch(&self) -> OriginEpoch {
        self.epoch
    }

    /// Capture one exact ordinary head under the current authority.
    ///
    /// One short SQLite authority writer lock serializes this local snapshot
    /// with later claim changes. The lock is released before returning, so an
    /// already captured value can still be delayed beyond a later handoff.
    pub fn capture_head(&mut self, scope: &Scope) -> Result<ClaimedNativeHead, EpochHeadError> {
        let source = &mut self.source;
        let head = self
            .authority
            .with_current_claim(&self.key, self.epoch, || source.source_head(scope))
            .map_err(EpochHeadError::Authority)?
            .map_err(EpochHeadError::Source)?;
        Ok(ClaimedNativeHead {
            key: self.key.clone(),
            epoch: self.epoch,
            head,
        })
    }
}
