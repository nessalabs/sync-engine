//! Host-origin writer authority identities, independent of scope access epochs.
//! These values alone do not grant trust or alter receiver progress.

use std::num::NonZeroU64;

use super::domain::{CommitPlan, Id, Scope, SourceHead};

/// Host-chosen, stable identity of a single writer authority.
///
/// A host may use the origin's ID for a same-origin restart. Across replacement
/// identities it must establish the association through independently trusted
/// pairing or registration. A received frame or restored backup cannot choose
/// that association.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OriginAuthorityKey(Id);

impl OriginAuthorityKey {
    /// Wrap an already-trusted authority identifier selected by the host.
    pub fn new(id: Id) -> Self {
        Self(id)
    }

    /// Return the key's canonical identifier for host storage adapters.
    pub fn id(&self) -> &Id {
        &self.0
    }
}

/// One claimed positive writer epoch for an origin authority.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OriginEpoch(NonZeroU64);

impl OriginEpoch {
    /// First claim; an unclaimed key has no epoch rather than epoch zero.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Reject zero, which is never a claimed epoch.
    pub const fn new(epoch: u64) -> Option<Self> {
        match NonZeroU64::new(epoch) {
            Some(epoch) => Some(Self(epoch)),
            None => None,
        }
    }

    /// Canonical numeric value for host storage and monotone comparisons.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Return the next epoch, or `None` at exhaustion. Never wrap.
    pub fn successor(self) -> Option<Self> {
        self.get().checked_add(1).and_then(Self::new)
    }
}

/// One host-admitted operation with its exact trusted binding, scope and epoch.
///
/// The constructor is for the injected host admission implementation. This
/// value does not authenticate untrusted metadata on its own.
#[derive(Debug)]
pub struct AdmittedEpochRead<T> {
    key: OriginAuthorityKey,
    scope: Scope,
    epoch: OriginEpoch,
    value: T,
}

impl<T> AdmittedEpochRead<T> {
    /// Package the exact result after checking its trusted source, current
    /// scope and operation correlation. Do not stamp an independent value.
    pub fn new(key: OriginAuthorityKey, scope: Scope, epoch: OriginEpoch, value: T) -> Self {
        Self {
            key,
            scope,
            epoch,
            value,
        }
    }

    /// Recover the already-admitted binding, scope, epoch and exact result.
    pub fn into_parts(self) -> (OriginAuthorityKey, Scope, OriginEpoch, T) {
        (self.key, self.scope, self.epoch, self.value)
    }
}

/// Trusted host port for current, exact native record-read admission.
///
/// The protected adapter takes this port on open. The host implementation
/// must authenticate the authority-to-source binding, recheck current scope
/// and correlate the actual result, not manufacture a stamp for arbitrary
/// caller data. Head and plan evidence are independent operation types.
pub trait NativeEpochAdmission {
    /// Host-defined input evidence for one head result.
    type HeadEvidence;
    /// Host-defined input evidence for one validated page plan.
    type PlanEvidence;

    /// Verify the exact head result under the current host binding.
    fn admit_head(&mut self, evidence: Self::HeadEvidence)
        -> Option<AdmittedEpochRead<SourceHead>>;

    /// Verify one exact record plan under the current host binding.
    fn admit_plan(&mut self, evidence: Self::PlanEvidence)
        -> Option<AdmittedEpochRead<CommitPlan>>;
}

/// Optional trusted admission for bounded native tail installation.
///
/// The protected store retains the same host port; adding this operation does
/// not change existing head/forward admission or expose generic history apply.
pub trait NativeEpochTailAdmission: NativeEpochAdmission {
    /// Host-defined evidence for one exact validated snapshot operation.
    type TailEvidence;

    /// Verify the exact tail result and its current scope/binding.
    fn admit_tail(
        &mut self,
        evidence: Self::TailEvidence,
    ) -> Option<AdmittedEpochRead<crate::replication::history::TailPlan>>;
}
