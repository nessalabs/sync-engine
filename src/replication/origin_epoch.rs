//! Host-origin writer authority identities, independent of scope access epochs.
//! These values alone do not grant trust or alter receiver progress.

use std::num::NonZeroU64;

use super::domain::Id;

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
