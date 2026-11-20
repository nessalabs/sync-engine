//! Pure host retention policy. This module neither records receiver progress nor
//! changes a source read floor.

use super::{Id, Scope};

/// Replication resource families have independent retention floors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionKind {
    /// Committed positions in a record stream.
    Records,
    /// Committed revisions in a catalogue.
    Catalogue,
}

/// Source history identity without the receiver or its access epoch.
///
/// A pin registered for one resource family, source, or incarnation cannot
/// affect another. Hosts must independently validate the holder's authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionScope {
    kind: RetentionKind,
    origin: Id,
    resource: Id,
    incarnation: Id,
}

impl RetentionScope {
    /// Select the source-owned coordinates of a currently validated scope.
    pub fn from_scope(kind: RetentionKind, scope: &Scope) -> Self {
        Self {
            kind,
            origin: scope.origin().clone(),
            resource: scope.stream().clone(),
            incarnation: scope.incarnation().clone(),
        }
    }
}

/// A position in one selected source history. Zero is a valid initial position.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RetentionPosition(u64);

impl RetentionPosition {
    /// Convert a host's checked source position or floor to a policy input.
    pub const fn new(position: u64) -> Self {
        Self(position)
    }

    /// Return the numeric position in the selected history.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A host-registered, expiring request to protect a durable completed position.
///
/// The engine cannot authenticate a holder or turn an active cursor into proof
/// of durable completion. The registering host owns both checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionPin {
    holder: Id,
    scope: RetentionScope,
    completed: RetentionPosition,
    expires_at_ms: u64,
}

impl RetentionPin {
    /// Represent a registered holder's durable completed position, not an active
    /// pass boundary, cursor, hint, or uncommitted work. Expiry is Unix milliseconds.
    pub fn for_completed(
        holder: Id,
        scope: RetentionScope,
        completed: RetentionPosition,
        expires_at_ms: u64,
    ) -> Self {
        Self {
            holder,
            scope,
            completed,
            expires_at_ms,
        }
    }

    /// Opaque registration identity, for the host's independent registry.
    pub fn holder(&self) -> &Id {
        &self.holder
    }
}

/// Invalid inputs to the retention policy; nothing may advance on error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionError {
    /// The existing floor exceeds the committed head.
    FloorAboveHead,
    /// The preferred cutoff exceeds the committed head.
    PreferredAboveHead,
    /// The forced cutoff exceeds the committed head.
    ForcedAboveHead,
    /// A current pin for the selected scope exceeds its committed head.
    PinAboveHead,
}

/// One coherent source observation and the host's optional pressure cutoffs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionWindow {
    head: RetentionPosition,
    floor: RetentionPosition,
    preferred: RetentionPosition,
    forced: Option<RetentionPosition>,
}

impl RetentionWindow {
    /// Validate the current floor and proposed cutoffs against a committed head.
    ///
    /// The preferred cutoff can yield to active pins. The optional forced
    /// cutoff is a minimum requested by hard storage pressure, not a maximum
    /// allowable position. Its policy and conversion from bytes remain host-owned.
    pub fn new(
        head: RetentionPosition,
        floor: RetentionPosition,
        preferred: RetentionPosition,
        forced: Option<RetentionPosition>,
    ) -> Result<Self, RetentionError> {
        if floor > head {
            return Err(RetentionError::FloorAboveHead);
        }
        if preferred > head {
            return Err(RetentionError::PreferredAboveHead);
        }
        if forced.is_some_and(|position| position > head) {
            return Err(RetentionError::ForcedAboveHead);
        }
        Ok(Self {
            head,
            floor,
            preferred,
            forced,
        })
    }

    /// Recommend a monotonic cutoff for this source snapshot and registered pins.
    ///
    /// Pins for other scopes and pins expiring at or before `now_ms` are ignored.
    /// No pin can lower a floor that was already advanced. Hosts must recompute
    /// under the source's mutation boundary before acting on this recommendation;
    /// it grants neither prune authority nor receiver progress.
    pub fn cutoff(
        self,
        scope: &RetentionScope,
        now_ms: u64,
        pins: &[RetentionPin],
    ) -> Result<RetentionPosition, RetentionError> {
        let mut cutoff = self.preferred;
        for pin in pins
            .iter()
            .filter(|pin| &pin.scope == scope && pin.expires_at_ms > now_ms)
        {
            if pin.completed > self.head {
                return Err(RetentionError::PinAboveHead);
            }
            cutoff = cutoff.min(pin.completed);
        }
        Ok(cutoff
            .max(self.floor)
            .max(self.forced.unwrap_or(self.floor)))
    }
}
