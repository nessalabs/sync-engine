//! Pure control-lifecycle bookkeeping. A Ready owner has negotiated metadata,
//! not an authenticated grant. No source/store/authorizer port is implemented.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use crate::replication::application::MAX_RECORD_BYTES;
use crate::replication::domain::Scope;

use super::{ControlFrame, Correlation, SessionLimits};

/// Opaque process-local connection identity. Clones identify the same read loop;
/// a new owner always differs, even if a peer reuses its wire session number.
/// Capture this before reading and never relabel old frames with a new identity.
#[derive(Clone, Debug)]
pub struct ConnectionId(Arc<()>);
impl PartialEq for ConnectionId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ConnectionId {}

/// Nonzero session number accepted from a Welcome, distinct from local connection identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionId(NonZeroU64);
impl SessionId {
    /// Wire value. It is not proof of authentication or global freshness.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// Nonreused binding-request identity, including its connection and session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestIdentity {
    connection: ConnectionId,
    session: SessionId,
    number: NonZeroU64,
}
impl RequestIdentity {
    /// Wire correlation to echo; the local connection tag is not serialized.
    pub fn correlation(&self) -> Correlation {
        Correlation {
            session: self.session.get(),
            request: self.number.get(),
        }
    }
}

/// Immutable scope context admitted by exact request correlation. This token is
/// passive metadata, never authorization or permission to commit records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeIdentity {
    connection: ConnectionId,
    session: SessionId,
    handle: NonZeroU32,
    generation: NonZeroU64,
    scope: Scope,
}
impl ScopeIdentity {
    /// Exact receiver/source identity and epoch retained from the request.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Session-local handle. It is never reused within this owner.
    pub fn handle(&self) -> u32 {
        self.handle.get()
    }
    /// Nonzero generation echoed by the peer.
    pub fn generation(&self) -> u64 {
        self.generation.get()
    }
    /// Wire session associated with this context.
    pub fn session(&self) -> SessionId {
        self.session
    }
}

/// Metadata lifecycle only; Ready is not an authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    /// Waiting for a Welcome matching the retained offer.
    Connecting,
    /// Limits accepted; binding requests may be correlated.
    Ready,
    /// Terminal; all contexts are invalid and a new owner is required.
    Closed,
}

/// Typed control-lifecycle refusal. None automatically retries or resets progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    /// Offer, pending count or peer limits exceeded supported bounds.
    InvalidLimits,
    /// A required wire identity/handle/generation was zero.
    InvalidIdentity,
    /// Event or token came from another local connection.
    WrongConnection,
    /// Welcome is required before starting or completing binding work.
    NotReady,
    /// This owner is terminal and cannot be reopened.
    Closed,
    /// Welcome exceeded the retained offer or supported record ceiling.
    IncompatibleWelcome,
    /// Control operation was not expected at this stage.
    UnexpectedReply,
    /// Fixed pending or lifetime binding-slot budget is exhausted.
    BudgetExhausted,
    /// Requested scope conflicts with active/pending context for the same target.
    ScopeConflict,
    /// Scope, handle, generation or wire session is stale or mismatched.
    StaleIdentity,
    /// Request numbers cannot wrap or be reused; create a new owner.
    RequestExhausted,
    /// Reply or cancellation did not identify a retained request.
    UnknownRequest,
    /// The retained request was already completed, cancelled or invalidated.
    TerminalRequest,
}

#[derive(Clone, Debug)]
enum SlotState {
    Pending,
    Bound(ScopeIdentity),
    Invalid(Option<NonZeroU32>),
}
#[derive(Clone, Debug)]
struct Slot {
    request: RequestIdentity,
    scope: Scope,
    state: SlotState,
}
impl Slot {
    fn handle(&self) -> Option<NonZeroU32> {
        match &self.state {
            SlotState::Bound(identity) => Some(identity.handle),
            SlotState::Invalid(handle) => *handle,
            SlotState::Pending => None,
        }
    }
}

/// Bounded owner for Welcome and binding requests only. Every issued binding
/// consumes one lifetime slot, including after cancellation/invalidation. This
/// bounds tombstones and forbids stale scope/handle reuse without an unbounded
/// retired-ID set. Exhaustion requires a fresh owner/connection. No data requests,
/// wire Cancel, commits, grants, sockets, policy callbacks or hidden retries exist.
#[derive(Debug)]
pub struct SessionLifecycle {
    connection: ConnectionId,
    offered: SessionLimits,
    selected: Option<(SessionId, SessionLimits)>,
    phase: SessionPhase,
    max_pending: u16,
    next_request: Option<NonZeroU64>,
    slots: Vec<Slot>,
}

impl SessionLifecycle {
    /// Retain a valid offer and a local pending budget no larger than its binding
    /// budget. Construction performs no authentication or network operation.
    pub fn new(offered: SessionLimits, max_pending: u16) -> Result<Self, LifecycleError> {
        offered
            .validate()
            .map_err(|_| LifecycleError::InvalidLimits)?;
        if max_pending == 0 || max_pending > offered.bindings {
            return Err(LifecycleError::InvalidLimits);
        }
        Ok(Self {
            connection: ConnectionId(Arc::new(())),
            offered,
            selected: None,
            phase: SessionPhase::Connecting,
            max_pending,
            next_request: NonZeroU64::new(1),
            slots: Vec::new(),
        })
    }

    /// Capture the connection tag for this read loop before acquiring frames.
    pub fn connection_id(&self) -> ConnectionId {
        self.connection.clone()
    }
    /// Current metadata phase, not authentication state.
    pub fn phase(&self) -> SessionPhase {
        self.phase
    }
    /// Offer to encode after the host has established its own transport boundary.
    pub fn hello(&self) -> Result<ControlFrame, LifecycleError> {
        match self.phase {
            SessionPhase::Connecting => Ok(ControlFrame::Hello(self.offered)),
            SessionPhase::Ready => Err(LifecycleError::UnexpectedReply),
            SessionPhase::Closed => Err(LifecycleError::Closed),
        }
    }
    /// Count of pending binding requests, excluding terminal tombstones.
    pub fn pending_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Pending))
            .count()
    }
    /// Lifetime slots retained, including invalidated/cancelled bindings.
    pub fn retained_slots(&self) -> usize {
        self.slots.len()
    }
    /// Close terminally, discarding all contexts. Repeated close is harmless.
    pub fn close(&mut self) {
        self.phase = SessionPhase::Closed;
        self.selected = None;
        self.slots.clear();
    }
    fn reject<T>(&mut self, error: LifecycleError) -> Result<T, LifecycleError> {
        self.close();
        Err(error)
    }
    fn connection(&self, connection: &ConnectionId) -> Result<(), LifecycleError> {
        if *connection == self.connection {
            Ok(())
        } else {
            Err(LifecycleError::WrongConnection)
        }
    }
    fn ready(&self) -> Result<(SessionId, SessionLimits), LifecycleError> {
        match self.phase {
            SessionPhase::Connecting => Err(LifecycleError::NotReady),
            SessionPhase::Closed => Err(LifecycleError::Closed),
            SessionPhase::Ready => self.selected.ok_or(LifecycleError::NotReady),
        }
    }

    /// Accept Welcome metadata only after comparing bounds with the retained
    /// offer. The host still owns channel authentication. Bad current-connection
    /// replies close this owner; events tagged with another connection do not.
    pub fn accept_welcome(
        &mut self,
        connection: &ConnectionId,
        frame: &ControlFrame,
    ) -> Result<SessionId, LifecycleError> {
        self.connection(connection)?;
        if self.phase == SessionPhase::Closed {
            return Err(LifecycleError::Closed);
        }
        if self.phase != SessionPhase::Connecting {
            return self.reject(LifecycleError::UnexpectedReply);
        }
        let ControlFrame::Welcome {
            session,
            limits,
            record_bytes,
        } = frame
        else {
            return self.reject(LifecycleError::UnexpectedReply);
        };
        let Some(session) = NonZeroU64::new(*session) else {
            return self.reject(LifecycleError::InvalidIdentity);
        };
        if limits.validate().is_err()
            || limits.control_bytes > self.offered.control_bytes
            || limits.bindings > self.offered.bindings
            || *record_bytes == 0
            || *record_bytes as usize > MAX_RECORD_BYTES
        {
            return self.reject(LifecycleError::IncompatibleWelcome);
        }
        let session = SessionId(session);
        self.selected = Some((session, *limits));
        self.phase = SessionPhase::Ready;
        Ok(session)
    }

    /// Reserve an exact scope and nonreused request number before producing Bind
    /// metadata. The host must authorize before sending it; no grant is created.
    /// Invalidated scopes cannot be requested again in this connection, and a
    /// changed scope for the same target requires explicit invalidation first.
    pub fn begin_bind(
        &mut self,
        scope: Scope,
    ) -> Result<(RequestIdentity, ControlFrame), LifecycleError> {
        let (session, limits) = self.ready()?;
        for slot in &self.slots {
            if slot.scope == scope && matches!(slot.state, SlotState::Invalid(_)) {
                return Err(LifecycleError::StaleIdentity);
            }
            if slot.scope.same_receiver_stream(&scope)
                && !matches!(slot.state, SlotState::Invalid(_))
            {
                return Err(LifecycleError::ScopeConflict);
            }
        }
        if self.slots.len() >= usize::from(limits.bindings)
            || self.pending_count() >= usize::from(self.max_pending.min(limits.bindings))
        {
            return Err(LifecycleError::BudgetExhausted);
        }
        let number = self.next_request.ok_or(LifecycleError::RequestExhausted)?;
        let request = RequestIdentity {
            connection: self.connection.clone(),
            session,
            number,
        };
        let frame = ControlFrame::Bind {
            correlation: request.correlation(),
            scope: scope.clone(),
        };
        self.slots.push(Slot {
            request: request.clone(),
            scope,
            state: SlotState::Pending,
        });
        self.next_request = number.get().checked_add(1).and_then(NonZeroU64::new);
        Ok((request, frame))
    }

    /// Cancel local pending Bind bookkeeping only. No wire cancellation or store
    /// rollback is implied. Repeated cancellation of an invalid slot is harmless;
    /// cancelling an already completed binding is refused instead of undoing it.
    pub fn cancel_bind(&mut self, request: &RequestIdentity) -> Result<(), LifecycleError> {
        self.connection(&request.connection)?;
        let (session, _) = self.ready()?;
        if request.session != session {
            return Err(LifecycleError::StaleIdentity);
        }
        let slot = self
            .slots
            .iter_mut()
            .find(|s| s.request == *request)
            .ok_or(LifecycleError::UnknownRequest)?;
        match slot.state {
            SlotState::Pending => {
                slot.state = SlotState::Invalid(None);
                Ok(())
            }
            SlotState::Invalid(_) => Ok(()),
            SlotState::Bound(_) => Err(LifecycleError::TerminalRequest),
        }
    }

    /// Correlate a Bound reply against the connection tag, session, nonterminal
    /// request and full exact scope before installing passive context. Duplicate
    /// or cancelled terminal replies are refused without reopening a slot. Unknown
    /// requests and malformed/mismatched current replies terminally close the owner.
    pub fn accept_bound(
        &mut self,
        connection: &ConnectionId,
        frame: &ControlFrame,
    ) -> Result<ScopeIdentity, LifecycleError> {
        self.connection(connection)?;
        if self.phase == SessionPhase::Connecting {
            return self.reject(LifecycleError::UnexpectedReply);
        }
        let (session, _) = self.ready()?;
        let ControlFrame::Bound {
            correlation,
            binding,
        } = frame
        else {
            return self.reject(LifecycleError::UnexpectedReply);
        };
        if correlation.session != session.get() {
            return self.reject(LifecycleError::StaleIdentity);
        }
        let Some(index) = self
            .slots
            .iter()
            .position(|s| s.request.number.get() == correlation.request)
        else {
            return self.reject(LifecycleError::UnknownRequest);
        };
        if !matches!(self.slots[index].state, SlotState::Pending) {
            return Err(LifecycleError::TerminalRequest);
        }
        let (Some(handle), Some(generation)) = (
            NonZeroU32::new(binding.handle),
            NonZeroU64::new(binding.generation),
        ) else {
            return self.reject(LifecycleError::InvalidIdentity);
        };
        if binding.scope != self.slots[index].scope
            || self.slots.iter().any(|s| s.handle() == Some(handle))
        {
            return self.reject(LifecycleError::StaleIdentity);
        }
        let identity = ScopeIdentity {
            connection: self.connection.clone(),
            session,
            handle,
            generation,
            scope: binding.scope.clone(),
        };
        self.slots[index].state = SlotState::Bound(identity.clone());
        Ok(identity)
    }

    /// Verify a previously returned context is still active in this owner.
    /// Success establishes identity only, never current read/apply permission.
    pub fn check_scope(&self, identity: &ScopeIdentity) -> Result<(), LifecycleError> {
        self.connection(&identity.connection)?;
        let (session, _) = self.ready()?;
        if identity.session != session {
            return Err(LifecycleError::StaleIdentity);
        }
        if self
            .slots
            .iter()
            .any(|s| matches!(&s.state, SlotState::Bound(current) if current == identity))
        {
            Ok(())
        } else {
            Err(LifecycleError::StaleIdentity)
        }
    }

    /// Explicitly invalidate active scope context on a host-observed identity or
    /// policy transition. Keep its scope/handle tombstone; never reset progress.
    pub fn invalidate_scope(&mut self, identity: &ScopeIdentity) -> Result<(), LifecycleError> {
        self.check_scope(identity)?;
        for slot in &mut self.slots {
            if matches!(&slot.state, SlotState::Bound(current) if current == identity) {
                slot.state = SlotState::Invalid(Some(identity.handle));
                return Ok(());
            }
        }
        Err(LifecycleError::StaleIdentity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;

    #[test]
    fn request_counter_exhaustion_never_wraps_or_reuses_zero() {
        let offer = SessionLimits {
            control_bytes: 2048,
            bindings: 3,
        };
        let mut owner = SessionLifecycle::new(offer, 2).unwrap();
        let key = owner.connection_id();
        owner
            .accept_welcome(
                &key,
                &ControlFrame::Welcome {
                    session: 1,
                    limits: offer,
                    record_bytes: 1,
                },
            )
            .unwrap();
        owner.next_request = NonZeroU64::new(u64::MAX);
        let scope = |stream| {
            let id = || Id::new("fixture").unwrap();
            Scope::new(id(), id(), Id::new(stream).unwrap(), id(), id(), id())
        };
        let (last, _) = owner.begin_bind(scope("last")).unwrap();
        assert_eq!(last.correlation().request, u64::MAX);
        owner.cancel_bind(&last).unwrap();
        assert_eq!(
            owner.begin_bind(scope("never-zero")),
            Err(LifecycleError::RequestExhausted)
        );
        assert_eq!(owner.retained_slots(), 1);
        assert_eq!(owner.pending_count(), 0);
    }
}
