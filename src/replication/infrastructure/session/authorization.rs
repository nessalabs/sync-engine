//! Compose passive binding identity with the host's current authorization port.
//! This is an application-side preflight, not source-endpoint authorization.

use crate::replication::application::{Access, ScopeAuthorizer};
use crate::replication::domain::Scope;

use super::{LifecycleError, ScopeIdentity, SessionLifecycle};

/// A fail-closed scope fence around a host-provided current policy port.
///
/// Binding metadata never supplies an Allowed result. Every call with a current,
/// exact context delegates to `current.authorize`; no decision is retained. The
/// host must supply an authoritative, authenticated policy composition satisfying
/// [`ScopeAuthorizer`], not a cached session grant or an always-allow substitute.
///
/// Use with existing application entry points so authorization still precedes
/// local progress loading, reads and apply admission. Construct short-lived gates
/// and drop them before processing lifecycle mutations; their shared owner borrow
/// intentionally prevents invalidation during the borrowed operation. The host
/// still owns the separate source endpoint's authorization and policy/commit races.
/// This adapter creates no socket, read route, cache invalidation or retry loop.
pub struct ScopedSessionAuthorizer<'a, A: ScopeAuthorizer + ?Sized> {
    lifecycle: &'a SessionLifecycle,
    identity: &'a ScopeIdentity,
    current: &'a mut A,
}

impl<'a, A: ScopeAuthorizer + ?Sized> ScopedSessionAuthorizer<'a, A> {
    /// Borrow the passive context and current host policy. Construction performs
    /// no authorization; the context and policy are checked at each port call.
    pub fn new(
        lifecycle: &'a SessionLifecycle,
        identity: &'a ScopeIdentity,
        current: &'a mut A,
    ) -> Self {
        Self {
            lifecycle,
            identity,
            current,
        }
    }
}

impl<A: ScopeAuthorizer + ?Sized> ScopeAuthorizer for ScopedSessionAuthorizer<'_, A> {
    fn authorize(&mut self, scope: &Scope) -> Access {
        if scope != self.identity.scope() {
            return Access::IdentityChanged;
        }
        match self.lifecycle.check_scope(self.identity) {
            Ok(()) => {}
            Err(LifecycleError::Closed | LifecycleError::NotReady) => return Access::Unverifiable,
            Err(LifecycleError::WrongConnection | LifecycleError::StaleIdentity) => {
                return Access::IdentityChanged
            }
            // An unexpected internal lifecycle outcome must never become a
            // retryable outage or an implicit grant.
            Err(_) => return Access::InvalidResponse,
        }
        match self.current.authorize(scope) {
            Access::Allowed(current) if current != *scope => Access::IdentityChanged,
            decision => decision,
        }
    }
}
