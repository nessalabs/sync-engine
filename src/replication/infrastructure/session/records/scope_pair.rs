//! Two setup-time bindings and per-scope heads on the original serialized
//! connection. No advisory hints, page routing, capability retry or cache effect.
use super::*;
use crate::replication::application::RecordReadView;
use crate::replication::domain::{CapabilitySupport, SourceHead};

/// Serve two independently authorized scopes on one host-authenticated
/// serialized channel. Only explicit pair setup and capability Head v1 are
/// admitted. Hosts supply current exact-scope policy, finite I/O and factories.
pub fn serve_scope_pair_heads<S, A, F, R>(
    stream: &mut S,
    session: u64,
    limits: SessionLimits,
    policy: &mut A,
    factory: F,
) -> Result<(), RecordSessionError>
where
    S: Read + Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordReadView,
{
    serve_bound(
        stream,
        session,
        limits,
        policy,
        factory,
        None,
        CapabilityRoutes {
            head: Some(R::view_head),
            history: None,
            scope_pair: true,
        },
    )
}

pub(super) fn bind_secondary<S, A, F, R>(
    stream: &mut S,
    primary: Correlation,
    primary_scope: &Scope,
    policy: &mut A,
    factory: &mut F,
    limit: usize,
) -> Result<Option<Scope>, RecordSessionError>
where
    S: Read + Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    let ControlFrame::Bind { correlation, scope } = read_control(stream, limit)? else {
        return Err(RecordSessionError::Protocol);
    };
    if correlation.session != primary.session
        || primary.request.checked_add(1) != Some(correlation.request)
        || primary_scope.same_receiver_stream(&scope)
    {
        return Err(RecordSessionError::Protocol);
    }
    write_binding(stream, correlation, &scope, 2, factory, policy, limit)
        .map(|bound| bound.then_some(scope))
}

pub(super) fn write_binding<S, A, F, R>(
    stream: &mut S,
    correlation: Correlation,
    scope: &Scope,
    slot: u32,
    factory: &mut F,
    policy: &mut A,
    limit: usize,
) -> Result<bool, RecordSessionError>
where
    S: Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    let initial = access_code(policy.authorize(scope), scope);
    let head = if initial == 0 {
        factory(scope).and_then(|mut source| source.head(scope))
    } else {
        Err(decode_source_error(initial))
    };
    let current = if initial == 0 {
        access_code(policy.authorize(scope), scope)
    } else {
        initial
    };
    let head = if current == 0 {
        head
    } else if current == 7 {
        Err(SourceError::InvalidRequest)
    } else {
        Err(decode_source_error(current))
    };
    let (bound, frame) = match head {
        Ok(head) => (
            true,
            ControlFrame::Bound {
                correlation,
                binding: BindingMetadata {
                    handle: slot,
                    generation: u64::from(slot),
                    scope: scope.clone(),
                    head,
                },
            },
        ),
        Err(reason) => (
            false,
            ControlFrame::Refused {
                correlation,
                reason: match reason {
                    SourceError::Incompatible(_) => SourceError::InvalidRequest,
                    other => other,
                },
            },
        ),
    };
    write_control(stream, &frame, limit)?;
    stream.flush()?;
    Ok(bound)
}

/// Stable bound identities for exactly two setup-time scopes, sharing the
/// original session mutex, lifecycle and wire allocator. No scope can borrow
/// another scope's binding, and an initially refused secondary installs no port.
pub struct ScopePairHeadsSession<S> {
    inner: RecordSession<S>,
    secondary: Result<ScopeIdentity, SourceError>,
    support: CapabilitySupport,
}

impl<S: InterruptibleSessionIo> ScopePairHeadsSession<S> {
    /// Open the explicit pair mode on a host-authenticated channel. Primary
    /// admission is required; a correlated refusal of the secondary is reported
    /// via `secondary_binding` and does not discard the primary binding.
    pub fn connect(
        io: S,
        primary: Scope,
        secondary: Scope,
        limits: SessionLimits,
        support: CapabilitySupport,
    ) -> Result<Self, RecordSessionError> {
        if limits.bindings != 2 || primary.same_receiver_stream(&secondary) {
            return Err(SourceError::InvalidRequest.into());
        }
        let interrupt = io.interrupt_handle();
        let inner =
            RecordSession::connect_with(io, primary, limits, Opening::ScopePair, Some(interrupt))?;
        let secondary = {
            let mut client = active_client(&inner.shared, &inner.closing)?;
            let key = client.owner.connection_id();
            let (request, frame) = client
                .owner
                .begin_bind(secondary)
                .map_err(|_| RecordSessionError::Protocol)?;
            if client.wire.next != Some(request.correlation().request) {
                return Err(RecordSessionError::Protocol);
            }
            let response = client.wire.control(&frame)?;
            client.wire.next = request.correlation().request.checked_add(1);
            match &response {
                ControlFrame::Refused {
                    correlation,
                    reason,
                } if *correlation == request.correlation() => {
                    client
                        .owner
                        .cancel_bind(&request)
                        .map_err(|_| RecordSessionError::Protocol)?;
                    Err(reason.clone())
                }
                ControlFrame::Bound { binding, .. }
                    if binding.handle == 2 && binding.generation == 2 =>
                {
                    Ok(client
                        .owner
                        .accept_bound(&key, &response)
                        .map_err(|_| RecordSessionError::Protocol)?)
                }
                _ => return Err(RecordSessionError::Protocol),
            }
        };
        Ok(Self {
            inner,
            secondary,
            support,
        })
    }
}
impl<S: Read + Write> ScopePairHeadsSession<S> {
    /// Report the setup outcome only; this is not current access or a head.
    pub fn secondary_binding(&self) -> Result<&Scope, &SourceError> {
        self.secondary.as_ref().map(ScopeIdentity::scope)
    }

    /// Read a fresh capability Head v1 on an exactly bound scope. Policy is
    /// evaluated by the service for every requested scope and no fallback occurs.
    /// Terminal framing or I/O failure also requests physical interruption. An
    /// interruption error is returned to the host; it must retry close/dispose.
    pub fn head(&self, scope: &Scope) -> Result<SourceHead, RecordSessionError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)?;
        let identity = if c.identity.scope() == scope {
            c.identity.clone()
        } else {
            self.secondary
                .as_ref()
                .ok()
                .filter(|bound| bound.scope() == scope)
                .ok_or(SourceError::IdentityChanged)?
                .clone()
        };
        c.owner
            .check_scope(&identity)
            .map_err(|error| match error {
                super::super::LifecycleError::StaleIdentity => SourceError::IdentityChanged,
                _ => SourceError::Unavailable,
            })?;
        let result = capabilities::read_head(&mut c.wire, &identity, self.support, true);
        if matches!(
            result,
            Err(RecordSessionError::Source(SourceError::IdentityChanged))
        ) {
            c.owner
                .invalidate_scope(&identity)
                .map_err(|_| RecordSessionError::Protocol)?;
        }
        c.observe_close();
        let terminal = c.wire.io.is_none();
        drop(c);
        if terminal {
            self.inner.close()?;
        }
        result
    }

    /// Read channel bytes and complete logical exchanges for both scopes.
    pub fn counters(&self) -> Result<WireCounters, RecordSessionError> {
        self.inner.counters()
    }

    /// Terminally close both scopes and their shared channel.
    pub fn close(&self) -> Result<(), RecordSessionError> {
        self.inner.close()
    }
}
