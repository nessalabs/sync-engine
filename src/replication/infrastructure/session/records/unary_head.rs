//! Opt-in unary SourceHead envelope on the existing serialized session owner.
use super::*;
use crate::replication::application::RecordReadView;
use crate::replication::domain::{validate_capabilities, CapabilitySupport, SourceHead};

// 1–10 and 16–18 are already assigned; 64 and 128 are flags.
pub(super) const KIND: u8 = 11;
const VERSION: u16 = 1;

/// Serve the explicitly selected unary head pilot on a host-authenticated channel.
/// The host owns peer authentication, current policy and finite I/O deadlines.
/// No legacy retry, listener, new reader or durable progress is provided.
pub fn serve_unary_head_session<S, A, F, R>(
    stream: &mut S,
    session: u64,
    limits: SessionLimits,
    policy: &mut A,
    mut factory: F,
) -> Result<(), RecordSessionError>
where
    S: Read + Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordReadView,
{
    serve_bound_impl(
        stream,
        session,
        limits,
        policy,
        &mut factory,
        BoundHooks {
            follow: None,
            #[cfg(feature = "tls")]
            artifact: None,
        },
        CapabilityRoutes {
            head: Some(R::view_head),
            history: None,
            scope_pair: false,
            pair_batch: false,
        },
        true,
        None,
    )
}

pub(super) fn server_response<A, F, R>(
    packet: &Packet,
    scope: &Scope,
    policy: &mut A,
    factory: &mut F,
    read_head: capabilities::ViewHead<R>,
) -> Result<Vec<u8>, RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    if !packet.payload.is_empty() {
        return Err(SourceError::InvalidRequest.into());
    }
    capabilities::policy(policy, scope)?;
    let mut view = factory(scope)?;
    let head = read_head(&mut view, scope)?;
    if head.scope() != scope {
        return Err(SourceError::IdentityChanged.into());
    }
    drop(view);
    let mut body = VERSION.to_be_bytes().to_vec();
    put_scope(&mut body, scope);
    capabilities::put_metadata(&mut body, &head);
    Ok(body)
}

/// Explicitly selected SourceHead pilot; shares the normal serialized owner.
/// A returned head is advisory, never an authorization grant or checkpoint.
pub struct UnaryHeadSession<S> {
    inner: RecordSession<S>,
    supported: CapabilitySupport,
}
impl<S: Read + Write> UnaryHeadSession<S> {
    /// Bind on an already authenticated channel. The host must configure both
    /// endpoints for this pilot. A rejected operation never falls back.
    pub fn connect(
        io: S,
        scope: Scope,
        limits: SessionLimits,
        supported: CapabilitySupport,
    ) -> Result<Self, RecordSessionError> {
        Ok(Self {
            inner: RecordSession::connect_open(io, scope, limits)?,
            supported,
        })
    }
    /// Obtain the existing fresh remote-policy port.
    pub fn authorizer(&self) -> RecordSessionAuthorizer<S> {
        self.inner.parts().0
    }
    /// Return counters owned by the shared session.
    pub fn counters(&self) -> Result<WireCounters, RecordSessionError> {
        self.inner.counters()
    }
    /// Close the shared owner; this cannot undo previously returned values.
    pub fn close(&self) -> Result<(), RecordSessionError> {
        self.inner.close()
    }
}
impl<S: Read + Write> UnaryHeadSession<S> {
    /// Read a correlated advisory head; caller must separately check current policy.
    pub fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)
            .map_err(|_| SourceError::Unavailable)?;
        c.scope(scope)?;
        c.wire.counters.head_checks += 1;
        let Client { identity, wire, .. } = &mut *c;
        let result = wire
            .exchange(
                KIND,
                identity,
                Vec::new(),
                2 + scope_bytes(scope) + capabilities::META_BYTES,
            )
            .and_then(|body| {
                let mut d = Decoder::new(&body);
                if capabilities::version(&mut d)? != VERSION {
                    return Err(RecordSessionError::Protocol);
                }
                let echo = d.scope()?;
                if echo != *scope {
                    return Err(RecordSessionError::Protocol);
                }
                let head = capabilities::metadata(&mut d, echo)?;
                d.finish()?;
                validate_capabilities(&head, self.supported).map_err(SourceError::Incompatible)?;
                Ok(head)
            });
        if matches!(
            &result,
            Err(RecordSessionError::Protocol | RecordSessionError::TooLarge)
        ) {
            c.wire.close();
        }
        c.observe_close();
        result.map_err(source_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;
    use std::io::Cursor;
    struct Duplex {
        incoming: Cursor<Vec<u8>>,
        outgoing: Vec<u8>,
    }
    impl Read for Duplex {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.incoming.read(b)
        }
    }
    impl Write for Duplex {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.outgoing.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn scope() -> Scope {
        let id = || Id::new("id").unwrap();
        Scope::new(id(), id(), id(), id(), id(), id())
    }
    fn limits() -> SessionLimits {
        SessionLimits {
            control_bytes: 2048,
            bindings: 1,
        }
    }
    fn client(mut reply: Packet) -> UnaryHeadSession<Duplex> {
        let mut incoming = Vec::new();
        write_control(
            &mut incoming,
            &ControlFrame::Welcome {
                session: 1,
                limits: limits(),
                record_bytes: MAX_RECORD_BYTES as u32,
            },
            2048,
        )
        .unwrap();
        write_control(
            &mut incoming,
            &ControlFrame::Bound {
                correlation: Correlation {
                    session: 1,
                    request: 1,
                },
                binding: BindingMetadata {
                    scope: scope(),
                    head: 0,
                    handle: 1,
                    generation: 1,
                },
            },
            2048,
        )
        .unwrap();
        reply.total = reply.payload.len();
        write_packet(&mut incoming, &reply, 2048).unwrap();
        UnaryHeadSession::connect(
            Duplex {
                incoming: Cursor::new(incoming),
                outgoing: Vec::new(),
            },
            scope(),
            limits(),
            CapabilitySupport::current(),
        )
        .unwrap()
    }
    fn response() -> Packet {
        let mut payload = VERSION.to_be_bytes().to_vec();
        put_scope(&mut payload, &scope());
        capabilities::put_metadata(&mut payload, &SourceHead::new(scope(), 42, 0, 0));
        Packet {
            kind: KIND | REPLY,
            status: 0,
            meta: Meta {
                session: 1,
                request: 2,
                handle: 1,
                generation: 1,
            },
            total: payload.len(),
            offset: 0,
            payload,
        }
    }
    #[test]
    fn envelope_correlation_status_and_bounds_are_strict() {
        // Canonical v1 vector: envelope, six "id" values, then SourceHead
        // v1 / position 42 / zero reader and writer masks.
        let hex = "00010269640269640269640269640269640269640001000000000000002a00000000000000000000000000000000";
        let expected: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        assert_eq!(response().payload, expected);
        let mut valid = client(response());
        assert_eq!(valid.source_head(&scope()).unwrap().position(), 42);
        let mut required = response();
        let reader_offset = 2 + scope_bytes(&scope()) + 2 + 8;
        required.payload[reader_offset + 7] = 1;
        assert_eq!(
            client(required).source_head(&scope()),
            Err(SourceError::Incompatible(
                crate::replication::domain::CapabilityError::MissingReader(1)
            ))
        );
        for case in 0..10 {
            let mut p = response();
            match case {
                0 => p.payload[1] = 2,
                1 => p.payload.push(0),
                2 => p.payload[3] ^= 1,
                3 => p.meta.session = 2,
                4 => p.meta.request = 3,
                5 => p.meta.handle = 2,
                6 => p.meta.generation = 2,
                7 => p.kind = HEAD | REPLY,
                8 => {
                    p.status = 8;
                    p.payload.clear();
                }
                _ => p.payload.resize(1000, 0),
            }
            let mut c = client(p);
            assert_eq!(
                c.source_head(&scope()),
                Err(SourceError::InvalidRequest),
                "case {case}"
            );
            assert!(c.inner.shared.lock().unwrap().wire.io.is_none());
        }
        for (code, expected) in [
            (1, SourceError::Denied),
            (2, SourceError::Unavailable),
            (3, SourceError::Pruned),
            (4, SourceError::InvalidRequest),
            (5, SourceError::IdentityChanged),
            (6, SourceError::OversizedRecord),
        ] {
            let mut p = response();
            p.status = code;
            p.payload.clear();
            assert_eq!(client(p).source_head(&scope()), Err(expected));
        }
    }
    #[test]
    fn policy_precedes_factory_and_source_drops_before_response() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPPED: AtomicUsize = AtomicUsize::new(0);
        struct View;
        impl Drop for View {
            fn drop(&mut self) {
                DROPPED.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl RecordSource for View {
            fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
                Ok(42)
            }
            fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
                panic!("head must not fetch pages")
            }
        }
        impl RecordReadView for View {
            fn view_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
                Ok(SourceHead::new(scope.clone(), 42, 0, 0))
            }
        }
        struct Policy(bool);
        impl ScopeAuthorizer for Policy {
            fn authorize(&mut self, scope: &Scope) -> Access {
                if self.0 {
                    Access::Allowed(scope.clone())
                } else {
                    Access::Denied
                }
            }
        }
        let request = Packet {
            kind: KIND,
            status: 0,
            meta: Meta {
                session: 1,
                request: 2,
                handle: 1,
                generation: 1,
            },
            total: 0,
            offset: 0,
            payload: Vec::new(),
        };
        let mut denied = Policy(false);
        let failure = server_response(
            &request,
            &scope(),
            &mut denied,
            &mut |_| -> Result<View, SourceError> { panic!("denied policy acquired source") },
            View::view_head,
        );
        assert!(matches!(
            failure,
            Err(RecordSessionError::Source(SourceError::Denied))
        ));
        DROPPED.store(0, Ordering::SeqCst);
        let body = server_response(
            &request,
            &scope(),
            &mut Policy(true),
            &mut |_| Ok(View),
            View::view_head,
        )
        .unwrap();
        assert_eq!(DROPPED.load(Ordering::SeqCst), 1);
        let mut response = Vec::new();
        respond(
            &mut response,
            &request,
            0,
            &body,
            &scope(),
            &mut Policy(false),
            2048,
        )
        .unwrap();
        let packet = read_packet(&mut response.as_slice(), 2048)
            .unwrap()
            .unwrap();
        assert_eq!(packet.status, 1);
        assert!(packet.payload.is_empty());
    }
    #[derive(Clone)]
    struct HarnessState {
        policies: std::rc::Rc<std::cell::Cell<usize>>,
        factories: std::rc::Rc<std::cell::Cell<usize>>,
        current: std::rc::Rc<std::cell::Cell<u8>>,
        failure: u8,
        withdraw: u8,
    }
    impl ScopeAuthorizer for HarnessState {
        fn authorize(&mut self, scope: &Scope) -> Access {
            self.policies.set(self.policies.get() + 1);
            match self.current.get() {
                0 => Access::Allowed(scope.clone()),
                1 => Access::Denied,
                _ => Access::Unverifiable,
            }
        }
    }
    struct HarnessView(HarnessState);
    impl RecordSource for HarnessView {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            Ok(42)
        }
        fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
            panic!("no page")
        }
    }
    impl RecordReadView for HarnessView {
        fn view_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
            if self.0.failure == 2 {
                self.0.current.set(self.0.withdraw);
                return Err(SourceError::Incompatible(
                    crate::replication::domain::CapabilityError::MissingReader(1),
                ));
            }
            Ok(SourceHead::new(scope.clone(), 42, 0, 0))
        }
    }
    fn run_server(
        pilot: bool,
        kind: Option<u8>,
        failure: u8,
        withdraw: u8,
    ) -> (usize, usize, Option<Packet>) {
        let state = HarnessState {
            policies: Default::default(),
            factories: Default::default(),
            current: Default::default(),
            failure,
            withdraw,
        };
        let mut incoming = Vec::new();
        write_control(
            &mut incoming,
            &ControlFrame::Open {
                limits: limits(),
                request: 1,
                scope: scope(),
            },
            2048,
        )
        .unwrap();
        if let Some(kind) = kind {
            write_packet(
                &mut incoming,
                &Packet {
                    kind,
                    status: 0,
                    meta: Meta {
                        session: 1,
                        request: 2,
                        handle: 1,
                        generation: 1,
                    },
                    total: 0,
                    offset: 0,
                    payload: Vec::new(),
                },
                2048,
            )
            .unwrap();
        }
        let mut io = Duplex {
            incoming: Cursor::new(incoming),
            outgoing: Vec::new(),
        };
        let mut policy = state.clone();
        let factory_state = state.clone();
        let factory = move |_: &Scope| {
            let n = factory_state.factories.get() + 1;
            factory_state.factories.set(n);
            if n == 2 && factory_state.failure == 1 {
                factory_state.current.set(factory_state.withdraw);
                return Err(SourceError::Incompatible(
                    crate::replication::domain::CapabilityError::MissingReader(1),
                ));
            }
            Ok(HarnessView(factory_state.clone()))
        };
        if pilot {
            let _ = serve_unary_head_session(&mut io, 1, limits(), &mut policy, factory);
        } else {
            let _ = serve_record_session(&mut io, 1, limits(), &mut policy, factory);
        }
        let mut output = io.outgoing.as_slice();
        assert!(matches!(
            read_control(&mut output, 2048).unwrap(),
            ControlFrame::Welcome { .. }
        ));
        assert!(matches!(
            read_control(&mut output, 2048).unwrap(),
            ControlFrame::Bound { .. }
        ));
        let reply = read_packet(&mut output, 2048).unwrap();
        (state.policies.get(), state.factories.get(), reply)
    }
    #[test]
    fn typed_incompatibility_from_factory_or_view_preserves_type_and_egress_privacy() {
        use crate::replication::domain::CapabilityError;
        for error in [
            CapabilityError::UnsupportedVersion(2),
            CapabilityError::MissingReader(1),
        ] {
            let mut packet = response();
            packet.status = 8;
            packet.payload = capabilities::encode_incompatible(error).unwrap();
            assert_eq!(
                client(packet).source_head(&scope()),
                Err(SourceError::Incompatible(error))
            );
        }
        for payload in [vec![255], vec![2, 0, 0, 0, 0, 0, 0, 0, 0], vec![1, 0, 1]] {
            let mut packet = response();
            packet.status = 8;
            packet.payload = payload;
            let mut session = client(packet);
            assert_eq!(
                session.source_head(&scope()),
                Err(SourceError::InvalidRequest)
            );
            assert!(session.inner.shared.lock().unwrap().wire.io.is_none());
        }
        for failure in [1, 2] {
            let (_, _, reply) = run_server(true, Some(KIND), failure, 0);
            let reply = reply.unwrap();
            assert_eq!(reply.status, 8, "failure={failure}");
            assert_eq!(
                client(reply).source_head(&scope()),
                Err(SourceError::Incompatible(CapabilityError::MissingReader(1)))
            );
            for (withdraw, status) in [(1, 1), (2, 2)] {
                let (_, _, reply) = run_server(true, Some(KIND), failure, withdraw);
                let reply = reply.unwrap();
                assert_eq!(reply.status, status);
                assert_eq!(reply.total, 0);
                assert!(reply.payload.is_empty());
            }
        }
    }
    #[test]
    fn wrong_endpoint_rejects_before_policy_or_factory_callbacks() {
        for (pilot, wrong_kind) in [(false, KIND), (true, HEAD)] {
            let (base_policy, base_factory, _) = run_server(pilot, None, 0, 0);
            let (policy, factory, reply) = run_server(pilot, Some(wrong_kind), 0, 0);
            assert_eq!(
                (policy, factory),
                (base_policy, base_factory),
                "pilot={pilot}"
            );
            assert!(reply.is_none());
        }
    }
}
