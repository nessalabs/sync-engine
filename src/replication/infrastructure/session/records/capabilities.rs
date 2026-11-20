//! Explicit version1 metadata operations; no legacy data fallback or listener.
use super::*;
use crate::replication::application::{RecordHeadSource, RecordReadView};
use crate::replication::domain::{
    validate_capabilities, CapabilityError, CapabilitySupport, SourceHead,
    KNOWN_READER_CAPABILITIES, SOURCE_HEAD_VERSION,
};
pub(super) const HEAD_CAP: u8 = 5;
pub(super) const OPEN_CAP: u8 = 6;
pub(super) const PAGE_CAP: u8 = 7;
const META_BYTES: usize = 26;
pub(super) type ViewHead<R> = fn(&mut R, &Scope) -> Result<SourceHead, SourceError>;
pub(super) fn operation(kind: u8) -> bool {
    matches!(kind, HEAD_CAP | OPEN_CAP | PAGE_CAP)
}
fn version(d: &mut Decoder<'_>) -> Result<u16, RecordSessionError> {
    Ok(u16::from_be_bytes(
        d.take(2)?
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    ))
}
pub(super) fn encode_incompatible(error: CapabilityError) -> Result<Vec<u8>, RecordSessionError> {
    let mut out = Vec::new();
    match error {
        CapabilityError::UnsupportedVersion(value) if value != SOURCE_HEAD_VERSION => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        CapabilityError::MissingReader(bits) if bits != 0 => {
            out.push(2);
            out.extend_from_slice(&bits.to_be_bytes());
        }
        _ => return Err(RecordSessionError::Protocol),
    }
    Ok(out)
}
pub(super) fn decode_incompatible(bytes: &[u8]) -> Result<CapabilityError, RecordSessionError> {
    let mut d = Decoder::new(bytes);
    let error = match d.byte()? {
        1 => CapabilityError::UnsupportedVersion(version(&mut d)?),
        2 => CapabilityError::MissingReader(d.u64()?),
        _ => return Err(RecordSessionError::Protocol),
    };
    d.finish()?;
    encode_incompatible(error)?;
    Ok(error)
}
fn prefix(supported: CapabilitySupport) -> Vec<u8> {
    let mut out = SOURCE_HEAD_VERSION.to_be_bytes().to_vec();
    out.extend_from_slice(&supported.reader_bits().to_be_bytes());
    out
}
fn put_metadata(out: &mut Vec<u8>, head: &SourceHead) {
    out.extend_from_slice(&head.version().to_be_bytes());
    for n in [
        head.position(),
        head.reader_required(),
        head.writer_required(),
    ] {
        out.extend_from_slice(&n.to_be_bytes());
    }
}
fn metadata(d: &mut Decoder<'_>, scope: Scope) -> Result<SourceHead, RecordSessionError> {
    Ok(SourceHead::advertised(
        scope,
        version(d)?,
        d.u64()?,
        d.u64()?,
        d.u64()?,
    ))
}
fn source_failure(error: RecordSessionError) -> SourceError {
    source_error(error)
}
fn policy<A: ScopeAuthorizer>(a: &mut A, s: &Scope) -> Result<(), RecordSessionError> {
    match access_code(a.authorize(s), s) {
        0 => Ok(()),
        7 => Err(RecordSessionError::Protocol),
        code => Err(RecordSessionError::Source(decode_source_error(code))),
    }
}
fn number(d: &mut Decoder<'_>) -> Result<usize, RecordSessionError> {
    usize::try_from(d.u64()?).map_err(|_| RecordSessionError::TooLarge)
}

/// Serve an explicitly capability-aware record endpoint over an authenticated
/// host channel. Legacy data operations are refused even for zero requirements;
/// the initial Bound remains passive metadata, not compatibility evidence.
/// The factory must supply RecordReadView's coherent requirements/encoding view.
/// Authorization is checked before each underlying read and emitted frame. Drop
/// the source view before network I/O. No durable requirement publication is added.
pub fn serve_capability_record_session<S, A, F, R>(
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
    serve_bound(
        stream,
        session,
        limits,
        policy,
        &mut factory,
        None,
        Some(R::view_head),
    )
}
pub(super) fn server_response<A, F, R>(
    packet: &Packet,
    scope: &Scope,
    authorizer: &mut A,
    factory: &mut F,
    read_head: ViewHead<R>,
) -> Result<Vec<u8>, RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    let mut d = Decoder::new(&packet.payload);
    let offered = version(&mut d).map_err(source_failure)?;
    if offered != SOURCE_HEAD_VERSION {
        return Err(SourceError::Incompatible(CapabilityError::UnsupportedVersion(offered)).into());
    }
    let reader = d.u64().map_err(|_| SourceError::InvalidRequest)?;
    let support = CapabilitySupport::new(reader & KNOWN_READER_CAPABILITIES, 0)
        .map_err(SourceError::Incompatible)?;
    // Validate all request bounds before acquisition and payload work.
    let request = if packet.kind == HEAD_CAP {
        d.finish().map_err(|_| SourceError::InvalidRequest)?;
        None
    } else {
        let after = d.u64().map_err(|_| SourceError::InvalidRequest)?;
        let target = if packet.kind == PAGE_CAP {
            Some(d.u64().map_err(|_| SourceError::InvalidRequest)?)
        } else {
            None
        };
        let r = OpenPageRequest {
            scope: scope.clone(),
            after,
            max_records: number(&mut d).map_err(source_failure)?,
            max_payload_bytes: number(&mut d).map_err(source_failure)?,
            max_record_bytes: number(&mut d).map_err(source_failure)?,
        };
        d.finish().map_err(|_| SourceError::InvalidRequest)?;
        validate_open_page_request(&r, physical(MAX_RECORD_BYTES))
            .map_err(|_| SourceError::InvalidRequest)?;
        if let Some(target) = target {
            validate_page_request(&r.for_target(target), physical(MAX_RECORD_BYTES))
                .map_err(|_| SourceError::InvalidRequest)?;
        }
        Some((r, target))
    };
    policy(authorizer, scope)?;
    let mut view = factory(scope)?;
    let head = read_head(&mut view, scope)?;
    if head.scope() != scope {
        return Err(SourceError::IdentityChanged.into());
    }
    validate_capabilities(&head, support).map_err(SourceError::Incompatible)?;
    policy(authorizer, scope)?;
    let mut body = Vec::new();
    if let Some((request, target)) = request {
        let target = target.unwrap_or(head.position());
        if target > head.position() {
            return Err(SourceError::InvalidRequest.into());
        }
        let captured = request.for_target(target);
        let page = if target > request.after {
            policy(authorizer, scope)?;
            let page = view.page(&captured)?;
            encode_record_page(&captured, page)?
        } else {
            if packet.kind == PAGE_CAP {
                return Err(SourceError::InvalidRequest.into());
            }
            let mut empty = vec![0];
            put_scope(&mut empty, scope);
            for n in [
                captured.after,
                captured.target,
                captured.max_records as u64,
                captured.max_payload_bytes as u64,
                captured.max_record_bytes as u64,
            ] {
                empty.extend_from_slice(&n.to_be_bytes());
            }
            empty.push(0);
            empty
        };
        put_metadata(&mut body, &head);
        body.extend_from_slice(&page);
    } else {
        put_scope(&mut body, head.scope());
        put_metadata(&mut body, &head);
    }
    Ok(body)
}

/// Explicit version1 capability record session. Peer endpoint support must be
/// known by the host. Uses existing bounded Open and current-policy RPCs; never
/// retries a failed capability operation as a legacy read.
pub struct CapabilityRecordSession<S> {
    inner: RecordSession<S>,
    supported: CapabilitySupport,
}
impl<S: InterruptibleSessionIo> CapabilityRecordSession<S> {
    /// Connect on an authenticated, configured channel. Bound alone grants no
    /// compatibility; first metadata/page operations enforce it before data.
    pub fn connect(
        io: S,
        scope: Scope,
        limits: SessionLimits,
        supported: CapabilitySupport,
    ) -> Result<Self, RecordSessionError> {
        RecordSession::connect_open_interruptible(io, scope, limits)
            .map(|inner| Self { inner, supported })
    }
}
impl<S: Read + Write> CapabilityRecordSession<S> {
    /// Fresh-authority and capability-aware record ports on this binding.
    pub fn parts(&self) -> (RecordSessionAuthorizer<S>, CapabilitySessionSource<S>) {
        let (a, inner) = self.inner.parts();
        (
            a,
            CapabilitySessionSource {
                inner,
                supported: self.supported,
            },
        )
    }
    /// Actual request/reply wire counters, including metadata bytes.
    pub fn counters(&self) -> Result<WireCounters, RecordSessionError> {
        self.inner.counters()
    }
    /// Terminal connection close; this cannot undo an accepted DTO or commit.
    pub fn close(&self) -> Result<(), RecordSessionError> {
        self.inner.close()
    }
}
/// Correlated version1 source ports. Every read checks fresh metadata, including
/// normal begin_pass post-checks; no SourceHead is cached as a grant/progress.
pub struct CapabilitySessionSource<S> {
    inner: RecordSessionSource<S>,
    supported: CapabilitySupport,
}
impl<S> CapabilitySessionSource<S> {
    pub(super) fn from_source(inner: RecordSessionSource<S>, supported: CapabilitySupport) -> Self {
        Self { inner, supported }
    }
}
impl<S: Read + Write> RecordHeadSource for CapabilitySessionSource<S> {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)
            .map_err(|_| SourceError::Unavailable)?;
        c.scope(scope)?;
        c.wire.counters.head_checks += 1;
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(
                HEAD_CAP,
                identity,
                prefix(self.supported),
                scope_bytes(scope) + META_BYTES,
            )
            .and_then(|body| {
                let mut d = Decoder::new(&body);
                let echo = d.scope()?;
                if echo != *scope {
                    return Err(RecordSessionError::Protocol);
                }
                let head = metadata(&mut d, echo)?;
                d.finish()?;
                validate_capabilities(&head, self.supported)
                    .map_err(|e| RecordSessionError::Source(SourceError::Incompatible(e)))?;
                Ok(head)
            })
        };
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
impl<S: Read + Write> CapabilitySessionSource<S> {
    fn read_page(&mut self, r: &OpenPageRequest, target: Option<u64>) -> Result<Page, SourceError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)
            .map_err(|_| SourceError::Unavailable)?;
        c.scope(&r.scope)?;
        validate_open_page_request(r, physical(c.wire.record_bytes))
            .map_err(|_| SourceError::InvalidRequest)?;
        if let Some(n) = target {
            validate_page_request(&r.for_target(n), physical(c.wire.record_bytes))
                .map_err(|_| SourceError::InvalidRequest)?;
        } else {
            c.wire.counters.head_checks += 1;
        }
        let kind = if target.is_some() { PAGE_CAP } else { OPEN_CAP };
        let mut payload = prefix(self.supported);
        payload.extend_from_slice(&r.after.to_be_bytes());
        if let Some(n) = target {
            payload.extend_from_slice(&n.to_be_bytes());
        }
        for n in [r.max_records, r.max_payload_bytes, r.max_record_bytes] {
            payload.extend_from_slice(&(n as u64).to_be_bytes());
        }
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(
                kind,
                identity,
                payload,
                page_bound(&r.scope, r.max_records, r.max_payload_bytes)
                    .saturating_add(META_BYTES)
                    .min(MAX_FRAME_BYTES),
            )
            .and_then(|body| {
                let mut d = Decoder::new(&body);
                let head = metadata(&mut d, r.scope.clone())?;
                let page_bytes = d.take(body.len().saturating_sub(META_BYTES))?;
                d.finish()?;
                // Validate metadata and the complete canonical request header
                // before allocating/decoding records in the canonical decoder.
                let mut echo = Decoder::new(page_bytes);
                if echo.byte()? != 0 {
                    return Err(RecordSessionError::Protocol);
                }
                let echoed = PageRequest {
                    scope: echo.scope()?,
                    after: echo.u64()?,
                    target: echo.u64()?,
                    max_records: number(&mut echo)?,
                    max_payload_bytes: number(&mut echo)?,
                    max_record_bytes: number(&mut echo)?,
                };
                if without_target(&echoed) != *r
                    || target.is_some_and(|n| n != echoed.target)
                    || (target.is_none() && head.position() != echoed.target)
                    || head.position() < echoed.target
                {
                    return Err(RecordSessionError::Protocol);
                }
                validate_capabilities(&head, self.supported)
                    .map_err(|e| RecordSessionError::Source(SourceError::Incompatible(e)))?;
                decode_page_envelope(page_bytes, r, target)
            })
        };
        let result = match result {
            Ok(page) => {
                c.received(&page.records);
                Ok(page)
            }
            Err(e) => {
                if matches!(
                    &e,
                    RecordSessionError::Protocol | RecordSessionError::TooLarge
                ) {
                    c.wire.close();
                }
                Err(source_error(e))
            }
        };
        c.observe_close();
        result
    }
}
impl<S: Read + Write> RecordSource for CapabilitySessionSource<S> {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.source_head(scope).map(|h| h.position())
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        self.read_page(&without_target(r), Some(r.target))
    }
}
impl<S: Read + Write> OpenRecordSource for CapabilitySessionSource<S> {
    fn open_page(&mut self, r: &OpenPageRequest) -> Result<OpenedPage, SourceError> {
        self.read_page(r, None).map(|page| OpenedPage {
            request: without_target(&page.request),
            target: page.request.target,
            records: page.records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;
    use std::io::Cursor;
    fn id(s: &str) -> Id {
        Id::new(s).unwrap()
    }
    fn scope() -> Scope {
        Scope::new(
            id("reader"),
            id("origin"),
            id("stream"),
            id("first"),
            id("schema"),
            id("epoch"),
        )
    }
    fn limits() -> SessionLimits {
        SessionLimits {
            control_bytes: 2048,
            bindings: 1,
        }
    }
    struct Duplex {
        incoming: Cursor<Vec<u8>>,
        outgoing: Vec<u8>,
    }
    impl Read for Duplex {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.incoming.read(out)
        }
    }
    impl Write for Duplex {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.outgoing.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn request() -> OpenPageRequest {
        OpenPageRequest {
            scope: scope(),
            after: 0,
            max_records: 1,
            max_payload_bytes: 4096,
            max_record_bytes: 4096,
        }
    }
    fn body(reader: u64, writer: u64) -> Vec<u8> {
        let mut out = Vec::new();
        put_metadata(&mut out, &SourceHead::new(scope(), 0, reader, writer));
        out.push(0);
        put_scope(&mut out, &scope());
        for n in [0, 0, 1, 4096, 4096] {
            out.extend_from_slice(&u64::to_be_bytes(n));
        }
        out.push(0);
        out
    }
    fn packet(payload: Vec<u8>, status: u8, total: usize, offset: usize) -> Packet {
        Packet {
            kind: OPEN_CAP | REPLY,
            status,
            meta: Meta {
                session: 1,
                request: 2,
                handle: 1,
                generation: 1,
            },
            payload,
            total,
            offset,
        }
    }
    fn source(packets: Vec<Packet>) -> CapabilitySessionSource<Duplex> {
        let mut bytes = Vec::new();
        write_control(
            &mut bytes,
            &ControlFrame::Welcome {
                session: 1,
                limits: limits(),
                record_bytes: MAX_RECORD_BYTES as u32,
            },
            2048,
        )
        .unwrap();
        write_control(
            &mut bytes,
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
        for p in packets {
            write_packet(&mut bytes, &p, 2048).unwrap();
        }
        let root = RecordSession::connect_open(
            Duplex {
                incoming: Cursor::new(bytes),
                outgoing: Vec::new(),
            },
            scope(),
            limits(),
        )
        .unwrap();
        CapabilityRecordSession {
            inner: root,
            supported: CapabilitySupport::current(),
        }
        .parts()
        .1
    }
    #[test]
    fn metadata_is_checked_before_decoding_opaque_records() {
        let mut wire = body(1, 0);
        *wire.last_mut().unwrap() = 1; // Declares a record without a record body.
        let n = wire.len();
        let mut s = source(vec![packet(wire, 0, n, 0)]);
        assert_eq!(
            s.open_page(&request()),
            Err(SourceError::Incompatible(CapabilityError::MissingReader(1)))
        );
        assert_eq!(
            s.inner.shared.lock().unwrap().wire.counters.payload_bytes,
            0
        );
    }
    #[test]
    fn wrong_canonical_echo_or_metadata_target_is_fatal_before_data() {
        for case in 0..3 {
            let mut wire = body(0, 0);
            if case == 0 {
                wire[2..10].copy_from_slice(&1_u64.to_be_bytes());
            }
            if case == 1 {
                let offset = META_BYTES + 1 + scope_bytes(&scope());
                wire[offset..offset + 8].copy_from_slice(&1_u64.to_be_bytes());
            }
            if case == 2 {
                let mut replacement = Vec::new();
                put_scope(
                    &mut replacement,
                    &Scope::new(
                        id("otherx"),
                        id("origin"),
                        id("stream"),
                        id("first"),
                        id("schema"),
                        id("epoch"),
                    ),
                );
                let old = scope_bytes(&scope());
                wire.splice(META_BYTES + 1..META_BYTES + 1 + old, replacement);
            }
            let n = wire.len();
            let mut s = source(vec![packet(wire, 0, n, 0)]);
            assert_eq!(s.open_page(&request()), Err(SourceError::InvalidRequest));
            assert!(s.inner.shared.lock().unwrap().wire.io.is_none());
        }
    }
    #[test]
    fn unknown_capability_refusal_codes_and_partial_refusal_switch_are_fatal() {
        for payload in [vec![255], vec![2, 0, 0, 0, 0, 0, 0, 0, 0], vec![1, 0, 1]] {
            let n = payload.len();
            let mut s = source(vec![packet(payload, 8, n, 0)]);
            assert_eq!(s.open_page(&request()), Err(SourceError::InvalidRequest));
            assert!(s.inner.shared.lock().unwrap().wire.io.is_none());
        }
        let detail = encode_incompatible(CapabilityError::MissingReader(1)).unwrap();
        let mut s = source(vec![
            packet(vec![0; 2004], 0, 2100, 0),
            packet(detail, 8, 9, 0),
        ]);
        assert_eq!(s.open_page(&request()), Err(SourceError::InvalidRequest));
        assert!(s.inner.shared.lock().unwrap().wire.io.is_none());
    }
    #[test]
    fn writer_only_unknown_bits_are_decoded_and_ignored_by_readers() {
        let wire = body(0, u64::MAX);
        let n = wire.len();
        let mut s = source(vec![packet(wire, 0, n, 0)]);
        let page = s.open_page(&request()).unwrap();
        assert_eq!(page.target, 0);
        assert!(page.records.is_empty());
    }
    struct Denied;
    impl ScopeAuthorizer for Denied {
        fn authorize(&mut self, _: &Scope) -> Access {
            Access::Denied
        }
    }
    #[test]
    fn final_policy_denial_erases_detailed_incompatibility() {
        let req = Packet {
            kind: OPEN_CAP,
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
        let mut bytes = Vec::new();
        respond(
            &mut bytes,
            &req,
            8,
            &encode_incompatible(CapabilityError::MissingReader(1)).unwrap(),
            &scope(),
            &mut Denied,
            2048,
        )
        .unwrap();
        let reply = read_packet(&mut bytes.as_slice(), 2048).unwrap().unwrap();
        assert_eq!(reply.status, 1);
        assert_eq!(reply.total, 0);
        assert!(reply.payload.is_empty());
    }
    struct Allowed;
    impl ScopeAuthorizer for Allowed {
        fn authorize(&mut self, s: &Scope) -> Access {
            Access::Allowed(s.clone())
        }
    }
    struct Unsupported;
    impl RecordSource for Unsupported {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            Err(SourceError::Incompatible(CapabilityError::MissingReader(1)))
        }
        fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
            panic!("unsupported setup cannot fetch payload")
        }
    }
    impl RecordReadView for Unsupported {
        fn view_head(&mut self, _: &Scope) -> Result<SourceHead, SourceError> {
            unreachable!()
        }
    }
    #[test]
    fn unsupported_source_preparation_has_a_metadata_free_setup_refusal() {
        let mut offer = Vec::new();
        write_control(
            &mut offer,
            &ControlFrame::Open {
                limits: limits(),
                request: 1,
                scope: scope(),
            },
            2048,
        )
        .unwrap();
        let mut io = Duplex {
            incoming: Cursor::new(offer),
            outgoing: Vec::new(),
        };
        serve_capability_record_session(&mut io, 1, limits(), &mut Allowed, |_| Ok(Unsupported))
            .unwrap();
        let mut bytes = io.outgoing.as_slice();
        assert!(matches!(
            read_control(&mut bytes, 2048).unwrap(),
            ControlFrame::Welcome { .. }
        ));
        assert_eq!(
            read_control(&mut bytes, 2048).unwrap(),
            ControlFrame::Refused {
                correlation: Correlation {
                    session: 1,
                    request: 1
                },
                reason: SourceError::InvalidRequest
            }
        );
        assert!(bytes.is_empty());
    }
    #[test]
    fn unsupported_request_version_is_refused_before_future_fields_or_view_acquisition() {
        let request = Packet {
            kind: OPEN_CAP,
            status: 0,
            meta: Meta {
                session: 1,
                request: 2,
                handle: 1,
                generation: 1,
            },
            payload: 2_u16.to_be_bytes().to_vec(),
            total: 2,
            offset: 0,
        };
        let result = server_response(
            &request,
            &scope(),
            &mut Allowed,
            &mut |_| -> Result<Unsupported, SourceError> {
                panic!("unknown request version cannot acquire a source view")
            },
            Unsupported::view_head,
        );
        assert!(matches!(
            result,
            Err(RecordSessionError::Source(SourceError::Incompatible(
                CapabilityError::UnsupportedVersion(2)
            )))
        ));
    }
}
