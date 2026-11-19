//! Serialized, one-scope record protocol over a host-owned authenticated duplex
//! channel. This module authenticates no peer and opens no listener. The host
//! must authenticate the channel before calling the server, bind its current
//! policy port to that principal, enforce the advertised append ceiling, and
//! configure finite I/O deadlines. No source handle survives response writes.

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

use super::{
    read_control, write_control, BindingMetadata, ControlError, ControlFrame, Correlation,
    ScopeIdentity, ScopedSessionAuthorizer, SessionLifecycle, SessionLimits, FIRST_REQUEST,
    MAX_CONTROL_BYTES, MIN_CONTROL_BYTES,
};
use crate::replication::application::{
    Access, OpenRecordSource, RecordSource, ScopeAuthorizer, SourceError, MAX_RECORD_BYTES,
};
use crate::replication::domain::{
    validate_open_page_request, validate_page_request, Limits, OpenPageRequest, OpenedPage, Page,
    PageRequest, Record, Scope,
};
use crate::replication::infrastructure::loopback::{
    decode_source_error, encode_record_page, put_scope, source_error_code, Decoder, FrameError,
    WireCounters, MAX_FRAME_BYTES, MAX_PAGE_PAYLOAD, MAX_PAGE_RECORDS,
};

const HEADER: usize = 44;
const AUTHORIZE: u8 = 1;
const HEAD: u8 = 2;
const PAGE: u8 = 3;
const OPEN_PAGE: u8 = 4;
const REPLY: u8 = 128;

/// Session connection/read refusal. Callers own retry/reconnect and durable
/// progress recovery; the adapter performs neither automatically.
#[derive(Debug)]
pub enum RecordSessionError {
    /// Channel failed or a reply was truncated.
    Io(io::Error),
    /// Invalid correlation, framing, operation or unsupported status.
    Protocol,
    /// A hard or negotiated resource budget was exceeded.
    TooLarge,
    /// Known typed source or current-policy refusal.
    Source(SourceError),
    /// Connection is closed or its shared owner became unavailable.
    Closed,
}
impl From<io::Error> for RecordSessionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<FrameError> for RecordSessionError {
    fn from(error: FrameError) -> Self {
        match error {
            FrameError::Io(e) => Self::Io(e),
            FrameError::TooLarge => Self::TooLarge,
            FrameError::Malformed => Self::Protocol,
        }
    }
}
impl From<ControlError> for RecordSessionError {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::Io(e) => Self::Io(e),
            ControlError::TooLarge => Self::TooLarge,
            _ => Self::Protocol,
        }
    }
}
fn source_error(error: RecordSessionError) -> SourceError {
    match error {
        RecordSessionError::Source(e) => e,
        RecordSessionError::Io(_) | RecordSessionError::Closed => SourceError::Unavailable,
        _ => SourceError::InvalidRequest,
    }
}
fn access_code(access: Access, scope: &Scope) -> u8 {
    match access {
        Access::Allowed(current) if current == *scope => 0,
        Access::Allowed(_) | Access::IdentityChanged => 5,
        Access::Denied => 1,
        Access::Unverifiable => 2,
        Access::InvalidResponse => 7,
    }
}
fn physical(record_bytes: usize) -> Limits {
    Limits::new(MAX_PAGE_RECORDS, MAX_PAGE_PAYLOAD, record_bytes).expect("fixed positive budgets")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Meta {
    session: u64,
    request: u64,
    handle: u32,
    generation: u64,
}
impl Meta {
    fn for_scope(identity: &ScopeIdentity, request: u64) -> Self {
        Self {
            session: identity.session().get(),
            request,
            handle: identity.handle(),
            generation: identity.generation(),
        }
    }
}
struct Packet {
    kind: u8,
    status: u8,
    meta: Meta,
    total: usize,
    offset: usize,
    payload: Vec<u8>,
}
fn write_packet(
    writer: &mut impl Write,
    packet: &Packet,
    limit: usize,
) -> Result<(), RecordSessionError> {
    if !(MIN_CONTROL_BYTES..=MAX_CONTROL_BYTES).contains(&limit)
        || packet.payload.len() > limit - HEADER
        || packet.total > MAX_FRAME_BYTES
    {
        return Err(RecordSessionError::TooLarge);
    }
    let mut bytes = Vec::with_capacity(4 + HEADER + packet.payload.len());
    bytes.extend_from_slice(&((HEADER + packet.payload.len()) as u32).to_be_bytes());
    bytes.extend_from_slice(b"NSR1");
    bytes.extend_from_slice(&1_u16.to_be_bytes());
    bytes.extend_from_slice(&[packet.kind, packet.status]);
    bytes.extend_from_slice(&packet.meta.session.to_be_bytes());
    bytes.extend_from_slice(&packet.meta.request.to_be_bytes());
    bytes.extend_from_slice(&packet.meta.handle.to_be_bytes());
    bytes.extend_from_slice(&packet.meta.generation.to_be_bytes());
    bytes.extend_from_slice(&(packet.total as u32).to_be_bytes());
    bytes.extend_from_slice(&(packet.offset as u32).to_be_bytes());
    bytes.extend_from_slice(&packet.payload);
    writer.write_all(&bytes)?;
    Ok(())
}
fn read_packet(reader: &mut impl Read, limit: usize) -> Result<Option<Packet>, RecordSessionError> {
    let mut header = [0; 4];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let length = u32::from_be_bytes(header) as usize;
    if length > limit || length > MAX_CONTROL_BYTES {
        return Err(RecordSessionError::TooLarge);
    }
    if length < HEADER {
        return Err(RecordSessionError::Protocol);
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let mut d = Decoder::new(&body);
    if d.take(4)? != b"NSR1" || d.take(2)? != 1_u16.to_be_bytes() {
        return Err(RecordSessionError::Protocol);
    }
    let kind = d.byte()?;
    let status = d.byte()?;
    if ![
        AUTHORIZE,
        HEAD,
        PAGE,
        OPEN_PAGE,
        AUTHORIZE | REPLY,
        HEAD | REPLY,
        PAGE | REPLY,
        OPEN_PAGE | REPLY,
    ]
    .contains(&kind)
    {
        return Err(RecordSessionError::Protocol);
    }
    let meta = Meta {
        session: d.u64()?,
        request: d.u64()?,
        handle: u32::from_be_bytes(
            d.take(4)?
                .try_into()
                .map_err(|_| RecordSessionError::Protocol)?,
        ),
        generation: d.u64()?,
    };
    if meta.session == 0 || meta.request == 0 || meta.handle == 0 || meta.generation == 0 {
        return Err(RecordSessionError::Protocol);
    }
    let total = u32::from_be_bytes(
        d.take(4)?
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    ) as usize;
    let offset = u32::from_be_bytes(
        d.take(4)?
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    ) as usize;
    if total > MAX_FRAME_BYTES {
        return Err(RecordSessionError::TooLarge);
    }
    let payload = d.take(length - HEADER)?.to_vec();
    d.finish()?;
    Ok(Some(Packet {
        kind,
        status,
        meta,
        total,
        offset,
        payload,
    }))
}

struct Wire<S> {
    io: Option<S>,
    counters: WireCounters,
    limit: usize,
    record_bytes: usize,
    next: Option<u64>,
}
impl<S: Read> Read for Wire<S> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self
            .io
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?
            .read(out)?;
        self.counters.protocol_bytes += n as u64;
        Ok(n)
    }
}
impl<S: Write> Write for Wire<S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = self
            .io
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?
            .write(bytes)?;
        self.counters.protocol_bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.io
            .as_mut()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?
            .flush()
    }
}
impl<S: Read + Write> Wire<S> {
    fn close(&mut self) {
        self.io.take();
    }
    fn control(&mut self, frame: &ControlFrame) -> Result<ControlFrame, RecordSessionError> {
        let limit = self.limit;
        write_control(self, frame, limit)?;
        self.flush()?;
        let response = read_control(self, limit)?;
        self.counters.exchanges += 1;
        Ok(response)
    }
    fn exchange(
        &mut self,
        kind: u8,
        identity: &ScopeIdentity,
        payload: Vec<u8>,
        max_reply: usize,
    ) -> Result<Vec<u8>, RecordSessionError> {
        if self.io.is_none() {
            return Err(RecordSessionError::Closed);
        }
        let request = self.next.ok_or(RecordSessionError::Protocol)?;
        let meta = Meta::for_scope(identity, request);
        self.next = request.checked_add(1);
        let limit = self.limit;
        let result = (|| {
            write_packet(
                self,
                &Packet {
                    kind,
                    status: 0,
                    meta,
                    total: payload.len(),
                    offset: 0,
                    payload,
                },
                limit,
            )?;
            self.flush()?;
            let mut result = Vec::new();
            let mut total = None;
            loop {
                let p = read_packet(self, limit)?
                    .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
                if p.kind != kind | REPLY || p.meta != meta {
                    return Err(RecordSessionError::Protocol);
                }
                if p.status != 0 {
                    if p.total != 0 || p.offset != 0 || !p.payload.is_empty() {
                        return Err(RecordSessionError::Protocol);
                    }
                    self.counters.exchanges += 1;
                    if !(1..=6).contains(&p.status) {
                        return Err(RecordSessionError::Protocol);
                    }
                    return Err(RecordSessionError::Source(decode_source_error(p.status)));
                }
                if p.total > max_reply {
                    return Err(RecordSessionError::TooLarge);
                }
                if p.total == 0
                    || p.offset != result.len()
                    || total.is_some_and(|n| n != p.total)
                    || p.payload.is_empty()
                    || p.payload.len() > p.total.saturating_sub(p.offset)
                {
                    return Err(RecordSessionError::Protocol);
                }
                if total.is_none() {
                    result.reserve(p.total);
                    total = Some(p.total);
                }
                result.extend_from_slice(&p.payload);
                if result.len() == p.total {
                    self.counters.exchanges += 1;
                    return Ok(result);
                }
                if p.payload.len() != limit - HEADER {
                    return Err(RecordSessionError::Protocol);
                }
            }
        })();
        match &result {
            Ok(_)
            | Err(RecordSessionError::Source(
                SourceError::Denied
                | SourceError::Unavailable
                | SourceError::Pruned
                | SourceError::InvalidRequest
                | SourceError::OversizedRecord,
            )) => {}
            _ => self.close(),
        }
        result
    }
}
struct Client<S> {
    owner: SessionLifecycle,
    identity: ScopeIdentity,
    wire: Wire<S>,
    seen: u64,
}
impl<S: Read + Write> Client<S> {
    fn scope(&self, scope: &Scope) -> Result<(), SourceError> {
        if self.identity.scope() != scope {
            return Err(SourceError::IdentityChanged);
        }
        self.owner.check_scope(&self.identity).map_err(|e| match e {
            super::LifecycleError::Closed | super::LifecycleError::NotReady => {
                SourceError::Unavailable
            }
            _ => SourceError::IdentityChanged,
        })
    }
    fn received(&mut self, records: &[Record]) {
        let bytes: usize = records.iter().map(|r| r.payload.len()).sum();
        let duplicates: usize = records
            .iter()
            .filter(|r| r.position <= self.seen)
            .map(|r| r.payload.len())
            .sum();
        self.seen = records
            .iter()
            .map(|r| r.position)
            .max()
            .unwrap_or(self.seen)
            .max(self.seen);
        self.wire.counters.payload_bytes += bytes as u64;
        self.wire.counters.duplicate_bytes += duplicates as u64;
        self.wire.counters.protocol_bytes = self
            .wire
            .counters
            .protocol_bytes
            .saturating_sub(bytes as u64);
    }
    fn observe_close(&mut self) {
        if self.wire.io.is_none() {
            self.owner.close();
        }
    }
}

/// One connected, serialized record session. Authentication and I/O deadlines
/// belong to the supplied channel's host. All parts share this connection; the
/// client never silently reconnects. Each part keeps the channel alive; dropping
/// this handle alone does not cancel surviving parts. Use `close` to invalidate
/// all parts. This first adapter supports one bound scope.
pub struct RecordSession<S> {
    shared: Arc<Mutex<Client<S>>>,
}
/// Current-policy RPC port, with local bound-scope preflight on every call.
pub struct RecordSessionAuthorizer<S> {
    shared: Arc<Mutex<Client<S>>>,
}
/// Bounded head/page RPC port. Source-side policy is checked independently of
/// the application's separate current-policy RPC before every read and fragment.
pub struct RecordSessionSource<S> {
    shared: Arc<Mutex<Client<S>>>,
}

impl<S: Read + Write> RecordSession<S> {
    /// Negotiate and bind on an already authenticated host channel. No source
    /// port is returned if binding is refused. This opens no socket itself.
    pub fn connect(
        io: S,
        scope: Scope,
        offered: SessionLimits,
    ) -> Result<Self, RecordSessionError> {
        Self::connect_with(io, scope, offered, false)
    }
    /// Combine the initial hello and binding offer into one bounded request.
    /// Use only when the host knows the peer supports Open. Welcome and Bound
    /// are still validated independently; there is no fallback or reused grant.
    pub fn connect_open(
        io: S,
        scope: Scope,
        offered: SessionLimits,
    ) -> Result<Self, RecordSessionError> {
        Self::connect_with(io, scope, offered, true)
    }
    fn connect_with(
        io: S,
        scope: Scope,
        offered: SessionLimits,
        opening: bool,
    ) -> Result<Self, RecordSessionError> {
        let mut owner =
            SessionLifecycle::new(offered, 1).map_err(|_| RecordSessionError::Protocol)?;
        let key = owner.connection_id();
        let mut wire = Wire {
            io: Some(io),
            counters: WireCounters::default(),
            limit: offered.control_bytes as usize,
            record_bytes: MAX_RECORD_BYTES,
            next: None,
        };
        let welcome = if opening {
            // The complete offer fits the mandatory minimum budget, before any
            // peer limit is selected. Retain the scope until Bound correlation.
            write_control(
                &mut wire,
                &ControlFrame::Open {
                    limits: offered,
                    request: FIRST_REQUEST,
                    scope: scope.clone(),
                },
                MIN_CONTROL_BYTES,
            )?;
            wire.flush()?;
            read_control(&mut wire, offered.control_bytes as usize)?
        } else {
            wire.control(&owner.hello().map_err(|_| RecordSessionError::Protocol)?)?
        };
        owner
            .accept_welcome(&key, &welcome)
            .map_err(|_| RecordSessionError::Protocol)?;
        if let ControlFrame::Welcome {
            limits,
            record_bytes,
            ..
        } = welcome
        {
            wire.limit = limits.control_bytes as usize;
            wire.record_bytes = record_bytes as usize;
        }
        let (request, frame) = owner
            .begin_bind(scope)
            .map_err(|_| RecordSessionError::Protocol)?;
        let response = if opening {
            if request.correlation().request != FIRST_REQUEST {
                return Err(RecordSessionError::Protocol);
            }
            let limit = wire.limit;
            let response = read_control(&mut wire, limit)?;
            wire.counters.exchanges += 1;
            response
        } else {
            wire.control(&frame)?
        };
        if let ControlFrame::Refused {
            correlation,
            reason,
        } = response
        {
            if correlation != request.correlation() {
                return Err(RecordSessionError::Protocol);
            }
            return Err(RecordSessionError::Source(reason));
        }
        let identity = owner
            .accept_bound(&key, &response)
            .map_err(|_| RecordSessionError::Protocol)?;
        wire.next = request.correlation().request.checked_add(1);
        Ok(Self {
            shared: Arc::new(Mutex::new(Client {
                owner,
                identity,
                wire,
                seen: 0,
            })),
        })
    }
    /// Obtain separate application ports sharing the same serialized channel.
    pub fn parts(&self) -> (RecordSessionAuthorizer<S>, RecordSessionSource<S>) {
        (
            RecordSessionAuthorizer {
                shared: Arc::clone(&self.shared),
            },
            RecordSessionSource {
                shared: Arc::clone(&self.shared),
            },
        )
    }
    /// Actual channel bytes and complete logical exchanges, including hello/bind.
    /// Payload classification covers complete decoded pages; interrupted bytes
    /// remain protocol/unclassified. Counters are not durable progress receipts.
    pub fn counters(&self) -> Result<WireCounters, RecordSessionError> {
        self.shared
            .lock()
            .map(|c| c.wire.counters)
            .map_err(|_| RecordSessionError::Closed)
    }
    /// Invalidate all parts and drop the held channel without resetting cache.
    /// This waits behind an in-flight RPC. Borrowed/externally shared channels
    /// still need host disposal; dropping a handle need not shut down its socket.
    pub fn close(&self) -> Result<(), RecordSessionError> {
        let mut c = self.shared.lock().map_err(|_| RecordSessionError::Closed)?;
        c.wire.close();
        c.owner.close();
        Ok(())
    }
}
struct RemotePolicy<'a, S> {
    wire: &'a mut Wire<S>,
    identity: &'a ScopeIdentity,
}
impl<S: Read + Write> ScopeAuthorizer for RemotePolicy<'_, S> {
    fn authorize(&mut self, scope: &Scope) -> Access {
        match self
            .wire
            .exchange(AUTHORIZE, self.identity, Vec::new(), scope_bytes(scope))
        {
            Ok(body) => {
                let mut d = Decoder::new(&body);
                match d.scope().and_then(|echo| {
                    d.finish()?;
                    Ok(echo)
                }) {
                    Ok(echo) if echo == *scope => Access::Allowed(echo),
                    _ => {
                        self.wire.close();
                        Access::InvalidResponse
                    }
                }
            }
            Err(RecordSessionError::Source(SourceError::Denied)) => Access::Denied,
            Err(RecordSessionError::Source(SourceError::Unavailable))
            | Err(RecordSessionError::Io(_))
            | Err(RecordSessionError::Closed) => Access::Unverifiable,
            Err(RecordSessionError::Source(SourceError::IdentityChanged)) => {
                Access::IdentityChanged
            }
            _ => {
                self.wire.close();
                Access::InvalidResponse
            }
        }
    }
}
impl<S: Read + Write> ScopeAuthorizer for RecordSessionAuthorizer<S> {
    fn authorize(&mut self, scope: &Scope) -> Access {
        let Ok(mut c) = self.shared.lock() else {
            return Access::Unverifiable;
        };
        let result = {
            let Client {
                owner,
                identity,
                wire,
                ..
            } = &mut *c;
            let mut current = RemotePolicy { wire, identity };
            ScopedSessionAuthorizer::new(owner, identity, &mut current).authorize(scope)
        };
        c.observe_close();
        result
    }
}
impl<S: Read + Write> RecordSource for RecordSessionSource<S> {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        let mut c = self.shared.lock().map_err(|_| SourceError::Unavailable)?;
        c.scope(scope)?;
        c.wire.counters.head_checks += 1;
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(HEAD, identity, Vec::new(), scope_bytes(scope) + 8)
                .and_then(|body| {
                    let mut d = Decoder::new(&body);
                    if d.scope()? != *scope {
                        return Err(RecordSessionError::Protocol);
                    }
                    let head = d.u64()?;
                    d.finish()?;
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
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        let mut c = self.shared.lock().map_err(|_| SourceError::Unavailable)?;
        c.scope(&request.scope)?;
        validate_page_request(request, physical(c.wire.record_bytes))
            .map_err(|_| SourceError::InvalidRequest)?;
        let mut payload = Vec::new();
        for n in [
            request.after,
            request.target,
            request.max_records as u64,
            request.max_payload_bytes as u64,
            request.max_record_bytes as u64,
        ] {
            payload.extend_from_slice(&n.to_be_bytes());
        }
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(
                PAGE,
                identity,
                payload,
                page_bound(
                    &request.scope,
                    request.max_records,
                    request.max_payload_bytes,
                ),
            )
            .and_then(|body| decode_page(&body, request))
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
impl<S: Read + Write> OpenRecordSource for RecordSessionSource<S> {
    fn open_page(&mut self, request: &OpenPageRequest) -> Result<OpenedPage, SourceError> {
        let mut c = self.shared.lock().map_err(|_| SourceError::Unavailable)?;
        c.scope(&request.scope)?;
        validate_open_page_request(request, physical(c.wire.record_bytes))
            .map_err(|_| SourceError::InvalidRequest)?;
        c.wire.counters.head_checks += 1;
        let mut payload = Vec::new();
        for n in [
            request.after,
            request.max_records as u64,
            request.max_payload_bytes as u64,
            request.max_record_bytes as u64,
        ] {
            payload.extend_from_slice(&n.to_be_bytes());
        }
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(
                OPEN_PAGE,
                identity,
                payload,
                page_bound(
                    &request.scope,
                    request.max_records,
                    request.max_payload_bytes,
                ),
            )
            .and_then(|body| decode_page_envelope(&body, request, None))
        };
        let result = match result {
            Ok(page) => {
                c.received(&page.records);
                Ok(OpenedPage {
                    request: without_target(&page.request),
                    target: page.request.target,
                    records: page.records,
                })
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
fn without_target(request: &PageRequest) -> OpenPageRequest {
    OpenPageRequest {
        scope: request.scope.clone(),
        after: request.after,
        max_records: request.max_records,
        max_payload_bytes: request.max_payload_bytes,
        max_record_bytes: request.max_record_bytes,
    }
}
fn scope_bytes(scope: &Scope) -> usize {
    [
        scope.receiver(),
        scope.origin(),
        scope.stream(),
        scope.incarnation(),
        scope.schema(),
        scope.access_epoch(),
    ]
    .iter()
    .map(|id| 1 + id.as_str().len())
    .sum()
}
fn page_bound(scope: &Scope, max_records: usize, max_payload_bytes: usize) -> usize {
    // Caller validated physical limits before this calculation. Valid record scopes
    // equal the admitted scope; record IDs retain the domain-owned byte ceiling.
    let scope = scope_bytes(scope);
    (1 + scope
        + 41
        + max_records * (13 + crate::replication::domain::MAX_ID_BYTES + scope)
        + max_payload_bytes)
        .min(MAX_FRAME_BYTES)
}
fn decode_page(body: &[u8], request: &PageRequest) -> Result<Page, RecordSessionError> {
    decode_page_envelope(body, &without_target(request), Some(request.target))
}
fn decode_page_envelope(
    body: &[u8],
    request: &OpenPageRequest,
    target: Option<u64>,
) -> Result<Page, RecordSessionError> {
    let mut d = Decoder::new(body);
    if d.byte()? != 0 {
        return Err(RecordSessionError::Protocol);
    }
    let echoed = PageRequest {
        scope: d.scope()?,
        after: d.u64()?,
        target: d.u64()?,
        max_records: usize::try_from(d.u64()?).map_err(|_| RecordSessionError::TooLarge)?,
        max_payload_bytes: usize::try_from(d.u64()?).map_err(|_| RecordSessionError::TooLarge)?,
        max_record_bytes: usize::try_from(d.u64()?).map_err(|_| RecordSessionError::TooLarge)?,
    };
    if without_target(&echoed) != *request || target.is_some_and(|target| target != echoed.target) {
        return Err(RecordSessionError::Protocol);
    }
    let count = d.byte()? as usize;
    if (target.is_some() && count == 0) || count > request.max_records {
        return Err(RecordSessionError::Protocol);
    }
    let mut records = Vec::with_capacity(count);
    let mut bytes = 0;
    for _ in 0..count {
        let record = d.wire_record(
            request
                .max_record_bytes
                .min(request.max_payload_bytes - bytes),
        )?;
        bytes += record.payload.len();
        records.push(record);
    }
    d.finish()?;
    Ok(Page {
        request: echoed,
        records,
    })
}

fn respond<S: Write, A: ScopeAuthorizer>(
    stream: &mut S,
    request: &Packet,
    status: u8,
    body: &[u8],
    scope: &Scope,
    policy: &mut A,
    limit: usize,
) -> Result<(), RecordSessionError> {
    let cap = limit - HEADER;
    let mut offset = 0;
    loop {
        let current = access_code(policy.authorize(scope), scope);
        let status = if current == 0 { status } else { current };
        if status != 0 {
            write_packet(
                stream,
                &Packet {
                    kind: request.kind | REPLY,
                    status,
                    meta: request.meta,
                    total: 0,
                    offset: 0,
                    payload: Vec::new(),
                },
                limit,
            )?;
            stream.flush()?;
            return Ok(());
        }
        if body.is_empty() {
            return Err(RecordSessionError::Protocol);
        }
        let end = (offset + cap).min(body.len());
        write_packet(
            stream,
            &Packet {
                kind: request.kind | REPLY,
                status: 0,
                meta: request.meta,
                total: body.len(),
                offset,
                payload: body[offset..end].to_vec(),
            },
            limit,
        )?;
        stream.flush()?;
        if end == body.len() {
            return Ok(());
        }
        offset = end;
    }
}

fn open_page_response<A, F, R>(
    request: &OpenPageRequest,
    policy: &mut A,
    factory: &mut F,
) -> Result<Vec<u8>, u8>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    let scope = &request.scope;
    let current = access_code(policy.authorize(scope), scope);
    if current != 0 {
        return Err(current);
    }
    let target = factory(scope)
        .and_then(|mut source| source.head(scope))
        .map_err(source_error_code)?;
    let current = access_code(policy.authorize(scope), scope);
    if current != 0 {
        return Err(current);
    }
    let captured = request.for_target(target);
    if target > request.after {
        let current = access_code(policy.authorize(scope), scope);
        if current != 0 {
            return Err(current);
        }
        factory(scope)
            .and_then(|mut source| source.page(&captured))
            .and_then(|page| encode_record_page(&captured, page))
            .map_err(source_error_code)
    } else {
        // Full source echo, including the captured target. No page or progress
        // is fabricated for an at-head or behind-progress observation.
        let mut body = vec![0];
        put_scope(&mut body, scope);
        for n in [
            request.after,
            target,
            request.max_records as u64,
            request.max_payload_bytes as u64,
            request.max_record_bytes as u64,
        ] {
            body.extend_from_slice(&n.to_be_bytes());
        }
        body.push(0);
        Ok(body)
    }
}

/// Serve one authenticated channel using the host's principal-bound current
/// policy and per-operation source factory. No listener/authentication is added.
/// Source handles drop before reply I/O. Policy is checked before acquisition
/// and again before every emitted fragment (including typed refusal replies).
/// This initial adapter binds one scope, permits one serialized read at a time,
/// and returns without retry on connection loss. Factory sources must enforce
/// the advertised [`MAX_RECORD_BYTES`] append ceiling. Caller owns deadlines.
pub fn serve_record_session<S, A, F, R>(
    stream: &mut S,
    session: u64,
    supported: SessionLimits,
    policy: &mut A,
    mut open_source: F,
) -> Result<(), RecordSessionError>
where
    S: Read + Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    supported
        .validate()
        .map_err(|_| RecordSessionError::Protocol)?;
    if session == 0 {
        return Err(RecordSessionError::Protocol);
    }
    let (offered, opening) = match read_control(stream, supported.control_bytes as usize)? {
        ControlFrame::Hello(offered) => (offered, None),
        ControlFrame::Open {
            limits,
            request,
            scope,
        } => (limits, Some((request, scope))),
        _ => return Err(RecordSessionError::Protocol),
    };
    let selected = SessionLimits {
        control_bytes: offered.control_bytes.min(supported.control_bytes),
        bindings: 1,
    };
    let limit = selected.control_bytes as usize;
    write_control(
        stream,
        &ControlFrame::Welcome {
            session,
            limits: selected,
            record_bytes: MAX_RECORD_BYTES as u32,
        },
        limit,
    )?;
    stream.flush()?;
    let (correlation, scope) = if let Some((request, scope)) = opening {
        (Correlation { session, request }, scope)
    } else {
        match read_control(stream, limit)? {
            ControlFrame::Bind { correlation, scope } => (correlation, scope),
            _ => return Err(RecordSessionError::Protocol),
        }
    };
    if correlation.session != session {
        return Err(RecordSessionError::Protocol);
    }
    let initial = access_code(policy.authorize(&scope), &scope);
    let head = if initial == 0 {
        open_source(&scope).and_then(|mut source| source.head(&scope))
    } else {
        Err(if initial == 7 {
            SourceError::InvalidRequest
        } else {
            decode_source_error(initial)
        })
    };
    let current = if initial == 0 {
        access_code(policy.authorize(&scope), &scope)
    } else {
        initial
    };
    let head = if current == 0 {
        head
    } else {
        Err(if current == 7 {
            SourceError::InvalidRequest
        } else {
            decode_source_error(current)
        })
    };
    match head {
        Ok(head) => write_control(
            stream,
            &ControlFrame::Bound {
                correlation,
                binding: BindingMetadata {
                    handle: 1,
                    generation: 1,
                    scope: scope.clone(),
                    head,
                },
            },
            limit,
        )?,
        Err(reason) => {
            write_control(
                stream,
                &ControlFrame::Refused {
                    correlation,
                    reason,
                },
                limit,
            )?;
            stream.flush()?;
            return Ok(());
        }
    }
    stream.flush()?;
    let mut last = correlation.request;
    while let Some(request) = read_packet(stream, limit)? {
        if request.kind & REPLY != 0
            || request.status != 0
            || request.meta.session != session
            || request.meta.handle != 1
            || request.meta.generation != 1
            || last.checked_add(1) != Some(request.meta.request)
            || request.offset != 0
            || request.total != request.payload.len()
        {
            return Err(RecordSessionError::Protocol);
        }
        last = request.meta.request;
        let current = access_code(policy.authorize(&scope), &scope);
        let result = if current != 0 {
            Err(current)
        } else {
            match request.kind {
                AUTHORIZE if request.payload.is_empty() => {
                    let mut body = Vec::new();
                    put_scope(&mut body, &scope);
                    Ok(body)
                }
                HEAD if request.payload.is_empty() => open_source(&scope)
                    .and_then(|mut source| source.head(&scope))
                    .map(|head| {
                        let mut body = Vec::new();
                        put_scope(&mut body, &scope);
                        body.extend_from_slice(&head.to_be_bytes());
                        body
                    })
                    .map_err(source_error_code),
                OPEN_PAGE => {
                    let decoded = (|| -> Result<OpenPageRequest, RecordSessionError> {
                        let mut d = Decoder::new(&request.payload);
                        let r = OpenPageRequest {
                            scope: scope.clone(),
                            after: d.u64()?,
                            max_records: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                            max_payload_bytes: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                            max_record_bytes: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                        };
                        d.finish()?;
                        Ok(r)
                    })();
                    match decoded {
                        Ok(r)
                            if validate_open_page_request(&r, physical(MAX_RECORD_BYTES))
                                .is_ok() =>
                        {
                            open_page_response(&r, policy, &mut open_source)
                        }
                        _ => Err(4),
                    }
                }
                PAGE => {
                    let decoded = (|| -> Result<PageRequest, RecordSessionError> {
                        let mut d = Decoder::new(&request.payload);
                        let request = PageRequest {
                            scope: scope.clone(),
                            after: d.u64()?,
                            target: d.u64()?,
                            max_records: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                            max_payload_bytes: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                            max_record_bytes: usize::try_from(d.u64()?)
                                .map_err(|_| RecordSessionError::TooLarge)?,
                        };
                        d.finish()?;
                        Ok(request)
                    })();
                    match decoded {
                        Ok(request)
                            if validate_page_request(&request, physical(MAX_RECORD_BYTES))
                                .is_ok() =>
                        {
                            open_source(&scope)
                                .and_then(|mut source| source.page(&request))
                                .and_then(|page| encode_record_page(&request, page))
                                .map_err(source_error_code)
                        }
                        _ => Err(4),
                    }
                }
                _ => Err(4),
            }
        };
        match result {
            Ok(body) => respond(stream, &request, 0, &body, &scope, policy, limit)?,
            Err(code) => respond(stream, &request, code, &[], &scope, policy, limit)?,
        }
    }
    Ok(())
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
    fn context() -> ScopeIdentity {
        let id = || Id::new("fixture").unwrap();
        let scope = Scope::new(id(), id(), id(), id(), id(), id());
        let limits = SessionLimits {
            control_bytes: 8192,
            bindings: 1,
        };
        let mut owner = SessionLifecycle::new(limits, 1).unwrap();
        let key = owner.connection_id();
        owner
            .accept_welcome(
                &key,
                &ControlFrame::Welcome {
                    session: 1,
                    limits,
                    record_bytes: MAX_RECORD_BYTES as u32,
                },
            )
            .unwrap();
        let (request, _) = owner.begin_bind(scope.clone()).unwrap();
        owner
            .accept_bound(
                &key,
                &ControlFrame::Bound {
                    correlation: request.correlation(),
                    binding: BindingMetadata {
                        handle: 1,
                        generation: 1,
                        scope,
                        head: 0,
                    },
                },
            )
            .unwrap()
    }
    fn packet(identity: &ScopeIdentity, request: u64) -> Packet {
        let mut payload = Vec::new();
        put_scope(&mut payload, identity.scope());
        payload.extend_from_slice(&7_u64.to_be_bytes());
        Packet {
            kind: HEAD | REPLY,
            status: 0,
            meta: Meta::for_scope(identity, request),
            total: payload.len(),
            offset: 0,
            payload,
        }
    }
    fn wire(bytes: Vec<u8>) -> Wire<Duplex> {
        Wire {
            io: Some(Duplex {
                incoming: Cursor::new(bytes),
                outgoing: Vec::new(),
            }),
            counters: WireCounters::default(),
            limit: 8192,
            record_bytes: MAX_RECORD_BYTES,
            next: Some(2),
        }
    }
    #[test]
    fn bad_correlations_statuses_offsets_and_totals_are_fatal() {
        let identity = context();
        let cap = scope_bytes(identity.scope()) + 8;
        for case in 0..9 {
            let mut response = packet(&identity, 2);
            match case {
                0 => response.meta.request = 3,
                1 => response.meta.handle = 2,
                2 => response.meta.generation = 2,
                3 => response.meta.session = 2,
                4 => {
                    response.status = 255;
                    response.total = 0;
                    response.payload.clear();
                }
                5 => response.offset = 1,
                6 => response.total = MAX_FRAME_BYTES,
                7 => {
                    response.total = cap;
                    response.payload = vec![1];
                }
                _ => response.kind = 255,
            }
            let mut bytes = Vec::new();
            write_packet(&mut bytes, &response, 8192).unwrap();
            let mut wire = wire(bytes);
            assert!(
                matches!(
                    wire.exchange(HEAD, &identity, Vec::new(), cap),
                    Err(RecordSessionError::Protocol | RecordSessionError::TooLarge)
                ),
                "case {case}"
            );
            assert!(wire.io.is_none());
            assert_eq!(wire.counters.payload_bytes, 0);
        }
    }
    #[test]
    fn version_mismatch_and_oversized_headers_refuse_without_body_acquisition() {
        let identity = context();
        let mut bytes = Vec::new();
        write_packet(&mut bytes, &packet(&identity, 2), 8192).unwrap();
        bytes[9] = 2;
        let mut wire = wire(bytes);
        assert!(matches!(
            wire.exchange(HEAD, &identity, Vec::new(), 1024),
            Err(RecordSessionError::Protocol)
        ));
        for (length, limit) in [(8193_u32, 8192), (2049, 2048)] {
            let mut bytes = length.to_be_bytes().to_vec();
            bytes.extend_from_slice(b"unread");
            let mut reader = Cursor::new(bytes);
            assert!(matches!(
                read_packet(&mut reader, limit),
                Err(RecordSessionError::TooLarge)
            ));
            assert_eq!(reader.position(), 4);
        }
    }
    #[test]
    fn known_unavailability_preserves_explicit_next_exchange_without_retrying() {
        let identity = context();
        let mut response = packet(&identity, 2);
        response.status = 2;
        response.total = 0;
        response.payload.clear();
        let mut bytes = Vec::new();
        write_packet(&mut bytes, &response, 8192).unwrap();
        write_packet(&mut bytes, &packet(&identity, 3), 8192).unwrap();
        let mut wire = wire(bytes);
        assert!(matches!(
            wire.exchange(HEAD, &identity, Vec::new(), 1024),
            Err(RecordSessionError::Source(SourceError::Unavailable))
        ));
        assert_eq!(wire.counters.exchanges, 1);
        assert!(wire.io.is_some());
        assert!(!wire
            .exchange(HEAD, &identity, Vec::new(), 1024)
            .unwrap()
            .is_empty());
        assert_eq!(wire.counters.exchanges, 2);
    }
}
