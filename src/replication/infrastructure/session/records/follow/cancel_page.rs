//! One lifetime PAGE request and one optional correlated Cancel. The server
//! retains its original ingress and full-frame writer; an isolated worker
//! only prepares/sends that one response. No reusable PAGE or general mux.

use super::*;

pub(super) const PREFACE: &[u8; 8] = b"NSPCAN01";
pub(in super::super) const CANCEL: u8 = 18;

/// Exact peer receipt for an explicitly cancelled one-page response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageCancelReceipt {
    /// No response frame for the page had been flushed.
    StoppedBeforeResponse,
    /// The peer had already flushed a full reply. This says nothing about
    /// receiver apply or rollback.
    TooLate,
}

impl PageCancelReceipt {
    fn code(self) -> u8 {
        match self {
            Self::StoppedBeforeResponse => 1,
            Self::TooLate => 2,
        }
    }
}

pub(in super::super) fn decode_ack(
    packet: &Packet,
    meta: Meta,
) -> Result<PageCancelReceipt, RecordSessionError> {
    if packet.kind != CANCEL | REPLY
        || packet.meta != meta
        || packet.status != 0
        || packet.offset != 0
        || packet.total != 1
    {
        return Err(RecordSessionError::Protocol);
    }
    match packet.payload.as_slice() {
        [1] => Ok(PageCancelReceipt::StoppedBeforeResponse),
        [2] => Ok(PageCancelReceipt::TooLate),
        _ => Err(RecordSessionError::Protocol),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sent {
    None,
    Complete,
    Stopped,
}

struct PageWriter<W> {
    physical: W,
    sent: Sent,
}

impl<W: Write> Write for PageWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.physical.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.physical.flush()
    }
}

struct Owner<W, A, F> {
    writer: Arc<Mutex<PageWriter<W>>>,
    backend: Arc<Mutex<Backend<A, F>>>,
    stop: Arc<AtomicBool>,
    interrupt: Arc<dyn SessionInterrupt>,
}

impl<W, A, F> Owner<W, A, F>
where
    W: Write,
    A: ScopeAuthorizer,
{
    fn authorize(&self, scope: &Scope) -> u8 {
        self.backend
            .lock()
            .map(|mut backend| access_code(backend.policy.authorize(scope), scope))
            .unwrap_or(2)
    }

    fn cancel(&self, meta: Meta, limit: usize) -> Result<(), RecordSessionError> {
        let mut writer = self.writer.lock().map_err(|_| RecordSessionError::Closed)?;
        if self.stop.load(Ordering::Acquire) {
            return Err(RecordSessionError::Closed);
        }
        let receipt = match writer.sent {
            Sent::None => PageCancelReceipt::StoppedBeforeResponse,
            Sent::Complete => PageCancelReceipt::TooLate,
            Sent::Stopped => return Err(RecordSessionError::Protocol),
        };
        // Prevent any further PAGE frame while holding the very same lock
        // the producer uses. A write/flush failure supplies no known receipt.
        writer.sent = Sent::Stopped;
        fence_failed_frame(&self.stop, || -> Result<(), RecordSessionError> {
            write_packet(
                &mut *writer,
                &Packet {
                    kind: CANCEL | REPLY,
                    status: 0,
                    meta,
                    total: 1,
                    offset: 0,
                    payload: vec![receipt.code()],
                },
                limit,
            )?;
            writer.flush()?;
            Ok(())
        })
    }

    fn response<R>(
        &self,
        request: &Packet,
        scope: &Scope,
        limit: usize,
    ) -> Result<(), RecordSessionError>
    where
        F: FnMut(&Scope) -> Result<R, SourceError>,
        R: RecordSource,
    {
        {
            let writer = self.writer.lock().map_err(|_| RecordSessionError::Closed)?;
            if self.stop.load(Ordering::Acquire) || writer.sent == Sent::Stopped {
                return Ok(());
            }
        }
        let access = self.authorize(scope);
        let prepared = if access != 0 {
            Err(access)
        } else {
            let request = decode_page_request(&request.payload, scope).and_then(|request| {
                check_one_frame(&request, limit)?;
                Ok(request)
            });
            prepare_page_response(request, |s| {
                self.backend
                    .lock()
                    .map_err(|_| SourceError::Unavailable)
                    .and_then(|mut backend| (backend.factory)(s))
            })
        };
        let (status, body) = match prepared {
            Ok(body) => (0, body),
            Err(status) => (status, Vec::new()),
        };
        // No source/transaction survives here. One reservation includes
        // policy, the bounded frame flush, and its local receipt. There is no partially delivered PAGE DTO.
        let mut writer = self.writer.lock().map_err(|_| RecordSessionError::Closed)?;
        if self.stop.load(Ordering::Acquire) || writer.sent == Sent::Stopped {
            return Ok(());
        }
        let (frame, complete) =
            response_fragment(request, status, &body, limit, 0, self.authorize(scope))?;
        if !complete {
            return Err(RecordSessionError::Protocol);
        }
        fence_failed_frame(&self.stop, || -> Result<(), RecordSessionError> {
            write_packet(&mut *writer, &frame, limit)?;
            writer.flush()?;
            Ok(())
        })?;
        writer.sent = Sent::Complete;
        Ok(())
    }
}

impl<W, A, F> Owner<W, A, F>
where
    W: Write + Send,
    A: ScopeAuthorizer + Send,
    F: Send,
{
    fn serve<R>(
        &self,
        mut reader: &mut dyn Read,
        correlation: Correlation,
        scope: Scope,
        limit: usize,
    ) -> Result<(), RecordSessionError>
    where
        F: FnMut(&Scope) -> Result<R, SourceError>,
        R: RecordSource,
    {
        let meta = Meta {
            session: correlation.session,
            request: FIRST_REQUEST + 1,
            handle: 1,
            generation: 1,
        };
        let Some(request) = read_packet_wait(&mut reader, limit, true)? else {
            return Ok(());
        };
        if request.kind != PAGE
            || request.meta != meta
            || request.status != 0
            || request.offset != 0
            || request.total != request.payload.len()
        {
            return Err(RecordSessionError::Protocol);
        }
        thread::scope(|threads| {
            let page = threads.spawn(|| {
                let response = self.response(&request, &scope, limit);
                if response.is_err() {
                    self.stop.store(true, Ordering::Release);
                    let _ = self.interrupt.interrupt();
                }
                response
            });
            let control = match read_packet_wait_kind(&mut reader, limit, true, Some(CANCEL)) {
                Ok(Some(cancel))
                    if cancel.kind == CANCEL
                        && cancel.meta == meta
                        && cancel.status == 0
                        && cancel.offset == 0
                        && cancel.total == 0
                        && cancel.payload.is_empty() =>
                {
                    self.cancel(meta, limit)
                }
                Ok(Some(_)) => Err(RecordSessionError::Protocol),
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            // One control (or EOF) ends this lifetime slot. A previously
            // running host callback may still need to return before cleanup.
            self.stop.store(true, Ordering::Release);
            control.and(page.join().map_err(|_| RecordSessionError::Protocol)?)
        })
    }
}

fn check_one_frame(request: &PageRequest, limit: usize) -> Result<(), RecordSessionError> {
    validate_page_request(request, physical(MAX_RECORD_BYTES))
        .map_err(|_| RecordSessionError::Source(SourceError::InvalidRequest))?;
    if page_bound(
        &request.scope,
        request.max_records,
        request.max_payload_bytes,
    ) > limit - HEADER
    {
        return Err(RecordSessionError::Source(SourceError::InvalidRequest));
    }
    Ok(())
}

/// Serve one PAGE and optional exact Cancel on an explicitly configured
/// host-authenticated duplex channel. No advisory hints or legacy fallback.
/// A typed stop receipt fences page egress, not a synchronous source callback
/// or receiver transaction. The host owns callback/socket bounds and teardown.
pub fn serve_page_cancel_session<D, A, F, R>(
    mut io: D,
    session: u64,
    limits: SessionLimits,
    policy: A,
    factory: F,
) -> Result<(), RecordSessionError>
where
    D: DuplexSessionIo,
    A: ScopeAuthorizer + Send + 'static,
    F: FnMut(&Scope) -> Result<R, SourceError> + Send + 'static,
    R: RecordSource,
{
    let interrupt = io.interrupt_handle();
    let result = (|| {
        let mut preface = [0; PREFACE.len()];
        io.read_exact(&mut preface)?;
        if preface != *PREFACE {
            return Err(RecordSessionError::Protocol);
        }
        let (reader, physical) = io.split()?;
        let owner = Owner {
            writer: Arc::new(Mutex::new(PageWriter {
                physical,
                sent: Sent::None,
            })),
            backend: Arc::new(Mutex::new(Backend { policy, factory })),
            stop: Arc::new(AtomicBool::new(false)),
            interrupt: interrupt.clone(),
        };
        let mut stream = ServerIo {
            reader,
            writer: owner.writer.clone(),
            stop: owner.stop.clone(),
            // Binding uses its existing policy check. This mode starts no
            // hint task or second binding send-admission context.
            admit: Box::new(|_| Ok(())),
        };
        let backend = owner.backend.clone();
        let mut hook = |reader: &mut dyn Read, correlation, scope, limit| {
            owner.serve(reader, correlation, scope, limit)
        };
        serve_bound(
            &mut stream,
            session,
            limits,
            &mut Policy(owner.backend.clone()),
            move |scope| {
                backend
                    .lock()
                    .map_err(|_| SourceError::Unavailable)
                    .and_then(|mut backend| (backend.factory)(scope))
            },
            Some(FollowBoundHook::SinglePage(&mut hook)),
            CapabilityRoutes::legacy(),
        )
    })();
    let disposed = interrupt.interrupt().map_err(RecordSessionError::Io);
    result.and(disposed)
}

#[derive(Default)]
pub(super) struct Control {
    pub(super) deadline: Option<Instant>,
    pub(super) receipt: Option<PageCancelReceipt>,
}

/// Completion of an explicitly sent Cancel on the one-page channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageCancelOutcome {
    /// The sole reader decoded the exact peer receipt.
    Acknowledged(PageCancelReceipt),
    /// No valid, timely receipt was obtained. This confirms neither stop nor
    /// continued response, and cannot authorize a receiver apply.
    Uncertain,
}

/// The PAGE result is independent from local commit ownership.
#[derive(Debug, PartialEq, Eq)]
pub enum CancelPageOutput {
    /// A complete, wire-validated page, not a durable receipt.
    Page(Page),
    /// The peer stopped page egress; no partial page escapes the adapter.
    Stopped(PageCancelReceipt),
}

/// One opt-in lifetime page, with a sole writer transferred after the request.
pub struct PageCancelSession<W: Write>(FollowSession<W>);

/// The sole PAGE consumer. It keeps the channel alive after a complete page
/// until the optional control result is observed or the host closes it.
pub struct PendingCancelPage<W: Write> {
    session: FollowSession<W>,
    request: Option<PageRequest>,
    meta: Meta,
}

/// One exact optional Cancel for an already flushed PAGE request. This
/// possesses the original writer, not another connection or writer owner.
pub struct PageCancelHandle<W: Write> {
    writer: W,
    shared: Arc<Shared>,
    meta: Meta,
    timeout: Duration,
}

/// Open the matching host-opt-in one-page channel on the existing Follow
/// reader/binding ownership. Use only if the peer is configured for this
/// endpoint; no fallback, advisory subscription, secure duplex, or retry.
pub fn connect_page_cancel_session<D: DuplexSessionIo>(
    mut io: D,
    scope: Scope,
    limits: SessionLimits,
    timeout: Duration,
) -> Result<PageCancelSession<D::Writer>, FollowError> {
    if timeout.is_zero() || timeout > Duration::from_secs(600) {
        return Err(FollowError::Protocol);
    }
    limits.validate().map_err(|_| FollowError::Protocol)?;
    let interrupt = io.interrupt_handle();
    if io.write_all(PREFACE).and_then(|()| io.flush()).is_err() {
        let _ = interrupt.interrupt();
        return Err(FollowError::Unavailable);
    }
    connect_follow_mode(io, scope, limits, timeout, true).map(PageCancelSession)
}

impl<W: Write> PageCancelSession<W> {
    /// Send the one exact page request, then hand its only writer to the
    /// optional Cancel handle. A failed request send returns no handle.
    pub fn start(
        self,
        request: PageRequest,
    ) -> Result<(PendingCancelPage<W>, PageCancelHandle<W>), FollowError> {
        let (meta, writer, timeout) = {
            let mut c = active_client(&self.0.inner.shared, &self.0.inner.closing)?;
            c.scope(&request.scope).map_err(FollowError::Refused)?;
            validate_page_request(&request, physical(c.wire.record_bytes))
                .map_err(|_| FollowError::Refused(SourceError::InvalidRequest))?;
            check_one_frame(&request, c.wire.limit)?;
            let Client { wire, identity, .. } = &mut *c;
            let meta = wire.send_request(PAGE, identity, encode_page_request(&request))?;
            let facade = wire.io.as_mut().ok_or(FollowError::Closed)?;
            let writer = facade.writer.take().ok_or(FollowError::Protocol)?;
            (meta, writer, facade.deadline)
        };
        let shared = self.0.shared.clone();
        Ok((
            PendingCancelPage {
                session: self.0,
                request: Some(request),
                meta,
            },
            PageCancelHandle {
                writer,
                shared,
                meta,
                timeout,
            },
        ))
    }
}

impl<W: Write> PendingCancelPage<W> {
    /// Consume the sole data result. Waiting a second time is not another
    /// operation. The caller still owns local validation/apply admission.
    pub fn wait(&mut self) -> Result<CancelPageOutput, FollowError> {
        let request = self.request.take().ok_or(FollowError::Protocol)?;
        let mut c = active_client(&self.session.inner.shared, &self.session.inner.closing)?;
        let bound = page_bound(
            &request.scope,
            request.max_records,
            request.max_payload_bytes,
        );
        let result = c.wire.receive_response(PAGE, self.meta, bound, true);
        match result {
            Ok(WireBody::Data(body)) => match decode_page(&body, &request) {
                Ok(page) => {
                    c.received(&page.records);
                    Ok(CancelPageOutput::Page(page))
                }
                Err(error) => {
                    c.wire.close();
                    c.observe_close();
                    Err(error.into())
                }
            },
            Ok(WireBody::Stopped(receipt)) => Ok(CancelPageOutput::Stopped(receipt)),
            Err(error @ RecordSessionError::Source(_)) => Err(error.into()),
            Err(error) => {
                c.wire.close();
                c.observe_close();
                Err(error.into())
            }
        }
    }

    /// Physical application bytes include the eight-byte opt-in preface and
    /// actual control bytes. The cancelled RPC counts once when stopped.
    pub fn counters(&self) -> Result<FollowCounters, FollowError> {
        let mut counters = self.session.counters()?;
        counters.wire.protocol_bytes += PREFACE.len() as u64;
        Ok(counters)
    }

    /// Explicitly end the lifetime channel without claiming a Cancel receipt.
    pub fn close(&self) -> Result<(), FollowError> {
        self.session.close()
    }
}

impl<W: Write> PageCancelHandle<W> {
    /// Send at most one exact Cancel and wait for its independently routed
    /// receipt. Failure or missing/invalid acknowledgement is uncertainty;
    /// no page apply, rollback, callback interruption, or retry is implied.
    pub fn cancel(mut self) -> PageCancelOutcome {
        let deadline = Instant::now() + self.timeout;
        let limit = {
            let Ok(mut state) = self.shared.state.lock() else {
                return PageCancelOutcome::Uncertain;
            };
            if state.closed {
                return PageCancelOutcome::Uncertain;
            }
            let Some(control) = state.cancel.as_mut() else {
                return PageCancelOutcome::Uncertain;
            };
            if control.deadline.is_some() {
                return PageCancelOutcome::Uncertain;
            }
            control.deadline = Some(deadline);
            state.limit
        };
        let mut bytes = Vec::new();
        let mut accepted = 0;
        let result = write_packet(&mut bytes, &cancel_packet(self.meta), limit).and_then(|()| {
            complete_frame(&mut self.writer, &bytes, &mut accepted).map_err(Into::into)
        });
        if let Ok(mut state) = self.shared.state.lock() {
            state.tx += accepted as u64;
        }
        if result.is_err() {
            let _ = self.shared.fail(FollowError::Unavailable);
            // A complete exact peer receipt already captured by the reader
            // cannot be undone by a later local flush error.
            return self
                .shared
                .state
                .lock()
                .ok()
                .and_then(|state| state.cancel.as_ref().and_then(|control| control.receipt))
                .map_or(
                    PageCancelOutcome::Uncertain,
                    PageCancelOutcome::Acknowledged,
                );
        }
        let Ok(mut state) = self.shared.state.lock() else {
            return PageCancelOutcome::Uncertain;
        };
        loop {
            if let Some(receipt) = state.cancel.as_ref().and_then(|control| control.receipt) {
                return PageCancelOutcome::Acknowledged(receipt);
            }
            if state.closed {
                return PageCancelOutcome::Uncertain;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(state);
                let _ = self.shared.fail(FollowError::Unavailable);
                return PageCancelOutcome::Uncertain;
            }
            let Ok((waited, _)) = self.shared.ready.wait_timeout(state, remaining) else {
                return PageCancelOutcome::Uncertain;
            };
            state = waited;
        }
    }
}

fn cancel_packet(meta: Meta) -> Packet {
    Packet {
        kind: CANCEL,
        status: 0,
        meta,
        total: 0,
        offset: 0,
        payload: Vec::new(),
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "cancel_page/tests.rs"]
mod tests;
