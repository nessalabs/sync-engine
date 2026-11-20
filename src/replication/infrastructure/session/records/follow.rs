//! One-scope persistent advisory hints, with one reader and bounded dispatch.
use super::capabilities::CapabilitySessionSource;
use super::*;
use crate::replication::application::{RecordHeadSource, RecordReadView};
use crate::replication::domain::{CapabilitySupport, SourceHead};
use crate::replication::history::{
    CapabilityHistorySource, HistorySource, HistorySourceError, OlderPage, OlderRequest,
    TailRequest, TailSnapshot,
};
use crate::replication::infrastructure::session::DuplexSessionIo;
use std::io::Cursor;
use std::sync::Condvar;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MIN_FRAME_BODY: usize = 7; // NSC1 magic, version and kind.

/// Terminal follow failure. It never supplies authority or resets cached state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FollowError {
    /// Explicit close or peer EOF; reconnect is an explicit host action.
    Closed,
    /// Framing, correlation, vocabulary or scope context was invalid.
    Protocol,
    /// Transport/deadline or host wake source became unavailable.
    Unavailable,
    /// Authenticated metadata-free source/policy refusal.
    Refused(SourceError),
}
impl From<RecordSessionError> for FollowError {
    fn from(e: RecordSessionError) -> Self {
        match e {
            RecordSessionError::Source(e) => Self::Refused(e),
            RecordSessionError::Protocol | RecordSessionError::TooLarge => Self::Protocol,
            _ => Self::Unavailable,
        }
    }
}
/// Advisory wake only. Neither branch acknowledges durable apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HintWake {
    /// At least one hint was received; the head is untrusted scheduling advice.
    Changed {
        /// Coalesced maximum advisory head, never a checkpoint.
        advisory_head: u64,
    },
    /// No hint arrived before the host deadline; perform an authorized fallback.
    Timeout,
}
/// Bounded receiver diagnostics; no record payload or progress is stored here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HintStats {
    /// Complete well-framed hint frames received (including terminal refusal).
    pub frames_received: u64,
    /// Valid hints merged while an advisory wake was already pending.
    pub coalesced: u64,
    /// Current pending advisory head, without consuming it.
    pub pending_head: Option<u64>,
}
/// Actual channel counters, including filtered one-way hint bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FollowCounters {
    /// Actual request/reply plus hint bytes; hints add no logical exchanges.
    pub wire: WireCounters,
    /// Coalescing and pending-wake diagnostics.
    pub hints: HintStats,
}
struct Pending {
    meta: Meta,
    result: Result<u64, SourceError>,
}
enum Expect {
    Handshake {
        session: Option<u64>,
    },
    Data {
        kind: u8,
        meta: Meta,
        total: Option<usize>,
        offset: usize,
    },
}
struct Rpc {
    expected: Expect,
    deadline: Instant,
    complete: bool,
    owned_refusal: bool,
}
impl Rpc {
    fn owned_outcome(&self) -> bool {
        self.setup_outcome() || self.owned_refusal
    }
    fn setup_outcome(&self) -> bool {
        self.complete && matches!(self.expected, Expect::Handshake { .. })
    }
}
struct Dispatch {
    frame: Option<Vec<u8>>,
    rpc: Option<Rpc>,
    closed: bool,
    error: Option<FollowError>,
    identity: Option<ScopeIdentity>,
    closing: Option<Arc<AtomicBool>>,
    stats: HintStats,
    rx: u64,
    tx: u64,
    limit: usize,
}
struct Shared {
    state: Mutex<Dispatch>,
    ready: Condvar,
    physical: Arc<dyn SessionInterrupt>,
}
impl Shared {
    fn fail(&self, error: FollowError) -> io::Result<()> {
        if let Ok(mut s) = self.state.lock() {
            if !s.closed {
                s.closed = true;
                s.error = Some(error);
                s.frame = None;
                s.stats.pending_head = None;
                if let Some(f) = &s.closing {
                    f.store(true, Ordering::Release);
                }
            }
            self.ready.notify_all();
        }
        self.physical.interrupt()
    }
    fn error(&self) -> Option<FollowError> {
        self.state
            .lock()
            .map(|s| {
                if s.closed {
                    Some(s.error.clone().unwrap_or(FollowError::Closed))
                } else {
                    None
                }
            })
            .unwrap_or(Some(FollowError::Closed))
    }
    fn admit(&self, mut bytes: &[u8], timeout: Duration) -> Result<(), FollowError> {
        let mut s = self.state.lock().map_err(|_| FollowError::Closed)?;
        if s.closed {
            return Err(s.error.clone().unwrap_or(FollowError::Closed));
        }
        if s.rpc.as_ref().is_some_and(|rpc| !rpc.complete) || s.frame.is_some() {
            return Err(FollowError::Protocol);
        }
        let expected = if bytes.get(4..8) == Some(b"NSC1") {
            if !matches!(
                read_control(&mut bytes, s.limit).map_err(|_| FollowError::Protocol)?,
                ControlFrame::FollowOpen {
                    request: FIRST_REQUEST,
                    ..
                }
            ) {
                return Err(FollowError::Protocol);
            }
            Expect::Handshake { session: None }
        } else {
            let packet = read_packet(&mut bytes, s.limit)
                .map_err(FollowError::from)?
                .ok_or(FollowError::Protocol)?;
            if !data_operation(packet.kind)
                || packet.status != 0
                || packet.offset != 0
                || packet.total != packet.payload.len()
            {
                return Err(FollowError::Protocol);
            }
            Expect::Data {
                kind: packet.kind | REPLY,
                meta: packet.meta,
                total: None,
                offset: 0,
            }
        };
        if !bytes.is_empty() {
            return Err(FollowError::Protocol);
        }
        s.rpc = Some(Rpc {
            expected,
            deadline: Instant::now() + timeout,
            complete: false,
            owned_refusal: false,
        });
        Ok(())
    }
    fn route(&self, mut bytes: &[u8]) -> Result<bool, FollowError> {
        let mut s = self.state.lock().map_err(|_| FollowError::Closed)?;
        let limit = s.limit;
        let rpc = s.rpc.as_mut().ok_or(FollowError::Protocol)?;
        if rpc.complete {
            return Err(FollowError::Protocol);
        }
        if Instant::now() > rpc.deadline {
            return Err(FollowError::Unavailable);
        }
        let mut handoff = false;
        let mut selected = None;
        match &mut rpc.expected {
            Expect::Handshake { session } => {
                match read_control(&mut bytes, limit).map_err(|_| FollowError::Protocol)? {
                    ControlFrame::Welcome {
                        session: chosen,
                        limits,
                        ..
                    } if session.is_none() => {
                        *session = Some(chosen);
                        selected = Some(limits.control_bytes as usize);
                    }
                    ControlFrame::Bound { correlation, .. }
                        if *session == Some(correlation.session)
                            && correlation.request == FIRST_REQUEST =>
                    {
                        rpc.complete = true;
                        handoff = true;
                    }
                    ControlFrame::Refused { correlation, .. }
                        if *session == Some(correlation.session)
                            && correlation.request == FIRST_REQUEST =>
                    {
                        rpc.complete = true;
                        handoff = true;
                    }
                    _ => return Err(FollowError::Protocol),
                }
            }
            Expect::Data {
                kind,
                meta,
                total,
                offset,
            } => {
                let p = read_packet(&mut bytes, limit)
                    .map_err(FollowError::from)?
                    .ok_or(FollowError::Protocol)?;
                if p.kind != *kind || p.meta != *meta {
                    return Err(FollowError::Protocol);
                }
                if p.status != 0 {
                    if p.status == 8 && capability_operation(p.kind & !REPLY) {
                        if p.offset != 0 || p.total != p.payload.len() {
                            return Err(FollowError::Protocol);
                        }
                        capabilities::decode_incompatible(&p.payload).map_err(FollowError::from)?;
                        rpc.owned_refusal = true;
                    } else if !(1..=7).contains(&p.status)
                        || p.total != 0
                        || p.offset != 0
                        || !p.payload.is_empty()
                    {
                        return Err(FollowError::Protocol);
                    }
                    rpc.complete = true;
                } else {
                    if p.total == 0
                        || p.offset != *offset
                        || total.is_some_and(|n| n != p.total)
                        || p.payload.is_empty()
                        || p.payload.len() > p.total.saturating_sub(p.offset)
                    {
                        return Err(FollowError::Protocol);
                    }
                    *total = Some(p.total);
                    *offset += p.payload.len();
                    rpc.complete = *offset == p.total;
                    if !rpc.complete && p.payload.len() != limit - HEADER {
                        return Err(FollowError::Protocol);
                    }
                }
            }
        }
        if let Some(selected) = selected {
            s.limit = s.limit.min(selected);
        }
        Ok(handoff)
    }
    fn activate(
        &self,
        identity: ScopeIdentity,
        closing: Arc<AtomicBool>,
    ) -> Result<(), FollowError> {
        let mut s = self.state.lock().map_err(|_| FollowError::Closed)?;
        if s.closed {
            return Err(s.error.clone().unwrap_or(FollowError::Closed));
        }
        if s.identity.is_some() || !s.rpc.as_ref().is_some_and(Rpc::setup_outcome) {
            return Err(FollowError::Protocol);
        }
        // Only the connecting owner calls this after complete lifecycle
        // validation. Publication releases the reader's setup-result handoff.
        s.identity = Some(identity);
        s.closing = Some(closing);
        self.ready.notify_all();
        Ok(())
    }
    fn hint(&self, p: Pending) -> Result<(), FollowError> {
        let mut s = self.state.lock().map_err(|_| FollowError::Closed)?;
        if s.closed {
            return Err(s.error.clone().unwrap_or(FollowError::Closed));
        }
        let identity = s.identity.as_ref().ok_or(FollowError::Protocol)?;
        if p.meta != Meta::for_scope(identity, FIRST_REQUEST) {
            return Err(FollowError::Protocol);
        }
        s.stats.frames_received += 1;
        match p.result {
            Ok(head) => {
                if let Some(old) = s.stats.pending_head {
                    s.stats.coalesced += 1;
                    s.stats.pending_head = Some(old.max(head));
                } else {
                    s.stats.pending_head = Some(head);
                }
            }
            Err(error) => return Err(FollowError::Refused(error)),
        }
        self.ready.notify_all();
        Ok(())
    }
}
struct DispatchInterrupt(Arc<Shared>);
impl SessionInterrupt for DispatchInterrupt {
    fn interrupt(&self) -> io::Result<()> {
        if let Ok(mut s) = self.0.state.lock() {
            if !s.closed {
                s.closed = true;
                s.error = Some(FollowError::Closed);
                s.frame = None;
                s.stats.pending_head = None;
                if let Some(f) = &s.closing {
                    f.store(true, Ordering::Release);
                }
            }
            self.0.ready.notify_all();
        }
        self.0.physical.interrupt()
    }
}
/// Read/write facade used only by this follow owner. One background reader feeds
/// one bounded reply-frame slot; records still pass through the original ports.
pub struct FollowIo<W: Write> {
    writer: W,
    shared: Arc<Shared>,
    current: Cursor<Vec<u8>>,
    deadline: Duration,
    deferred_write_error: Option<io::Error>,
}
impl<W: Write> Read for FollowIo<W> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.current.position() as usize == self.current.get_ref().len() {
            let mut s = self
                .shared
                .state
                .lock()
                .map_err(|_| io::ErrorKind::NotConnected)?;
            let until = s.rpc.as_ref().ok_or(io::ErrorKind::InvalidData)?.deadline;
            while s.frame.is_none() && !s.closed {
                if s.rpc.as_ref().is_some_and(Rpc::owned_outcome) {
                    // The complete correlated outcome arrived on time. The
                    // consumer now owns even its reader-local frame;
                    // this wait is local publication, not network arrival.
                    s = self
                        .shared
                        .ready
                        .wait(s)
                        .map_err(|_| io::ErrorKind::NotConnected)?;
                    continue;
                }
                let left = until.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    drop(s);
                    let _ = self.shared.fail(FollowError::Unavailable);
                    return Err(io::ErrorKind::TimedOut.into());
                }
                s = self
                    .shared
                    .ready
                    .wait_timeout(s, left)
                    .map_err(|_| io::ErrorKind::NotConnected)?
                    .0;
            }
            if s.closed {
                return Err(io::ErrorKind::NotConnected.into());
            }
            self.current = Cursor::new(s.frame.take().ok_or(io::ErrorKind::NotConnected)?);
            self.shared.ready.notify_all();
        }
        self.current.read(out)
    }
}
impl<W: Write> Write for FollowIo<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if let Some(error) = self.deferred_write_error.take() {
            return Err(error);
        }
        if self.shared.error().is_some() {
            return Err(io::ErrorKind::NotConnected.into());
        }
        if self.current.position() as usize != self.current.get_ref().len() {
            let _ = self.shared.fail(FollowError::Protocol);
            return Err(io::ErrorKind::InvalidData.into());
        }
        // Both Wire encoders supply a single complete buffer to write_all.
        // Its first Write::write is the whole frame; short physical writes
        // are completed here, rather than handing the frame back to Wire.
        if let Err(error) = self.shared.admit(bytes, self.deadline) {
            let _ = self.shared.fail(error);
            return Err(io::ErrorKind::InvalidData.into());
        }

        let mut accepted = 0;
        let result = (|| {
            while accepted < bytes.len() {
                match self.writer.write(&bytes[accepted..]) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(written) => accepted += written,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                }
            }
            self.writer.flush()
        })();
        if let Ok(mut state) = self.shared.state.lock() {
            state.tx += accepted as u64;
        }
        if let Err(error) = result {
            // Do not publish a reply or admit another frame after an
            // ambiguous write/flush. Write::write must first report any real
            // accepted prefix; Wire gets the original error on retry/flush.
            let _ = self.shared.fail(FollowError::Unavailable);
            if accepted == 0 {
                return Err(error);
            }
            self.deferred_write_error = Some(error);
        }
        Ok(accepted)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(error) = self.deferred_write_error.take() {
            return Err(error);
        }
        if self.shared.error().is_some() {
            return Err(io::ErrorKind::NotConnected.into());
        }
        // A complete write transaction already flushed the physical writer.
        Ok(())
    }
}
impl<W: Write> InterruptibleSessionIo for FollowIo<W> {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        Arc::new(DispatchInterrupt(self.shared.clone()))
    }
}
impl<W: Write> Drop for FollowIo<W> {
    fn drop(&mut self) {
        let _ = DispatchInterrupt(self.shared.clone()).interrupt();
    }
}
struct CountRead<R> {
    inner: R,
    shared: Arc<Shared>,
}
impl<R: Read> Read for CountRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        if let Ok(mut s) = self.shared.state.lock() {
            s.rx += n as u64;
        }
        Ok(n)
    }
}
fn reader<R: Read>(inner: R, shared: Arc<Shared>) {
    reader_frames(inner, shared, |_| {});
}
fn reader_frames<R: Read>(inner: R, shared: Arc<Shared>, mut before_publish: impl FnMut(bool)) {
    let mut r = CountRead {
        inner,
        shared: shared.clone(),
    };
    let result = (|| -> Result<(), FollowError> {
        loop {
            if let Some(e) = shared.error() {
                return Err(e);
            }
            let mut prefix = [0; 4];
            loop {
                match r.read(&mut prefix[..1]) {
                    Ok(0) => return Err(FollowError::Closed),
                    Ok(_) => break,
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::Interrupted
                                | io::ErrorKind::WouldBlock
                                | io::ErrorKind::TimedOut
                        ) =>
                    {
                        if let Some(e) = shared.error() {
                            return Err(e);
                        }
                        if e.kind() == io::ErrorKind::WouldBlock {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    Err(_) => return Err(FollowError::Unavailable),
                }
            }
            r.read_exact(&mut prefix[1..])
                .map_err(|_| FollowError::Unavailable)?;
            let len = u32::from_be_bytes(prefix) as usize;
            let limit = shared.state.lock().map_err(|_| FollowError::Closed)?.limit;
            if len < MIN_FRAME_BODY || len > limit {
                return Err(FollowError::Protocol);
            }
            let mut frame = Vec::with_capacity(len + 4);
            frame.extend_from_slice(&prefix);
            frame.resize(len + 4, 0);
            r.read_exact(&mut frame[4..])
                .map_err(|_| FollowError::Unavailable)?;
            if &frame[4..8] == b"NSR1" && frame[10] == HINT {
                let packet = read_packet(&mut frame.as_slice(), limit)
                    .map_err(FollowError::from)?
                    .ok_or(FollowError::Protocol)?;
                let result = if packet.status == 0 {
                    if packet.total != 8 || packet.offset != 0 || packet.payload.len() != 8 {
                        return Err(FollowError::Protocol);
                    }
                    Ok(u64::from_be_bytes(
                        packet
                            .payload
                            .try_into()
                            .map_err(|_| FollowError::Protocol)?,
                    ))
                } else {
                    if !(1..=6).contains(&packet.status)
                        || packet.total != 0
                        || packet.offset != 0
                        || !packet.payload.is_empty()
                    {
                        return Err(FollowError::Protocol);
                    }
                    Err(decode_source_error(packet.status))
                };
                shared.hint(Pending {
                    meta: packet.meta,
                    result,
                })?;
                continue;
            }
            let handoff = shared.route(&frame)?;
            before_publish(handoff);
            let mut s = shared.state.lock().map_err(|_| FollowError::Closed)?;
            while s.frame.is_some() && !s.closed {
                s = shared.ready.wait(s).map_err(|_| FollowError::Closed)?;
            }
            if s.closed {
                return Err(s.error.clone().unwrap_or(FollowError::Closed));
            }
            s.frame = Some(frame);
            shared.ready.notify_all();
            // A complete correlated setup result belongs to the connecting
            // owner until validation. A canonical capability refusal belongs to
            // the RPC consumer until its complete bounded frame is transferred
            // into FollowIo's local Cursor. EOF cannot erase either owned result;
            // after transfer the reader resumes normal terminal-hint handling.
            while !s.closed
                && ((handoff && s.identity.is_none())
                    || (s.rpc.as_ref().is_some_and(|rpc| rpc.owned_refusal) && s.frame.is_some()))
            {
                s = shared.ready.wait(s).map_err(|_| FollowError::Closed)?;
            }
        }
    })();
    if let Err(e) = result {
        let _ = shared.fail(e);
    }
}
/// Independent advisory receiver. Waiting/consuming never loads or applies data.
#[derive(Clone)]
pub struct HintReceiver {
    shared: Arc<Shared>,
}
impl HintReceiver {
    /// Wait for one coalesced hint or a host-selected fallback deadline. No retry
    /// or catch-up starts here; both outcomes require the normal authorized path.
    pub fn wait(&self, timeout: Duration) -> Result<HintWake, FollowError> {
        let until = Instant::now()
            .checked_add(timeout)
            .ok_or(FollowError::Protocol)?;
        let mut s = self.shared.state.lock().map_err(|_| FollowError::Closed)?;
        loop {
            if s.closed {
                return Err(s.error.clone().unwrap_or(FollowError::Closed));
            }
            if let Some(head) = s.stats.pending_head.take() {
                return Ok(HintWake::Changed {
                    advisory_head: head,
                });
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(HintWake::Timeout);
            }
            s = self
                .shared
                .ready
                .wait_timeout(s, left)
                .map_err(|_| FollowError::Closed)?
                .0;
        }
    }
    /// Non-consuming bounded diagnostics; advisory state is not authority.
    pub fn stats(&self) -> Result<HintStats, FollowError> {
        self.shared
            .state
            .lock()
            .map(|s| s.stats)
            .map_err(|_| FollowError::Closed)
    }
}
fn access_terminal(e: FollowError) -> Access {
    match e {
        FollowError::Refused(SourceError::Denied) => Access::Denied,
        FollowError::Refused(SourceError::IdentityChanged) => Access::IdentityChanged,
        FollowError::Protocol
        | FollowError::Refused(
            SourceError::InvalidRequest | SourceError::OversizedRecord | SourceError::Pruned,
        ) => Access::InvalidResponse,
        _ => Access::Unverifiable,
    }
}
fn source_terminal(e: FollowError) -> SourceError {
    match e {
        FollowError::Refused(e) => e,
        FollowError::Protocol => SourceError::InvalidRequest,
        _ => SourceError::Unavailable,
    }
}
/// Current-policy port preserving authenticated terminal hint refusal classes.
pub struct FollowAuthorizer<W: Write> {
    inner: RecordSessionAuthorizer<FollowIo<W>>,
    shared: Arc<Shared>,
}
impl<W: Write> ScopeAuthorizer for FollowAuthorizer<W> {
    fn authorize(&mut self, s: &Scope) -> Access {
        if let Some(e) = self.shared.error() {
            return access_terminal(e);
        }
        let result = self.inner.authorize(s);
        match self.shared.error() {
            Some(FollowError::Closed) | None => result,
            Some(e) => access_terminal(e),
        }
    }
}
/// Record ports retain canonical validation and preserve terminal refusal classes.
pub struct FollowSource<W: Write> {
    inner: RecordSessionSource<FollowIo<W>>,
    shared: Arc<Shared>,
}
impl<W: Write> RecordSource for FollowSource<W> {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        if let Some(e) = self.shared.error() {
            return Err(source_terminal(e));
        }
        let r = self.inner.head(s);
        if let Some(e) = self.shared.error() {
            if e != FollowError::Closed {
                return Err(source_terminal(e));
            }
        }
        r
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        if let Some(e) = self.shared.error() {
            return Err(source_terminal(e));
        }
        let result = self.inner.page(r);
        if let Some(e) = self.shared.error() {
            if e != FollowError::Closed {
                return Err(source_terminal(e));
            }
        }
        result
    }
}
impl<W: Write> OpenRecordSource for FollowSource<W> {
    fn open_page(&mut self, r: &OpenPageRequest) -> Result<OpenedPage, SourceError> {
        if let Some(e) = self.shared.error() {
            return Err(source_terminal(e));
        }
        let result = self.inner.open_page(r);
        if let Some(e) = self.shared.error() {
            if e != FollowError::Closed {
                return Err(source_terminal(e));
            }
        }
        result
    }
}
/// Capability-aware source ports over the existing follow reader/RPC owner.
/// Peer endpoint support is explicit. Metadata is read from the source, never
/// synthesized from a hint or legacy head. Terminal hint refusals retain precedence.
pub struct CapabilityFollowSource<W: Write> {
    inner: CapabilitySessionSource<FollowIo<W>>,
    shared: Arc<Shared>,
}
impl<W: Write> CapabilityFollowSource<W> {
    fn read<T>(
        &mut self,
        operation: impl FnOnce(&mut CapabilitySessionSource<FollowIo<W>>) -> Result<T, SourceError>,
    ) -> Result<T, SourceError> {
        if let Some(e) = self.shared.error() {
            return Err(source_terminal(e));
        }
        let result = operation(&mut self.inner);
        match self.shared.error() {
            Some(e) if e != FollowError::Closed => Err(source_terminal(e)),
            _ => result,
        }
    }
}
impl<W: Write> RecordHeadSource for CapabilityFollowSource<W> {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        self.read(|source| source.source_head(scope))
    }
}
impl<W: Write> RecordSource for CapabilityFollowSource<W> {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.source_head(scope).map(|head| head.position())
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.read(|source| source.page(request))
    }
}
impl<W: Write> OpenRecordSource for CapabilityFollowSource<W> {
    fn open_page(&mut self, request: &OpenPageRequest) -> Result<OpenedPage, SourceError> {
        self.read(|source| source.open_page(request))
    }
}

/// Bounded history reads over the same persistent connection and dispatcher.
pub struct CapabilityFollowHistorySource<W: Write> {
    inner: history::HistorySessionSource<FollowIo<W>>,
    support: CapabilitySupport,
    shared: Arc<Shared>,
}
impl<W: Write> CapabilityFollowHistorySource<W> {
    fn read<T>(
        &mut self,
        operation: impl FnOnce(
            &mut history::HistorySessionSource<FollowIo<W>>,
            CapabilitySupport,
        ) -> Result<T, HistorySourceError>,
    ) -> Result<T, HistorySourceError> {
        if let Some(e) = self.shared.error() {
            return Err(history::history_source(source_terminal(e)));
        }
        let result = operation(&mut self.inner, self.support);
        match self.shared.error() {
            Some(e) if e != FollowError::Closed => Err(history::history_source(source_terminal(e))),
            _ => result,
        }
    }
}
impl<W: Write> HistorySource for CapabilityFollowHistorySource<W> {
    fn tail(&mut self, r: &TailRequest) -> Result<TailSnapshot, HistorySourceError> {
        self.read(|inner, support| inner.tail(r, support))
            .map(|(_, tail)| tail)
    }

    fn older(&mut self, r: &OlderRequest) -> Result<OlderPage, HistorySourceError> {
        self.read(|inner, support| inner.older(r, support))
            .map(|(_, older)| older)
    }
}

/// One opt-in follow connection: one bound scope, one RPC and one pending wake.
pub struct FollowSession<W: Write> {
    inner: RecordSession<FollowIo<W>>,
    shared: Arc<Shared>,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl<W: Write> FollowSession<W> {
    /// Obtain fresh-authority/source ports; hints cannot bypass these ports.
    pub fn parts(&self) -> (FollowAuthorizer<W>, FollowSource<W>) {
        let (a, s) = self.inner.parts();
        (
            FollowAuthorizer {
                inner: a,
                shared: self.shared.clone(),
            },
            FollowSource {
                inner: s,
                shared: self.shared.clone(),
            },
        )
    }
    /// Select explicit capability-aware reads on this same connection. The host
    /// must route it to serve_capability_follow_record_session; there is no retry
    /// as legacy data after a refusal. Returned ports share the sole RPC owner.
    pub fn capability_parts(
        &self,
        supported: CapabilitySupport,
    ) -> (FollowAuthorizer<W>, CapabilityFollowSource<W>) {
        let (authorizer, source) = self.parts();
        (
            authorizer,
            CapabilityFollowSource {
                inner: CapabilitySessionSource::from_source(source.inner, supported),
                shared: self.shared.clone(),
            },
        )
    }
    /// Select explicit capability-aware history over the existing binding.
    /// The host must know the peer has opted in to the history endpoint. A
    /// refusal never retries as legacy history or replaces this connection.
    pub fn history_parts(
        &self,
        supported: CapabilitySupport,
    ) -> (FollowAuthorizer<W>, CapabilityFollowHistorySource<W>) {
        let (authorizer, source) = self.parts();
        (
            authorizer,
            CapabilityFollowHistorySource {
                inner: history::HistorySessionSource {
                    inner: source.inner,
                },
                support: supported,
                shared: self.shared.clone(),
            },
        )
    }
    /// Get an independent receiver; cloned receivers share one consuming slot.
    pub fn hints(&self) -> HintReceiver {
        HintReceiver {
            shared: self.shared.clone(),
        }
    }
    /// Actual bytes include one-way hint frames, with no fabricated exchange.
    pub fn counters(&self) -> Result<FollowCounters, FollowError> {
        let mut wire = self.inner.counters().map_err(FollowError::from)?;
        let s = self.shared.state.lock().map_err(|_| FollowError::Closed)?;
        wire.protocol_bytes = (s.rx + s.tx).saturating_sub(wire.payload_bytes);
        Ok(FollowCounters {
            wire,
            hints: s.stats,
        })
    }
    /// Terminal close wakes both waits and interrupts before joining the reader.
    /// Already returned DTOs/atomic commits cannot be recalled by this operation.
    pub fn close(&self) -> Result<(), FollowError> {
        self.inner.close().map_err(FollowError::from)?;
        if let Some(task) = self.task.lock().map_err(|_| FollowError::Closed)?.take() {
            task.join().map_err(|_| FollowError::Protocol)?;
        }
        Ok(())
    }
}
impl<W: Write> Drop for FollowSession<W> {
    fn drop(&mut self) {
        let _ = DispatchInterrupt(self.shared.clone()).interrupt();
    }
}
/// Establish an opt-in subscription before catch-up on a host-authenticated
/// duplex channel. No compatibility retry, automatic reconnect or policy cache.
/// The reply deadline is positive and at most 600s; idle first-byte timeouts do not
/// imply a lost subscription, but truncated-frame/deadline failures are terminal.
pub fn connect_follow<D: DuplexSessionIo>(
    io: D,
    scope: Scope,
    offered: SessionLimits,
    reply_deadline: Duration,
) -> Result<FollowSession<D::Writer>, FollowError> {
    if reply_deadline.is_zero() || reply_deadline > Duration::from_secs(600) {
        return Err(FollowError::Protocol);
    }
    offered.validate().map_err(|_| FollowError::Protocol)?;
    let physical = io.interrupt_handle();
    let (r, w) = io.split().map_err(|_| FollowError::Unavailable)?;
    let shared = Arc::new(Shared {
        state: Mutex::new(Dispatch {
            frame: None,
            rpc: None,
            closed: false,
            error: None,
            identity: None,
            closing: None,
            stats: HintStats::default(),
            rx: 0,
            tx: 0,
            limit: offered.control_bytes as usize,
        }),
        ready: Condvar::new(),
        physical,
    });
    let held = shared.clone();
    let task = thread::spawn(move || reader(r, held));
    let facade = FollowIo {
        writer: w,
        shared: shared.clone(),
        current: Cursor::new(Vec::new()),
        deadline: reply_deadline,
        deferred_write_error: None,
    };
    let interrupt = facade.interrupt_handle();
    let inner =
        match RecordSession::connect_with(facade, scope, offered, Opening::Follow, Some(interrupt))
        {
            Ok(inner) => inner,
            Err(error) => {
                let failure = match shared.error() {
                    Some(FollowError::Closed) | None => error.into(),
                    Some(e) => e,
                };
                if shared.fail(failure.clone()).is_ok() {
                    let _ = task.join();
                }
                return Err(failure);
            }
        };
    let identity = inner
        .shared
        .lock()
        .map_err(|_| FollowError::Closed)?
        .identity
        .clone();
    if let Err(error) = shared.activate(identity, inner.closing.clone()) {
        if shared.fail(error.clone()).is_ok() {
            let _ = task.join();
        }
        return Err(error);
    }
    Ok(FollowSession {
        inner,
        shared,
        task: Mutex::new(Some(task)),
    })
}

/// Host-owned lossy commit/policy wake signal. It stores one monotonic version,
/// no records or claimed head. Call notify after commit and on policy invalidation.
#[derive(Clone)]
pub struct HintSignal {
    inner: Arc<(Mutex<(u64, bool)>, Condvar)>,
}
impl Default for HintSignal {
    fn default() -> Self {
        Self::new()
    }
}
impl HintSignal {
    /// Create one bounded signal; sharing it across scopes only adds extra checks.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new((0, false)), Condvar::new())),
        }
    }
    /// Announce a committed change/policy invalidation. Versions never wrap.
    pub fn notify(&self) -> Result<(), FollowError> {
        let mut s = self.inner.0.lock().map_err(|_| FollowError::Unavailable)?;
        if s.1 {
            return Err(FollowError::Closed);
        }
        if let Some(n) = s.0.checked_add(1) {
            s.0 = n;
        } else {
            s.1 = true;
        }
        self.inner.1.notify_all();
        if s.1 {
            Err(FollowError::Unavailable)
        } else {
            Ok(())
        }
    }
    fn version(&self) -> Result<u64, RecordSessionError> {
        self.inner
            .0
            .lock()
            .map(|s| s.0)
            .map_err(|_| RecordSessionError::Closed)
    }
    fn wake(&self) {
        let _held = self.inner.0.lock();
        self.inner.1.notify_all();
    }
    fn wait(&self, last: u64, stop: &AtomicBool) -> Result<Option<u64>, RecordSessionError> {
        let mut s = self
            .inner
            .0
            .lock()
            .map_err(|_| RecordSessionError::Closed)?;
        while s.0 == last && !s.1 && !stop.load(Ordering::Acquire) {
            s = self
                .inner
                .1
                .wait(s)
                .map_err(|_| RecordSessionError::Closed)?;
        }
        if stop.load(Ordering::Acquire) {
            Ok(None)
        } else if s.1 {
            Err(RecordSessionError::Closed)
        } else {
            Ok(Some(s.0))
        }
    }
}
struct Backend<A, F> {
    policy: A,
    factory: F,
}
struct Policy<A, F>(Arc<Mutex<Backend<A, F>>>);
impl<A: ScopeAuthorizer, F> ScopeAuthorizer for Policy<A, F> {
    fn authorize(&mut self, s: &Scope) -> Access {
        self.0
            .lock()
            .map(|mut b| b.policy.authorize(s))
            .unwrap_or(Access::Unverifiable)
    }
}
type BoundContext = Arc<Mutex<Option<(Scope, Meta, usize)>>>;
fn send_admission<W: Write, A: ScopeAuthorizer + Send + 'static, F: Send + 'static>(
    gate_backend: Arc<Mutex<Backend<A, F>>>,
    binding: BoundContext,
    gate_stop: Arc<AtomicBool>,
) -> SendAdmission<W> {
    Box::new(move |w| {
        let current = binding
            .lock()
            .map_err(|_| io::ErrorKind::NotConnected)?
            .clone();
        if let Some((scope, meta, limit)) = current {
            let code = gate_backend
                .lock()
                .map(|mut b| access_code(b.policy.authorize(&scope), &scope))
                .unwrap_or(2);
            if code != 0 {
                // We own the complete-frame writer now: refuse before a
                // staged RPC can expose bytes after waiting for this lock.
                gate_stop.store(true, Ordering::Release);
                write_packet(
                    w,
                    &Packet {
                        kind: HINT,
                        status: code,
                        meta,
                        total: 0,
                        offset: 0,
                        payload: Vec::new(),
                    },
                    limit,
                )
                .map_err(|error| match error {
                    RecordSessionError::Io(error) => error,
                    _ => io::ErrorKind::InvalidData.into(),
                })?;
                w.flush()?;
                return Err(io::ErrorKind::NotConnected.into());
            }
        }
        Ok(())
    })
}

type SendAdmission<W> = Box<dyn FnMut(&mut W) -> io::Result<()> + Send>;
struct ServerIo<R, W> {
    reader: R,
    writer: Arc<Mutex<W>>,
    stop: Arc<AtomicBool>,
    admit: SendAdmission<W>,
}
impl<R: Read, W> Read for ServerIo<R, W> {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        if self.stop.load(Ordering::Acquire) {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.reader.read(b)
    }
}
// Called under the existing complete-frame writer lock. A failed data or
// hint send must fence the other producer before relinquishing that lock.
fn fence_failed_frame<T, E>(
    stop: &AtomicBool,
    send: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let result = send();
    if result.is_err() {
        stop.store(true, Ordering::Release);
    }
    result
}

impl<R, W: Write> Write for ServerIo<R, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        // This facade is only fed by the complete-frame codecs, not a byte
        // stream. Reject a partial-write API call before emitting anything.
        Err(io::ErrorKind::InvalidInput.into())
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let prefix: [u8; 4] = bytes
            .get(..4)
            .and_then(|prefix| prefix.try_into().ok())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if !(MIN_FRAME_BODY..=MAX_CONTROL_BYTES).contains(&length) || bytes.len() != 4 + length {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::ErrorKind::NotConnected)?;
        if self.stop.load(Ordering::Acquire) {
            return Err(io::ErrorKind::NotConnected.into());
        }
        fence_failed_frame(&self.stop, || {
            (self.admit)(&mut *w)?;
            w.write_all(bytes)?;
            w.flush()
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        // write_all already flushed its frame. An independent later hint
        // failure cannot change that completed write's receipt. The next
        // read or write still checks the shared terminal fence.
        Ok(())
    }
}
fn emit<A, F, R, W>(
    backend: &Arc<Mutex<Backend<A, F>>>,
    writer: &Arc<Mutex<W>>,
    stop: &AtomicBool,
    scope: &Scope,
    meta: Meta,
    limit: usize,
    last: Option<u64>,
) -> Result<Option<u64>, RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
    W: Write,
{
    let candidate = {
        let mut b = backend.lock().map_err(|_| RecordSessionError::Closed)?;
        let code = access_code(b.policy.authorize(scope), scope);
        if code != 0 {
            Err(code)
        } else {
            (b.factory)(scope).map_err(source_error_code)
        }
    };
    let head = candidate.and_then(|mut source| source.head(scope).map_err(source_error_code));
    let mut w = writer.lock().map_err(|_| RecordSessionError::Closed)?;
    if stop.load(Ordering::Acquire) {
        return Ok(last);
    }
    let current = backend
        .lock()
        .map(|mut b| access_code(b.policy.authorize(scope), scope))
        .unwrap_or(2);
    let result = if current != 0 { Err(current) } else { head };
    let (status, payload) = match result {
        Ok(head) => {
            if last.is_some_and(|old| head <= old) {
                return Ok(last);
            }
            (0, head.to_be_bytes().to_vec())
        }
        Err(code) => (code, Vec::new()),
    };
    fence_failed_frame(stop, || -> Result<(), RecordSessionError> {
        write_packet(
            &mut *w,
            &Packet {
                kind: HINT,
                status,
                meta,
                total: payload.len(),
                offset: 0,
                payload: payload.clone(),
            },
            limit,
        )?;
        w.flush()?;
        Ok(())
    })?;
    if status != 0 {
        stop.store(true, Ordering::Release);
        return Err(RecordSessionError::Source(decode_source_error(status)));
    }
    Ok(Some(u64::from_be_bytes(
        payload
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    )))
}
/// Serve one authenticated channel with persistent advisory hints for FollowOpen.
/// Legacy openings remain non-subscribed. Host policy/factories must be finite,
/// current, principal-bound and release their own resources before returning.
/// Call signal.notify only after commit or policy invalidation; missed wakes are
/// recovered by host-authorized fallback. This adds no listener or retry loop.
pub fn serve_follow_record_session<D, A, F, R>(
    io: D,
    session: u64,
    limits: SessionLimits,
    policy: A,
    factory: F,
    signal: HintSignal,
) -> Result<(), RecordSessionError>
where
    D: DuplexSessionIo,
    A: ScopeAuthorizer + Send + 'static,
    F: FnMut(&Scope) -> Result<R, SourceError> + Send + 'static,
    R: RecordSource,
{
    serve_follow(
        io,
        session,
        limits,
        policy,
        factory,
        signal,
        CapabilityRoutes::legacy(),
    )
}

/// Serve explicit capability reads and advisory hints on one bound connection.
/// The factory owns one coherent RecordReadView per operation. Legacy data reads
/// are refused; legacy serve_follow_record_session remains unchanged. No second
/// reader, RPC, timer or checkpoint owner is created. Host supplies current policy.
pub fn serve_capability_follow_record_session<D, A, F, R>(
    io: D,
    session: u64,
    limits: SessionLimits,
    policy: A,
    factory: F,
    signal: HintSignal,
) -> Result<(), RecordSessionError>
where
    D: DuplexSessionIo,
    A: ScopeAuthorizer + Send + 'static,
    F: FnMut(&Scope) -> Result<R, SourceError> + Send + 'static,
    R: RecordReadView,
{
    serve_follow(
        io,
        session,
        limits,
        policy,
        factory,
        signal,
        CapabilityRoutes {
            head: Some(R::view_head),
            history: None,
            scope_pair: false,
            pair_batch: false,
        },
    )
}

/// Serve explicit capability history alongside capability record reads over one
/// existing follow binding. The source must supply coherent capability/history
/// admission in its native read snapshot. Current channel policy remains host-owned.
pub fn serve_history_follow_record_session<D, A, F, R>(
    io: D,
    session: u64,
    limits: SessionLimits,
    policy: A,
    factory: F,
    signal: HintSignal,
) -> Result<(), RecordSessionError>
where
    D: DuplexSessionIo,
    A: ScopeAuthorizer + Send + 'static,
    F: FnMut(&Scope) -> Result<R, SourceError> + Send + 'static,
    R: RecordReadView + CapabilityHistorySource,
{
    serve_follow(
        io,
        session,
        limits,
        policy,
        factory,
        signal,
        CapabilityRoutes {
            head: Some(R::view_head),
            history: Some(history::ServerReads::new()),
            scope_pair: false,
            pair_batch: false,
        },
    )
}

fn serve_follow<D, A, F, R>(
    io: D,
    session: u64,
    limits: SessionLimits,
    policy: A,
    factory: F,
    signal: HintSignal,
    routes: CapabilityRoutes<R>,
) -> Result<(), RecordSessionError>
where
    D: DuplexSessionIo,
    A: ScopeAuthorizer + Send + 'static,
    F: FnMut(&Scope) -> Result<R, SourceError> + Send + 'static,
    R: RecordSource,
{
    let version = signal.version()?;
    let physical = io.interrupt_handle();
    let (reader, writer) = io.split()?;
    let writer = Arc::new(Mutex::new(writer));
    let stop = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(Mutex::new(Backend { policy, factory }));
    let binding = Arc::new(Mutex::new(None));
    let bound = binding.clone();
    let mut task = None;
    let mut hook = |correlation: Correlation, scope: Scope, limit: usize| {
        let meta = Meta {
            session: correlation.session,
            request: correlation.request,
            handle: 1,
            generation: 1,
        };
        *bound.lock().map_err(|_| RecordSessionError::Closed)? = Some((scope.clone(), meta, limit));
        let b = backend.clone();
        let w = writer.clone();
        let stopped = stop.clone();
        let wake = signal.clone();
        let abort = physical.clone();
        task = Some(thread::spawn(move || {
            let mut cursor = version;
            let mut last = None;
            let result = (|| {
                while let Some(next) = wake.wait(cursor, &stopped)? {
                    cursor = next;
                    last = emit(
                        &b,
                        &w,
                        &stopped,
                        &scope,
                        Meta {
                            session: correlation.session,
                            request: correlation.request,
                            handle: 1,
                            generation: 1,
                        },
                        limit,
                        last,
                    )?;
                }
                Ok(())
            })();
            if result.is_err() {
                stopped.store(true, Ordering::Release);
                wake.wake();
                let _ = abort.interrupt();
            }
            result
        }));
        Ok(())
    };
    let gate_backend = backend.clone();
    let gate_stop = stop.clone();
    let mut stream = ServerIo {
        reader,
        writer: writer.clone(),
        stop: stop.clone(),
        admit: send_admission(gate_backend, binding, gate_stop),
    };
    let mut policy = Policy(backend.clone());
    let factories = backend.clone();
    let result = serve_bound(
        &mut stream,
        session,
        limits,
        &mut policy,
        move |s| {
            factories
                .lock()
                .map_err(|_| SourceError::Unavailable)
                .and_then(|mut b| (b.factory)(s))
        },
        Some(&mut hook),
        routes,
    );
    stop.store(true, Ordering::Release);
    signal.wake();
    let interrupted = physical.interrupt();
    // A failed interrupt must return promptly; the host deadline owns eventual
    // cleanup of a detached worker in that case.
    if let Err(error) = interrupted {
        return Err(RecordSessionError::Io(error));
    }
    let joined = if let Some(task) = task {
        task.join().map_err(|_| RecordSessionError::Protocol)?
    } else {
        Ok(())
    };
    result.and(joined)
}

#[cfg(test)]
#[path = "follow/egress_tests.rs"]
mod egress_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;
    use crate::replication::infrastructure::session::{SessionLifecycle, TcpSessionIo};
    use std::net::{Ipv4Addr, TcpListener, TcpStream};

    pub(super) fn identity() -> ScopeIdentity {
        let id = || Id::new("owned-fixture").unwrap();
        let scope = Scope::new(id(), id(), id(), id(), id(), id());
        let limits = SessionLimits {
            control_bytes: 2048,
            bindings: 1,
        };
        let mut lifecycle = SessionLifecycle::new(limits, 1).unwrap();
        let connection = lifecycle.connection_id();
        lifecycle
            .accept_welcome(
                &connection,
                &ControlFrame::Welcome {
                    session: 1,
                    limits,
                    record_bytes: MAX_RECORD_BYTES as u32,
                },
            )
            .unwrap();
        let (request, _) = lifecycle.begin_bind(scope.clone()).unwrap();
        lifecycle
            .accept_bound(
                &connection,
                &ControlFrame::Bound {
                    correlation: request.correlation(),
                    binding: BindingMetadata {
                        scope,
                        head: 0,
                        handle: 1,
                        generation: 1,
                    },
                },
            )
            .unwrap()
    }
    pub(super) fn fixture() -> (Arc<Shared>, TcpStream, JoinHandle<()>) {
        fixture_with_interrupt(None)
    }
    fn fixture_with_interrupt(
        override_interrupt: Option<Arc<dyn SessionInterrupt>>,
    ) -> (Arc<Shared>, TcpStream, JoinHandle<()>) {
        fixture_with_publish(override_interrupt, |_| {})
    }
    fn fixture_with_publish(
        override_interrupt: Option<Arc<dyn SessionInterrupt>>,
        before_publish: impl FnMut(bool) + Send + 'static,
    ) -> (Arc<Shared>, TcpStream, JoinHandle<()>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let io = TcpSessionIo::new(socket).unwrap();
        let physical = override_interrupt.unwrap_or_else(|| io.interrupt_handle());
        let (reader_io, _) = io.split().unwrap();
        let shared = Arc::new(Shared {
            state: Mutex::new(Dispatch {
                frame: None,
                rpc: None,
                closed: false,
                error: None,
                identity: Some(identity()),
                closing: Some(Arc::new(AtomicBool::new(false))),
                stats: HintStats::default(),
                rx: 0,
                tx: 0,
                limit: 2048,
            }),
            ready: Condvar::new(),
            physical,
        });
        let held = shared.clone();
        let task = thread::spawn(move || reader_frames(reader_io, held, before_publish));
        (shared, peer, task)
    }
    pub(super) fn packet(kind: u8, payload: Vec<u8>, total: usize, offset: usize) -> Packet {
        Packet {
            kind,
            status: 0,
            meta: Meta::for_scope(&identity(), 2),
            total,
            offset,
            payload,
        }
    }
    pub(super) fn bytes(p: &Packet) -> Vec<u8> {
        let mut b = Vec::new();
        write_packet(&mut b, p, 2048).unwrap();
        b
    }
    pub(super) fn hint() -> Packet {
        Packet {
            meta: Meta::for_scope(&identity(), FIRST_REQUEST),
            ..packet(HINT, 9_u64.to_be_bytes().to_vec(), 8, 0)
        }
    }
    fn until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !condition() {
            assert!(Instant::now() < deadline, "controlled boundary not reached");
            thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn close_wakes_reader_waiting_on_a_full_reply_slot() {
        let (shared, mut peer, task) = fixture();
        shared
            .admit(&bytes(&packet(PAGE, vec![1], 1, 0)), Duration::from_secs(1))
            .unwrap();
        write_packet(
            &mut peer,
            &packet(PAGE | REPLY, vec![1; 2004], 2100, 0),
            2048,
        )
        .unwrap();
        write_packet(
            &mut peer,
            &packet(PAGE | REPLY, vec![2; 96], 2100, 2004),
            2048,
        )
        .unwrap();
        until(|| {
            let s = shared.state.lock().unwrap();
            s.frame.is_some() && s.rpc.as_ref().unwrap().complete
        });
        let start = Instant::now();
        DispatchInterrupt(shared.clone()).interrupt().unwrap();
        task.join().unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(shared.state.lock().unwrap().frame.is_none());
    }
    #[test]
    fn hint_between_fragments_does_not_consume_the_reply_slot() {
        let (shared, mut peer, task) = fixture();
        shared
            .admit(&bytes(&packet(PAGE, vec![1], 1, 0)), Duration::from_secs(1))
            .unwrap();
        let first = bytes(&packet(PAGE | REPLY, vec![1; 2004], 2100, 0));
        let last = bytes(&packet(PAGE | REPLY, vec![2; 96], 2100, 2004));
        peer.write_all(&first).unwrap();
        peer.write_all(&bytes(&hint())).unwrap();
        peer.write_all(&last).unwrap();
        until(|| shared.state.lock().unwrap().stats.pending_head == Some(9));
        let mut facade = FollowIo {
            writer: Vec::new(),
            shared: shared.clone(),
            current: Cursor::new(Vec::new()),
            deadline: Duration::from_secs(1),
            deferred_write_error: None,
        };
        let mut received = vec![0; first.len() + last.len()];
        facade.read_exact(&mut received).unwrap();
        assert_eq!(received, [first, last].concat());
        assert_eq!(
            HintReceiver {
                shared: shared.clone()
            }
            .wait(Duration::ZERO),
            Ok(HintWake::Changed { advisory_head: 9 })
        );
        DispatchInterrupt(shared).interrupt().unwrap();
        task.join().unwrap();
    }
    #[test]
    fn hint_traffic_cannot_extend_an_admitted_rpc_deadline() {
        let (shared, mut peer, task) = fixture();
        let mut facade = FollowIo {
            writer: Vec::new(),
            shared: shared.clone(),
            current: Cursor::new(Vec::new()),
            deadline: Duration::from_millis(100),
            deferred_write_error: None,
        };
        facade
            .write_all(&bytes(&packet(HEAD, vec![1], 1, 0)))
            .unwrap();
        let producer = thread::spawn(move || {
            for _ in 0..200 {
                if peer.write_all(&bytes(&hint())).is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
        let start = Instant::now();
        assert!(facade.read(&mut [0; 1]).is_err());
        assert!(start.elapsed() < Duration::from_millis(500));
        assert_eq!(shared.error(), Some(FollowError::Unavailable));
        task.join().unwrap();
        producer.join().unwrap();
    }
    #[test]
    fn idle_unsolicited_and_duplicate_replies_are_terminal() {
        for duplicate in [false, true] {
            let (shared, mut peer, task) = fixture();
            if duplicate {
                shared
                    .admit(&bytes(&packet(HEAD, vec![1], 1, 0)), Duration::from_secs(1))
                    .unwrap();
                peer.write_all(&bytes(&packet(HEAD | REPLY, vec![1], 1, 0)))
                    .unwrap();
                until(|| shared.state.lock().unwrap().frame.is_some());
            }
            peer.write_all(&bytes(&packet(HEAD | REPLY, vec![1], 1, 0)))
                .unwrap();
            until(|| shared.error().is_some());
            assert_eq!(shared.error(), Some(FollowError::Protocol));
            task.join().unwrap();
        }
    }
    #[test]
    fn pre_bound_hint_and_unknown_hint_status_fail_closed() {
        for early in [false, true] {
            let (shared, mut peer, task) = fixture();
            if early {
                shared.state.lock().unwrap().identity = None;
            }
            let mut p = hint();
            if !early {
                p.status = 255;
                p.total = 0;
                p.payload.clear();
            }
            peer.write_all(&bytes(&p)).unwrap();
            until(|| shared.error().is_some());
            assert_eq!(shared.error(), Some(FollowError::Protocol));
            task.join().unwrap();
        }
    }
    struct ChangingPolicy(Arc<std::sync::atomic::AtomicU8>);
    impl ScopeAuthorizer for ChangingPolicy {
        fn authorize(&mut self, scope: &Scope) -> Access {
            match self.0.load(Ordering::SeqCst) {
                0 => Access::Allowed(scope.clone()),
                1 => Access::Denied,
                _ => Access::IdentityChanged,
            }
        }
    }
    #[test]
    fn queued_rpc_rechecks_policy_after_acquiring_the_frame_writer() {
        for (mode, code) in [(1, 1), (2, 5)] {
            let policy = Arc::new(std::sync::atomic::AtomicU8::new(0));
            let identity = identity();
            let backend = Arc::new(Mutex::new(Backend {
                policy: ChangingPolicy(policy.clone()),
                factory: (),
            }));
            assert_eq!(
                backend.lock().unwrap().policy.authorize(identity.scope()),
                Access::Allowed(identity.scope().clone())
            );
            let writer = Arc::new(Mutex::new(Vec::<u8>::new()));
            // Represent a complete-frame hint writer already holding admission.
            let held = writer.lock().unwrap();
            let stopped = Arc::new(AtomicBool::new(false));
            let binding = Arc::new(Mutex::new(Some((
                identity.scope().clone(),
                Meta::for_scope(&identity, FIRST_REQUEST),
                2048,
            ))));
            let mut io = ServerIo {
                reader: Cursor::new(Vec::<u8>::new()),
                writer: writer.clone(),
                stop: stopped.clone(),
                admit: send_admission(backend, binding, stopped.clone()),
            };
            let (entered, queued) = std::sync::mpsc::channel();
            let task = thread::spawn(move || {
                entered.send(()).unwrap();
                io.write_all(&bytes(&packet(PAGE | REPLY, vec![1], 1, 0)))
            });
            queued.recv_timeout(Duration::from_secs(1)).unwrap();
            policy.store(mode, Ordering::SeqCst);
            drop(held);
            assert!(task.join().unwrap().is_err());
            let wire = writer.lock().unwrap();
            let mut bytes = wire.as_slice();
            let refusal = read_packet(&mut bytes, 2048).unwrap().unwrap();
            assert!(bytes.is_empty());
            assert_eq!(refusal.kind, HINT);
            assert_eq!(refusal.status, code);
            assert!(refusal.payload.is_empty());
            assert!(stopped.load(Ordering::Acquire));
        }
    }
    struct OwnerGate {
        inner: FollowIo<Vec<u8>>,
        calls: usize,
        pause_at: usize,
        final_read: Option<std::sync::mpsc::Sender<bool>>,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Read for OwnerGate {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            // read_control consumes Welcome's prefix and body in two reads.
            // Pause the actual connecting owner before consuming its outcome.
            if self.calls == self.pause_at {
                self.entered
                    .send(())
                    .map_err(|_| io::ErrorKind::NotConnected)?;
                self.release
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(|_| io::ErrorKind::TimedOut)?;
            }
            if self.calls == 3 {
                if let Some(progress) = &self.final_read {
                    progress.send(false).unwrap();
                }
            }
            let result = self.inner.read(out);
            if self.calls == 3 {
                if let Some(progress) = &self.final_read {
                    progress.send(true).unwrap();
                }
            }
            result
        }
    }
    impl Write for OwnerGate {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.inner.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    struct ConnectingOwner {
        entered: std::sync::mpsc::Receiver<()>,
        release: std::sync::mpsc::Sender<()>,
        task: JoinHandle<Result<RecordSession<OwnerGate>, RecordSessionError>>,
    }
    fn connect_owner(shared: &Arc<Shared>) -> ConnectingOwner {
        connect_owner_at(shared, 3, None)
    }
    fn connect_owner_at(
        shared: &Arc<Shared>,
        pause_at: usize,
        final_read: Option<std::sync::mpsc::Sender<bool>>,
    ) -> ConnectingOwner {
        {
            let mut s = shared.state.lock().unwrap();
            s.identity = None;
            s.closing = None;
        }
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, resumed) = std::sync::mpsc::channel();
        let facade = FollowIo {
            writer: Vec::new(),
            shared: shared.clone(),
            current: Cursor::new(Vec::new()),
            deadline: Duration::from_secs(1),
            deferred_write_error: None,
        };
        let interrupt = facade.interrupt_handle();
        let gate = OwnerGate {
            inner: facade,
            calls: 0,
            pause_at,
            final_read,
            entered,
            release: resumed,
        };
        let task = thread::spawn(move || {
            RecordSession::connect_with(
                gate,
                identity().scope().clone(),
                SessionLimits {
                    control_bytes: 2048,
                    bindings: 1,
                },
                Opening::Follow,
                Some(interrupt),
            )
        });
        until(|| shared.state.lock().unwrap().rpc.is_some());
        ConnectingOwner {
            entered: waiting,
            release,
            task,
        }
    }
    fn welcome(peer: &mut TcpStream) {
        write_control(
            peer,
            &ControlFrame::Welcome {
                session: 1,
                limits: SessionLimits {
                    control_bytes: 2048,
                    bindings: 1,
                },
                record_bytes: MAX_RECORD_BYTES as u32,
            },
            2048,
        )
        .unwrap();
    }
    fn bound() -> ControlFrame {
        ControlFrame::Bound {
            correlation: Correlation {
                session: 1,
                request: FIRST_REQUEST,
            },
            binding: BindingMetadata {
                handle: 1,
                generation: 1,
                scope: identity().scope().clone(),
                head: 0,
            },
        }
    }
    fn parked(shared: &Shared, owner: &ConnectingOwner) {
        owner.entered.recv_timeout(Duration::from_secs(1)).unwrap();
        until(|| {
            let s = shared.state.lock().unwrap();
            s.frame.is_some() && s.rpc.as_ref().unwrap().complete
        });
    }
    #[test]
    fn complete_setup_refusals_belong_to_the_connecting_owner_across_eof() {
        for reason in [
            SourceError::Denied,
            SourceError::Unavailable,
            SourceError::Pruned,
            SourceError::InvalidRequest,
            SourceError::IdentityChanged,
            SourceError::OversizedRecord,
        ] {
            let (shared, mut peer, reader) = fixture();
            let owner = connect_owner(&shared);
            welcome(&mut peer);
            write_control(
                &mut peer,
                &ControlFrame::Refused {
                    correlation: Correlation {
                        session: 1,
                        request: FIRST_REQUEST,
                    },
                    reason: reason.clone(),
                },
                2048,
            )
            .unwrap();
            parked(&shared, &owner);
            peer.shutdown(std::net::Shutdown::Both).unwrap();
            // A completed on-time outcome is not an incomplete reply. Expiry
            // during local validation must not replace its typed result.
            shared.state.lock().unwrap().rpc.as_mut().unwrap().deadline = Instant::now();
            thread::sleep(Duration::from_millis(30));
            let before_decision = shared.error();
            owner.release.send(()).unwrap();
            let result = owner.task.join().unwrap();
            reader.join().unwrap();
            assert_eq!(before_decision, None, "reader read past its setup handoff");
            match result {
                Err(RecordSessionError::Source(actual)) => assert_eq!(actual, reason),
                Err(error) => panic!("wrong setup result: {error:?}"),
                Ok(_) => panic!("refusal supplied ports"),
            }
        }
    }
    #[test]
    fn bound_handoff_precedes_terminal_hint_and_eof() {
        for (status, expected) in [
            (1, FollowError::Refused(SourceError::Denied)),
            (5, FollowError::Refused(SourceError::IdentityChanged)),
        ] {
            let (shared, mut peer, reader) = fixture();
            let owner = connect_owner(&shared);
            welcome(&mut peer);
            write_control(&mut peer, &bound(), 2048).unwrap();
            parked(&shared, &owner);
            let mut p = hint();
            p.status = status;
            p.total = 0;
            p.payload.clear();
            peer.write_all(&bytes(&p)).unwrap();
            peer.shutdown(std::net::Shutdown::Both).unwrap();
            thread::sleep(Duration::from_millis(30));
            let before_decision = shared.error();
            let before_stats = shared.state.lock().unwrap().stats;
            owner.release.send(()).unwrap();
            let root = owner.task.join().unwrap().unwrap();
            assert_eq!(before_decision, None);
            assert_eq!(before_stats.frames_received, 0);
            let accepted = root.shared.lock().unwrap().identity.clone();
            shared.activate(accepted, root.closing.clone()).unwrap();
            reader.join().unwrap();
            assert_eq!(shared.error(), Some(expected));
        }
    }
    #[test]
    fn bound_identity_is_validated_before_any_post_setup_hint() {
        for case in 0..3 {
            let (shared, mut peer, reader) = fixture();
            let owner = connect_owner(&shared);
            welcome(&mut peer);
            let mut frame = bound();
            if let ControlFrame::Bound { binding, .. } = &mut frame {
                match case {
                    0 => {
                        binding.scope = Scope::new(
                            Id::new("other-receiver").unwrap(),
                            Id::new("origin").unwrap(),
                            Id::new("stream").unwrap(),
                            Id::new("first").unwrap(),
                            Id::new("schema").unwrap(),
                            Id::new("epoch").unwrap(),
                        )
                    }
                    1 => binding.handle = 2,
                    _ => binding.generation = 2,
                }
            }
            write_control(&mut peer, &frame, 2048).unwrap();
            parked(&shared, &owner);
            peer.write_all(&bytes(&hint())).unwrap();
            peer.shutdown(std::net::Shutdown::Both).unwrap();
            owner.release.send(()).unwrap();
            let result = owner.task.join().unwrap();
            if case == 0 {
                match result {
                    Err(RecordSessionError::Protocol) => (),
                    Err(error) => panic!("wrong invalid-bound result {error:?}"),
                    Ok(root) => {
                        root.close().unwrap();
                        panic!("invalid scope supplied ports")
                    }
                }
                reader.join().unwrap();
                assert!(shared.state.lock().unwrap().identity.is_none());
            } else {
                // Handles/generations are peer-assigned nonzero values. Retain
                // the exact accepted binding; a hint using another is invalid.
                let root = result.unwrap();
                let accepted = root.shared.lock().unwrap().identity.clone();
                shared.activate(accepted, root.closing.clone()).unwrap();
                reader.join().unwrap();
                assert_eq!(shared.error(), Some(FollowError::Protocol));
            }
            assert_eq!(shared.state.lock().unwrap().stats.frames_received, 0);
        }
    }
    #[test]
    fn successful_bound_then_normal_eof_is_terminal_without_authority() {
        let (shared, mut peer, reader) = fixture();
        let owner = connect_owner(&shared);
        welcome(&mut peer);
        write_control(&mut peer, &bound(), 2048).unwrap();
        parked(&shared, &owner);
        peer.shutdown(std::net::Shutdown::Both).unwrap();
        owner.release.send(()).unwrap();
        let root = owner.task.join().unwrap().unwrap();
        let accepted = root.shared.lock().unwrap().identity.clone();
        shared.activate(accepted, root.closing.clone()).unwrap();
        reader.join().unwrap();
        assert_eq!(shared.error(), Some(FollowError::Closed));
        let (mut a, mut source) = root.parts();
        assert_eq!(a.authorize(identity().scope()), Access::Unverifiable);
        assert_eq!(
            source.head(identity().scope()),
            Err(SourceError::Unavailable)
        );
    }
    #[test]
    fn success_hint_waits_in_channel_until_bound_activation() {
        let (shared, mut peer, reader) = fixture();
        let owner = connect_owner(&shared);
        welcome(&mut peer);
        write_control(&mut peer, &bound(), 2048).unwrap();
        parked(&shared, &owner);
        peer.write_all(&bytes(&hint())).unwrap();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(shared.state.lock().unwrap().stats.frames_received, 0);
        owner.release.send(()).unwrap();
        let root = owner.task.join().unwrap().unwrap();
        let accepted = root.shared.lock().unwrap().identity.clone();
        shared.activate(accepted, root.closing.clone()).unwrap();
        assert_eq!(
            HintReceiver {
                shared: shared.clone()
            }
            .wait(Duration::from_secs(1)),
            Ok(HintWake::Changed { advisory_head: 9 })
        );
        root.close().unwrap();
        reader.join().unwrap();
    }
    #[test]
    fn invalid_or_unsolicited_setup_frames_are_never_preserved_as_outcomes() {
        for case in 0..6 {
            let (shared, mut peer, reader) = fixture();
            shared.state.lock().unwrap().identity = None;
            let mut facade = FollowIo {
                writer: Vec::new(),
                shared: shared.clone(),
                current: Cursor::new(Vec::new()),
                deadline: Duration::from_secs(1),
                deferred_write_error: None,
            };
            write_control(
                &mut facade,
                &ControlFrame::FollowOpen {
                    limits: SessionLimits {
                        control_bytes: 2048,
                        bindings: 1,
                    },
                    request: FIRST_REQUEST,
                    scope: identity().scope().clone(),
                },
                2048,
            )
            .unwrap();
            if case != 0 {
                welcome(&mut peer);
                assert!(matches!(
                    read_control(&mut facade, 2048).unwrap(),
                    ControlFrame::Welcome { .. }
                ));
            }
            let mut frame = Vec::new();
            if case == 0 {
                frame = bytes(&hint());
            } else if case == 5 {
                write_control(&mut frame, &bound(), 2048).unwrap();
                frame[27..31].fill(0);
            } else {
                let correlation = Correlation {
                    session: if case == 1 { 2 } else { 1 },
                    request: if case == 2 { 2 } else { FIRST_REQUEST },
                };
                write_control(
                    &mut frame,
                    &ControlFrame::Refused {
                        correlation,
                        reason: SourceError::Denied,
                    },
                    2048,
                )
                .unwrap();
                if case == 3 {
                    *frame.last_mut().unwrap() = 255;
                }
                if case == 4 {
                    frame.clear();
                    welcome(&mut peer);
                }
            }
            if !frame.is_empty() {
                peer.write_all(&frame).unwrap();
            }
            until(|| shared.error().is_some());
            assert_eq!(shared.error(), Some(FollowError::Protocol));
            assert!(shared.state.lock().unwrap().identity.is_none());
            reader.join().unwrap();
        }
    }
    #[test]
    fn duplicate_setup_frame_is_protocol_failure_after_validated_activation() {
        let (shared, mut peer, reader) = fixture();
        let owner = connect_owner(&shared);
        welcome(&mut peer);
        write_control(&mut peer, &bound(), 2048).unwrap();
        parked(&shared, &owner);
        write_control(&mut peer, &bound(), 2048).unwrap();
        owner.release.send(()).unwrap();
        let root = owner.task.join().unwrap().unwrap();
        let accepted = root.shared.lock().unwrap().identity.clone();
        shared.activate(accepted, root.closing.clone()).unwrap();
        reader.join().unwrap();
        assert_eq!(shared.error(), Some(FollowError::Protocol));
    }
    struct FailedInterrupt;
    impl SessionInterrupt for FailedInterrupt {
        fn interrupt(&self) -> io::Result<()> {
            Err(io::ErrorKind::Other.into())
        }
    }
    #[test]
    fn close_wakes_a_parked_setup_result_even_when_physical_interrupt_fails() {
        let (shared, mut peer, reader) = fixture_with_interrupt(Some(Arc::new(FailedInterrupt)));
        let owner = connect_owner(&shared);
        welcome(&mut peer);
        write_control(&mut peer, &bound(), 2048).unwrap();
        parked(&shared, &owner);
        assert!(DispatchInterrupt(shared.clone()).interrupt().is_err());
        owner.release.send(()).unwrap();
        assert!(owner.task.join().unwrap().is_err());
        reader.join().unwrap();
        assert!(shared.state.lock().unwrap().frame.is_none());
        assert!(shared.state.lock().unwrap().identity.is_none());
    }
    #[test]
    fn incomplete_setup_is_unavailable_and_respects_the_reply_deadline() {
        for partial in [false, true] {
            let (shared, mut peer, reader) = fixture();
            shared.state.lock().unwrap().identity = None;
            let mut facade = FollowIo {
                writer: Vec::new(),
                shared: shared.clone(),
                current: Cursor::new(Vec::new()),
                deadline: Duration::from_millis(100),
                deferred_write_error: None,
            };
            write_control(
                &mut facade,
                &ControlFrame::FollowOpen {
                    limits: SessionLimits {
                        control_bytes: 2048,
                        bindings: 1,
                    },
                    request: FIRST_REQUEST,
                    scope: identity().scope().clone(),
                },
                2048,
            )
            .unwrap();
            welcome(&mut peer);
            read_control(&mut facade, 2048).unwrap();
            if partial {
                let mut frame = Vec::new();
                write_control(&mut frame, &bound(), 2048).unwrap();
                peer.write_all(&frame[..5]).unwrap();
            }
            let started = Instant::now();
            assert!(read_control(&mut facade, 2048).is_err());
            assert!(started.elapsed() < Duration::from_millis(500));
            assert_eq!(shared.error(), Some(FollowError::Unavailable));
            reader.join().unwrap();
        }
    }
    #[test]
    fn close_wakes_empty_reply_wait_even_when_physical_interrupt_fails() {
        let (shared, _peer, reader) = fixture_with_interrupt(Some(Arc::new(FailedInterrupt)));
        shared
            .admit(&bytes(&packet(HEAD, vec![1], 1, 0)), Duration::from_secs(1))
            .unwrap();
        let owned = shared.clone();
        let (started, entered) = std::sync::mpsc::channel();
        let consumer = thread::spawn(move || {
            let mut io = FollowIo {
                writer: Vec::new(),
                shared: owned,
                current: Cursor::new(Vec::new()),
                deadline: Duration::from_secs(1),
                deferred_write_error: None,
            };
            started.send(()).unwrap();
            io.read(&mut [0; 1])
        });
        entered.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(DispatchInterrupt(shared).interrupt().is_err());
        assert!(consumer.join().unwrap().is_err());
        reader.join().unwrap();
    }
    #[test]
    fn decoded_setup_outcome_owns_its_reader_local_frame_before_publication() {
        let (decoded, awaiting) = std::sync::mpsc::channel();
        let (publish, resumed) = std::sync::mpsc::channel();
        let (shared, mut peer, reader) = fixture_with_publish(None, move |handoff| {
            if handoff {
                decoded.send(()).unwrap();
                resumed.recv_timeout(Duration::from_secs(2)).unwrap();
            }
        });
        let (progress, final_read) = std::sync::mpsc::channel();
        let owner = connect_owner_at(&shared, 1, Some(progress));
        owner.entered.recv_timeout(Duration::from_secs(1)).unwrap();
        welcome(&mut peer);
        write_control(
            &mut peer,
            &ControlFrame::Refused {
                correlation: Correlation {
                    session: 1,
                    request: FIRST_REQUEST,
                },
                reason: SourceError::Denied,
            },
            2048,
        )
        .unwrap();
        awaiting.recv_timeout(Duration::from_secs(1)).unwrap();
        {
            let mut s = shared.state.lock().unwrap();
            assert!(s.rpc.as_ref().unwrap().setup_outcome());
            assert!(s.frame.is_some());
            s.rpc.as_mut().unwrap().deadline = Instant::now();
        }
        owner.release.send(()).unwrap();
        assert!(!final_read.recv_timeout(Duration::from_secs(1)).unwrap());
        let returned_before_publication =
            final_read.recv_timeout(Duration::from_millis(30)).is_ok();
        publish.send(()).unwrap();
        let result = owner.task.join().unwrap();
        reader.join().unwrap();
        assert!(
            !returned_before_publication,
            "arrival deadline replaced an owned complete outcome"
        );
        assert!(matches!(
            result,
            Err(RecordSessionError::Source(SourceError::Denied))
        ));
    }
    #[test]
    fn capability_refusal_dispatch_requires_exact_kind_complete_body_and_known_reason() {
        for kind in [
            capabilities::HEAD_CAP,
            capabilities::OPEN_CAP,
            capabilities::PAGE_CAP,
        ] {
            let (shared, _peer, task) = fixture();
            shared
                .admit(&bytes(&packet(kind, vec![1], 1, 0)), Duration::from_secs(1))
                .unwrap();
            let body = capabilities::encode_incompatible(
                crate::replication::domain::CapabilityError::MissingReader(1),
            )
            .unwrap();
            let mut reply = packet(kind | REPLY, body.clone(), body.len(), 0);
            reply.status = 8;
            assert_eq!(shared.route(&bytes(&reply)), Ok(false));
            assert!(shared.state.lock().unwrap().rpc.as_ref().unwrap().complete);
            let _ = shared.fail(FollowError::Closed);
            task.join().unwrap();
        }
        for case in 0..6 {
            let (shared, _peer, task) = fixture();
            let kind = if case == 0 {
                HEAD
            } else {
                capabilities::HEAD_CAP
            };
            shared
                .admit(&bytes(&packet(kind, vec![1], 1, 0)), Duration::from_secs(1))
                .unwrap();
            let body = capabilities::encode_incompatible(
                crate::replication::domain::CapabilityError::MissingReader(1),
            )
            .unwrap();
            let mut reply = packet(kind | REPLY, body.clone(), body.len(), 0);
            reply.status = 8;
            match case {
                0 => {}
                1 => reply.status = 9,
                2 => {
                    reply.payload.clear();
                    reply.total = 0;
                }
                3 => reply.offset = 1,
                4 => reply.total += 1,
                _ => reply.payload[0] = 255,
            }
            assert_eq!(shared.route(&bytes(&reply)), Err(FollowError::Protocol));
            let _ = shared.fail(FollowError::Closed);
            task.join().unwrap();
        }
    }
    #[test]
    fn complete_capability_refusal_owns_local_publication_and_queued_frame_before_eof() {
        for (pause_publication, terminal_hint) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (arrived, arrival) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let (shared, mut peer, task) = fixture_with_publish(None, move |_| {
                arrived.send(()).unwrap();
                if pause_publication {
                    released.recv().unwrap();
                }
            });
            let mut facade = FollowIo {
                writer: Vec::new(),
                shared: shared.clone(),
                current: Cursor::new(Vec::new()),
                deadline: Duration::from_secs(2),
                deferred_write_error: None,
            };
            facade
                .write_all(&bytes(&packet(capabilities::HEAD_CAP, vec![1], 1, 0)))
                .unwrap();
            let body = capabilities::encode_incompatible(
                crate::replication::domain::CapabilityError::MissingReader(1),
            )
            .unwrap();
            let mut reply = packet(capabilities::HEAD_CAP | REPLY, body.clone(), body.len(), 0);
            reply.status = 8;
            let frame = bytes(&reply);
            peer.write_all(&frame).unwrap();
            if terminal_hint {
                let mut refused = hint();
                refused.status = 1;
                refused.total = 0;
                refused.payload.clear();
                peer.write_all(&bytes(&refused)).unwrap();
            }
            peer.shutdown(std::net::Shutdown::Write).unwrap();
            arrival.recv_timeout(Duration::from_secs(1)).unwrap();
            if !pause_publication {
                until(|| shared.state.lock().unwrap().frame.is_some());
            }
            // Network arrival was on time; local consumer scheduling may exceed it.
            shared.state.lock().unwrap().rpc.as_mut().unwrap().deadline = Instant::now();
            let (progress, reading) = std::sync::mpsc::channel();
            let consumer = thread::spawn(move || {
                let mut received = vec![0; frame.len()];
                progress.send(false).unwrap();
                let result = facade.read_exact(&mut received);
                progress.send(true).unwrap();
                result.unwrap();
                assert_eq!(received, frame);
                facade
            });
            let mut returned_before_publication = false;
            if pause_publication {
                assert!(!reading.recv_timeout(Duration::from_secs(1)).unwrap());
                returned_before_publication =
                    reading.recv_timeout(Duration::from_millis(30)).is_ok();
                release.send(()).unwrap();
            }
            let _held = consumer.join().unwrap();
            task.join().unwrap();
            assert!(
                !returned_before_publication,
                "arrival deadline replaced an owned refusal"
            );
            assert_eq!(
                shared.error(),
                Some(if terminal_hint {
                    FollowError::Refused(SourceError::Denied)
                } else {
                    FollowError::Closed
                })
            );
        }
    }
    #[test]
    fn explicit_close_cancels_a_queued_capability_refusal() {
        let (shared, mut peer, task) = fixture();
        shared
            .admit(
                &bytes(&packet(capabilities::HEAD_CAP, vec![1], 1, 0)),
                Duration::from_secs(1),
            )
            .unwrap();
        let body = capabilities::encode_incompatible(
            crate::replication::domain::CapabilityError::MissingReader(1),
        )
        .unwrap();
        let mut reply = packet(capabilities::HEAD_CAP | REPLY, body.clone(), body.len(), 0);
        reply.status = 8;
        peer.write_all(&bytes(&reply)).unwrap();
        until(|| shared.state.lock().unwrap().frame.is_some());
        DispatchInterrupt(shared.clone()).interrupt().unwrap();
        task.join().unwrap();
        assert_eq!(shared.error(), Some(FollowError::Closed));
        assert!(shared.state.lock().unwrap().frame.is_none());
    }
}
