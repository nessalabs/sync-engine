//! Dedicated pinned-TLS advisory Follow stream; record reads remain separate.
use super::follow::{self, FollowError, HintSignal};
use super::*;
use crate::replication::infrastructure::session::TlsSessionIo;
use std::time::{Duration, Instant};

/// Host-owned stop fence for one dedicated secure Follow server connection.
/// Stop wakes an idle signal wait and interrupts this exact TLS socket. The host
/// owns the worker and joins it, with finite I/O and callback deadlines.
pub struct SecureFollowStop {
    stopped: Arc<AtomicBool>,
    admission: Mutex<()>,
    signal: HintSignal,
    interrupt: Arc<dyn SessionInterrupt>,
}
impl SecureFollowStop {
    /// Retain the original channel interruption capability before moving it.
    pub fn new(io: &TlsSessionIo, signal: HintSignal) -> Self {
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
            admission: Mutex::new(()),
            signal,
            interrupt: io.interrupt_handle(),
        }
    }
    /// Fence further hints, wake an idle server, and interrupt the connection.
    /// On interruption failure the fence remains set and the host owns cleanup.
    pub fn stop(&self) -> io::Result<()> {
        {
            let _guard = self.admission.lock().unwrap_or_else(|e| e.into_inner());
            self.stopped.store(true, Ordering::Release);
        }
        self.signal.wake();
        self.interrupt.interrupt()
    }
}

// Admission and stop are serialized only for the decision. The lock is never
// held over I/O or host callbacks; already-admitted frames may race interruption.
struct AdmittedIo<'a, S> {
    inner: S,
    stop: &'a SecureFollowStop,
}
impl<S> AdmittedIo<'_, S> {
    fn admit(&self) -> io::Result<()> {
        let _guard = self
            .stop
            .admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.stop.stopped.load(Ordering::Acquire) {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            Ok(())
        }
    }
}
impl<S: Read> Read for AdmittedIo<'_, S> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}
impl<S: Write> Write for AdmittedIo<'_, S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.admit()?;
        self.inner.write(bytes)
    }
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.admit()?;
        self.inner.write_all(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// Own terminal interruption once the token has been matched to this channel.
// Explicit finish reports cleanup failure only if there is no primary error;
// Drop is the best-effort unwind path and leaves the token available for retry.
struct Terminal<'a> {
    stop: &'a SecureFollowStop,
    armed: bool,
}
impl Terminal<'_> {
    fn finish(mut self, result: Result<(), RecordSessionError>) -> Result<(), RecordSessionError> {
        let cleanup = self.stop.stop();
        self.armed = false;
        match result {
            Err(error) => Err(error),
            Ok(()) => cleanup.map_err(RecordSessionError::Io),
        }
    }
}
impl Drop for Terminal<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.stop.stop();
        }
    }
}

// A callback already admitted before stop may finish, but a new policy or
// source callback must not start after observing the permanent stop fence.
struct StopPolicy<A> {
    inner: A,
    stopped: Arc<AtomicBool>,
}
impl<A: ScopeAuthorizer> ScopeAuthorizer for StopPolicy<A> {
    fn authorize(&mut self, scope: &Scope) -> Access {
        if self.stopped.load(Ordering::Acquire) {
            Access::Unverifiable
        } else {
            self.inner.authorize(scope)
        }
    }
}

/// Serve only existing FollowOpen and advisory hints over one already admitted
/// pinned TLS stream. The host supplies current principal-bound policy and finite
/// source callbacks and a finite, nonzero `wait_lease` starting after binding.
/// Lease expiry ends the wait and interrupts this exact channel. The host owns
/// setup/callback deadlines, stop on shutdown, and joining its worker. An already
/// admitted frame may complete or be interrupted while stop races its I/O.
/// This opens no socket, supplies no record ports and creates no background task.
pub fn serve_secure_follow<A, F, R>(
    io: TlsSessionIo,
    session: u64,
    supported: SessionLimits,
    policy: A,
    factory: F,
    stop: &SecureFollowStop,
    wait_lease: Duration,
) -> Result<(), RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    if !Arc::ptr_eq(&stop.interrupt, &io.interrupt_handle()) {
        return Err(RecordSessionError::Protocol);
    }
    let terminal = Terminal { stop, armed: true };
    let result =
        serve_secure_follow_inner(io, session, supported, policy, factory, stop, wait_lease);
    terminal.finish(result)
}

fn serve_secure_follow_inner<A, F, R>(
    io: TlsSessionIo,
    session: u64,
    supported: SessionLimits,
    policy: A,
    mut factory: F,
    stop: &SecureFollowStop,
    wait_lease: Duration,
) -> Result<(), RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
{
    supported
        .validate()
        .map_err(|_| RecordSessionError::Protocol)?;
    let mut io = AdmittedIo { inner: io, stop };
    if wait_lease.is_zero() || Instant::now().checked_add(wait_lease).is_none() {
        return Err(RecordSessionError::Protocol);
    }
    if session == 0 {
        return Err(RecordSessionError::Protocol);
    }
    if stop.stopped.load(Ordering::Acquire) {
        return Err(RecordSessionError::Closed);
    }
    let mut policy = StopPolicy {
        inner: policy,
        stopped: stop.stopped.clone(),
    };
    let stopped = stop.stopped.clone();
    let mut factory = move |scope: &Scope| {
        if stopped.load(Ordering::Acquire) {
            Err(SourceError::Unavailable)
        } else {
            factory(scope)
        }
    };
    // Capture before binding's authorized source read to avoid losing a wake.
    let mut cursor = stop.signal.version()?;
    let ControlFrame::FollowOpen {
        limits,
        request,
        scope,
    } = read_control(&mut io, supported.control_bytes as usize)?
    else {
        return Err(RecordSessionError::Protocol);
    };
    let selected = SessionLimits {
        control_bytes: limits.control_bytes.min(supported.control_bytes),
        bindings: 1,
    };
    let limit = selected.control_bytes as usize;
    let correlation = Correlation { session, request };
    write_control(
        &mut io,
        &ControlFrame::Welcome {
            session,
            limits: selected,
            record_bytes: MAX_RECORD_BYTES as u32,
        },
        limit,
    )?;
    io.flush()?;
    if !scope_pair::write_binding(
        &mut io,
        correlation,
        &scope,
        1,
        &mut factory,
        &mut policy,
        limit,
    )? {
        return Ok(());
    }
    let deadline = Instant::now()
        .checked_add(wait_lease)
        .ok_or(RecordSessionError::Protocol)?;
    let meta = Meta {
        session,
        request,
        handle: 1,
        generation: 1,
    };
    let backend = Arc::new(Mutex::new(follow::Backend { policy, factory }));
    let writer = Arc::new(Mutex::new(io));
    let mut last = None;
    while let Some(next) = stop.signal.wait_until(cursor, &stop.stopped, deadline)? {
        cursor = next;
        last = follow::emit(&backend, &writer, &stop.stopped, &scope, meta, limit, last)?;
    }
    Ok(())
}

/// One exclusively owned TLS hint receiver. It never grants access, reads
/// records or advances durable progress; fetch separately under current policy.
pub struct SecureFollow {
    inner: RecordSession<TlsSessionIo>,
    last_head: Option<u64>,
}
impl SecureFollow {
    /// Select FollowOpen on an already admitted pinned TLS channel. No fallback.
    /// Retain the channel interrupt before moving it if another task must cancel
    /// a blocked hint read. The host must retry interruption if cleanup fails.
    pub fn connect(
        io: TlsSessionIo,
        scope: Scope,
        limits: SessionLimits,
    ) -> Result<Self, RecordSessionError> {
        let interrupt = io.interrupt_handle();
        Ok(Self {
            inner: RecordSession::connect_with(
                io,
                scope,
                limits,
                Opening::Follow,
                Some(interrupt),
            )?,
            last_head: None,
        })
    }
    /// Wait for one exact advisory hint. Idle first-byte timeouts are retried;
    /// partial/truncated, stale or malformed frames and EOF terminally close it.
    pub fn next_hint(&mut self) -> Result<u64, FollowError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)
            .map_err(|_| FollowError::Closed)?;
        let identity = c.identity.clone();
        let limit = c.wire.limit;
        let result = (|| {
            let p = read_packet_wait(&mut c.wire, limit, true)
                .map_err(FollowError::from)?
                .ok_or(FollowError::Closed)?;
            if p.kind != HINT || p.meta != Meta::for_scope(&identity, FIRST_REQUEST) {
                return Err(FollowError::Protocol);
            }
            let head = follow::decode_hint(&p)?.map_err(FollowError::Refused)?;
            if self.last_head.is_some_and(|old| head <= old) {
                return Err(FollowError::Protocol);
            }
            Ok(head)
        })();
        if let Ok(head) = result {
            self.last_head = Some(head);
        }
        if result.is_err() {
            self.inner.closing.store(true, Ordering::Release);
            // Preserve the primary terminal reason; the host may retry close if
            // the independent physical interrupt fails.
            if let Some(interrupt) = &self.inner.interrupt {
                let _ = interrupt.interrupt();
            }
            c.wire.close();
            c.owner.close();
        }
        result
    }
    /// Exact application bytes and control exchange counts for this channel.
    pub fn counters(&self) -> Result<WireCounters, RecordSessionError> {
        self.inner.counters()
    }
    /// Fence and interrupt this receiver; does not undo previously returned hints.
    pub fn close(&self) -> Result<(), RecordSessionError> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;
    use std::sync::atomic::AtomicUsize;
    struct Noop;
    impl SessionInterrupt for Noop {
        fn interrupt(&self) -> io::Result<()> {
            Ok(())
        }
    }
    struct FailOnce(AtomicUsize);
    impl SessionInterrupt for FailOnce {
        fn interrupt(&self) -> io::Result<()> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::ErrorKind::Other.into())
            } else {
                Ok(())
            }
        }
    }
    fn token(interrupt: Arc<dyn SessionInterrupt>) -> SecureFollowStop {
        SecureFollowStop {
            stopped: Arc::new(AtomicBool::new(false)),
            admission: Mutex::new(()),
            signal: HintSignal::new(),
            interrupt,
        }
    }
    fn scope() -> Scope {
        let id = || Id::new("id").unwrap();
        Scope::new(id(), id(), id(), id(), id(), id())
    }
    struct Source;
    impl RecordSource for Source {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            Ok(42)
        }
        fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
            panic!("no page")
        }
    }
    struct StopOnFinal<'a> {
        stop: &'a SecureFollowStop,
        calls: usize,
    }
    impl ScopeAuthorizer for StopOnFinal<'_> {
        fn authorize(&mut self, scope: &Scope) -> Access {
            self.calls += 1;
            if self.calls == 2 {
                self.stop.stop().unwrap();
            }
            Access::Allowed(scope.clone())
        }
    }
    #[test]
    fn stop_during_final_binding_policy_suppresses_output_without_transport_help() {
        let stop = token(Arc::new(Noop));
        let mut output = Vec::new();
        let mut writer = AdmittedIo {
            inner: &mut output,
            stop: &stop,
        };
        let mut policy = StopOnFinal {
            stop: &stop,
            calls: 0,
        };
        let result = scope_pair::write_binding(
            &mut writer,
            Correlation {
                session: 1,
                request: 1,
            },
            &scope(),
            1,
            &mut |_| Ok(Source),
            &mut policy,
            2048,
        );
        assert!(matches!(result, Err(RecordSessionError::Io(_))));
        assert!(output.is_empty());
    }
    #[test]
    fn stop_during_final_hint_policy_suppresses_output_without_transport_help() {
        let stop = token(Arc::new(Noop));
        let backend = Arc::new(Mutex::new(follow::Backend {
            policy: StopOnFinal {
                stop: &stop,
                calls: 0,
            },
            factory: |_: &Scope| Ok(Source),
        }));
        let writer = Arc::new(Mutex::new(AdmittedIo {
            inner: Vec::new(),
            stop: &stop,
        }));
        let result = follow::emit(
            &backend,
            &writer,
            &stop.stopped,
            &scope(),
            Meta {
                session: 1,
                request: 1,
                handle: 1,
                generation: 1,
            },
            2048,
            None,
        );
        assert!(matches!(result, Err(RecordSessionError::Io(_))));
        assert!(writer.lock().unwrap().inner.is_empty());
    }
    #[test]
    fn terminal_preserves_primary_error_and_allows_interrupt_retry() {
        let interrupt = Arc::new(FailOnce(AtomicUsize::new(0)));
        let stop = token(interrupt.clone());
        let terminal = Terminal {
            stop: &stop,
            armed: true,
        };
        assert!(matches!(
            terminal.finish(Err(RecordSessionError::Source(SourceError::Denied))),
            Err(RecordSessionError::Source(SourceError::Denied))
        ));
        assert!(stop.stopped.load(Ordering::Acquire));
        assert_eq!(interrupt.0.load(Ordering::SeqCst), 1);
        stop.stop().unwrap();
        assert_eq!(interrupt.0.load(Ordering::SeqCst), 2);
        let stop = token(Arc::new(FailOnce(AtomicUsize::new(0))));
        assert!(matches!(
            (Terminal {
                stop: &stop,
                armed: true
            })
            .finish(Ok(())),
            Err(RecordSessionError::Io(_))
        ));
    }
    #[test]
    fn expired_lease_does_not_admit_pending_wake() {
        let signal = HintSignal::new();
        signal.notify().unwrap();
        let stop = AtomicBool::new(false);
        assert_eq!(signal.wait_until(0, &stop, Instant::now()).unwrap(), None);
        assert_eq!(
            signal
                .wait_until(0, &stop, Instant::now() + Duration::from_secs(1))
                .unwrap(),
            Some(1)
        );
    }
}
