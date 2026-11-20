use super::*;
use crate::replication::domain::Id;
use crate::replication::infrastructure::session::{
    TcpSessionIo, TcpSessionReader, TcpSessionWriter,
};
use crate::replication::infrastructure::SqliteReferenceSource;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

const WAIT: Duration = Duration::from_secs(3);
const LIMITS: SessionLimits = SessionLimits {
    control_bytes: 2048,
    bindings: 1,
};

fn scope() -> Scope {
    let id = |s: &str| Id::new(s).unwrap();
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}

fn source(path: &std::path::Path) -> SqliteReferenceSource {
    let scope = scope();
    SqliteReferenceSource::open(
        path,
        scope.origin().clone(),
        scope.stream().clone(),
        scope.incarnation().clone(),
        scope.schema().clone(),
    )
    .unwrap()
}

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "one-page-cancel-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
type PageGate = (SyncSender<()>, Receiver<()>);
struct GatedSource {
    inner: SqliteReferenceSource,
    gate: Arc<Mutex<Option<PageGate>>>,
}
impl RecordSource for GatedSource {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        self.inner.head(s)
    }
    fn page(&mut self, req: &PageRequest) -> Result<Page, SourceError> {
        let page = self.inner.page(req)?;
        let gate = self.gate.lock().unwrap().take();
        if let Some((ready, release)) = gate {
            ready.send(()).unwrap();
            release.recv_timeout(WAIT).unwrap();
        }
        Ok(page)
    }
}
struct Allow;
impl ScopeAuthorizer for Allow {
    fn authorize(&mut self, requested: &Scope) -> Access {
        if *requested == scope() {
            Access::Allowed(requested.clone())
        } else {
            Access::Denied
        }
    }
}

fn tcp(stream: &TcpStream) {
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
}

fn request() -> PageRequest {
    PageRequest {
        scope: scope(),
        after: 0,
        target: 1,
        max_records: 1,
        max_payload_bytes: 1500,
        max_record_bytes: 1500,
    }
}

#[test]
fn existing_server_ingress_cancels_prepared_sqlite_page_before_it_can_emit() {
    let dir = TestDirectory::new();
    let db = dir.0.join("source.db");
    source(&db)
        .append(&Id::new("record").unwrap(), &[17; 1000])
        .unwrap();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    tcp(&client);
    let (ready, page_materialized) = sync_channel(1);
    let (release, continue_page) = sync_channel(1);
    let gate = Arc::new(Mutex::new(Some((ready, continue_page))));
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        tcp(&socket);
        serve_page_cancel_session(
            TcpSessionIo::new(socket).unwrap(),
            7,
            LIMITS,
            Allow,
            move |_| {
                Ok(GatedSource {
                    inner: source(&db),
                    gate: gate.clone(),
                })
            },
        )
    });
    // Test the actual server ingress first; there is deliberately no
    // independent raw reader in the eventual production client.
    client.write_all(PREFACE).unwrap();
    write_control(
        &mut client,
        &ControlFrame::FollowOpen {
            limits: LIMITS,
            request: FIRST_REQUEST,
            scope: scope(),
        },
        2048,
    )
    .unwrap();
    assert!(matches!(
        read_control(&mut client, 2048).unwrap(),
        ControlFrame::Welcome { session: 7, .. }
    ));
    assert!(matches!(
        read_control(&mut client, 2048).unwrap(),
        ControlFrame::Bound { .. }
    ));
    let meta = Meta {
        session: 7,
        request: 2,
        handle: 1,
        generation: 1,
    };
    let payload = encode_page_request(&request());
    write_packet(
        &mut client,
        &Packet {
            kind: PAGE,
            status: 0,
            meta,
            total: payload.len(),
            offset: 0,
            payload,
        },
        2048,
    )
    .unwrap();
    page_materialized.recv_timeout(WAIT).unwrap();
    write_packet(&mut client, &cancel_packet(meta), 2048).unwrap();
    let ack = read_packet_wait_kind(&mut client, 2048, false, Some(CANCEL))
        .unwrap()
        .unwrap();
    assert_eq!(
        (ack.kind, ack.meta, ack.status, ack.offset, ack.total),
        (CANCEL | REPLY, meta, 0, 0, 1)
    );
    assert_eq!(
        ack.payload,
        [PageCancelReceipt::StoppedBeforeResponse.code()]
    );
    release.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert!(read_packet(&mut client, 2048).unwrap().is_none());
}

struct Relay {
    address: SocketAddr,
    sockets: Receiver<(TcpStream, TcpStream)>,
    task: JoinHandle<()>,
}
impl Relay {
    fn spawn(target: SocketAddr) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (established, sockets) = sync_channel(1);
        let task = thread::spawn(move || {
            let (client, _) = listener.accept().unwrap();
            let server = TcpStream::connect(target).unwrap();
            tcp(&client);
            tcp(&server);
            established
                .send((client.try_clone().unwrap(), server.try_clone().unwrap()))
                .unwrap();
            fn pump(mut from: TcpStream, mut to: TcpStream) {
                let result = io::copy(&mut from, &mut to);
                let _ = to.shutdown(Shutdown::Write);
                if let Err(e) = result {
                    assert!(
                        matches!(
                            e.kind(),
                            io::ErrorKind::BrokenPipe
                                | io::ErrorKind::ConnectionReset
                                | io::ErrorKind::NotConnected
                        ),
                        "{e}"
                    );
                }
            }
            let client_read = client.try_clone().unwrap();
            let server_read = server.try_clone().unwrap();
            let upstream = thread::spawn(move || pump(client_read, server));
            pump(server_read, client);
            upstream.join().unwrap();
        });
        Self {
            address,
            sockets,
            task,
        }
    }
    fn cut(&self) {
        let sockets = self.sockets.recv_timeout(WAIT).unwrap();
        sockets.0.shutdown(Shutdown::Both).unwrap();
        sockets.1.shutdown(Shutdown::Both).unwrap();
    }
}

struct AckGateIo(TcpSessionIo, Option<PageGate>);
impl Read for AckGateIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
impl Write for AckGateIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl InterruptibleSessionIo for AckGateIo {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        self.0.interrupt_handle()
    }
}
struct AckGateWriter(TcpSessionWriter, Option<PageGate>);
impl Write for AckGateWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.get(4..8) == Some(b"NSR1") && buf.get(10) == Some(&(CANCEL | REPLY)) {
            if let Some((accepted, release)) = self.1.take() {
                accepted.send(()).unwrap();
                release.recv_timeout(WAIT).unwrap();
            }
        }
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl DuplexSessionIo for AckGateIo {
    type Reader = TcpSessionReader;
    type Writer = AckGateWriter;
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let (reader, writer) = self.0.split()?;
        Ok((reader, AckGateWriter(writer, self.1)))
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Ordinary,
    Stop,
    Late,
    Lost,
}
fn run_native(scenario: Scenario, relayed: bool) -> FollowCounters {
    let dir = TestDirectory::new();
    let db = dir.0.join("source.db");
    source(&db)
        .append(&Id::new("record").unwrap(), &[17; 1000])
        .unwrap();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let target = listener.local_addr().unwrap();
    let relay = relayed.then(|| Relay::spawn(target));
    let socket = TcpStream::connect(relay.as_ref().map(|r| r.address).unwrap_or(target)).unwrap();
    tcp(&socket);
    let (ready, prepared) = sync_channel(1);
    let (release_source, finish_source) = sync_channel(1);
    let gate = Arc::new(Mutex::new(
        matches!(scenario, Scenario::Stop | Scenario::Lost).then_some((ready, finish_source)),
    ));
    let (ack_ready, before_ack) = sync_channel(1);
    let (release_ack, finish_ack) = sync_channel(1);
    let ack_gate = matches!(scenario, Scenario::Lost).then_some((ack_ready, finish_ack));
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        tcp(&socket);
        serve_page_cancel_session(
            AckGateIo(TcpSessionIo::new(socket).unwrap(), ack_gate),
            7,
            LIMITS,
            Allow,
            move |_| {
                Ok(GatedSource {
                    inner: source(&db),
                    gate: gate.clone(),
                })
            },
        )
    });
    let (mut page, cancel) =
        connect_page_cancel_session(TcpSessionIo::new(socket).unwrap(), scope(), LIMITS, WAIT)
            .unwrap()
            .start(request())
            .unwrap();
    let mut close_called = false;
    let arrived = match scenario {
        Scenario::Ordinary => {
            drop(cancel);
            Some(page.wait().unwrap())
        }
        Scenario::Stop => {
            prepared.recv_timeout(WAIT).unwrap();
            let outcome = cancel.cancel();
            assert_eq!(
                outcome,
                PageCancelOutcome::Acknowledged(PageCancelReceipt::StoppedBeforeResponse)
            );
            let stopped = page.wait().unwrap();
            assert_eq!(
                stopped,
                CancelPageOutput::Stopped(PageCancelReceipt::StoppedBeforeResponse)
            );
            release_source.send(()).unwrap();
            None
        }
        Scenario::Late => {
            // A complete PAGE is physically received but still parked in
            // the original sole reader's one frame slot. ACK must be
            // observable independently, before the data consumer runs.
            let shared = &page.session.shared;
            let initial = shared.state.lock().unwrap();
            let (state, _) = shared
                .ready
                .wait_timeout_while(initial, WAIT, |state| {
                    state.frame.is_none() || !state.rpc.as_ref().is_some_and(|rpc| rpc.complete)
                })
                .unwrap();
            assert!(state.frame.is_some() && state.rpc.as_ref().unwrap().complete);
            drop(state);
            assert_eq!(
                cancel.cancel(),
                PageCancelOutcome::Acknowledged(PageCancelReceipt::TooLate)
            );
            Some(page.wait().unwrap())
        }
        Scenario::Lost => {
            prepared.recv_timeout(WAIT).unwrap();
            let owner = thread::spawn(move || cancel.cancel());
            // The real server computed its receipt, but this test holds
            // the outgoing frame before physical acceptance.
            before_ack.recv_timeout(WAIT).unwrap();
            if let Some(ref relay) = relay {
                relay.cut();
            } else {
                page.close().unwrap();
                close_called = true;
            }
            assert_eq!(owner.join().unwrap(), PageCancelOutcome::Uncertain);
            release_ack.send(()).unwrap();
            release_source.send(()).unwrap();
            assert!(page.wait().is_err());
            None
        }
    };
    if let Some(arrived) = arrived {
        let CancelPageOutput::Page(body) = arrived else {
            panic!("expected complete page")
        };
        assert_eq!(body.request, request());
        assert_eq!(body.records.len(), 1);
        assert_eq!(body.records[0].payload, vec![17; 1000]);
    }
    let counters = page.counters().unwrap();
    if !close_called {
        page.close().unwrap();
    }
    let ended = server.join().unwrap();
    if !matches!(scenario, Scenario::Lost) {
        ended.unwrap();
    } else if let Err(error) = ended {
        assert!(matches!(
            error,
            RecordSessionError::Io(_) | RecordSessionError::Closed
        ));
    }
    if let Some(relay) = relay {
        relay.task.join().unwrap();
    }
    eprintln!("single PAGE relayed={relayed} {scenario:?}: {} logical PAGE/setup exchanges / {} protocol / {} complete payload",
        counters.wire.exchanges,counters.wire.protocol_bytes,counters.wire.payload_bytes);
    counters
}

#[test]
fn native_one_frame_page_and_independent_control_match_on_direct_and_opaque_paths() {
    for relayed in [false, true] {
        let plain = run_native(Scenario::Ordinary, relayed).wire;
        let stopped = run_native(Scenario::Stop, relayed).wire;
        let late = run_native(Scenario::Late, relayed).wire;
        let uncertain = run_native(Scenario::Lost, relayed).wire;
        assert_eq!(plain.payload_bytes, 1000);
        assert_eq!(late.payload_bytes, plain.payload_bytes);
        assert_eq!(
            late.protocol_bytes - plain.protocol_bytes,
            2 * (4 + HEADER) as u64 + 1
        );
        assert_eq!(late.exchanges, plain.exchanges);
        assert_eq!(stopped.payload_bytes, 0);
        assert_eq!(uncertain.payload_bytes, 0);
    }
}

#[test]
fn explicitly_admitted_page_budget_cannot_require_a_second_response_frame() {
    let mut req = request();
    assert!(check_one_frame(&req, LIMITS.control_bytes as usize).is_ok());
    req.max_payload_bytes = LIMITS.control_bytes as usize;
    assert!(matches!(
        check_one_frame(&req, LIMITS.control_bytes as usize),
        Err(RecordSessionError::Source(SourceError::InvalidRequest))
    ));
}
