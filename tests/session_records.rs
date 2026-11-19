#![cfg(all(feature = "transport", feature = "sqlite"))]
use nessa_sync::replication::{
    application::{
        begin_pass, begin_pass_with_page, finish_pass, Access, RecordSource, ReplicaStore,
        ScopeAuthorizer, SourceError, StoreError, SyncError, MAX_RECORD_BYTES,
    },
    domain::{Checkpoint, CommitPlan, Id, Limits, Page, PageRequest, Scope},
    infrastructure::session::{
        read_control, serve_record_session, write_control, ControlFrame, RecordSession,
        RecordSessionError, SessionLimits,
    },
    infrastructure::{SqliteReferenceSource, SqliteReplicaStore},
};
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("device-a"),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("local-epoch-1"),
    )
}
fn with_epoch(epoch: &str) -> Scope {
    Scope::new(
        id("device-a"),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id(epoch),
    )
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings: 1,
    }
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        path,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )
    .unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        loop {
            let path = std::env::temp_dir().join(format!(
                "session-records-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => panic!("{e}"),
            }
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn seven(&self) -> PathBuf {
        let path = self.path("source.db");
        let mut source = source(&path);
        for (index, payload) in [
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"gamma".to_vec(),
            b"delta".to_vec(),
            b"epsilon".to_vec(),
            b"zeta".to_vec(),
            vec![b'p'; 4096],
        ]
        .into_iter()
        .enumerate()
        {
            source
                .append(&id(&format!("fact-{}", index + 1)), &payload)
                .unwrap();
        }
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Policy {
    mode: Arc<AtomicU8>,
    current: Arc<Mutex<Scope>>,
}
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, requested: &Scope) -> Access {
        let current = self.current.lock().unwrap().clone();
        if requested.receiver() != current.receiver() {
            return Access::Denied;
        }
        match self.mode.load(Ordering::SeqCst) {
            0 => Access::Allowed(current),
            1 => Access::Denied,
            2 => Access::Unverifiable,
            _ => Access::InvalidResponse,
        }
    }
}
struct LiveSource {
    inner: SqliteReferenceSource,
    live: Arc<AtomicUsize>,
    deny_after_head: Option<(Arc<AtomicU8>, usize)>,
    head_reads: Arc<AtomicUsize>,
    append_after_capture: bool,
}
impl Drop for LiveSource {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}
impl RecordSource for LiveSource {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        let head = self.inner.head(scope);
        let number = self.head_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((mode, trigger)) = &self.deny_after_head {
            if number == *trigger {
                mode.store(1, Ordering::SeqCst);
            }
        }
        if self.append_after_capture && number == 2 {
            self.inner
                .append(&id("after-capture"), b"after captured target")
                .unwrap();
        }
        head
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.inner.page(request)
    }
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    TruncatePage,
    DenyAfterFragment,
    WrongVersion,
    DenyAfterHead,
    DenyAfterCapture,
    AppendAfterCapture,
    BadOpenEcho(u8),
    BadBound(u8),
}
struct Channel {
    socket: TcpStream,
    live: Arc<AtomicUsize>,
    mode: Arc<AtomicU8>,
    fault: Fault,
    fired: bool,
    broken: bool,
}
impl Read for Channel {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.socket.read(out)
    }
}
impl Write for Channel {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(
            self.live.load(Ordering::SeqCst),
            0,
            "source handle retained during network write"
        );
        if bytes.len() > 12 && &bytes[4..8] == b"NSC1" && bytes[10] == 4 {
            if let Fault::BadBound(case) = self.fault {
                let mut changed = bytes.to_vec();
                match case {
                    0 => {
                        self.socket.write_all(&changed[..12])?;
                        self.socket.shutdown(Shutdown::Both)?;
                        self.broken = true;
                        return Ok(12);
                    }
                    1 => changed[11..19].copy_from_slice(&999_u64.to_be_bytes()),
                    2 => changed[19..27].copy_from_slice(&999_u64.to_be_bytes()),
                    3 => changed[40] = b'X',
                    4 => {
                        changed.clear();
                        write_control(
                            &mut changed,
                            &ControlFrame::Welcome {
                                session: 1,
                                limits: limits(),
                                record_bytes: MAX_RECORD_BYTES as u32,
                            },
                            8192,
                        )
                        .unwrap();
                    }
                    6 => changed.extend_from_slice(bytes),
                    _ => {
                        self.socket.shutdown(Shutdown::Both)?;
                        return Ok(bytes.len());
                    }
                }
                self.socket.write_all(&changed)?;
                return Ok(bytes.len());
            }
        }
        if self.broken {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if matches!(self.fault, Fault::WrongVersion)
            && !self.fired
            && bytes.len() > 12
            && &bytes[4..8] == b"NSR1"
        {
            self.fired = true;
            let mut changed = bytes.to_vec();
            changed[9] = 2;
            self.socket.write_all(&changed)?;
            return Ok(bytes.len());
        }
        if bytes.len() > 48
            && &bytes[4..8] == b"NSR1"
            && bytes[10] == 132
            && bytes[11] == 0
            && !self.fired
        {
            if let Fault::BadOpenEcho(case) = self.fault {
                self.fired = true;
                let scope = scope();
                let scope_len = [
                    scope.receiver(),
                    scope.origin(),
                    scope.stream(),
                    scope.incarnation(),
                    scope.schema(),
                    scope.access_epoch(),
                ]
                .iter()
                .map(|id| 1 + id.as_str().len())
                .sum::<usize>();
                let after = 48 + 1 + scope_len;
                let mut changed = bytes.to_vec();
                match case {
                    0 => changed[after..after + 8].copy_from_slice(&1_u64.to_be_bytes()),
                    1 => changed[after + 8..after + 16].copy_from_slice(&0_u64.to_be_bytes()),
                    _ => {
                        let count = after + 40;
                        changed[count] = 0;
                        changed.truncate(count + 1);
                        let body = (changed.len() - 4) as u32;
                        let payload = (changed.len() - 48) as u32;
                        changed[..4].copy_from_slice(&body.to_be_bytes());
                        changed[40..44].copy_from_slice(&payload.to_be_bytes());
                    }
                }
                self.socket.write_all(&changed)?;
                return Ok(bytes.len());
            }
        }
        let page = bytes.len() > 12
            && &bytes[4..8] == b"NSR1"
            && [131, 132].contains(&bytes[10])
            && bytes[11] == 0;
        if page && !self.fired {
            self.fired = true;
            match self.fault {
                Fault::TruncatePage => {
                    self.socket.write_all(&bytes[..12])?;
                    self.socket.shutdown(Shutdown::Both)?;
                    self.broken = true;
                    return Ok(12);
                }
                Fault::DenyAfterFragment => {
                    self.socket.write_all(bytes)?;
                    self.mode.store(1, Ordering::SeqCst);
                    return Ok(bytes.len());
                }
                Fault::None
                | Fault::WrongVersion
                | Fault::DenyAfterHead
                | Fault::BadBound(_)
                | Fault::DenyAfterCapture
                | Fault::AppendAfterCapture
                | Fault::BadOpenEcho(_) => {}
            }
        }
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}
struct Server {
    address: SocketAddrV4,
    join: thread::JoinHandle<()>,
    mode: Arc<AtomicU8>,
    current: Arc<Mutex<Scope>>,
    opens: Arc<AtomicUsize>,
}
impl Server {
    fn spawn(path: PathBuf, connections: usize, fault: Fault) -> Self {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let mode = Arc::new(AtomicU8::new(0));
        let current = Arc::new(Mutex::new(scope()));
        let opens = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(0));
        let head_reads = Arc::new(AtomicUsize::new(0));
        let (m, c, o) = (mode.clone(), current.clone(), opens.clone());
        let join = thread::spawn(move || {
            for number in 1..=connections {
                let (mut socket, _) = listener.accept().unwrap();
                socket.set_nodelay(true).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                // Fixture-only channel authentication precedes policy construction.
                // Production callers must supply their real authenticated channel.
                let mut length = [0];
                socket.read_exact(&mut length).unwrap();
                let mut token = vec![0; length[0] as usize];
                socket.read_exact(&mut token).unwrap();
                if token != b"fixture-read" {
                    socket.write_all(&[1]).unwrap();
                    continue;
                }
                socket.write_all(&[0]).unwrap();
                let mut channel = Channel {
                    socket,
                    live: live.clone(),
                    mode: m.clone(),
                    fault,
                    fired: false,
                    broken: false,
                };
                let mut policy = Policy {
                    mode: m.clone(),
                    current: c.clone(),
                };
                let result = serve_record_session(
                    &mut channel,
                    number as u64,
                    limits(),
                    &mut policy,
                    |_| {
                        o.fetch_add(1, Ordering::SeqCst);
                        let inner = SqliteReferenceSource::open(
                            &path,
                            id("example-origin"),
                            id("transcript"),
                            id("first"),
                            id("text-v1"),
                        )
                        .map_err(|_| SourceError::Unavailable)?;
                        live.fetch_add(1, Ordering::SeqCst);
                        Ok(LiveSource {
                            inner,
                            live: live.clone(),
                            deny_after_head: match fault {
                                Fault::DenyAfterHead => Some((m.clone(), 1)),
                                Fault::DenyAfterCapture => Some((m.clone(), 2)),
                                _ => None,
                            },
                            head_reads: head_reads.clone(),
                            append_after_capture: matches!(fault, Fault::AppendAfterCapture),
                        })
                    },
                );
                if !matches!(fault, Fault::TruncatePage | Fault::BadBound(_)) {
                    result.unwrap();
                }
                assert_eq!(live.load(Ordering::SeqCst), 0);
            }
        });
        Self {
            address,
            join,
            mode,
            current,
            opens,
        }
    }
    fn connect(&self, requested: Scope) -> RecordSession<TcpStream> {
        self.connect_token(requested, b"fixture-read").unwrap()
    }
    fn connect_token(
        &self,
        requested: Scope,
        token: &[u8],
    ) -> Result<RecordSession<TcpStream>, RecordSessionError> {
        self.connect_mode(requested, token, false)
    }
    fn connect_mode(
        &self,
        requested: Scope,
        token: &[u8],
        opening: bool,
    ) -> Result<RecordSession<TcpStream>, RecordSessionError> {
        let mut socket = TcpStream::connect(self.address)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        socket.set_write_timeout(Some(Duration::from_secs(5)))?;
        socket.write_all(&[token.len() as u8])?;
        socket.write_all(token)?;
        let mut ack = [0];
        socket.read_exact(&mut ack)?;
        if ack != [0] {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
        }
        if opening {
            RecordSession::connect_open(socket, requested, limits())
        } else {
            RecordSession::connect(socket, requested, limits())
        }
    }
    fn finish(self) {
        self.join.join().unwrap();
    }
}
fn catch_up<A: ScopeAuthorizer, S: RecordSource, D: ReplicaStore>(
    requested: &Scope,
    a: &mut A,
    s: &mut S,
    d: &mut D,
    record_bytes: usize,
) -> Result<u64, SyncError> {
    let mut pass = begin_pass(requested, a, s, d)?;
    loop {
        finish_pass(
            &mut pass,
            Limits::new(32, record_bytes, record_bytes).unwrap(),
            a,
            s,
            d,
        )?;
        let applied = pass.position();
        pass = begin_pass(requested, a, s, d)?; // Deliberate post-pass head recheck.
        if pass.is_complete() {
            return Ok(applied);
        }
    }
}

#[test]
fn real_tcp_records_commit_atomically_to_reopened_sqlite_cache() {
    let fixture = Fixture::new();
    let source = fixture.seven();
    let replica = fixture.path("replica.db");
    let server = Server::spawn(source, 1, Fault::None);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    {
        let mut store = SqliteReplicaStore::open(&replica).unwrap();
        assert_eq!(
            catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
            Ok(7)
        );
    }
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    let records = store.read_after(&scope(), 0, 64, MAX_RECORD_BYTES).unwrap();
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 7);
    assert_eq!(
        records.iter().map(|r| r.position).collect::<Vec<_>>(),
        (1..=7).collect::<Vec<_>>()
    );
    assert_eq!(
        records.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["fact-1", "fact-2", "fact-3", "fact-4", "fact-5", "fact-6", "fact-7"]
    );
    assert_eq!(records.iter().map(|r| r.payload.len()).sum::<usize>(), 4126);
    let before = session.counters().unwrap();
    assert_eq!(before.exchanges, 8);
    assert_eq!(before.payload_bytes, 4126);
    assert_eq!(before.duplicate_bytes, 0);
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
        Ok(7)
    );
    let after = session.counters().unwrap();
    assert_eq!(after.exchanges - before.exchanges, 4);
    assert_eq!(after.payload_bytes, before.payload_bytes);
    session.close().unwrap();
    server.finish();
}

#[test]
fn exact_source_ceiling_fragments_but_no_partial_page_becomes_progress() {
    let fixture = Fixture::new();
    let path = fixture.path("source.db");
    {
        let mut src = source(&path);
        assert!(src
            .append(&id("oversize"), &vec![0; MAX_RECORD_BYTES + 1])
            .is_err());
        src.append(&id("exact"), &vec![7; MAX_RECORD_BYTES])
            .unwrap();
    }
    let replica = fixture.path("replica.db");
    let server = Server::spawn(path, 1, Fault::DenyAfterFragment);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, MAX_RECORD_BYTES),
        Err(SyncError::Source(SourceError::Denied))
    );
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert!(store
        .read_after(&scope(), 0, 64, MAX_RECORD_BYTES)
        .unwrap()
        .is_empty());
    assert_eq!(session.counters().unwrap().payload_bytes, 0);
    server.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, MAX_RECORD_BYTES),
        Ok(1)
    );
    drop(store);
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        store.read_after(&scope(), 0, 64, MAX_RECORD_BYTES).unwrap()[0].payload,
        vec![7; MAX_RECORD_BYTES]
    );
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 1);
    session.close().unwrap();
    server.finish();
}

#[test]
fn partial_reply_disconnect_never_advances_durable_cursor_and_explicit_reconnect_recovers() {
    let fixture = Fixture::new();
    let path = fixture.seven();
    let replica = fixture.path("replica.db");
    let broken = Server::spawn(path.clone(), 1, Fault::TruncatePage);
    let session = broken.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
        Err(SyncError::Source(SourceError::Unavailable))
    );
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert!(store
        .read_after(&scope(), 0, 64, MAX_RECORD_BYTES)
        .unwrap()
        .is_empty());
    assert_eq!(a.authorize(&scope()), Access::Unverifiable);
    session.close().unwrap();
    broken.finish();
    drop(store);
    let restored = Server::spawn(path, 1, Fault::None);
    let session = restored.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
        Ok(7)
    );
    assert_eq!(
        store
            .read_after(&scope(), 0, 64, MAX_RECORD_BYTES)
            .unwrap()
            .len(),
        7
    );
    session.close().unwrap();
    restored.finish();
}

#[test]
fn current_authority_precedes_cache_load_and_each_source_operation() {
    for (mode, expected) in [
        (1, SyncError::Denied),
        (2, SyncError::Unverifiable),
        (3, SyncError::InvalidAccessResponse),
    ] {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::None);
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        server.mode.store(mode, Ordering::SeqCst);
        struct NoLoad;
        impl ReplicaStore for NoLoad {
            fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
                panic!("policy refusal reached local progress")
            }
            fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
                panic!("policy refusal reached commit")
            }
        }
        assert_eq!(
            begin_pass(&scope(), &mut a, &mut s, &mut NoLoad),
            Err(expected)
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 1); // Authorized Bind metadata only.
        session.close().unwrap();
        server.finish();
    }
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::None);
    let session = server.connect(scope());
    let (_, mut s) = session.parts();
    server.mode.store(1, Ordering::SeqCst);
    assert_eq!(s.head(&scope()), Err(SourceError::Denied));
    assert_eq!(server.opens.load(Ordering::SeqCst), 1);
    session.close().unwrap();
    server.finish();
}

#[test]
fn binding_refusals_do_not_open_source_or_return_read_ports() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 5, Fault::None);
    let bad_receiver = Scope::new(
        id("other"),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("local-epoch-1"),
    );
    assert!(matches!(
        server.connect_token(bad_receiver, b"fixture-read"),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
    assert!(matches!(
        server.connect_token(with_epoch("stale"), b"fixture-read"),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert!(
        matches!(server.connect_token(scope(),b"wrong-fixture-token"),Err(RecordSessionError::Io(e)) if e.kind()==io::ErrorKind::PermissionDenied)
    );
    for requested in [
        Scope::new(
            id("device-a"),
            id("example-origin"),
            id("transcript"),
            id("old"),
            id("text-v1"),
            id("local-epoch-1"),
        ),
        Scope::new(
            id("device-a"),
            id("example-origin"),
            id("transcript"),
            id("first"),
            id("old-schema"),
            id("local-epoch-1"),
        ),
    ] {
        assert!(matches!(
            server.connect_token(requested, b"fixture-read"),
            Err(RecordSessionError::Source(SourceError::IdentityChanged))
        ));
    }
    assert_eq!(server.opens.load(Ordering::SeqCst), 0);
    server.finish();
}

#[test]
fn epoch_change_stops_old_session_and_new_binding_does_not_reset_saved_scope() {
    let fixture = Fixture::new();
    let path = fixture.seven();
    let replica = fixture.path("replica.db");
    let server = Server::spawn(path, 2, Fault::None);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
        Ok(7)
    );
    *server.current.lock().unwrap() = with_epoch("new-epoch");
    assert_eq!(
        begin_pass(&scope(), &mut a, &mut s, &mut store),
        Err(SyncError::WrongAccessScope)
    );
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 7);
    session.close().unwrap();
    let next = server.connect(with_epoch("new-epoch"));
    let (mut a, mut s) = next.parts();
    assert!(matches!(
        begin_pass(&with_epoch("new-epoch"), &mut a, &mut s, &mut store),
        Err(SyncError::Store(StoreError::ScopeMismatch { .. }))
    ));
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 7);
    next.close().unwrap();
    server.finish();
}

struct CommitBoundary {
    inner: SqliteReplicaStore,
    after: bool,
    once: bool,
}
impl ReplicaStore for CommitBoundary {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.inner.load(scope)
    }
    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        if !self.once {
            return self.inner.apply(plan);
        }
        self.once = false;
        if self.after {
            self.inner.apply(plan)?;
            Err(StoreError::Uncertain)
        } else {
            Err(StoreError::Failed)
        }
    }
}
#[test]
fn commit_failure_and_lost_commit_confirmation_resume_from_actual_saved_progress() {
    for after in [false, true] {
        let fixture = Fixture::new();
        let replica = fixture.path("replica.db");
        let server = Server::spawn(fixture.seven(), 2, Fault::None);
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        let mut store = CommitBoundary {
            inner: SqliteReplicaStore::open(&replica).unwrap(),
            after,
            once: true,
        };
        assert_eq!(
            catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024),
            Err(SyncError::Store(if after {
                StoreError::Uncertain
            } else {
                StoreError::Failed
            }))
        );
        session.close().unwrap();
        drop(store);
        let mut reopened = SqliteReplicaStore::open(&replica).unwrap();
        assert_eq!(
            reopened
                .checkpoint(&scope())
                .unwrap()
                .map_or(0, |c| c.position()),
            if after { 7 } else { 0 }
        );
        let retry = server.connect(scope());
        let (mut a, mut s) = retry.parts();
        assert_eq!(
            catch_up(&scope(), &mut a, &mut s, &mut reopened, 128 * 1024),
            Ok(7)
        );
        assert_eq!(
            reopened
                .read_after(&scope(), 0, 64, MAX_RECORD_BYTES)
                .unwrap()
                .len(),
            7
        );
        assert_eq!(
            retry.counters().unwrap().payload_bytes,
            if after { 0 } else { 4126 }
        );
        retry.close().unwrap();
        server.finish();
    }
}

#[test]
fn minimum_negotiated_fragment_budget_delivers_the_exact_append_ceiling() {
    let fixture = Fixture::new();
    let path = fixture.path("source.db");
    source(&path)
        .append(&id("exact"), &vec![3; MAX_RECORD_BYTES])
        .unwrap();
    let server = Server::spawn(path, 1, Fault::None);
    let mut socket = TcpStream::connect(server.address).unwrap();
    socket.set_nodelay(true).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket.write_all(&[12]).unwrap();
    socket.write_all(b"fixture-read").unwrap();
    let mut ack = [0];
    socket.read_exact(&mut ack).unwrap();
    assert_eq!(ack, [0]);
    let session = RecordSession::connect(
        socket,
        scope(),
        SessionLimits {
            control_bytes: 2048,
            bindings: 1,
        },
    )
    .unwrap();
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
    assert_eq!(
        catch_up(&scope(), &mut a, &mut s, &mut store, MAX_RECORD_BYTES),
        Ok(1)
    );
    assert_eq!(
        store.read_after(&scope(), 0, 1, MAX_RECORD_BYTES).unwrap()[0].payload,
        vec![3; MAX_RECORD_BYTES]
    );
    session.close().unwrap();
    server.finish();
}

#[test]
fn invalid_backend_gap_or_duplicate_identity_never_reaches_durable_cache() {
    use nessa_sync::replication::domain::Record;
    struct BadSource {
        duplicate: bool,
    }
    impl RecordSource for BadSource {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            Ok(2)
        }
        fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
            let record = |position| Record {
                position,
                id: id("same"),
                scope: request.scope.clone(),
                payload: b"fixture".to_vec(),
            };
            Ok(Page {
                request: request.clone(),
                records: if self.duplicate {
                    vec![record(1), record(2)]
                } else {
                    vec![record(2)]
                },
            })
        }
    }
    for (duplicate, first) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = Fixture::new();
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut policy = Policy {
                mode: Arc::new(AtomicU8::new(0)),
                current: Arc::new(Mutex::new(scope())),
            };
            // The test owns both endpoints; no public listener or production authentication claim.
            serve_record_session(&mut socket, 1, limits(), &mut policy, |_| {
                Ok(BadSource { duplicate })
            })
            .unwrap();
        });
        let socket = TcpStream::connect(address).unwrap();
        socket.set_nodelay(true).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let session = RecordSession::connect(socket, scope(), limits()).unwrap();
        let (mut a, mut s) = session.parts();
        let mut store = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
        assert_eq!(
            if first {
                begin_pass_with_page(
                    &scope(),
                    Limits::new(32, 128 * 1024, 128 * 1024).unwrap(),
                    &mut a,
                    &mut s,
                    &mut store,
                )
                .map(|p| p.position())
            } else {
                catch_up(&scope(), &mut a, &mut s, &mut store, 128 * 1024)
            },
            Err(SyncError::Source(if duplicate {
                SourceError::IdentityChanged
            } else {
                SourceError::InvalidRequest
            }))
        );
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        assert_eq!(store.count(&scope()).unwrap(), 0);
        session.close().unwrap();
        server.join().unwrap();
    }
}

#[test]
fn unsupported_wire_version_is_fatal_before_local_progress_access() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::WrongVersion);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    struct NoProgress;
    impl ReplicaStore for NoProgress {
        fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
            panic!("invalid version reached progress")
        }
        fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
            panic!("invalid version reached commit")
        }
    }
    assert_eq!(
        begin_pass(&scope(), &mut a, &mut s, &mut NoProgress),
        Err(SyncError::InvalidAccessResponse)
    );
    assert_eq!(a.authorize(&scope()), Access::Unverifiable);
    assert_eq!(server.opens.load(Ordering::SeqCst), 1);
    session.close().unwrap();
    server.finish();
}

#[test]
fn combined_open_preserves_real_durable_catch_up_and_current_access_checks() {
    let mut opening_bytes = Vec::new();
    for opening in [false, true] {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::None);
        let session = server
            .connect_mode(scope(), b"fixture-read", opening)
            .unwrap();
        assert_eq!(
            session.counters().unwrap().exchanges,
            if opening { 1 } else { 2 }
        );
        opening_bytes.push(session.counters().unwrap().protocol_bytes);
        let (mut a, mut s) = session.parts();
        let mut store = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
        assert_eq!(
            catch_up(&scope(), &mut a, &mut s, &mut store, MAX_RECORD_BYTES),
            Ok(7)
        );
        assert_eq!(
            session.counters().unwrap().exchanges,
            if opening { 7 } else { 8 }
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 4);
        server.mode.store(1, Ordering::SeqCst);
        assert_eq!(
            begin_pass(&scope(), &mut a, &mut s, &mut store),
            Err(SyncError::Denied)
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 4);
        drop(store);
        let reopened = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
        assert_eq!(
            reopened.checkpoint(&scope()).unwrap().unwrap().position(),
            7
        );
        assert_eq!(reopened.count(&scope()).unwrap(), 7);
        session.close().unwrap();
        server.finish();
    }
    assert_eq!(opening_bytes[0] - opening_bytes[1], 19);
}

#[test]
fn combined_open_refuses_before_factory_and_rechecks_before_bound() {
    for (mode, error) in [
        (1, SourceError::Denied),
        (2, SourceError::Unavailable),
        (3, SourceError::InvalidRequest),
    ] {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::None);
        server.mode.store(mode, Ordering::SeqCst);
        assert!(
            matches!(server.connect_mode(scope(), b"fixture-read", true), Err(RecordSessionError::Source(e)) if e == error)
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);
        server.finish();
    }
    let other = Scope::new(
        id("other"),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("local-epoch-1"),
    );
    for (requested, expected) in [
        (with_epoch("stale"), SourceError::IdentityChanged),
        (other, SourceError::Denied),
    ] {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::None);
        assert!(
            matches!(server.connect_mode(requested, b"fixture-read", true), Err(RecordSessionError::Source(e)) if e == expected)
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 0);
        server.finish();
    }
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::DenyAfterHead);
    assert!(matches!(
        server.connect_mode(scope(), b"fixture-read", true),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
    assert_eq!(server.opens.load(Ordering::SeqCst), 1);
    server.finish();
}

#[test]
fn combined_open_never_exposes_ports_on_missing_duplicate_or_mismatched_bound() {
    for case in 0..6 {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::BadBound(case));
        assert!(
            server.connect_mode(scope(), b"fixture-read", true).is_err(),
            "case {case}"
        );
        assert_eq!(server.opens.load(Ordering::SeqCst), 1);
        server.finish();
    }
}

#[test]
fn unsupported_combined_open_closes_without_fallback() {
    // Trusted test endpoint, no source/cache or authentication service involved.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let peer = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert!(matches!(
            read_control(&mut socket, 2048).unwrap(),
            ControlFrame::Open { request: 1, .. }
        ));
        socket.write_all(b"\0\0\0\x07NSC1\0\x01\xff").unwrap();
        let mut next = [0];
        assert_eq!(
            socket.read(&mut next).unwrap(),
            0,
            "unexpected fallback request"
        );
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert!(matches!(
        RecordSession::connect_open(socket, scope(), limits()),
        Err(RecordSessionError::Protocol)
    ));
    peer.join().unwrap();
}

#[test]
fn duplicate_bound_cannot_be_used_as_later_authorization() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::BadBound(6));
    let session = server.connect_mode(scope(), b"fixture-read", true).unwrap();
    let (mut a, mut s) = session.parts();
    struct NoLoad;
    impl ReplicaStore for NoLoad {
        fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
            panic!("late binding reached local progress")
        }
        fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
            panic!("late binding reached commit")
        }
    }
    assert_eq!(
        begin_pass(&scope(), &mut a, &mut s, &mut NoLoad),
        Err(SyncError::InvalidAccessResponse)
    );
    assert_eq!(server.opens.load(Ordering::SeqCst), 1);
    session.close().unwrap();
    server.finish();
}

#[test]
fn open_page_finite_target_survives_append_and_post_pass_recheck_finds_it() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::AppendAfterCapture);
    let session = server.connect_mode(scope(), b"fixture-read", true).unwrap();
    let (mut a, mut s) = session.parts();
    let path = fixture.path("replica.db");
    let mut store = SqliteReplicaStore::open(&path).unwrap();
    let budget = Limits::new(2, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap();
    let mut pass = begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut store).unwrap();
    assert_eq!((pass.position(), pass.target()), (2, 7));
    finish_pass(&mut pass, budget, &mut a, &mut s, &mut store).unwrap();
    assert_eq!(pass.position(), 7);
    let mut recheck = begin_pass(&scope(), &mut a, &mut s, &mut store).unwrap();
    assert_eq!(recheck.target(), 8);
    finish_pass(&mut recheck, budget, &mut a, &mut s, &mut store).unwrap();
    assert_eq!(session.counters().unwrap().head_checks, 2);
    drop(store);
    let reopened = SqliteReplicaStore::open(&path).unwrap();
    assert_eq!(
        reopened.checkpoint(&scope()).unwrap().unwrap().position(),
        8
    );
    assert_eq!(reopened.count(&scope()).unwrap(), 8);
    session.close().unwrap();
    server.finish();
}
#[test]
fn open_page_policy_change_during_load_refuses_before_capture() {
    struct LoadChange {
        inner: SqliteReplicaStore,
        mode: Arc<AtomicU8>,
    }
    impl ReplicaStore for LoadChange {
        fn load(&mut self, s: &Scope) -> Result<Option<Checkpoint>, StoreError> {
            let saved = self.inner.load(s)?;
            self.mode.store(1, Ordering::SeqCst);
            Ok(saved)
        }
        fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
            panic!("denied apply")
        }
    }
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::None);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = LoadChange {
        inner: SqliteReplicaStore::open(fixture.path("replica.db")).unwrap(),
        mode: server.mode.clone(),
    };
    assert_eq!(
        begin_pass_with_page(
            &scope(),
            Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap(),
            &mut a,
            &mut s,
            &mut store
        ),
        Err(SyncError::Denied)
    );
    assert_eq!(server.opens.load(Ordering::SeqCst), 1);
    assert_eq!(session.counters().unwrap().head_checks, 0);
    session.close().unwrap();
    server.finish();
}
#[test]
fn open_page_policy_change_after_capture_refuses_before_page_factory() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::DenyAfterCapture);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    let mut store = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
    assert_eq!(
        begin_pass_with_page(
            &scope(),
            Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap(),
            &mut a,
            &mut s,
            &mut store
        ),
        Err(SyncError::Source(SourceError::Denied))
    );
    assert_eq!(server.opens.load(Ordering::SeqCst), 2);
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    session.close().unwrap();
    server.finish();
}
#[test]
fn open_page_fragment_refusal_or_drop_cannot_commit_partial_records_or_target() {
    for fault in [Fault::DenyAfterFragment, Fault::TruncatePage] {
        let fixture = Fixture::new();
        let path = fixture.path("source.db");
        source(&path)
            .append(&id("large"), &vec![3; MAX_RECORD_BYTES])
            .unwrap();
        let server = Server::spawn(path.clone(), 1, fault);
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        let replica = fixture.path("replica.db");
        let mut store = SqliteReplicaStore::open(&replica).unwrap();
        assert!(begin_pass_with_page(
            &scope(),
            Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap(),
            &mut a,
            &mut s,
            &mut store
        )
        .is_err());
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        assert_eq!(store.count(&scope()).unwrap(), 0);
        session.close().unwrap();
        server.finish();
        drop(store);
        let server = Server::spawn(path, 1, Fault::None);
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        let mut reopened = SqliteReplicaStore::open(&replica).unwrap();
        let pass = begin_pass_with_page(
            &scope(),
            Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap(),
            &mut a,
            &mut s,
            &mut reopened,
        )
        .unwrap();
        assert_eq!(pass.position(), 1);
        assert!(pass.is_complete());
        assert_eq!(reopened.count(&scope()).unwrap(), 1);
        session.close().unwrap();
        server.finish();
    }
}
#[test]
fn open_page_failed_or_uncertain_commit_reloads_real_sqlite_progress() {
    for after in [false, true] {
        let fixture = Fixture::new();
        let replica = fixture.path("replica.db");
        let server = Server::spawn(fixture.seven(), 2, Fault::None);
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        let mut store = CommitBoundary {
            inner: SqliteReplicaStore::open(&replica).unwrap(),
            after,
            once: true,
        };
        let budget = Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap();
        assert_eq!(
            begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut store),
            Err(SyncError::Store(if after {
                StoreError::Uncertain
            } else {
                StoreError::Failed
            }))
        );
        session.close().unwrap();
        drop(store);
        let mut reopened = SqliteReplicaStore::open(&replica).unwrap();
        assert_eq!(
            reopened
                .checkpoint(&scope())
                .unwrap()
                .map_or(0, |c| c.position()),
            if after { 7 } else { 0 }
        );
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        assert_eq!(
            begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut reopened)
                .unwrap()
                .position(),
            7
        );
        assert_eq!(reopened.count(&scope()).unwrap(), 7);
        assert_eq!(
            session.counters().unwrap().payload_bytes,
            if after { 0 } else { 4126 }
        );
        session.close().unwrap();
        server.finish();
    }
}
#[test]
fn open_page_empty_has_no_apply_and_behind_progress_preserves_saved_cache() {
    let fixture = Fixture::new();
    let server = Server::spawn(fixture.seven(), 1, Fault::None);
    let session = server.connect(scope());
    let (mut a, mut s) = session.parts();
    let replica = fixture.path("replica.db");
    let mut store = SqliteReplicaStore::open(&replica).unwrap();
    let budget = Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap();
    assert_eq!(
        begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut store)
            .unwrap()
            .position(),
        7
    );
    let before = session.counters().unwrap();
    let pass = begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut store).unwrap();
    let after = session.counters().unwrap();
    assert!(pass.is_complete());
    assert_eq!(after.exchanges - before.exchanges, 3);
    assert_eq!(after.payload_bytes - before.payload_bytes, 0);
    assert_eq!(store.count(&scope()).unwrap(), 7);
    let req = PageRequest {
        scope: scope(),
        after: 7,
        target: 8,
        max_records: 1,
        max_payload_bytes: 16,
        max_record_bytes: 16,
    };
    let record = nessa_sync::replication::domain::Record {
        scope: scope(),
        id: id("fixture-eight"),
        position: 8,
        payload: vec![8],
    };
    let plan = nessa_sync::replication::domain::validate_page(
        &Checkpoint::new(scope(), 7),
        &req,
        Page {
            request: req.clone(),
            records: vec![record],
        },
        Limits::new(1, 16, 16).unwrap(),
    )
    .unwrap();
    store.apply(plan).unwrap();
    let before = server.opens.load(Ordering::SeqCst);
    assert_eq!(
        begin_pass_with_page(&scope(), budget, &mut a, &mut s, &mut store),
        Err(SyncError::SourceBehindCheckpoint)
    );
    assert_eq!(server.opens.load(Ordering::SeqCst) - before, 1);
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 8);
    assert_eq!(store.count(&scope()).unwrap(), 8);
    session.close().unwrap();
    server.finish();
}
#[test]
fn open_page_wire_echo_target_and_empty_before_target_are_validated() {
    for case in 0..3 {
        let fixture = Fixture::new();
        let server = Server::spawn(fixture.seven(), 1, Fault::BadOpenEcho(case));
        let session = server.connect(scope());
        let (mut a, mut s) = session.parts();
        let mut store = SqliteReplicaStore::open(fixture.path("replica.db")).unwrap();
        assert!(begin_pass_with_page(
            &scope(),
            Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap(),
            &mut a,
            &mut s,
            &mut store
        )
        .is_err());
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        assert_eq!(store.count(&scope()).unwrap(), 0);
        session.close().unwrap();
        server.finish();
    }
}
