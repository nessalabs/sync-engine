#![cfg(all(feature = "transport", feature = "sqlite"))]
use nessa_sync::replication::{
    application::{
        begin_pass, begin_pass_with_page, finish_pass, Access, OpenRecordSource, RecordReadView,
        RecordSource, ScopeAuthorizer, SourceError, SyncError,
    },
    domain::{
        CapabilityError, CapabilitySupport, Id, Limits, Page, PageRequest, Scope, SourceHead,
    },
    infrastructure::session::{
        serve_capability_record_session, serve_record_session, CapabilityRecordSession,
        RecordSession, SessionLimits, TcpSessionIo,
    },
    infrastructure::{SqliteReferenceSource, SqliteReplicaStore},
};
use std::{
    fs,
    io::{Read, Write},
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
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
fn budget() -> Limits {
    Limits::new(32, 32 * 1024, 32 * 1024).unwrap()
}
fn source(p: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(p, id("origin"), id("stream"), id("first"), id("schema")).unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-cap-wire-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self, n: &str) -> PathBuf {
        self.0.join(n)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Clone, Default)]
struct Requirements {
    reader: u64,
    writer: u64,
    version: Option<u16>,
    wrong_scope: bool,
}
struct View {
    source: SqliteReferenceSource,
    requirements: Requirements,
    pages: Arc<AtomicUsize>,
    heads: Arc<AtomicUsize>,
    policy: Arc<AtomicU8>,
    after_head: Arc<AtomicU8>,
}
impl RecordSource for View {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        self.source.head(s)
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        self.pages.fetch_add(1, Ordering::SeqCst);
        self.source.page(r)
    }
}
impl RecordReadView for View {
    fn view_head(&mut self, s: &Scope) -> Result<SourceHead, SourceError> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        let position = self.source.head(s)?;
        let mode = self.after_head.load(Ordering::SeqCst);
        if mode != 0 {
            self.policy.store(mode, Ordering::SeqCst);
        }
        let echo = if self.requirements.wrong_scope {
            Scope::new(
                id("other"),
                id("origin"),
                id("stream"),
                id("first"),
                id("schema"),
                id("epoch"),
            )
        } else {
            s.clone()
        };
        Ok(SourceHead::advertised(
            echo,
            self.requirements.version.unwrap_or(1),
            position,
            self.requirements.reader,
            self.requirements.writer,
        ))
    }
}
struct Policy(Arc<AtomicU8>);
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        match self.0.load(Ordering::SeqCst) {
            0 => Access::Allowed(s.clone()),
            1 => Access::Denied,
            2 => Access::Unverifiable,
            3 => Access::InvalidResponse,
            _ => Access::IdentityChanged,
        }
    }
}
struct Server {
    address: SocketAddr,
    requirements: Arc<Mutex<Requirements>>,
    pages: Arc<AtomicUsize>,
    heads: Arc<AtomicUsize>,
    after_head: Arc<AtomicU8>,
    task: thread::JoinHandle<()>,
}
fn tune(s: &TcpStream) {
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
}
impl Server {
    fn new(path: PathBuf, connections: usize, capable: bool) -> Self {
        drop(source(&path));
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let requirements = Arc::new(Mutex::new(Requirements::default()));
        let req = requirements.clone();
        let pages = Arc::new(AtomicUsize::new(0));
        let p = pages.clone();
        let heads = Arc::new(AtomicUsize::new(0));
        let h = heads.clone();
        let after_head = Arc::new(AtomicU8::new(0));
        let after = after_head.clone();
        let task = thread::spawn(move || {
            for n in 1..=connections {
                let (mut socket, _) = listener.accept().unwrap();
                tune(&socket);
                let mut auth = [0];
                socket.read_exact(&mut auth).unwrap();
                assert_eq!(auth, [42]);
                socket.write_all(&[0]).unwrap();
                let mode = Arc::new(AtomicU8::new(0));
                let mut policy = Policy(mode.clone());
                let mut factory = |_: &Scope| {
                    Ok(View {
                        source: source(&path),
                        requirements: req.lock().unwrap().clone(),
                        pages: p.clone(),
                        heads: h.clone(),
                        policy: mode.clone(),
                        after_head: after.clone(),
                    })
                };
                if capable {
                    let _ = serve_capability_record_session(
                        &mut socket,
                        n as u64,
                        limits(),
                        &mut policy,
                        &mut factory,
                    );
                } else {
                    let _ = serve_record_session(
                        &mut socket,
                        n as u64,
                        limits(),
                        &mut policy,
                        &mut factory,
                    );
                }
            }
        });
        Self {
            address,
            requirements,
            pages,
            heads,
            after_head,
            task,
        }
    }
    fn dial(&self, address: SocketAddr) -> TcpSessionIo {
        let mut s = TcpStream::connect(address).unwrap();
        tune(&s);
        s.write_all(&[42]).unwrap();
        let mut ack = [1];
        s.read_exact(&mut ack).unwrap();
        assert_eq!(ack, [0]);
        TcpSessionIo::new(s).unwrap()
    }
    fn connect(&self, address: SocketAddr) -> CapabilityRecordSession<TcpSessionIo> {
        CapabilityRecordSession::connect(
            self.dial(address),
            scope(),
            limits(),
            CapabilitySupport::current(),
        )
        .unwrap()
    }
    fn finish(self) {
        self.task.join().unwrap();
    }
}
struct Relay {
    address: SocketAddr,
    task: thread::JoinHandle<()>,
}
impl Relay {
    fn new(upstream: SocketAddr, connections: usize) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..connections {
                let (mut downstream, _) = listener.accept().unwrap();
                let mut upstream = TcpStream::connect(upstream).unwrap();
                tune(&downstream);
                tune(&upstream);
                workers.push(thread::spawn(move || {
                    let mut up_read = downstream.try_clone().unwrap();
                    let mut up_write = upstream.try_clone().unwrap();
                    let first = thread::spawn(move || {
                        let _ = std::io::copy(&mut up_read, &mut up_write);
                        let _ = up_write.shutdown(Shutdown::Write);
                    });
                    let _ = std::io::copy(&mut upstream, &mut downstream);
                    let _ = downstream.shutdown(Shutdown::Write);
                    first.join().unwrap();
                }));
            }
            for w in workers {
                w.join().unwrap();
            }
        });
        Self { address, task }
    }
    fn finish(self) {
        self.task.join().unwrap();
    }
}
fn catch_up(s: &CapabilityRecordSession<TcpSessionIo>, store: &mut SqliteReplicaStore) -> u64 {
    let (mut a, mut r) = s.parts();
    let mut pass = begin_pass_with_page(&scope(), budget(), &mut a, &mut r, store).unwrap();
    loop {
        finish_pass(&mut pass, budget(), &mut a, &mut r, store).unwrap();
        let n = pass.position();
        pass = begin_pass(&scope(), &mut a, &mut r, store).unwrap();
        if pass.is_complete() {
            return n;
        }
    }
}
#[test]
fn unknown_reader_cold_refusal_precedes_page_and_keeps_reopened_cache_direct_and_relay() {
    for relayed in [false, true] {
        let f = Directory::new();
        let src = f.path("source.db");
        source(&src).append(&id("fact"), b"value").unwrap();
        let server = Server::new(src, 1, true);
        server.requirements.lock().unwrap().reader = 1;
        let relay = relayed.then(|| Relay::new(server.address, 1));
        let address = relay.as_ref().map_or(server.address, |r| r.address);
        let s = server.connect(address);
        let path = f.path("cache.db");
        let mut store = SqliteReplicaStore::open(&path).unwrap();
        let (mut a, mut r) = s.parts();
        assert_eq!(
            begin_pass_with_page(&scope(), budget(), &mut a, &mut r, &mut store),
            Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
        );
        assert_eq!(server.pages.load(Ordering::SeqCst), 0);
        assert_eq!(s.counters().unwrap().payload_bytes, 0);
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        s.close().unwrap();
        drop(store);
        let cache = SqliteReplicaStore::open(path).unwrap();
        assert_eq!(cache.count(&scope()).unwrap(), 0);
        assert!(cache.checkpoint(&scope()).unwrap().is_none());
        server.finish();
        if let Some(r) = relay {
            r.finish();
        }
    }
}
#[test]
fn writer_only_reads_keep_exchange_count_and_reconnect_refusal_preserves_progress() {
    for relayed in [false, true] {
        let f = Directory::new();
        let src = f.path("source.db");
        for n in 1..=7 {
            source(&src)
                .append(&id(&format!("fact{n}")), b"value")
                .unwrap();
        }
        let server = Server::new(src.clone(), 2, true);
        server.requirements.lock().unwrap().writer = u64::MAX;
        let relay = relayed.then(|| Relay::new(server.address, 2));
        let address = relay.as_ref().map_or(server.address, |r| r.address);
        let path = f.path("cache.db");
        let mut store = SqliteReplicaStore::open(&path).unwrap();
        let s = server.connect(address);
        assert_eq!(catch_up(&s, &mut store), 7);
        assert_eq!(s.counters().unwrap().exchanges, 6);
        assert_eq!(s.counters().unwrap().head_checks, 2);
        assert_eq!(server.heads.load(Ordering::SeqCst), 2);
        s.close().unwrap();
        let saved = store.checkpoint(&scope()).unwrap();
        source(&src).append(&id("fact8"), b"later").unwrap();
        server.requirements.lock().unwrap().reader = 1;
        let s = server.connect(address);
        let (mut a, mut r) = s.parts();
        assert_eq!(
            begin_pass_with_page(&scope(), budget(), &mut a, &mut r, &mut store),
            Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
        );
        assert_eq!(server.pages.load(Ordering::SeqCst), 1);
        assert_eq!(store.checkpoint(&scope()).unwrap(), saved);
        assert_eq!(s.counters().unwrap().payload_bytes, 0);
        s.close().unwrap();
        drop(store);
        let mut cache = SqliteReplicaStore::open(path).unwrap();
        assert_eq!(cache.count(&scope()).unwrap(), 7);
        assert_eq!(cache.read_after(&scope(), 0, 10, 1000).unwrap().len(), 7);
        assert_eq!(cache.checkpoint(&scope()).unwrap(), saved);
        server.finish();
        if let Some(r) = relay {
            r.finish();
        }
    }
}
#[test]
fn ordinary_pass_steps_check_metadata_and_post_pass_head_stays_fresh() {
    let f = Directory::new();
    let src = f.path("source.db");
    for n in 1..=3 {
        source(&src)
            .append(&id(&format!("fact{n}")), b"value")
            .unwrap();
    }
    let server = Server::new(src, 1, true);
    let s = server.connect(server.address);
    let (mut a, mut r) = s.parts();
    let mut store = SqliteReplicaStore::open(f.path("cache.db")).unwrap();
    let mut pass = begin_pass(&scope(), &mut a, &mut r, &mut store).unwrap();
    finish_pass(
        &mut pass,
        Limits::new(1, 100, 100).unwrap(),
        &mut a,
        &mut r,
        &mut store,
    )
    .unwrap();
    assert_eq!(pass.position(), 3);
    assert!(begin_pass(&scope(), &mut a, &mut r, &mut store)
        .unwrap()
        .is_complete());
    assert_eq!(server.pages.load(Ordering::SeqCst), 3);
    assert_eq!(server.heads.load(Ordering::SeqCst), 5);
    s.close().unwrap();
    server.finish();
}
#[test]
fn incompatible_after_initial_admission_stops_before_next_page_or_apply() {
    let f = Directory::new();
    let src = f.path("source.db");
    source(&src).append(&id("fact"), b"value").unwrap();
    let server = Server::new(src, 1, true);
    let s = server.connect(server.address);
    let (mut a, mut r) = s.parts();
    let mut store = SqliteReplicaStore::open(f.path("cache.db")).unwrap();
    let mut pass = begin_pass(&scope(), &mut a, &mut r, &mut store).unwrap();
    server.requirements.lock().unwrap().reader = 1;
    assert_eq!(
        finish_pass(&mut pass, budget(), &mut a, &mut r, &mut store),
        Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
    );
    assert_eq!(pass.position(), 0);
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert_eq!(server.pages.load(Ordering::SeqCst), 0);
    s.close().unwrap();
    server.finish();
}
#[test]
fn policy_refusal_overrides_incompatibility_and_invalid_policy_is_fatal() {
    for mode in [1, 3, 4] {
        let f = Directory::new();
        let src = f.path("source.db");
        source(&src).append(&id("fact"), b"value").unwrap();
        let server = Server::new(src, 1, true);
        server.requirements.lock().unwrap().reader = 1;
        server.after_head.store(mode, Ordering::SeqCst);
        let s = server.connect(server.address);
        let (mut a, mut r) = s.parts();
        let request = nessa_sync::replication::domain::OpenPageRequest {
            scope: scope(),
            after: 0,
            max_records: 1,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        };
        let expected = match mode {
            1 => SourceError::Denied,
            4 => SourceError::IdentityChanged,
            _ => SourceError::InvalidRequest,
        };
        assert_eq!(r.open_page(&request), Err(expected));
        assert_eq!(server.pages.load(Ordering::SeqCst), 0);
        if mode == 3 {
            assert_eq!(a.authorize(&scope()), Access::Unverifiable);
        }
        s.close().unwrap();
        server.finish();
    }
}
#[test]
fn explicit_capability_endpoint_refuses_legacy_data_and_no_client_falls_back() {
    let f = Directory::new();
    let src = f.path("source.db");
    source(&src).append(&id("fact"), b"value").unwrap();
    let server = Server::new(src.clone(), 1, true);
    let s =
        RecordSession::connect_open_interruptible(server.dial(server.address), scope(), limits())
            .unwrap();
    let (mut a, mut r) = s.parts();
    let mut store = SqliteReplicaStore::open(f.path("legacy-cache.db")).unwrap();
    assert_eq!(
        begin_pass_with_page(&scope(), budget(), &mut a, &mut r, &mut store),
        Err(SyncError::Source(SourceError::InvalidRequest))
    );
    assert_eq!(r.head(&scope()), Err(SourceError::InvalidRequest));
    assert_eq!(server.pages.load(Ordering::SeqCst), 0);
    s.close().unwrap();
    server.finish();
    let server = Server::new(src, 1, false);
    let s = server.connect(server.address);
    let (mut a, mut r) = s.parts();
    assert_eq!(
        begin_pass_with_page(&scope(), budget(), &mut a, &mut r, &mut store),
        Err(SyncError::Source(SourceError::InvalidRequest))
    );
    assert_eq!(server.pages.load(Ordering::SeqCst), 0);
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    s.close().unwrap();
    server.finish();
}

#[test]
fn unsupported_metadata_version_or_wrong_scope_stops_before_payload() {
    for wrong_scope in [false, true] {
        let f = Directory::new();
        let src = f.path("source.db");
        source(&src).append(&id("fact"), b"value").unwrap();
        let server = Server::new(src, 1, true);
        if wrong_scope {
            server.requirements.lock().unwrap().wrong_scope = true;
        } else {
            server.requirements.lock().unwrap().version = Some(2);
        }
        let s = server.connect(server.address);
        let (mut a, mut r) = s.parts();
        let mut store = SqliteReplicaStore::open(f.path("cache.db")).unwrap();
        let expected = if wrong_scope {
            SyncError::Source(SourceError::IdentityChanged)
        } else {
            SyncError::Incompatible(CapabilityError::UnsupportedVersion(2))
        };
        assert_eq!(
            begin_pass_with_page(&scope(), budget(), &mut a, &mut r, &mut store),
            Err(expected)
        );
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        assert_eq!(server.pages.load(Ordering::SeqCst), 0);
        s.close().unwrap();
        server.finish();
    }
}

#[test]
fn maximum_record_reassembles_with_metadata_and_post_pass_requirement_is_fresh() {
    let f = Directory::new();
    let src = f.path("source.db");
    let bytes = vec![b'p'; nessa_sync::replication::application::MAX_RECORD_BYTES];
    source(&src).append(&id("maximum"), &bytes).unwrap();
    let server = Server::new(src, 1, true);
    let s = server.connect(server.address);
    let (mut a, mut r) = s.parts();
    let path = f.path("cache.db");
    let mut store = SqliteReplicaStore::open(&path).unwrap();
    let bounds = Limits::new(1, bytes.len(), bytes.len()).unwrap();
    let pass = begin_pass_with_page(&scope(), bounds, &mut a, &mut r, &mut store).unwrap();
    assert!(pass.is_complete());
    assert_eq!(s.counters().unwrap().payload_bytes, bytes.len() as u64);
    let saved = store.checkpoint(&scope()).unwrap();
    server.requirements.lock().unwrap().reader = 1;
    assert_eq!(
        begin_pass(&scope(), &mut a, &mut r, &mut store),
        Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
    );
    assert_eq!(store.checkpoint(&scope()).unwrap(), saved);
    s.close().unwrap();
    drop(store);
    let mut cache = SqliteReplicaStore::open(path).unwrap();
    assert_eq!(
        cache.read_after(&scope(), 0, 1, bytes.len()).unwrap()[0].payload,
        bytes
    );
    server.finish();
}
