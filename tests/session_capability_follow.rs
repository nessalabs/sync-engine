#![cfg(all(feature = "transport", feature = "sqlite"))]
//! Ordinary capability/follow composition on one real TCP channel and SQLite cache.
use nessa_sync::replication::{
    application::*,
    domain::{
        CapabilityError, CapabilitySupport, Id, Limits, Page, PageRequest, Scope, SourceHead,
    },
    infrastructure::session::{
        connect_follow, serve_capability_follow_record_session, serve_follow_record_session,
        HintSignal, HintWake, SessionLimits, TcpSessionIo,
    },
    infrastructure::{MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore},
};
use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("first"),
        id("schema"),
        id("epoch"),
    )
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings: 1,
    }
}
fn budget() -> Limits {
    Limits::new(1, 100, 100).unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-cap-follow-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn source(&self) -> SqliteReferenceSource {
        open(self.0.join("source.db"))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn open(p: PathBuf) -> SqliteReferenceSource {
    SqliteReferenceSource::open(p, id("origin"), id("stream"), id("first"), id("schema")).unwrap()
}
struct Policy(Arc<AtomicU8>);
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, _: &Scope) -> Access {
        match self.0.load(Ordering::SeqCst) {
            0 => Access::Allowed(scope()),
            1 => Access::Denied,
            _ => Access::Unverifiable,
        }
    }
}
struct View {
    inner: SqliteReferenceSource,
    reader: Arc<AtomicU64>,
    pages: Arc<AtomicUsize>,
}
impl RecordSource for View {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        self.inner.head(s)
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        self.pages.fetch_add(1, Ordering::SeqCst);
        self.inner.page(r)
    }
}
impl RecordReadView for View {
    fn view_head(&mut self, s: &Scope) -> Result<SourceHead, SourceError> {
        Ok(SourceHead::new(
            s.clone(),
            self.inner.head(s)?,
            self.reader.load(Ordering::SeqCst),
            u64::MAX,
        ))
    }
}
fn tune(s: &TcpStream) {
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
}
#[test]
fn one_connection_preserves_capability_refusal_hint_coalescing_and_reopened_progress() {
    let f = Fixture::new();
    f.source().append(&id("one"), b"first").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let reader = Arc::new(AtomicU64::new(0));
    let pages = Arc::new(AtomicUsize::new(0));
    let signal = HintSignal::new();
    let (m, r, p, w, path) = (
        mode.clone(),
        reader.clone(),
        pages.clone(),
        signal.clone(),
        f.0.join("source.db"),
    );
    let task = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        tune(&socket);
        serve_capability_follow_record_session(
            TcpSessionIo::new(socket).unwrap(),
            1,
            limits(),
            Policy(m),
            move |_| {
                Ok(View {
                    inner: open(path.clone()),
                    reader: r.clone(),
                    pages: p.clone(),
                })
            },
            w,
        )
    });
    let socket = TcpStream::connect(addr).unwrap();
    tune(&socket);
    let root = connect_follow(
        TcpSessionIo::new(socket).unwrap(),
        scope(),
        limits(),
        Duration::from_secs(2),
    )
    .unwrap();
    let (mut a, mut s) = root.capability_parts(CapabilitySupport::current());
    reader.store(1, Ordering::SeqCst);
    assert_eq!(
        begin_pass_with_capabilities(
            &scope(),
            CapabilitySupport::current(),
            &mut a,
            &mut s,
            &mut SqliteReplicaStore::open(f.0.join("cache.db")).unwrap()
        ),
        Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
    );
    assert_eq!(pages.load(Ordering::SeqCst), 0);
    // Ordinary future requirement fixture changes back only to independently test
    // supported operation decoding; this is not a durable publication/rollout claim.
    reader.store(0, Ordering::SeqCst);
    let mut cache = SqliteReplicaStore::open(f.0.join("cache.db")).unwrap();
    let mut pass = begin_pass_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut a,
        &mut s,
        &mut cache,
    )
    .unwrap();
    finish_pass(&mut pass, budget(), &mut a, &mut s, &mut cache).unwrap();
    assert!(begin_pass_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut a,
        &mut s,
        &mut cache
    )
    .unwrap()
    .is_complete());
    f.source().append(&id("two"), b"second").unwrap();
    for _ in 0..16 {
        signal.notify().unwrap();
    }
    assert!(matches!(
        root.hints().wait(Duration::from_secs(2)).unwrap(),
        HintWake::Changed { advisory_head: 2 }
    ));
    assert_eq!(cache.checkpoint(&scope()).unwrap().unwrap().position(), 1);
    let mut pass = begin_pass_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut a,
        &mut s,
        &mut cache,
    )
    .unwrap();
    finish_pass(&mut pass, budget(), &mut a, &mut s, &mut cache).unwrap();
    assert!(begin_pass_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut a,
        &mut s,
        &mut cache
    )
    .unwrap()
    .is_complete());
    mode.store(1, Ordering::SeqCst);
    assert_eq!(a.authorize(&scope()), Access::Denied);
    root.close().unwrap();
    let _ = task.join().unwrap();
    drop(cache);
    let mut reopened = SqliteReplicaStore::open(f.0.join("cache.db")).unwrap();
    assert_eq!(reopened.count(&scope()).unwrap(), 2);
    assert_eq!(
        reopened.read_after(&scope(), 0, 10, 100).unwrap()[1].payload,
        b"second"
    );
}
#[test]
fn explicit_endpoint_selection_never_downgrades_data_operations() {
    for capability_server in [false, true] {
        let f = Fixture::new();
        f.source().append(&id("one"), b"value").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let path = f.0.join("source.db");
        let task = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            tune(&socket);
            let io = TcpSessionIo::new(socket).unwrap();
            let policy = MemoryAuthorizer::allowed(scope());
            let factory = move |_: &Scope| Ok(open(path.clone()));
            if capability_server {
                serve_capability_follow_record_session(
                    io,
                    1,
                    limits(),
                    policy,
                    factory,
                    HintSignal::new(),
                )
            } else {
                serve_follow_record_session(io, 1, limits(), policy, factory, HintSignal::new())
            }
        });
        let socket = TcpStream::connect(addr).unwrap();
        tune(&socket);
        let root = connect_follow(
            TcpSessionIo::new(socket).unwrap(),
            scope(),
            limits(),
            Duration::from_secs(2),
        )
        .unwrap();
        if capability_server {
            let (_, mut source) = root.parts();
            assert_eq!(source.head(&scope()), Err(SourceError::InvalidRequest));
        } else {
            let (_, mut source) = root.capability_parts(CapabilitySupport::current());
            assert_eq!(
                source.source_head(&scope()),
                Err(SourceError::InvalidRequest)
            );
        }
        root.close().unwrap();
        let _ = task.join().unwrap();
    }
}
