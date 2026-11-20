#![cfg(all(feature = "transport", feature = "sqlite"))]
//! Real TCP and SQLite tests for bounded history across one follow binding.
use nessa_sync::replication::{
    application::{Access, RecordSource, ScopeAuthorizer, SourceError},
    domain::{CapabilityError, CapabilitySupport, Id, Limits, Scope},
    history::{
        fetch_older, fetch_tail, HistoryError, HistorySource, HistorySourceError, HistoryStore,
        OlderRequest, TailRequest,
    },
    infrastructure::{
        session::{
            connect_follow, serve_capability_follow_record_session,
            serve_history_follow_record_session, DuplexSessionIo, HintSignal,
            InterruptibleSessionIo, SessionInterrupt, SessionLimits, TcpSessionIo,
            TcpSessionReader, TcpSessionWriter,
        },
        SqliteReferenceSource, SqliteReplicaStore,
    },
};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope::new(id("r"), id("o"), id("s"), id("i"), id("k"), id("e"))
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(path, id("o"), id("s"), id("i"), id("k")).unwrap()
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 2048,
        bindings: 1,
    }
}
fn bounds() -> Limits {
    Limits::new(1, 8192, 8192).unwrap()
}
fn tail() -> TailRequest {
    TailRequest {
        scope: scope(),
        generation: 100,
        max_records: 1,
        max_payload_bytes: 8192,
        max_record_bytes: 8192,
    }
}
fn older() -> OlderRequest {
    OlderRequest {
        scope: scope(),
        generation: 100,
        before: 3,
        max_records: 1,
        max_payload_bytes: 8192,
        max_record_bytes: 8192,
    }
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "bounded-history-follow-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        let mut s = source(&p.join("source.db"));
        s.append(&id("one"), b"one").unwrap();
        s.append(&id("two"), &[2; 4096]).unwrap();
        s.append(&id("three"), &[3; 4096]).unwrap();
        Self(p)
    }
    fn cache(&self) -> SqliteReplicaStore {
        SqliteReplicaStore::open(self.0.join("cache.db")).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    Normal,
    RevokeAfterFirstFragment,
    RevokeDuringOlder,
    InterruptAfterFirstFragment,
    NoHistoryEndpoint,
}
struct CurrentPolicy(Arc<AtomicBool>);
impl ScopeAuthorizer for CurrentPolicy {
    fn authorize(&mut self, requested: &Scope) -> Access {
        if self.0.load(Ordering::Acquire) || requested != &scope() {
            Access::Denied
        } else {
            Access::Allowed(requested.clone())
        }
    }
}

struct GatedIo {
    tcp: TcpSessionIo,
    boundary: Boundary,
    revoked: Arc<AtomicBool>,
    frames: Arc<AtomicUsize>,
}
struct GatedWriter {
    tcp: TcpSessionWriter,
    interrupt: Arc<dyn SessionInterrupt>,
    boundary: Boundary,
    revoked: Arc<AtomicBool>,
    frames: Arc<AtomicUsize>,
}
impl Read for GatedIo {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.tcp.read(b)
    }
}
impl Write for GatedIo {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.tcp.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.tcp.flush()
    }
}
impl InterruptibleSessionIo for GatedIo {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        self.tcp.interrupt_handle()
    }
}
impl DuplexSessionIo for GatedIo {
    type Reader = TcpSessionReader;
    type Writer = GatedWriter;
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let interrupt = self.interrupt_handle();
        let (reader, tcp) = self.tcp.split()?;
        Ok((
            reader,
            GatedWriter {
                tcp,
                interrupt,
                boundary: self.boundary,
                revoked: self.revoked,
                frames: self.frames,
            },
        ))
    }
}
impl Write for GatedWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.tcp.write_all(b)?;
        let is_history_fragment = b.get(4..8) == Some(b"NSR1")
            && matches!(b.get(10), Some(136 | 137))
            && b.get(11) == Some(&0);
        if is_history_fragment {
            let index = self.frames.fetch_add(1, Ordering::AcqRel);
            match self.boundary {
                Boundary::RevokeAfterFirstFragment if index == 0 => {
                    self.revoked.store(true, Ordering::Release)
                }
                Boundary::RevokeDuringOlder if b.get(10) == Some(&137) => {
                    self.revoked.store(true, Ordering::Release)
                }
                Boundary::InterruptAfterFirstFragment if index == 0 => {
                    self.interrupt.interrupt()?
                }
                _ => {}
            }
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.tcp.flush()
    }
}
struct Run {
    tcp: TcpSessionIo,
    server: thread::JoinHandle<
        Result<(), nessa_sync::replication::infrastructure::session::RecordSessionError>,
    >,
    factory: Arc<AtomicUsize>,
    frames: Arc<AtomicUsize>,
}
fn start(fixture: &Fixture, boundary: Boundary) -> Run {
    fn tune(s: &TcpStream) {
        let timeout = Some(Duration::from_secs(3));
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(timeout).unwrap();
        s.set_write_timeout(timeout).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    tune(&client);
    let path = fixture.0.join("source.db");
    let revoked = Arc::new(AtomicBool::new(false));
    let factory = Arc::new(AtomicUsize::new(0));
    let frames = Arc::new(AtomicUsize::new(0));
    let (server_factory, server_frames) = (factory.clone(), frames.clone());
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        tune(&socket);
        let io = GatedIo {
            tcp: TcpSessionIo::new(socket).unwrap(),
            boundary,
            revoked: revoked.clone(),
            frames: server_frames,
        };
        let current = CurrentPolicy(revoked);
        let factory = move |_: &Scope| -> Result<SqliteReferenceSource, SourceError> {
            server_factory.fetch_add(1, Ordering::Relaxed);
            Ok(source(&path))
        };
        if boundary == Boundary::NoHistoryEndpoint {
            serve_capability_follow_record_session(
                io,
                1,
                limits(),
                current,
                factory,
                HintSignal::new(),
            )
        } else {
            serve_history_follow_record_session(
                io,
                1,
                limits(),
                current,
                factory,
                HintSignal::new(),
            )
        }
    });
    Run {
        tcp: TcpSessionIo::new(client).unwrap(),
        server,
        factory,
        frames,
    }
}

#[test]
fn same_connection_installs_tail_then_older_and_reopens_without_rewinding_live_head() {
    let fixture = Fixture::new();
    let run = start(&fixture, Boundary::Normal);
    let root = connect_follow(run.tcp, scope(), limits(), Duration::from_secs(3)).unwrap();
    let (mut auth, mut history) = root.history_parts(CapabilitySupport::current());
    let (_, mut records) = root.capability_parts(CapabilitySupport::current());
    let mut db = fixture.cache();
    let tail = db
        .install_tail(fetch_tail(&tail(), bounds(), &mut auth, &mut history).unwrap())
        .unwrap();
    assert_eq!(
        (tail.generation, tail.live_head, tail.lower_bound),
        (100, 3, 3)
    );
    assert_eq!(records.head(&scope()).unwrap(), 3);
    let older = db
        .install_older(fetch_older(&older(), bounds(), &mut auth, &mut history).unwrap())
        .unwrap();
    assert_eq!(
        (older.generation, older.live_head, older.lower_bound),
        (100, 3, 2)
    );
    drop(db);
    let mut reopened = fixture.cache();
    assert_eq!(reopened.history_progress(&scope()).unwrap(), Some(older));
    let cached = reopened.read_after(&scope(), 1, 3, 8192).unwrap();
    assert_eq!(
        cached.iter().map(|r| r.position).collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(cached[0].payload, [2; 4096]);
    assert_eq!(cached[1].payload, [3; 4096]);
    let count = root.counters().unwrap().wire;
    assert_eq!(count.payload_bytes, 8192);
    assert_eq!(count.duplicate_bytes, 0);
    assert!(run.frames.load(Ordering::Acquire) >= 6);
    root.close().unwrap();
    run.server.join().unwrap().unwrap();
}

#[test]
fn revocation_or_disconnect_mid_tail_leaves_no_installable_plan() {
    for boundary in [
        Boundary::RevokeAfterFirstFragment,
        Boundary::InterruptAfterFirstFragment,
    ] {
        let fixture = Fixture::new();
        let run = start(&fixture, boundary);
        let root = connect_follow(run.tcp, scope(), limits(), Duration::from_secs(3)).unwrap();
        let (mut auth, mut history) = root.history_parts(CapabilitySupport::current());
        let result = fetch_tail(&tail(), bounds(), &mut auth, &mut history);
        let expected = match boundary {
            Boundary::RevokeAfterFirstFragment => HistorySourceError::Denied,
            _ => HistorySourceError::Unavailable,
        };
        assert!(
            matches!(result, Err(HistoryError::Source(e)) if e == expected),
            "{boundary:?}"
        );
        assert_eq!(run.frames.load(Ordering::Acquire), 1);
        let mut db = fixture.cache();
        assert_eq!(db.history_progress(&scope()).unwrap(), None);
        assert_eq!(db.count(&scope()).unwrap(), 0);
        drop(db);
        assert_eq!(fixture.cache().history_progress(&scope()).unwrap(), None);
        let _ = root.close();
        let _ = run.server.join().unwrap();
    }
}

#[test]
fn older_endpoint_refuses_history_without_legacy_retry_and_native_requirement_propagates() {
    let fixture = Fixture::new();
    let run = start(&fixture, Boundary::NoHistoryEndpoint);
    let root = connect_follow(run.tcp, scope(), limits(), Duration::from_secs(3)).unwrap();
    let (_, mut history) = root.history_parts(CapabilitySupport::current());
    assert_eq!(
        history.tail(&tail()),
        Err(HistorySourceError::InvalidRequest)
    );
    // The only factory call was initial Bind; neither legacy nor history was read.
    assert_eq!(run.factory.load(Ordering::Acquire), 1);
    root.close().unwrap();
    run.server.join().unwrap().unwrap();

    let run = start(&fixture, Boundary::Normal);
    let root = connect_follow(run.tcp, scope(), limits(), Duration::from_secs(3)).unwrap();
    let (_, mut history) = root.history_parts(CapabilitySupport::current());
    let db = rusqlite::Connection::open(fixture.0.join("source.db")).unwrap();
    db.execute(
        "UPDATE source_meta SET reader_required=?1",
        [1u64.to_be_bytes()],
    )
    .unwrap();
    let error = HistorySourceError::Incompatible(CapabilityError::MissingReader(1));
    assert_eq!(history.tail(&tail()), Err(error.clone()));
    assert_eq!(history.older(&older()), Err(error));
    assert_eq!(run.frames.load(Ordering::Acquire), 0);
    root.close().unwrap();
    run.server.join().unwrap().unwrap();
}

#[test]
fn revocation_during_older_keeps_the_existing_durable_tail() {
    let fixture = Fixture::new();
    let run = start(&fixture, Boundary::RevokeDuringOlder);
    let root = connect_follow(run.tcp, scope(), limits(), Duration::from_secs(3)).unwrap();
    let (mut auth, mut history) = root.history_parts(CapabilitySupport::current());
    let mut db = fixture.cache();
    let saved = db
        .install_tail(fetch_tail(&tail(), bounds(), &mut auth, &mut history).unwrap())
        .unwrap();
    let frames = run.frames.load(Ordering::Acquire);
    assert!(matches!(
        fetch_older(&older(), bounds(), &mut auth, &mut history),
        Err(HistoryError::Source(HistorySourceError::Denied))
    ));
    assert_eq!(run.frames.load(Ordering::Acquire), frames + 1);
    drop(db);
    let mut reopened = fixture.cache();
    assert_eq!(reopened.history_progress(&scope()).unwrap(), Some(saved));
    assert_eq!(reopened.count(&scope()).unwrap(), 1);
    let _ = root.close();
    let _ = run.server.join().unwrap();
}
