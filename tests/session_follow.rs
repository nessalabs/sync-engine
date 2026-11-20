#![cfg(all(feature = "transport", feature = "sqlite"))]
use nessa_sync::replication::{
    application::{
        begin_pass, begin_pass_with_page, finish_pass, Access, OpenRecordSource, RecordSource,
        ScopeAuthorizer, SourceError, MAX_RECORD_BYTES,
    },
    domain::{Id, Limits, OpenPageRequest, Page, PageRequest, Scope},
    infrastructure::session::{
        connect_follow, serve_follow_record_session, FollowError, FollowSession, HintSignal,
        HintWake, RecordSession, SessionLimits, TcpSessionIo, TcpSessionWriter,
    },
    infrastructure::{SqliteReferenceSource, SqliteReplicaStore},
};
use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("device-a"),
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
    Limits::new(32, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap()
}
fn source(p: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(p, id("origin"), id("stream"), id("first"), id("schema")).unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "follow-owned-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
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
    fn authorize(&mut self, s: &Scope) -> Access {
        if s.receiver() != self.current.lock().unwrap().receiver() {
            return Access::Denied;
        }
        match self.mode.load(Ordering::SeqCst) {
            0 => Access::Allowed(self.current.lock().unwrap().clone()),
            1 => Access::Denied,
            2 => Access::Unverifiable,
            _ => Access::InvalidResponse,
        }
    }
}
struct Server {
    address: SocketAddr,
    signal: HintSignal,
    mode: Arc<AtomicU8>,
    current: Arc<Mutex<Scope>>,
    after_head: Arc<AtomicU8>,
    task: thread::JoinHandle<()>,
}
fn tune(s: &TcpStream) {
    s.set_nodelay(true).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
}
impl Server {
    fn new(path: PathBuf, connections: usize) -> Self {
        drop(source(&path));
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let signal = HintSignal::new();
        let held = signal.clone();
        let mode = Arc::new(AtomicU8::new(0));
        let m = mode.clone();
        let current = Arc::new(Mutex::new(scope()));
        let c = current.clone();
        let after_head = Arc::new(AtomicU8::new(0));
        let after = after_head.clone();
        let task = thread::spawn(move || {
            for n in 1..=connections {
                let (mut s, _) = listener.accept().unwrap();
                tune(&s);
                let mut auth = [0; 1];
                s.read_exact(&mut auth).unwrap();
                assert_eq!(auth, [42]);
                s.write_all(&[0]).unwrap();
                let path = path.clone();
                let calls = Arc::new(AtomicUsize::new(0));
                let after = after.clone();
                let mode = m.clone();
                let signal = held.clone();
                let _ = serve_follow_record_session(
                    TcpSessionIo::new(s).unwrap(),
                    n as u64,
                    limits(),
                    Policy {
                        mode: m.clone(),
                        current: c.clone(),
                    },
                    move |_| {
                        SqliteReferenceSource::open(
                            &path,
                            id("origin"),
                            id("stream"),
                            id("first"),
                            id("schema"),
                        )
                        .map(|inner| HeadSource {
                            inner,
                            path: path.clone(),
                            calls: calls.clone(),
                            after: after.clone(),
                            mode: mode.clone(),
                            signal: signal.clone(),
                        })
                        .map_err(|_| SourceError::Unavailable)
                    },
                    held.clone(),
                );
            }
        });
        Self {
            address,
            signal,
            mode,
            current,
            after_head,
            task,
        }
    }
    fn dial(&self) -> TcpSessionIo {
        let mut s = TcpStream::connect(self.address).unwrap();
        tune(&s);
        s.write_all(&[42]).unwrap();
        let mut ack = [1];
        s.read_exact(&mut ack).unwrap();
        assert_eq!(ack, [0]);
        TcpSessionIo::new(s).unwrap()
    }
    fn connect(&self) -> FollowSession<TcpSessionWriter> {
        connect_follow(self.dial(), scope(), limits(), Duration::from_secs(2)).unwrap()
    }
    fn finish(self) {
        self.task.join().unwrap();
    }
}

struct HeadSource {
    inner: SqliteReferenceSource,
    path: PathBuf,
    calls: Arc<AtomicUsize>,
    after: Arc<AtomicU8>,
    mode: Arc<AtomicU8>,
    signal: HintSignal,
}
impl RecordSource for HeadSource {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        if self.after.load(Ordering::SeqCst) == 3 {
            return Err(SourceError::Pruned);
        }
        let result = self.inner.head(s);
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        match (self.after.load(Ordering::SeqCst), call) {
            (1, 1) => {
                source(&self.path).append(&id("setup"), b"x").unwrap();
                self.signal.notify().unwrap();
            }
            (2, 2) => {
                self.mode.store(1, Ordering::SeqCst);
            }
            _ => (),
        }
        result
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        self.inner.page(r)
    }
}

fn catch_up(s: &FollowSession<TcpSessionWriter>, store: &mut SqliteReplicaStore) -> u64 {
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
fn pending(s: &FollowSession<TcpSessionWriter>, head: u64) {
    let until = Instant::now() + Duration::from_secs(2);
    while s.hints().stats().unwrap().pending_head != Some(head) {
        assert!(Instant::now() < until, "hint did not arrive");
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn persistent_hint_only_wakes_authorized_reads_and_idle_timeout_keeps_subscription() {
    let f = Fixture::new();
    let path = f.path("source.db");
    let server = Server::new(path.clone(), 1);
    let s = server.connect();
    let mut store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
    let before = s.counters().unwrap();
    assert_eq!(
        s.hints().wait(Duration::from_millis(250)).unwrap(),
        HintWake::Timeout
    );
    assert_eq!(s.counters().unwrap(), before);
    source(&path).append(&id("one"), b"a").unwrap();
    server.signal.notify().unwrap();
    assert_eq!(
        s.hints().wait(Duration::from_secs(2)).unwrap(),
        HintWake::Changed { advisory_head: 1 }
    );
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert_eq!(catch_up(&s, &mut store), 1);
    source(&path).append(&id("two"), b"b").unwrap();
    server.signal.notify().unwrap();
    assert_eq!(
        s.hints().wait(Duration::from_secs(2)).unwrap(),
        HintWake::Changed { advisory_head: 2 }
    );
    assert_eq!(catch_up(&s, &mut store), 2);
    drop(store);
    let store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
    assert_eq!(store.count(&scope()).unwrap(), 2);
    s.close().unwrap();
    server.finish();
}
#[test]
fn distinct_commits_coalesce_without_moving_progress_and_repeated_wakes_are_deduplicated() {
    let f = Fixture::new();
    let path = f.path("source.db");
    let server = Server::new(path.clone(), 1);
    let s = server.connect();
    let mut store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
    for n in 1..=3 {
        source(&path)
            .append(&id(&format!("fact{n}")), b"x")
            .unwrap();
        server.signal.notify().unwrap();
        pending(&s, n);
    }
    let stats = s.hints().stats().unwrap();
    assert_eq!(stats.frames_received, 3);
    assert_eq!(stats.coalesced, 2);
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert_eq!(
        s.hints().wait(Duration::ZERO).unwrap(),
        HintWake::Changed { advisory_head: 3 }
    );
    for _ in 0..16 {
        server.signal.notify().unwrap();
    }
    assert_eq!(catch_up(&s, &mut store), 3);
    assert_eq!(
        s.hints().wait(Duration::from_millis(150)).unwrap(),
        HintWake::Timeout
    );
    assert_eq!(s.hints().stats().unwrap().frames_received, 3);
    s.close().unwrap();
    server.finish();
}
#[test]
fn terminal_hint_outcomes_remain_typed_on_wait_and_all_ports() {
    for (mode, expected) in [
        (1, FollowError::Refused(SourceError::Denied)),
        (2, FollowError::Refused(SourceError::Unavailable)),
        (3, FollowError::Protocol),
    ] {
        let f = Fixture::new();
        let server = Server::new(f.path("source.db"), 1);
        let s = server.connect();
        server.mode.store(mode, Ordering::SeqCst);
        server.signal.notify().unwrap();
        assert_eq!(
            s.hints().wait(Duration::from_secs(2)),
            Err(expected.clone())
        );
        let (mut a, mut r) = s.parts();
        assert_eq!(
            a.authorize(&scope()),
            match mode {
                1 => Access::Denied,
                2 => Access::Unverifiable,
                _ => Access::InvalidResponse,
            }
        );
        assert_eq!(
            r.head(&scope()),
            Err(match mode {
                1 => SourceError::Denied,
                2 => SourceError::Unavailable,
                _ => SourceError::InvalidRequest,
            })
        );
        let error = match mode {
            1 => SourceError::Denied,
            2 => SourceError::Unavailable,
            _ => SourceError::InvalidRequest,
        };
        let request = OpenPageRequest {
            scope: scope(),
            after: 0,
            max_records: 1,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        };
        assert_eq!(r.page(&request.for_target(1)), Err(error.clone()));
        assert_eq!(r.open_page(&request), Err(error));
        s.close().unwrap();
        server.finish();
    }
}
#[test]
fn legacy_open_on_follow_server_receives_no_unsolicited_hints() {
    let f = Fixture::new();
    let path = f.path("source.db");
    let server = Server::new(path.clone(), 1);
    let s = RecordSession::connect_open_interruptible(server.dial(), scope(), limits()).unwrap();
    source(&path).append(&id("one"), b"a").unwrap();
    server.signal.notify().unwrap();
    let (mut a, mut r) = s.parts();
    assert_eq!(a.authorize(&scope()), Access::Allowed(scope()));
    assert_eq!(r.head(&scope()).unwrap(), 1);
    s.close().unwrap();
    server.finish();
}

#[test]
fn setup_notification_is_retained_before_initial_catch_up() {
    let f = Fixture::new();
    let server = Server::new(f.path("source.db"), 1);
    server.after_head.store(1, Ordering::SeqCst);
    let s = server.connect();
    assert_eq!(
        s.hints().wait(Duration::from_secs(2)),
        Ok(HintWake::Changed { advisory_head: 1 })
    );
    let mut store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
    assert!(store.checkpoint(&scope()).unwrap().is_none());
    assert_eq!(catch_up(&s, &mut store), 1);
    s.close().unwrap();
    server.finish();
}
#[test]
fn policy_is_rechecked_after_hint_head_capture_before_emission() {
    let f = Fixture::new();
    let path = f.path("source.db");
    let server = Server::new(path.clone(), 1);
    let s = server.connect();
    server.after_head.store(2, Ordering::SeqCst);
    source(&path).append(&id("fact"), b"x").unwrap();
    server.signal.notify().unwrap();
    assert_eq!(
        s.hints().wait(Duration::from_secs(2)),
        Err(FollowError::Refused(SourceError::Denied))
    );
    assert_eq!(s.hints().stats().unwrap().pending_head, None);
    s.close().unwrap();
    server.finish();
}
#[test]
fn stale_epoch_is_typed_and_explicit_reconnect_resumes_saved_progress() {
    let f = Fixture::new();
    let path = f.path("source.db");
    let server = Server::new(path.clone(), 2);
    source(&path).append(&id("one"), b"a").unwrap();
    let s = server.connect();
    let mut store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
    assert_eq!(catch_up(&s, &mut store), 1);
    *server.current.lock().unwrap() = Scope::new(
        id("device-a"),
        id("origin"),
        id("stream"),
        id("first"),
        id("schema"),
        id("next-epoch"),
    );
    server.signal.notify().unwrap();
    assert_eq!(
        s.hints().wait(Duration::from_secs(2)),
        Err(FollowError::Refused(SourceError::IdentityChanged))
    );
    let (mut a, mut r) = s.parts();
    assert_eq!(a.authorize(&scope()), Access::IdentityChanged);
    assert_eq!(r.head(&scope()), Err(SourceError::IdentityChanged));
    assert_eq!(store.count(&scope()).unwrap(), 1);
    s.close().unwrap();
    *server.current.lock().unwrap() = scope();
    source(&path).append(&id("two"), b"b").unwrap();
    server.signal.notify().unwrap();
    let fresh = server.connect();
    assert_eq!(catch_up(&fresh, &mut store), 2);
    assert_eq!(r.head(&scope()), Err(SourceError::IdentityChanged));
    fresh.close().unwrap();
    server.finish();
    drop(store);
    assert_eq!(
        SqliteReplicaStore::open(f.path("replica.db"))
            .unwrap()
            .count(&scope())
            .unwrap(),
        2
    );
}

#[test]
fn initial_policy_and_source_refusals_stay_typed_when_the_server_closes() {
    for expected in [
        SourceError::Denied,
        SourceError::Unavailable,
        SourceError::IdentityChanged,
        SourceError::Pruned,
    ] {
        let f = Fixture::new();
        let server = Server::new(f.path("source.db"), 1);
        let store = SqliteReplicaStore::open(f.path("replica.db")).unwrap();
        match expected {
            SourceError::Denied => server.mode.store(1, Ordering::SeqCst),
            SourceError::Unavailable => server.mode.store(2, Ordering::SeqCst),
            SourceError::IdentityChanged => {
                *server.current.lock().unwrap() = Scope::new(
                    id("device-a"),
                    id("origin"),
                    id("stream"),
                    id("first"),
                    id("schema"),
                    id("next-epoch"),
                );
            }
            SourceError::Pruned => server.after_head.store(3, Ordering::SeqCst),
            _ => unreachable!(),
        }
        let result = connect_follow(server.dial(), scope(), limits(), Duration::from_secs(2));
        match result {
            Err(actual) => assert_eq!(actual, FollowError::Refused(expected)),
            Ok(s) => {
                s.close().unwrap();
                panic!("initial refusal supplied ports")
            }
        }
        assert!(store.checkpoint(&scope()).unwrap().is_none());
        assert_eq!(store.count(&scope()).unwrap(), 0);
        server.finish();
    }
}

#[path = "session_follow/local_cancel.rs"]
mod local_cancel;
