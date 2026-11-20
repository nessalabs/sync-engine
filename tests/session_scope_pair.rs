#![cfg(all(feature = "transport", feature = "sqlite"))]
//! Two native sources on one persistent TCP connection.
use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer, SourceError},
    domain::{CapabilitySupport, Checkpoint, Id, Scope},
    infrastructure::{
        session::{
            read_control, serve_capability_record_session, serve_scope_pair_batched_heads,
            serve_scope_pair_heads, write_control, BindingMetadata, ControlFrame,
            RecordSessionError, ScopePairHeadsSession, SessionLimits, TcpSessionIo,
        },
        SqliteReferenceSource,
    },
};
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope(stream: &str) -> Scope {
    Scope::new(
        id("device"),
        id("origin"),
        id(stream),
        id("first"),
        id("schema"),
        id("policy"),
    )
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 2048,
        bindings: 2,
    }
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Sources(PathBuf);
impl Sources {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "session-scope-pair-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        let mut left = open(&p, &scope("left"));
        let mut right = open(&p, &scope("right"));
        left.append(&id("left-a"), b"left").unwrap();
        right.append(&id("right-a"), b"right one").unwrap();
        right.append(&id("right-b"), b"right two").unwrap();
        Self(p)
    }
}
impl Drop for Sources {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn open(path: &Path, s: &Scope) -> SqliteReferenceSource {
    // Only the fixture's exact host-authorized stream names reach this method.
    SqliteReferenceSource::open(
        path.join(s.stream().as_str()),
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        s.schema().clone(),
    )
    .unwrap()
}
struct Policy(Arc<AtomicU8>);
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        if s == &scope("left") {
            if self.0.load(Ordering::Acquire) == 3 {
                Access::Denied
            } else {
                Access::Allowed(s.clone())
            }
        } else if s == &scope("right") {
            match self.0.load(Ordering::Acquire) {
                0 | 3 | 4 => Access::Allowed(s.clone()),
                1 => Access::Denied,
                _ => Access::IdentityChanged,
            }
        } else {
            Access::Denied
        }
    }
}
struct Connection {
    io: TcpSessionIo,
    task: thread::JoinHandle<Result<(), RecordSessionError>>,
    calls: Arc<[AtomicUsize; 2]>,
    mode: Arc<AtomicU8>,
}
#[derive(Clone, Copy)]
enum Endpoint {
    Records,
    Pair,
    Batch,
}
fn start(sources: &Sources, mode: u8, endpoint: Endpoint) -> Connection {
    fn tune(s: &TcpStream) {
        s.set_nodelay(true).unwrap();
        let deadline = Some(Duration::from_secs(2));
        s.set_read_timeout(deadline).unwrap();
        s.set_write_timeout(deadline).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    tune(&client);
    let mode = Arc::new(AtomicU8::new(mode));
    let calls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let (m, count, p) = (mode.clone(), calls.clone(), sources.0.clone());
    let task = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        tune(&socket);
        let mut io = TcpSessionIo::new(socket).unwrap();
        let mut policy = Policy(m.clone());
        let factory = |scope: &Scope| {
            let index = match scope.stream().as_str() {
                "left" => 0,
                "right" => 1,
                _ => unreachable!(),
            };
            count[index].fetch_add(1, Ordering::Relaxed);
            // Test gate: when the second view is opened, revoke access to the
            // first view whose metadata the server has already captured.
            if index == 1 && m.load(Ordering::Acquire) == 4 {
                m.store(3, Ordering::Release);
            }
            Ok(open(&p, scope))
        };
        match endpoint {
            Endpoint::Pair => serve_scope_pair_heads(&mut io, 1, limits(), &mut policy, factory),
            Endpoint::Batch => {
                serve_scope_pair_batched_heads(&mut io, 1, limits(), &mut policy, factory)
            }
            Endpoint::Records => {
                serve_capability_record_session(&mut io, 1, limits(), &mut policy, factory)
            }
        }
    });
    Connection {
        io: TcpSessionIo::new(client).unwrap(),
        task,
        calls,
        mode,
    }
}
fn pair(io: TcpSessionIo) -> Result<ScopePairHeadsSession<TcpSessionIo>, RecordSessionError> {
    ScopePairHeadsSession::connect(
        io,
        scope("left"),
        scope("right"),
        limits(),
        CapabilitySupport::current(),
    )
}

#[test]
fn heads_stay_exact_and_refusals_are_independent_on_one_connection() {
    let sources = Sources::new();
    let c = start(&sources, 0, Endpoint::Pair);
    let pair = pair(c.io).unwrap();
    assert_eq!(pair.secondary_binding(), Ok(&scope("right")));
    assert_eq!(pair.counters().unwrap().head_checks, 0);
    for (s, position) in [(scope("left"), 1), (scope("right"), 2), (scope("left"), 1)] {
        let head = pair.head(&s).unwrap();
        assert_eq!(head.scope(), &s);
        assert_eq!(head.position(), position);
    }
    let before = pair.counters().unwrap().exchanges;
    assert!(matches!(
        pair.head(&scope("unknown")),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(pair.counters().unwrap().exchanges, before);
    let right_calls = c.calls[1].load(Ordering::Acquire);

    c.mode.store(1, Ordering::Release);
    assert!(matches!(
        pair.head(&scope("right")),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
    assert_eq!(c.calls[1].load(Ordering::Acquire), right_calls);
    assert_eq!(pair.head(&scope("left")).unwrap().position(), 1);
    c.mode.store(0, Ordering::Release);
    open(&sources.0, &scope("right"))
        .append(&id("right-c"), b"right three")
        .unwrap();
    assert_eq!(pair.head(&scope("right")).unwrap().position(), 3);

    c.mode.store(2, Ordering::Release);
    assert!(matches!(
        pair.head(&scope("right")),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    let after = pair.counters().unwrap().exchanges;
    c.mode.store(0, Ordering::Release);
    assert!(matches!(
        pair.head(&scope("right")),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(pair.counters().unwrap().exchanges, after);
    assert_eq!(pair.head(&scope("left")).unwrap().position(), 1);
    pair.close().unwrap();
    c.task.join().unwrap().unwrap();
}

#[test]
fn secondary_setup_refusal_keeps_primary_and_never_acquires_denied_source() {
    let sources = Sources::new();
    let c = start(&sources, 1, Endpoint::Pair);
    let pair = pair(c.io).unwrap();
    assert_eq!(pair.secondary_binding(), Err(&SourceError::Denied));
    assert_eq!(pair.head(&scope("left")).unwrap().position(), 1);
    let before = pair.counters().unwrap().exchanges;
    c.mode.store(0, Ordering::Release);
    assert!(matches!(
        pair.head(&scope("right")),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(pair.counters().unwrap().exchanges, before);
    assert_eq!(c.calls[1].load(Ordering::Acquire), 0);
    pair.close().unwrap();
    c.task.join().unwrap().unwrap();
}

#[test]
fn older_endpoint_does_not_open_a_legacy_scope_when_pair_mode_is_requested() {
    let sources = Sources::new();
    let c = start(&sources, 0, Endpoint::Records);
    assert!(pair(c.io).is_err());
    assert!(matches!(
        c.task.join().unwrap(),
        Err(RecordSessionError::Protocol)
    ));
    assert_eq!(c.calls[0].load(Ordering::Acquire), 0);
    assert_eq!(c.calls[1].load(Ordering::Acquire), 0);
}

#[test]
fn peer_eof_fences_both_bindings_and_physically_ends_the_live_handle() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let deadline = Some(Duration::from_secs(2));
    client.set_read_timeout(deadline).unwrap();
    client.set_write_timeout(deadline).unwrap();
    let task = thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(deadline).unwrap();
        peer.set_write_timeout(deadline).unwrap();
        assert_eq!(
            read_control(&mut peer, 2048).unwrap(),
            ControlFrame::ScopePairHello(limits())
        );
        write_control(
            &mut peer,
            &ControlFrame::Welcome {
                session: 7,
                limits: limits(),
                record_bytes: 8192,
            },
            2048,
        )
        .unwrap();
        for slot in [1, 2] {
            let ControlFrame::Bind { correlation, scope } = read_control(&mut peer, 2048).unwrap()
            else {
                panic!("expected Bind")
            };
            write_control(
                &mut peer,
                &ControlFrame::Bound {
                    correlation,
                    binding: BindingMetadata {
                        scope,
                        head: 0,
                        handle: slot,
                        generation: u64::from(slot),
                    },
                },
                2048,
            )
            .unwrap();
            peer.flush().unwrap();
        }
        // Receive the complete ordinary Head request, then end the response
        // direction without a result. The peer still owns its read half.
        let mut header = [0; 4];
        peer.read_exact(&mut header).unwrap();
        let length = u32::from_be_bytes(header) as usize;
        assert!(length <= 2048);
        peer.read_exact(&mut vec![0; length]).unwrap();
        peer.shutdown(Shutdown::Write).unwrap();
        assert_eq!(peer.read(&mut [0; 1]).unwrap(), 0);
    });
    let pair = pair(TcpSessionIo::new(client).unwrap()).unwrap();
    assert!(matches!(
        pair.head(&scope("left")),
        Err(RecordSessionError::Io(_))
    ));
    assert!(matches!(
        pair.head(&scope("right")),
        Err(RecordSessionError::Closed)
    ));
    // The witness runs before the handle is dropped or explicitly closed.
    task.join().unwrap();
}

fn batch(io: TcpSessionIo) -> Result<ScopePairHeadsSession<TcpSessionIo>, RecordSessionError> {
    ScopePairHeadsSession::connect_batched(
        io,
        scope("left"),
        scope("right"),
        limits(),
        CapabilitySupport::current(),
    )
}
fn checkpoints() -> [Checkpoint; 2] {
    [
        Checkpoint::new(scope("left"), 1),
        Checkpoint::new(scope("right"), 2),
    ]
}
#[test]
fn batch_saves_one_exchange_and_compares_fresh_and_resumed_exact_observations() {
    let sources = Sources::new();
    let saved = checkpoints();
    // Compare two independent Head RPCs with one batch on matched fresh
    // connections to the same two source paths. Opening costs are identical.
    for resume in [false, true] {
        if resume {
            open(&sources.0, &scope("right"))
                .append(&id("right-c"), b"resumed update")
                .unwrap();
        }
        let baseline = start(&sources, 0, Endpoint::Batch);
        let session = batch(baseline.io).unwrap();
        let before = session.counters().unwrap();
        let expected = [
            session.head(saved[0].scope()).unwrap(),
            session.head(saved[1].scope()).unwrap(),
        ];
        let sequential = session.counters().unwrap();
        session.close().unwrap();
        baseline.task.join().unwrap().unwrap();
        let c = start(&sources, 0, Endpoint::Batch);
        let session = batch(c.io).unwrap();
        assert_eq!(
            session.counters().unwrap().protocol_bytes,
            before.protocol_bytes
        );
        let rows = session.heads(&saved[0], &saved[1]).unwrap();
        let pair = session.counters().unwrap();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(r.checkpoint(), &saved[i]);
            assert_eq!(r.outcome(), &Ok(expected[i].clone()));
        }
        assert!(rows[0].at_checkpoint());
        assert_eq!(rows[1].at_checkpoint(), !resume);
        assert_eq!(pair.exchanges - before.exchanges, 1);
        assert_eq!(sequential.exchanges - before.exchanges, 2);
        assert_eq!(pair.head_checks, 2);
        assert_eq!(sequential.head_checks, 2);
        assert_eq!(pair.payload_bytes, 0);
        assert_eq!(sequential.payload_bytes, 0);
        eprintln!(
            "resume={resume} opening_bytes={} opening_exchanges={} sequential_bytes={} batch_bytes={} sequential_exchanges=2 batch_exchanges=1",
            before.protocol_bytes, before.exchanges, sequential.protocol_bytes-before.protocol_bytes,
            pair.protocol_bytes-before.protocol_bytes
        );
        session.close().unwrap();
        c.task.join().unwrap().unwrap();
    }
}

#[test]
fn batch_denials_do_not_hide_other_outcomes_and_recheck_after_source_read() {
    let sources = Sources::new();
    let c = start(&sources, 0, Endpoint::Batch);
    let session = batch(c.io).unwrap();
    let saved = checkpoints();
    for mode in [1, 3, 4] {
        c.mode.store(mode, Ordering::Release);
        let count = [
            c.calls[0].load(Ordering::Acquire),
            c.calls[1].load(Ordering::Acquire),
        ];
        let rows = session.heads(&saved[0], &saved[1]).unwrap();
        let denied = usize::from(mode == 1);
        assert_eq!(rows[denied].outcome(), &Err(SourceError::Denied));
        assert_eq!(
            rows[1 - denied].outcome().as_ref().unwrap().scope(),
            saved[1 - denied].scope()
        );
        if mode != 4 {
            assert_eq!(c.calls[denied].load(Ordering::Acquire), count[denied]);
        } else {
            assert_eq!(c.calls[denied].load(Ordering::Acquire), count[denied] + 1);
        }
    }
    c.mode.store(2, Ordering::Release);
    let rows = session.heads(&saved[0], &saved[1]).unwrap();
    assert!(rows[0].at_checkpoint());
    assert_eq!(rows[1].outcome(), &Err(SourceError::IdentityChanged));
    let exchanges = session.counters().unwrap().exchanges;
    assert!(matches!(
        session.heads(&saved[0], &saved[1]),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(session.counters().unwrap().exchanges, exchanges);
    assert_eq!(session.head(saved[0].scope()).unwrap().position(), 1);
    session.close().unwrap();
    c.task.join().unwrap().unwrap();
}

#[test]
fn explicit_batch_refuses_legacy_endpoint_and_swapped_checkpoint_without_source_use() {
    let sources = Sources::new();
    let c = start(&sources, 0, Endpoint::Pair);
    assert!(batch(c.io).is_err());
    assert!(matches!(
        c.task.join().unwrap(),
        Err(RecordSessionError::Protocol)
    ));
    assert_eq!(c.calls[0].load(Ordering::Acquire), 0);
    assert_eq!(c.calls[1].load(Ordering::Acquire), 0);
    let c = start(&sources, 0, Endpoint::Batch);
    let session = batch(c.io).unwrap();
    let [left, right] = checkpoints();
    let exchanges = session.counters().unwrap().exchanges;
    assert!(matches!(
        session.heads(&right, &left),
        Err(RecordSessionError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(session.counters().unwrap().exchanges, exchanges);
    assert_eq!(c.calls[0].load(Ordering::Acquire), 1);
    assert_eq!(c.calls[1].load(Ordering::Acquire), 1);
    session.close().unwrap();
    c.task.join().unwrap().unwrap();
}

#[test]
fn equal_checkpoint_keeps_capability_metadata_and_incompatible_is_slot_local() {
    use nessa_sync::replication::domain::CapabilityError;
    let sources = Sources::new();
    let c = start(&sources, 0, Endpoint::Batch);
    let session = batch(c.io).unwrap();
    let saved = checkpoints();
    let left = rusqlite::Connection::open(sources.0.join("left")).unwrap();
    left.execute(
        "UPDATE source_meta SET writer_required = ?1",
        [1_u64.to_be_bytes()],
    )
    .unwrap();
    let rows = session.heads(&saved[0], &saved[1]).unwrap();
    assert!(rows[0].at_checkpoint());
    assert_eq!(rows[0].outcome().as_ref().unwrap().writer_required(), 1);
    left.execute(
        "UPDATE source_meta SET reader_required = ?1",
        [1_u64.to_be_bytes()],
    )
    .unwrap();
    let rows = session.heads(&saved[0], &saved[1]).unwrap();
    assert!(!rows[0].at_checkpoint());
    assert_eq!(
        rows[0].outcome(),
        &Err(SourceError::Incompatible(CapabilityError::MissingReader(1)))
    );
    assert!(rows[1].at_checkpoint());
    session.close().unwrap();
    c.task.join().unwrap().unwrap();
}
