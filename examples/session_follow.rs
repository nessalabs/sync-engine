//! Owned loopback fixture for real polling/push comparisons; not production trust.
use nessa_sync::replication::{
    application::{
        begin_pass, begin_pass_with_page, finish_pass, Access, Clock, FreshnessPolicy,
        FreshnessWake, FreshnessWork, OpenRecordSource, RecordFreshness, RecordSource,
        ReplicaStore, RetryJitter, RetryPolicy, ScopeAuthorizer, SourceError, StoreError,
        SyncError, MAX_RECORD_BYTES,
    },
    domain::{CapabilitySupport, Checkpoint, CommitPlan, Id, Limits, Scope},
    infrastructure::session::{
        connect_follow, serve_capability_follow_record_session, serve_follow_record_session,
        CapabilityFollowSource, DuplexSessionIo, FollowAuthorizer, FollowSession, FollowSource,
        HintSignal, HintWake, InterruptibleSessionIo, RecordSession, RecordSessionAuthorizer,
        RecordSessionSource, SessionInterrupt, SessionLimits, TcpSessionIo, TcpSessionWriter,
    },
    infrastructure::{SqliteReferenceSource, SqliteReplicaStore, WireCounters},
};
use std::collections::HashMap;
use std::env;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
type Error = Box<dyn std::error::Error>;
fn error<E: std::fmt::Debug>(e: E) -> io::Error {
    io::Error::other(format!("{e:?}"))
}
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
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
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings: 1,
    }
}
fn budget() -> Limits {
    Limits::new(32, 128 * 1024, 128 * 1024).unwrap()
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .try_into()
        .unwrap()
}
fn address(s: &str) -> Result<SocketAddrV4, Error> {
    match s.parse::<SocketAddr>()? {
        SocketAddr::V4(a) if *a.ip() == Ipv4Addr::LOCALHOST => Ok(a),
        _ => Err("fixture requires IPv4 loopback".into()),
    }
}
fn setup(s: &TcpStream) -> io::Result<()> {
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.set_write_timeout(Some(Duration::from_secs(5)))
}
fn source(p: &Path) -> Result<SqliteReferenceSource, Error> {
    Ok(SqliteReferenceSource::open(
        p,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )?)
}
fn fact(position: usize) -> (Id, Vec<u8>) {
    let batch = (position - 1) / 7;
    let index = (position - 1) % 7;
    let base = if index == 6 {
        "bulk-fact".to_owned()
    } else {
        format!("fact-{}", index + 1)
    };
    let name = if batch == 0 {
        base
    } else {
        format!("{base}-batch-{batch}")
    };
    let payload = if index == 6 {
        vec![b'p'; 4096]
    } else {
        ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"][index]
            .as_bytes()
            .to_vec()
    };
    (id(&name), payload)
}
struct Policy;
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        if s.receiver() != scope().receiver() {
            Access::Denied
        } else {
            Access::Allowed(scope())
        }
    }
}
type Sent = Arc<Mutex<HashMap<u64, (u64, u64)>>>;
struct ObserveIo {
    inner: TcpSessionIo,
    sent: Sent,
}
struct ObserveWriter {
    inner: TcpSessionWriter,
    sent: Sent,
}
impl Read for ObserveIo {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.inner.read(b)
    }
}
impl Write for ObserveIo {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.inner.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl InterruptibleSessionIo for ObserveIo {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        self.inner.interrupt_handle()
    }
}
impl DuplexSessionIo for ObserveIo {
    type Reader = <TcpSessionIo as DuplexSessionIo>::Reader;
    type Writer = ObserveWriter;
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let (r, w) = self.inner.split()?;
        Ok((
            r,
            ObserveWriter {
                inner: w,
                sent: self.sent,
            },
        ))
    }
}
impl Write for ObserveWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.len() == 56 && &b[4..8] == b"NSR1" && b[10] == 64 && b[11] == 0 {
            let head = u64::from_be_bytes(b[48..56].try_into().unwrap());
            let mut map = self.sent.lock().unwrap();
            let value = map.entry(head).or_insert((0, 0));
            value.0 += 1;
            value.1 = now();
        }
        self.inner.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
fn authenticate(s: &mut TcpStream) -> io::Result<u8> {
    let mut size = [0];
    s.read_exact(&mut size)?;
    if size[0] == 0 || size[0] > 128 {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    let mut token = vec![0; size[0] as usize];
    s.read_exact(&mut token)?;
    let role = if token == b"dev-read-secret" {
        1
    } else if token == b"dev-write-secret" {
        2
    } else {
        0
    };
    s.write_all(&[if role == 0 { 1 } else { 0 }])?;
    if role == 0 {
        Err(io::ErrorKind::PermissionDenied.into())
    } else {
        Ok(role)
    }
}
fn dial(addr: SocketAddrV4, token: &[u8]) -> Result<TcpStream, Error> {
    let mut s = TcpStream::connect_timeout(&addr.into(), Duration::from_secs(5))?;
    setup(&s)?;
    s.write_all(&[token.len() as u8])?;
    s.write_all(token)?;
    let mut ack = [1];
    s.read_exact(&mut ack)?;
    if ack != [0] {
        return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
    }
    Ok(s)
}
fn u64_from(s: &mut TcpStream) -> io::Result<u64> {
    let mut b = [0; 8];
    s.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}
fn produce(addr: SocketAddrV4, after: u64, count: u8, repeat: u8) -> Result<u64, Error> {
    let mut s = dial(addr, b"dev-write-secret")?;
    s.write_all(&[1])?;
    s.write_all(&after.to_be_bytes())?;
    s.write_all(&[count, repeat])?;
    let head = u64_from(&mut s)?;
    if head != after + count as u64 {
        return Err("fixture producer head mismatch".into());
    }
    Ok(u64_from(&mut s)?)
}
fn hint_sent(addr: SocketAddrV4, head: u64) -> Result<(u64, u64), Error> {
    let mut s = dial(addr, b"dev-write-secret")?;
    s.write_all(&[2])?;
    s.write_all(&head.to_be_bytes())?;
    Ok((u64_from(&mut s)?, u64_from(&mut s)?))
}
fn serve(path: PathBuf, port: u16, capability: bool) -> Result<(), Error> {
    drop(source(&path)?);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    eprintln!("follow fixture listening on {}", listener.local_addr()?);
    let signal = HintSignal::new();
    let serial = Arc::new(Mutex::new(1_u64));
    let sent: Sent = Arc::new(Mutex::new(HashMap::new()));
    for incoming in listener.incoming() {
        let mut socket = incoming?;
        setup(&socket)?;
        let path = path.clone();
        let signal = signal.clone();
        let serial = serial.clone();
        let sent = sent.clone();
        thread::spawn(move || {
            let run = (|| -> Result<(), Error> {
                match authenticate(&mut socket)? {
                    1 => {
                        let number = {
                            let mut n = serial.lock().unwrap();
                            let value = *n;
                            *n = n.checked_add(1).ok_or("namespace exhausted")?;
                            value
                        };
                        let io = ObserveIo {
                            inner: TcpSessionIo::new(socket)?,
                            sent,
                        };
                        let factory = move |_: &Scope| {
                            SqliteReferenceSource::open(
                                &path,
                                id("example-origin"),
                                id("transcript"),
                                id("first"),
                                id("text-v1"),
                            )
                            .map_err(|_| SourceError::Unavailable)
                        };
                        if capability {
                            serve_capability_follow_record_session(
                                io,
                                number,
                                limits(),
                                Policy,
                                factory,
                                signal,
                            )
                        } else {
                            serve_follow_record_session(
                                io,
                                number,
                                limits(),
                                Policy,
                                factory,
                                signal,
                            )
                        }
                        .map_err(error)?;
                    }
                    2 => {
                        let mut cmd = [0];
                        socket.read_exact(&mut cmd)?;
                        match cmd[0] {
                            1 => {
                                let after = u64_from(&mut socket)?;
                                let mut params = [0; 2];
                                socket.read_exact(&mut params)?;
                                if params[0] == 0 || params[0] > 7 || params[1] > 32 {
                                    return Err("invalid owned fixture batch".into());
                                }
                                let mut source = source(&path)?;
                                if source.head(&scope()).map_err(error)? != after {
                                    return Err(
                                        "fixture requires exact expected source head".into()
                                    );
                                }
                                for n in after + 1..=after + params[0] as u64 {
                                    let (id, payload) = fact(n as usize);
                                    source.append(&id, &payload).map_err(error)?;
                                }
                                let committed = now();
                                for _ in 0..params[1] {
                                    signal.notify().map_err(error)?;
                                }
                                socket.write_all(&(after + params[0] as u64).to_be_bytes())?;
                                socket.write_all(&committed.to_be_bytes())?;
                            }
                            2 => {
                                let head = u64_from(&mut socket)?;
                                let record =
                                    sent.lock().unwrap().get(&head).copied().unwrap_or_default();
                                socket.write_all(&record.0.to_be_bytes())?;
                                socket.write_all(&record.1.to_be_bytes())?;
                            }
                            _ => return Err("unknown fixture command".into()),
                        }
                    }
                    _ => unreachable!(),
                }
                Ok(())
            })();
            if let Err(e) = run {
                eprintln!("follow fixture channel ended: {e}");
            }
        });
    }
    Ok(())
}
struct TimedStore {
    inner: SqliteReplicaStore,
    applied: u64,
}
impl ReplicaStore for TimedStore {
    fn load(&mut self, s: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.inner.load(s)
    }
    fn apply(&mut self, p: CommitPlan) -> Result<(), StoreError> {
        self.inner.apply(p)?;
        self.applied = now();
        Ok(())
    }
}
fn catch_up<A: ScopeAuthorizer, S: OpenRecordSource>(
    a: &mut A,
    s: &mut S,
    store: &mut TimedStore,
) -> Result<u64, SyncError> {
    let mut pass = begin_pass_with_page(&scope(), budget(), a, s, store)?;
    loop {
        finish_pass(&mut pass, budget(), a, s, store)?;
        let applied = pass.position();
        pass = begin_pass(&scope(), a, s, store)?;
        if pass.is_complete() {
            return Ok(applied);
        }
    }
}
struct Elapsed(Instant);
impl Clock for Elapsed {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}
struct Half;
impl RetryJitter for Half {
    fn delay(&mut self, ceiling: Duration) -> Duration {
        ceiling / 2
    }
}
type FreshnessOwner = RecordFreshness<Elapsed, Half>;
enum Ports {
    Freshness {
        root: FollowSession<TcpSessionWriter>,
        a: FollowAuthorizer<TcpSessionWriter>,
        s: CapabilityFollowSource<TcpSessionWriter>,
        owner: Box<FreshnessOwner>,
    },
    Poll {
        root: RecordSession<TcpSessionIo>,
        a: RecordSessionAuthorizer<TcpSessionIo>,
        s: RecordSessionSource<TcpSessionIo>,
    },
    Push {
        root: FollowSession<TcpSessionWriter>,
        a: FollowAuthorizer<TcpSessionWriter>,
        s: FollowSource<TcpSessionWriter>,
    },
}
impl Ports {
    fn connect(mode: &str, addr: SocketAddrV4) -> Result<Self, Error> {
        let io = TcpSessionIo::new(dial(addr, b"dev-read-secret")?)?;
        match mode {
            "poll" => {
                let root = RecordSession::connect_open_interruptible(io, scope(), limits())
                    .map_err(error)?;
                let (a, s) = root.parts();
                Ok(Self::Poll { root, a, s })
            }
            "freshness" => {
                let root = connect_follow(io, scope(), limits(), Duration::from_secs(20))
                    .map_err(error)?;
                let (a, s) = root.capability_parts(CapabilitySupport::current());
                let owner = RecordFreshness::new(
                    scope(),
                    FreshnessPolicy::Never,
                    CapabilitySupport::current(),
                    Elapsed(Instant::now()),
                    Half,
                    RetryPolicy::new(Duration::from_millis(100), Duration::from_secs(2))
                        .map_err(error)?,
                );
                Ok(Self::Freshness {
                    root,
                    a,
                    s,
                    owner: Box::new(owner),
                })
            }
            "push" => {
                let root = connect_follow(io, scope(), limits(), Duration::from_secs(20))
                    .map_err(error)?;
                let (a, s) = root.parts();
                Ok(Self::Push { root, a, s })
            }
            _ => Err("mode must be poll or push".into()),
        }
    }
    fn counters(&self) -> Result<(WireCounters, u64, u64), Error> {
        match self {
            Self::Poll { root, .. } => Ok((root.counters().map_err(error)?, 0, 0)),
            Self::Push { root, .. } | Self::Freshness { root, .. } => {
                let c = root.counters().map_err(error)?;
                Ok((c.wire, c.hints.frames_received, c.hints.coalesced))
            }
        }
    }
    fn wait(&self, cadence: Duration) -> Result<&'static str, Error> {
        match self {
            Self::Poll { .. } => {
                thread::sleep(cadence);
                Ok("explicit_poll")
            }
            Self::Push { root, .. } | Self::Freshness { root, .. } => {
                match root.hints().wait(cadence).map_err(error)? {
                    HintWake::Changed { .. } => Ok("push"),
                    HintWake::Timeout => Ok("fallback"),
                }
            }
        }
    }
    fn pending(&self, head: u64) -> Result<(), Error> {
        if let Self::Push { root, .. } | Self::Freshness { root, .. } = self {
            let until = Instant::now() + Duration::from_secs(5);
            while root.hints().stats().map_err(error)?.pending_head != Some(head) {
                if Instant::now() > until {
                    return Err("coalesced fixture hint deadline".into());
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
        Ok(())
    }
    fn catch_up(&mut self, store: &mut TimedStore) -> Result<u64, Error> {
        match self {
            Self::Poll { a, s, .. } => Ok(catch_up(a, s, store).map_err(error)?),
            Self::Push { a, s, .. } => Ok(catch_up(a, s, store).map_err(error)?),
            Self::Freshness { a, s, owner, .. } => {
                // Explicit host wake only in this prerequisite comparison. The
                // owner retains the pass and mandatory recheck; no second loop.
                owner.request(FreshnessWake::Explicit);
                for _ in 0..128 {
                    if owner.drive(budget(), a, s, store).map_err(error)? == FreshnessWork::Waiting
                    {
                        return owner
                            .metadata()
                            .map_err(error)?
                            .position
                            .ok_or_else(|| "unknown progress".into());
                    }
                }
                Err("bounded fixture did not settle".into())
            }
        }
    }
    fn close(&self) -> Result<(), Error> {
        match self {
            Self::Poll { root, .. } => root.close().map_err(error)?,
            Self::Push { root, .. } | Self::Freshness { root, .. } => {
                root.close().map_err(error)?
            }
        }
        Ok(())
    }
}
fn inspect(path: &Path) -> Result<(u64, u64, u64), Error> {
    let mut store = SqliteReplicaStore::open(path)?;
    let saved = store
        .checkpoint(&scope())
        .map_err(error)?
        .map_or(0, |c| c.position());
    let records = store
        .read_after(&scope(), 0, 64, MAX_RECORD_BYTES)
        .map_err(error)?;
    if records.len() as u64 != saved || store.count(&scope()).map_err(error)? != saved || saved > 35
    {
        return Err("fixture cache count mismatch".into());
    }
    for (index, r) in records.iter().enumerate() {
        let (id, payload) = fact(index + 1);
        if r.position != (index + 1) as u64
            || r.id != id
            || r.payload != payload
            || r.scope != scope()
        {
            return Err("fixture cache record mismatch".into());
        }
    }
    Ok((
        saved,
        saved,
        records.iter().map(|r| r.payload.len() as u64).sum(),
    ))
}
fn bench(
    mode: &str,
    data: SocketAddrV4,
    control: SocketAddrV4,
    path: &Path,
    cadence: Duration,
) -> Result<(), Error> {
    if inspect(path)? != (0, 0, 0) {
        return Err("bench requires new empty fixture cache".into());
    }
    let mut ports: Option<Ports> = None;
    let mut reports = Vec::new();
    for index in 0..6 {
        if index == 2 {
            if let Some(old) = ports.take() {
                old.close()?;
            }
        }
        let before = ports.as_ref().map(Ports::counters).transpose()?.unwrap_or((
            WireCounters::default(),
            0,
            0,
        ));
        let target = [7, 14, 21, 21, 28, 30][index];
        let committed = match index {
            0..=2 => produce(control, target - 7, 7, 1)?,
            3 => 0,
            4 => produce(control, 21, 7, 16)?,
            _ => {
                produce(control, 28, 1, 1)?;
                ports.as_ref().unwrap().pending(29)?;
                let stamp = produce(control, 29, 1, 1)?;
                ports.as_ref().unwrap().pending(30)?;
                stamp
            }
        };
        let start = Instant::now();
        let mut host_auth = 0;
        if ports.is_none() {
            ports = Some(Ports::connect(mode, data)?);
            host_auth = 1;
        }
        let wake = if [1, 3, 4, 5].contains(&index) {
            ports.as_ref().unwrap().wait(cadence)?
        } else {
            "connect"
        };
        let wake_consumed = now();
        let mut store = TimedStore {
            inner: SqliteReplicaStore::open(path)?,
            applied: 0,
        };
        let applied = ports.as_mut().unwrap().catch_up(&mut store)?;
        let end = now();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let durable = store.applied;
        drop(store);
        let verified = inspect(path)?;
        let expected_bytes = (1..=target as usize).map(|n| fact(n).1.len() as u64).sum();
        if applied != target || verified != (target, target, expected_bytes) {
            return Err("durable fixture convergence failed".into());
        }
        let (after, hints, coalesced) = ports.as_ref().unwrap().counters()?;
        let protocol = after.protocol_bytes - before.0.protocol_bytes + host_auth * 17;
        let payload = after.payload_bytes - before.0.payload_bytes;
        let frames = hints - before.1;
        let count = after.exchanges - before.0.exchanges + host_auth;
        let (sent_count, sent) = if mode != "poll" && [1, 4, 5].contains(&index) {
            hint_sent(control, target)?
        } else {
            (0, 0)
        };
        let expected_payload = match index {
            3 => 0,
            5 => 9,
            _ => 4126,
        };
        if payload != expected_payload || after.duplicate_bytes - before.0.duplicate_bytes != 0 {
            return Err("payload accounting mismatch".into());
        }
        let latency = |from: u64| -> Result<String, Error> {
            if from == 0 || durable == 0 {
                Ok("null".into())
            } else if durable < from {
                Err("fixture wall clock changed".into())
            } else {
                Ok(format!("{:.3}", (durable - from) as f64 / 1e6))
            }
        };
        let phase = [
            "cold_connect",
            "warm_update",
            "reconnect",
            "idle_fallback",
            "lost_hint_fallback",
            "coalesced_updates",
        ][index];
        reports.push(format!("{{\"phase\":\"{phase}\",\"wake\":\"{wake}\",\"elapsed_ms\":{elapsed:.3},\"commit_to_durable_ms\":{},\"notification_to_durable_ms\":{},\"wake_to_durable_ms\":{},\"exchanges\":{count},\"protocol_bytes\":{protocol},\"payload_bytes\":{payload},\"total_bytes\":{},\"hint_frames\":{frames},\"coalesced\":{},\"source_hint_writes\":{sent_count},\"head_checks\":{},\"checkpoint\":{target},\"records\":{target},\"ordered_complete\":true,\"commit_ns\":{committed},\"hint_write_ns\":{sent},\"durable_ns\":{durable},\"complete_ns\":{end}}}",latency(committed)?,latency(if wake=="push"{sent}else{0})?,latency(wake_consumed)?,protocol+payload,coalesced-before.2,after.head_checks-before.0.head_checks));
    }
    if let Some(root) = ports {
        root.close()?;
    }
    println!("{{\"mode\":\"{mode}\",\"phases\":[{}]}}", reports.join(","));
    Ok(())
}
fn run() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str){Some("serve") if args.len()==4=>serve(PathBuf::from(&args[2]),args[3].parse()?,false),Some("serve-cap") if args.len()==4=>serve(PathBuf::from(&args[2]),args[3].parse()?,true),Some("bench") if args.len()==7=>bench(&args[2],address(&args[3])?,address(&args[4])?,Path::new(&args[5]),Duration::from_millis(args[6].parse()?)),Some("inspect") if args.len()==3=>{let(c,n,b)=inspect(Path::new(&args[2]))?;println!("{{\"checkpoint\":{c},\"records\":{n},\"payload_bytes\":{b},\"ordered_complete\":true}}");Ok(())},_=>Err("usage: session_follow serve SOURCE PORT | bench poll|push DATA_ADDR CONTROL_ADDR NEW_CACHE CADENCE_MS | inspect CACHE".into())}
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
