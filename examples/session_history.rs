//! Matched local history workload. The plaintext fixture is not production authentication.
use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer, SourceError},
    domain::{CapabilitySupport, Id, Limits, Scope},
    history::{fetch_older, fetch_tail, HistorySource, HistoryStore, OlderRequest, TailRequest},
    infrastructure::{
        session::{
            connect_follow, serve_history_follow_record_session, HintSignal, SessionLimits,
            TcpSessionIo,
        },
        LoopbackClient, SqliteReferenceSource, SqliteReplicaStore, WireCounters,
    },
};
use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream},
    path::Path,
    time::{Duration, Instant},
};

const TOKEN: &str = "history-test";
const PAYLOAD: usize = 1500;
const BUDGET: usize = 4000;
const LIMITS: SessionLimits = SessionLimits {
    control_bytes: 2048,
    bindings: 1,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn other(e: impl std::fmt::Debug) -> io::Error {
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
fn open(path: &Path) -> Result<SqliteReferenceSource> {
    Ok(SqliteReferenceSource::open(
        path,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )?)
}
struct Policy;
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, request: &Scope) -> Access {
        if request == &scope() {
            Access::Allowed(scope())
        } else {
            Access::Denied
        }
    }
}
fn tune(socket: &TcpStream) -> Result<()> {
    let deadline = Some(Duration::from_secs(12));
    socket.set_nodelay(true)?;
    socket.set_read_timeout(deadline)?;
    socket.set_write_timeout(deadline)?;
    Ok(())
}
fn serve(path: &Path) -> Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    eprintln!("listening on {}", listener.local_addr()?);
    let (mut socket, _) = listener.accept()?;
    tune(&socket)?;
    let mut length = [0; 1];
    socket.read_exact(&mut length)?;
    if usize::from(length[0]) != TOKEN.len() {
        return Err("wrong fixture token length".into());
    }
    let mut token = [0; TOKEN.len()];
    socket.read_exact(&mut token)?;
    if token != TOKEN.as_bytes() {
        return Err("wrong fixture token".into());
    }
    socket.write_all(&[0])?;
    let path = path.to_owned();
    serve_history_follow_record_session(
        TcpSessionIo::new(socket)?,
        1,
        LIMITS,
        Policy,
        move |_| open(&path).map_err(|_| SourceError::Unavailable),
        HintSignal::new(),
    )
    .map_err(other)?;
    Ok(())
}
fn work(
    cache: &Path,
    auth: &mut impl ScopeAuthorizer,
    source: &mut impl HistorySource,
) -> Result<()> {
    let mut store = SqliteReplicaStore::open(cache)?;
    let limits = Limits::new(2, BUDGET, PAYLOAD).map_err(other)?;
    let tail = TailRequest {
        scope: scope(),
        generation: 1,
        max_records: 2,
        max_payload_bytes: BUDGET,
        max_record_bytes: PAYLOAD,
    };
    let progress = store
        .install_tail(fetch_tail(&tail, limits, auth, source).map_err(other)?)
        .map_err(other)?;
    assert_eq!((progress.live_head, progress.lower_bound), (4, 3));
    let older = OlderRequest {
        scope: scope(),
        generation: 1,
        before: progress.lower_bound,
        max_records: 2,
        max_payload_bytes: BUDGET,
        max_record_bytes: PAYLOAD,
    };
    let progress = store
        .install_older(fetch_older(&older, limits, auth, source).map_err(other)?)
        .map_err(other)?;
    assert_eq!((progress.live_head, progress.lower_bound), (4, 1));
    Ok(())
}
fn inspect(cache: &Path) -> Result<()> {
    let mut store = SqliteReplicaStore::open(cache)?;
    let progress = store
        .history_progress(&scope())
        .map_err(other)?
        .ok_or("cache not installed")?;
    assert_eq!(
        (
            progress.live_head,
            progress.lower_bound,
            progress.generation
        ),
        (4, 1, 1)
    );
    let records = store
        .read_after(&scope(), 0, 4, 4 * PAYLOAD)
        .map_err(other)?;
    assert_eq!(records.len(), 4);
    for (i, record) in records.iter().enumerate() {
        let n = i + 1;
        assert_eq!(record.position, n as u64);
        assert_eq!(record.id, id(&format!("event-{n}")));
        assert_eq!(record.payload, vec![n as u8; PAYLOAD]);
    }
    println!("{{\"live_head\":4,\"lower_bound\":1,\"records\":4}}");
    Ok(())
}
fn bench(mode: &str, endpoint: SocketAddrV4, cache: &Path) -> Result<()> {
    if !endpoint.ip().is_loopback() {
        return Err("loopback only".into());
    }
    let started = Instant::now();
    let (wire, setup_bytes, elapsed_ms) = if mode == "session" {
        let mut socket = TcpStream::connect(endpoint)?;
        tune(&socket)?;
        let token_len = TOKEN.len() as u8;
        let mut preamble = vec![token_len];
        preamble.extend_from_slice(TOKEN.as_bytes());
        socket.write_all(&preamble)?;
        let mut ack = [1];
        socket.read_exact(&mut ack)?;
        if ack != [0] {
            return Err("fixture authorization failed".into());
        }
        let session = connect_follow(
            TcpSessionIo::new(socket)?,
            scope(),
            LIMITS,
            Duration::from_secs(12),
        )
        .map_err(other)?;
        let (mut auth, mut source) = session.history_parts(CapabilitySupport::current());
        work(cache, &mut auth, &mut source)?;
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let wire = session.counters().map_err(other)?.wire;
        session.close().map_err(other)?;
        (wire, u64::from(token_len) + 2, elapsed)
    } else if mode == "loopback" {
        let mut auth = LoopbackClient::new(endpoint, id(TOKEN))?;
        let mut source = LoopbackClient::new(endpoint, id(TOKEN))?;
        work(cache, &mut auth, &mut source)?;
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let s = source.counters();
        let a = auth.counters();
        (
            WireCounters {
                exchanges: s.exchanges + a.exchanges,
                protocol_bytes: s.protocol_bytes + a.protocol_bytes,
                ..s
            },
            0,
            elapsed,
        )
    } else {
        return Err("unknown mode".into());
    };
    let protocol = wire.protocol_bytes + setup_bytes;
    let total = protocol + wire.payload_bytes;
    let exchanges = wire.exchanges + u64::from(setup_bytes != 0);
    println!("{{\"payload_bytes\":{},\"protocol_bytes\":{protocol},\"total_bytes\":{total},\"exchanges\":{exchanges},\"elapsed_ms\":{elapsed_ms:.3}}}", wire.payload_bytes);
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.as_slice() {
        [_, mode, path] if mode == "seed" => {
            let mut source = open(Path::new(path))?;
            for n in 1..=4 {
                source.append(&id(&format!("event-{n}")), &vec![n; PAYLOAD]).map_err(other)?;
            }
            Ok(())
        }
        [_, mode, path] if mode == "serve" => serve(Path::new(path)),
        [_, mode, path] if mode == "inspect" => inspect(Path::new(path)),
        [_, command, mode, endpoint, cache] if command == "bench" => bench(mode, endpoint.parse()?, Path::new(cache)),
        _ => Err("usage: session_history seed|serve|inspect PATH, or bench session|loopback ENDPOINT CACHE".into()),
    }
}
