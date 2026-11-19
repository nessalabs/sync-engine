//! Loopback-only fixture host and paired record measurement driver. Not a
//! deployed authenticated transport: the test token is plaintext on loopback.
//! The protocol adapter requires a real host-authenticated channel in production.
use nessa_sync::replication::{
    application::{
        begin_pass, finish_pass, Access, RecordSource, ReplicaStore, ScopeAuthorizer, SourceError,
        SyncError,
    },
    domain::{Id, Limits, Scope, MAX_ID_BYTES},
    infrastructure::session::{
        serve_record_session, RecordSession, RecordSessionAuthorizer, RecordSessionSource,
        SessionLimits,
    },
    infrastructure::{LoopbackClient, SqliteReferenceSource, SqliteReplicaStore, WireCounters},
};
use std::env;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
type Error = Box<dyn std::error::Error>;
fn error<E: std::fmt::Debug>(e: E) -> io::Error {
    io::Error::other(format!("{e:?}"))
}
fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("local-epoch-1"),
    )
}
fn source(path: &Path) -> Result<SqliteReferenceSource, Error> {
    Ok(SqliteReferenceSource::open(
        path,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )?)
}
fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings: 1,
    }
}
fn address(value: &str) -> Result<SocketAddrV4, Error> {
    match value.parse::<SocketAddr>()? {
        SocketAddr::V4(a) if *a.ip() == Ipv4Addr::LOCALHOST => Ok(a),
        _ => Err("fixture requires IPv4 loopback".into()),
    }
}
struct FixturePolicy;
impl ScopeAuthorizer for FixturePolicy {
    fn authorize(&mut self, requested: &Scope) -> Access {
        if !["device-a", "device-b"].contains(&requested.receiver().as_str()) {
            return Access::Denied;
        }
        Access::Allowed(scope(requested.receiver().as_str()))
    }
}
fn setup(socket: &TcpStream) -> io::Result<()> {
    socket.set_nodelay(true)?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    socket.set_write_timeout(Some(Duration::from_secs(10)))
}
fn authenticate(socket: &mut TcpStream, token: &Id) -> io::Result<bool> {
    let mut length = [0];
    socket.read_exact(&mut length)?;
    let size = length[0] as usize;
    if size == 0 || size > MAX_ID_BYTES {
        return Ok(false);
    }
    let mut received = vec![0; size];
    socket.read_exact(&mut received)?;
    let allowed = received == token.as_str().as_bytes();
    socket.write_all(&[if allowed { 0 } else { 1 }])?;
    Ok(allowed)
}
fn dial(endpoint: SocketAddrV4, token: &Id) -> Result<(RecordSession<TcpStream>, u64), Error> {
    let mut socket = TcpStream::connect_timeout(&endpoint.into(), Duration::from_secs(5))?;
    setup(&socket)?;
    socket.write_all(&[token.as_str().len() as u8])?;
    socket.write_all(token.as_str().as_bytes())?;
    let mut ack = [0];
    socket.read_exact(&mut ack)?;
    if ack != [0] {
        return Err(io::Error::from(io::ErrorKind::PermissionDenied).into());
    }
    let session = RecordSession::connect(socket, scope("device-a"), limits()).map_err(error)?;
    Ok((session, (token.as_str().len() + 2) as u64))
}
fn catch_up<A: ScopeAuthorizer, S: RecordSource, D: ReplicaStore>(
    a: &mut A,
    s: &mut S,
    d: &mut D,
) -> Result<u64, SyncError> {
    let target = scope("device-a");
    let mut pass = begin_pass(&target, a, s, d)?;
    loop {
        finish_pass(
            &mut pass,
            Limits::new(32, 128 * 1024, 128 * 1024).unwrap(),
            a,
            s,
            d,
        )?;
        let applied = pass.position();
        pass = begin_pass(&target, a, s, d)?; // Preserve the deliberate post-pass recheck.
        if pass.is_complete() {
            return Ok(applied);
        }
    }
}
enum Ports {
    Loopback {
        a: Box<LoopbackClient>,
        s: Box<LoopbackClient>,
    },
    Session {
        session: RecordSession<TcpStream>,
        a: RecordSessionAuthorizer<TcpStream>,
        s: RecordSessionSource<TcpStream>,
    },
}
impl Ports {
    fn connect(mode: &str, endpoint: SocketAddrV4, token: &Id) -> Result<(Self, u64, u64), Error> {
        if mode == "loopback" {
            Ok((
                Self::Loopback {
                    a: Box::new(LoopbackClient::new(endpoint, token.clone())?),
                    s: Box::new(LoopbackClient::new(endpoint, token.clone())?),
                },
                0,
                0,
            ))
        } else if mode == "session" {
            let (session, auth_bytes) = dial(endpoint, token)?;
            let (a, s) = session.parts();
            Ok((Self::Session { session, a, s }, auth_bytes, 1))
        } else {
            Err("mode must be loopback or session".into())
        }
    }
    fn counters(&self) -> Result<WireCounters, Error> {
        match self {
            Self::Loopback { a, s } => {
                let (a, s) = (a.counters(), s.counters());
                Ok(WireCounters {
                    exchanges: a.exchanges + s.exchanges,
                    protocol_bytes: a.protocol_bytes + s.protocol_bytes,
                    payload_bytes: a.payload_bytes + s.payload_bytes,
                    duplicate_bytes: a.duplicate_bytes + s.duplicate_bytes,
                    head_checks: a.head_checks + s.head_checks,
                    ..WireCounters::default()
                })
            }
            Self::Session { session, .. } => Ok(session.counters().map_err(error)?),
        }
    }
    fn catch_up(&mut self, store: &mut SqliteReplicaStore) -> Result<u64, Error> {
        match self {
            Self::Loopback { a, s } => {
                let _subscription = s.subscribe(&scope("device-a")).map_err(error)?;
                Ok(catch_up(a.as_mut(), s.as_mut(), store).map_err(error)?)
            }
            Self::Session { a, s, .. } => Ok(catch_up(a, s, store).map_err(error)?),
        }
    }
    fn close(&self) -> Result<(), Error> {
        if let Self::Session { session, .. } = self {
            session.close().map_err(error)?;
        }
        Ok(())
    }
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
fn inspect(path: &Path) -> Result<(u64, u64, u64), Error> {
    let mut store = SqliteReplicaStore::open(path)?;
    let target = scope("device-a");
    let checkpoint = store
        .checkpoint(&target)
        .map_err(error)?
        .map_or(0, |c| c.position());
    let count = store.count(&target).map_err(error)?;
    if count > 21 || count != checkpoint {
        return Err("fixture cache count/checkpoint mismatch".into());
    }
    let records = store
        .read_after(&target, 0, 64, 1024 * 1024)
        .map_err(error)?;
    if records.len() as u64 != count {
        return Err("fixture cache incomplete".into());
    }
    for (index, record) in records.iter().enumerate() {
        let (expected_id, payload) = fact(index + 1);
        if record.position != (index + 1) as u64
            || record.id != expected_id
            || record.payload != payload
            || record.scope != target
        {
            return Err("fixture cache record mismatch".into());
        }
    }
    Ok((
        checkpoint,
        count,
        records.iter().map(|r| r.payload.len() as u64).sum(),
    ))
}
fn bench(
    mode: &str,
    endpoint: SocketAddrV4,
    source_path: &Path,
    replica: &Path,
    token: &Id,
) -> Result<(), Error> {
    let mut producer = source(source_path)?;
    if producer.head(&scope("device-a")).map_err(error)? != 0 || inspect(replica)? != (0, 0, 0) {
        return Err("bench requires new empty fixture source and replica".into());
    }
    let mut ports: Option<Ports> = None;
    let mut reports = Vec::new();
    for phase in 0..3 {
        for position in phase * 7 + 1..=phase * 7 + 7 {
            let (record, payload) = fact(position);
            if producer.append(&record, &payload).map_err(error)? != position as u64 {
                return Err("fixture source changed unexpectedly".into());
            }
        }
        if phase == 2 {
            if let Some(old) = ports.take() {
                old.close()?;
            }
        }
        let before = ports
            .as_ref()
            .map(Ports::counters)
            .transpose()?
            .unwrap_or_default();
        let start = Instant::now();
        let mut auth_bytes = 0;
        let mut auth_exchanges = 0;
        if ports.is_none() {
            let (client, bytes, exchanges) = Ports::connect(mode, endpoint, token)?;
            ports = Some(client);
            auth_bytes = bytes;
            auth_exchanges = exchanges;
        }
        let mut store = SqliteReplicaStore::open(replica)?;
        let applied = ports.as_mut().unwrap().catch_up(&mut store)?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        drop(store); // Verification reopens the durable cache after the measured commit.
        let expected = (phase * 7 + 7) as u64;
        let verified = inspect(replica)?;
        if applied != expected || verified != (expected, expected, expected / 7 * 4126) {
            return Err("fixture convergence failed".into());
        }
        let after = ports.as_ref().unwrap().counters()?;
        let protocol = after.protocol_bytes - before.protocol_bytes + auth_bytes;
        let payload = after.payload_bytes - before.payload_bytes;
        let duplicate = after.duplicate_bytes - before.duplicate_bytes;
        if payload != 4126 || duplicate != 0 {
            return Err("fixture payload accounting mismatch".into());
        }
        let phase_name = ["cold_connect", "warm_catch_up", "reconnect"][phase];
        reports.push(format!("{{\"phase\":\"{phase_name}\",\"elapsed_ms\":{elapsed_ms:.3},\"exchanges\":{},\"protocol_exchanges\":{},\"host_auth_exchanges\":{auth_exchanges},\"protocol_bytes\":{protocol},\"host_auth_bytes\":{auth_bytes},\"payload_bytes\":{payload},\"total_bytes\":{},\"duplicate_bytes\":{duplicate},\"head_checks\":{},\"checkpoint\":{applied},\"records\":{},\"ordered_complete\":true}}",after.exchanges-before.exchanges+auth_exchanges,after.exchanges-before.exchanges,protocol+payload,after.head_checks-before.head_checks,verified.1));
    }
    if let Some(client) = ports.take() {
        client.close()?;
    }
    println!(
        "{{\"transport\":\"{mode}\",\"phases\":[{}]}}",
        reports.join(",")
    );
    Ok(())
}
fn run() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str){
        Some("serve") if args.len()==5=>{
            let path=std::path::PathBuf::from(&args[2]); drop(source(&path)?);
            let listener=TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST,args[3].parse()?))?;
            let token=Id::new(&args[4]).map_err(error)?; let next=Arc::new(Mutex::new(1_u64));
            eprintln!("fixture session listening on {}",listener.local_addr()?);
            for incoming in listener.incoming(){let mut socket=incoming?;setup(&socket)?;let path=path.clone();let token=token.clone();let next=next.clone();
                thread::spawn(move || {
                    let mut run = || -> Result<(), Error> {
                        if !authenticate(&mut socket, &token)? { return Ok(()); }
                        let number = {
                            let mut value = next.lock().map_err(error)?;
                            let number = *value;
                            *value = number.checked_add(1).ok_or("fixture namespace exhausted")?;
                            number
                        };
                        serve_record_session(&mut socket, number, limits(), &mut FixturePolicy, |_| {
                            SqliteReferenceSource::open(&path, id("example-origin"), id("transcript"), id("first"), id("text-v1"))
                                .map_err(|_| SourceError::Unavailable)
                        }).map_err(error)?;
                        Ok(())
                    };
                    if let Err(e) = run() { eprintln!("fixture session ended: {e}"); }
                });
            }Ok(())
        }
        Some("bench") if args.len()==7=>bench(&args[2],address(&args[3])?,Path::new(&args[4]),Path::new(&args[5]),&Id::new(&args[6]).map_err(error)?),
        Some("inspect") if args.len()==3=>{let(c,n,p)=inspect(Path::new(&args[2]))?;println!("{{\"checkpoint\":{c},\"records\":{n},\"payload_bytes\":{p},\"ordered_complete\":true}}");Ok(())}
        _=>Err("usage: session_records serve SOURCE PORT READ_TOKEN | bench loopback|session ADDR EMPTY_SOURCE EMPTY_REPLICA READ_TOKEN | inspect REPLICA".into()),
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
