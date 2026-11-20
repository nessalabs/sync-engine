//! Owned TCP/SQLite fixture; no deployed identity or transport-security claim.
use nessa_sync::replication::{
    application::{Access, RecordSource, ScopeAuthorizer, SourceError, MAX_RECORD_BYTES},
    domain::{Id, Limits, Scope},
    infrastructure::{
        session::{
            serve_capability_follow_record_session, FollowError, HintSignal,
            InterruptibleSessionIo, SessionLimits, TcpSessionIo,
        },
        SqliteReferenceSource,
    },
};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
pub type Error = Box<dyn std::error::Error>;
pub fn err(e: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{e:?}"))
}
pub fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
pub fn scope() -> Scope {
    Scope::new(
        id("device-a"),
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("local-epoch-1"),
    )
}
pub fn limits() -> SessionLimits {
    SessionLimits {
        control_bytes: 8192,
        bindings: 1,
    }
}
pub fn budget(records: usize) -> Limits {
    Limits::new(records, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap()
}
pub fn fact(n: usize) -> (Id, Vec<u8>) {
    let i = (n - 1) % 7;
    let batch = (n - 1) / 7;
    let name = if i == 6 {
        "bulk-fact".to_owned()
    } else {
        format!("fact-{}", i + 1)
    };
    let name = if batch == 0 {
        name
    } else {
        format!("{name}-batch-{batch}")
    };
    (
        id(&name),
        if i == 6 {
            vec![b'p'; 4096]
        } else {
            ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"][i]
                .as_bytes()
                .to_vec()
        },
    )
}
pub fn source(path: &Path) -> Result<SqliteReferenceSource, Error> {
    Ok(SqliteReferenceSource::open(
        path,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )?)
}
pub fn tune(s: &TcpStream) -> io::Result<()> {
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(20)))?;
    s.set_write_timeout(Some(Duration::from_secs(20)))
}
pub fn dial(address: &str, token: &[u8]) -> Result<TcpStream, FollowError> {
    let selected: std::net::SocketAddr = address.parse().map_err(|_| FollowError::Protocol)?;
    if !selected.ip().is_loopback() {
        return Err(FollowError::Protocol);
    }
    let mut s = TcpStream::connect_timeout(&selected, Duration::from_secs(5))
        .map_err(|_| FollowError::Unavailable)?;
    tune(&s).map_err(|_| FollowError::Unavailable)?;
    s.write_all(&[token.len() as u8])
        .and_then(|_| s.write_all(token))
        .map_err(|_| FollowError::Unavailable)?;
    let mut ack = [0];
    s.read_exact(&mut ack)
        .map_err(|_| FollowError::Unavailable)?;
    match ack[0] {
        0 => Ok(s),
        1 => Err(FollowError::Refused(SourceError::Denied)),
        _ => Err(FollowError::Protocol),
    }
}

pub fn control(address: &str, command: u8, value: u8) -> Result<u64, Error> {
    let mut s = dial(address, b"dev-write-secret").map_err(err)?;
    s.write_all(&[command, value])?;
    let mut n = [0; 8];
    s.read_exact(&mut n)?;
    Ok(u64::from_be_bytes(n))
}
struct Policy(Arc<AtomicU8>);
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        if s != &scope() {
            return Access::IdentityChanged;
        }
        match self.0.load(Ordering::SeqCst) {
            0 => Access::Allowed(scope()),
            1 => Access::Denied,
            2 => Access::Unverifiable,
            3 => Access::IdentityChanged,
            _ => Access::InvalidResponse,
        }
    }
}
pub fn serve(path: PathBuf, port: u16) -> Result<(), Error> {
    drop(source(&path)?);
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("freshness fixture listening on {}", listener.local_addr()?);
    let policy = Arc::new(AtomicU8::new(0));
    let unavailable = Arc::new(AtomicBool::new(false));
    let cut = Arc::new(AtomicBool::new(false));
    let serial = Arc::new(AtomicU64::new(1));
    let signal = HintSignal::new();
    for incoming in listener.incoming() {
        let mut socket = incoming?;
        tune(&socket)?;
        let (path, policy, unavailable, cut, serial, signal) = (
            path.clone(),
            policy.clone(),
            unavailable.clone(),
            cut.clone(),
            serial.clone(),
            signal.clone(),
        );
        thread::spawn(move || {
            let run = (|| -> Result<(), Error> {
                let mut n = [0];
                socket.read_exact(&mut n)?;
                if n[0] > 32 {
                    return Err("fixture token bound".into());
                }
                let mut token = vec![0; n[0] as usize];
                socket.read_exact(&mut token)?;
                if token != b"dev-read-secret" && token != b"dev-write-secret" {
                    socket.write_all(&[1])?;
                    return Ok(());
                }
                socket.write_all(&[0])?;
                if token == b"dev-read-secret" {
                    let io = TcpSessionIo::new(socket)?;
                    let abort = io.interrupt_handle();
                    let number = serial.fetch_add(1, Ordering::SeqCst);
                    serve_capability_follow_record_session(
                        io,
                        number,
                        limits(),
                        Policy(policy),
                        move |_| {
                            if cut.swap(false, Ordering::SeqCst) {
                                let _ = abort.interrupt();
                                return Err(SourceError::Unavailable);
                            }
                            if unavailable.load(Ordering::SeqCst) {
                                return Err(SourceError::Unavailable);
                            }
                            source(&path).map_err(|_| SourceError::Unavailable)
                        },
                        signal,
                    )
                    .map_err(err)?;
                } else {
                    let mut command = [0; 2];
                    socket.read_exact(&mut command)?;
                    let mut src = source(&path)?;
                    let mut head = src.head(&scope()).map_err(err)?;
                    match command[0] {
                        0 => {}
                        1 => {
                            if command[1] == 0 || command[1] > 7 {
                                return Err("fixture append bound".into());
                            }
                            for n in head + 1..=head + u64::from(command[1]) {
                                let (id, payload) = fact(n as usize);
                                src.append(&id, &payload).map_err(err)?;
                            }
                            head += u64::from(command[1]);
                        }
                        2 => {
                            unavailable.store(command[1] != 0, Ordering::SeqCst);
                        }
                        3 => {
                            policy.store(command[1], Ordering::SeqCst);
                        }
                        4 => {
                            for _ in 0..command[1] {
                                signal.notify().map_err(err)?;
                            }
                        }
                        5 => {
                            cut.store(true, Ordering::SeqCst);
                        }
                        _ => return Err("fixture command".into()),
                    };
                    socket.write_all(&head.to_be_bytes())?;
                }
                Ok(())
            })();
            if let Err(e) = run {
                eprintln!("owned freshness channel: {e}");
            }
        });
    }
    Ok(())
}
