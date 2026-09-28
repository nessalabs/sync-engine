//! Development-only loopback source and receiver host. All data paths supplied
//! here belong to the caller; this example never deletes or resets them.

use std::env;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use nessa_sync::replication::application::{begin_pass, finish_pass, SyncError};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::infrastructure::{
    read_frame, LoopbackClient, LoopbackConfig, LoopbackServer, SqliteReplicaStore, WireCounters,
};

type AnyError = Box<dyn std::error::Error>;

fn id(value: &str) -> Id {
    Id::new(value).expect("valid development identity")
}

fn address(value: &str) -> Result<SocketAddrV4, AnyError> {
    match value.parse::<SocketAddr>()? {
        SocketAddr::V4(address) => Ok(address),
        SocketAddr::V6(_) => Err("IPv4 loopback required".into()),
    }
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

fn client(endpoint: &str, token: &str) -> Result<LoopbackClient, AnyError> {
    Ok(LoopbackClient::new(address(endpoint)?, id(token))?)
}

fn error<E: std::fmt::Debug>(value: E) -> io::Error {
    io::Error::other(format!("{value:?}"))
}

fn counter_line(
    receiver: &str,
    checkpoint: u64,
    auth: WireCounters,
    source: WireCounters,
    hint_bytes: u64,
) {
    let protocol = auth.protocol_bytes + source.protocol_bytes + hint_bytes;
    println!(
        "{{\"receiver\":\"{receiver}\",\"checkpoint\":{checkpoint},\"applied_lag\":0,\"protocol_bytes\":{protocol},\"payload_bytes\":{},\"duplicate_bytes\":{},\"head_checks\":{}}}",
        source.payload_bytes, source.duplicate_bytes, source.head_checks,
    );
}

fn catch_up(
    target: &Scope,
    authorizer: &mut LoopbackClient,
    source: &mut LoopbackClient,
    store: &mut SqliteReplicaStore,
) -> Result<u64, SyncError> {
    let limits = Limits::new(32, 128 * 1024, 128 * 1024).expect("static limits");
    // Recheck after every finite pass. A source write during a pass can never
    // extend its captured target; the next pass observes the newer head.
    let mut pass = begin_pass(target, authorizer, source, store)?;
    loop {
        finish_pass(&mut pass, limits, authorizer, source, store)?;
        let applied = pass.position();
        // `begin_pass` reauthorizes before the head recheck. The host never
        // bypasses that rule on the subscribe/catch-up/idle boundary.
        pass = begin_pass(target, authorizer, source, store)?;
        if pass.is_complete() {
            return Ok(applied);
        }
    }
}

fn run() -> Result<(), AnyError> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") if args.len() == 6 => {
            let port: u16 = args[3].parse()?;
            let server = LoopbackServer::bind(
                port,
                LoopbackConfig {
                    source_path: PathBuf::from(&args[2]),
                    origin: id("example-origin"),
                    stream: id("transcript"),
                    incarnation: id("first"),
                    schema: id("text-v1"),
                    access_epoch: id("local-epoch-1"),
                    read_token: id(&args[4]),
                    allowed_receivers: vec![id("device-a"), id("device-b")],
                    write_token: id(&args[5]),
                },
            )?;
            eprintln!("listening on {}", server.local_addr()?);
            server.serve()?;
        }
        Some("append") if args.len() == 6 => {
            let position = client(&args[2], &args[3])?
                .append(&id(&args[4]), args[5].as_bytes())
                .map_err(error)?;
            println!("{{\"position\":{position}}}");
        }
        Some("stats") if args.len() == 4 => {
            let (heads, pages, refused) = client(&args[2], &args[3])?
                .server_counters()
                .map_err(error)?;
            println!(
                "{{\"head_reads\":{heads},\"page_reads\":{pages},\"refused_reads\":{refused}}}"
            );
        }
        Some("show") if args.len() == 4 => {
            let target = scope(&args[3]);
            let store = SqliteReplicaStore::open(&args[2])?;
            let checkpoint = store
                .checkpoint(&target)
                .map_err(error)?
                .map_or(0, |saved| saved.position());
            let count = store.count(&target).map_err(error)?;
            println!("{{\"checkpoint\":{checkpoint},\"count\":{count}}}");
        }
        Some("sync") if args.len() == 6 => {
            let target = scope(&args[4]);
            let mut authorizer = client(&args[2], &args[5])?;
            let mut source = client(&args[2], &args[5])?;
            let mut store = SqliteReplicaStore::open(&args[3])?;
            // Subscription is established before first catch-up, even for a
            // one-shot pass. A continuous host keeps the socket and falls back.
            let _subscription = source.subscribe(&target).map_err(error)?;
            let checkpoint =
                catch_up(&target, &mut authorizer, &mut source, &mut store).map_err(error)?;
            counter_line(
                &args[4],
                checkpoint,
                authorizer.counters(),
                source.counters(),
                0,
            );
        }
        Some("follow") if args.len() == 7 => {
            let target = scope(&args[4]);
            let fallback_ms: u64 = args[6].parse()?;
            if fallback_ms == 0 {
                return Err("fallback must be positive".into());
            }
            let fallback = Duration::from_millis(fallback_ms);
            let mut authorizer = client(&args[2], &args[5])?;
            let mut source = client(&args[2], &args[5])?;
            let mut store = SqliteReplicaStore::open(&args[3])?;
            let mut hint_bytes = 0;
            loop {
                match source.subscribe(&target) {
                    Ok(mut hints) => {
                        // First catch-up and post-pass head check close the
                        // subscribe/catch-up/idle race.
                        match catch_up(&target, &mut authorizer, &mut source, &mut store) {
                            Ok(checkpoint) => counter_line(
                                &args[4],
                                checkpoint,
                                authorizer.counters(),
                                source.counters(),
                                hint_bytes,
                            ),
                            Err(error) => eprintln!("catch-up error: {error:?}"),
                        }
                        hints.set_read_timeout(Some(fallback))?;
                        loop {
                            match read_frame(&mut hints) {
                                Ok(frame) if frame == [7] || frame == [8] => {
                                    hint_bytes += 5;
                                }
                                Err(nessa_sync::replication::infrastructure::FrameError::Io(
                                    error,
                                )) if error.kind() == io::ErrorKind::TimedOut
                                    || error.kind() == io::ErrorKind::WouldBlock => {}
                                _ => break,
                            }
                            match catch_up(&target, &mut authorizer, &mut source, &mut store) {
                                Ok(checkpoint) => counter_line(
                                    &args[4],
                                    checkpoint,
                                    authorizer.counters(),
                                    source.counters(),
                                    hint_bytes,
                                ),
                                Err(error) => {
                                    eprintln!("catch-up error: {error:?}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => eprintln!("subscribe error: {error:?}"),
                }
                thread::sleep(fallback);
            }
        }
        _ => {
            return Err("usage: loopback_sync serve SOURCE PORT READ_TOKEN WRITE_TOKEN | append ADDR WRITE_TOKEN ID TEXT | stats ADDR WRITE_TOKEN | show REPLICA RECEIVER | sync ADDR REPLICA RECEIVER READ_TOKEN | follow ADDR REPLICA RECEIVER READ_TOKEN FALLBACK_MS".into());
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}
