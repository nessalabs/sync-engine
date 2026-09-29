//! Development-only loopback source and receiver host. All data paths supplied
//! here belong to the caller; this example never deletes or resets them.

use std::env;
use std::io::{self, Read};
use std::net::{SocketAddr, SocketAddrV4, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use nessa_sync::replication::application::{begin_pass, finish_pass, SyncError};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::infrastructure::{
    FrameError, LoopbackClient, LoopbackConfig, LoopbackServer, SqliteReplicaStore, WireCounters,
    MAX_FRAME_BYTES,
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

fn take_hint_frame(buffer: &mut Vec<u8>) -> Result<Option<Vec<u8>>, FrameError> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let length =
        u32::from_be_bytes(buffer[..4].try_into().map_err(|_| FrameError::Malformed)?) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge);
    }
    if buffer.len() < length + 4 {
        return Ok(None);
    }
    let frame = buffer[4..length + 4].to_vec();
    buffer.drain(..length + 4);
    Ok(Some(frame))
}

fn read_hint(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Option<Vec<u8>>, FrameError> {
    loop {
        if let Some(frame) = take_hint_frame(buffer)? {
            return Ok(Some(frame));
        }
        let mut chunk = [0_u8; 256];
        match stream.read(&mut chunk) {
            Ok(0) => {
                return Err(FrameError::Io(io::Error::from(
                    io::ErrorKind::UnexpectedEof,
                )));
            }
            Ok(count) => buffer.extend_from_slice(&chunk[..count]),
            Err(error)
                if error.kind() == io::ErrorKind::TimedOut
                    || error.kind() == io::ErrorKind::WouldBlock =>
            {
                return Ok(None);
            }
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
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
                    catalogue_source_path: None,
                    catalogue_fault: None,
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
                        let mut hint_buffer = Vec::new();
                        loop {
                            match read_hint(&mut hints, &mut hint_buffer) {
                                Ok(Some(frame)) if frame == [7] || frame == [8] => {
                                    hint_bytes += 5;
                                }
                                Ok(None) => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_frame_keeps_partial_header_and_body() {
        let mut buffer = vec![0, 0];
        assert!(take_hint_frame(&mut buffer).unwrap().is_none());
        buffer.extend_from_slice(&[0, 1]);
        assert!(take_hint_frame(&mut buffer).unwrap().is_none());
        buffer.push(7);
        assert_eq!(take_hint_frame(&mut buffer).unwrap(), Some(vec![7]));
        assert!(buffer.is_empty());
    }
}
