//! Process-level host for bounded transcript tails and older-page hydration.
//! The source server and append commands are provided by `loopback_sync`.

use std::env;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;

use nessa_sync::replication::application::{begin_pass, finish_pass};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::history::{
    fetch_older, fetch_tail, HistoryReadState, HistoryStore, HydrationQueue, OlderRequest,
    TailRequest,
};
use nessa_sync::replication::infrastructure::{LoopbackClient, SqliteReplicaStore};

type AnyError = Box<dyn std::error::Error>;

fn other<E: std::fmt::Debug>(value: E) -> io::Error {
    io::Error::other(format!("{value:?}"))
}

fn id(value: &str) -> Result<Id, AnyError> {
    Id::new(value).map_err(|error| other(error).into())
}

fn scope(receiver: &str) -> Result<Scope, AnyError> {
    Ok(Scope::new(
        id(receiver)?,
        id("example-origin")?,
        id("transcript")?,
        id("first")?,
        id("text-v1")?,
        id("local-epoch-1")?,
    ))
}

fn address(value: &str) -> Result<SocketAddrV4, AnyError> {
    match value.parse::<SocketAddr>()? {
        SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST => Ok(address),
        _ => Err("IPv4 loopback required".into()),
    }
}

fn client(endpoint: &str, token: &str) -> Result<LoopbackClient, AnyError> {
    Ok(LoopbackClient::new(address(endpoint)?, id(token)?)?)
}

fn limits(count: usize) -> Result<Limits, AnyError> {
    Ok(Limits::new(count, 128 * 1024, 128 * 1024).map_err(other)?)
}

fn tail(scope: Scope, generation: u64, count: usize) -> TailRequest {
    TailRequest {
        scope,
        generation,
        max_records: count,
        max_payload_bytes: 128 * 1024,
        max_record_bytes: 128 * 1024,
    }
}

fn older(scope: Scope, generation: u64, before: u64, count: usize) -> OlderRequest {
    OlderRequest {
        scope,
        generation,
        before,
        max_records: count,
        max_payload_bytes: 128 * 1024,
        max_record_bytes: 128 * 1024,
    }
}

fn show(db: &Path, receiver: &str) -> Result<(), AnyError> {
    let scope = scope(receiver)?;
    if !db.exists() {
        println!("{{\"state\":\"unloaded\",\"live_head\":0,\"lower_bound\":0,\"count\":0}}");
        return Ok(());
    }
    let mut store = SqliteReplicaStore::open(db)?;
    let progress = store.history_progress(&scope).map_err(other)?;
    let state = HistoryReadState::from_progress(progress.clone());
    let label = match state {
        HistoryReadState::Unloaded => "unloaded",
        HistoryReadState::Partial(_) => "partial",
        HistoryReadState::CompleteEmpty(_) => "complete_empty",
        HistoryReadState::Complete(_) => "complete",
        HistoryReadState::Deleted(_) => "deleted",
        _ => "transient",
    };
    let (head, lower, count, generation) = match progress {
        None => (0, 0, 0, 0),
        Some(progress) => (
            progress.live_head,
            progress.lower_bound,
            store.count(&scope).map_err(other)?,
            progress.generation,
        ),
    };
    println!(
        "{{\"state\":\"{label}\",\"live_head\":{head},\"lower_bound\":{lower},\"count\":{count},\"generation\":{generation}}}"
    );
    Ok(())
}

fn bootstrap(
    endpoint: &str,
    db: &Path,
    receiver: &str,
    token: &str,
    generation: u64,
    count: usize,
) -> Result<(), AnyError> {
    let scope = scope(receiver)?;
    let mut store = SqliteReplicaStore::open(db)?;
    let mut auth = client(endpoint, token)?;
    let mut source = client(endpoint, token)?;
    let request = tail(scope, generation, count);
    let plan = fetch_tail(&request, limits(count)?, &mut auth, &mut source).map_err(other)?;
    let progress = store.install_tail(plan).map_err(other)?;
    let counters = source.counters();
    println!(
        "{{\"state\":\"{}\",\"live_head\":{},\"lower_bound\":{},\"payload_bytes\":{},\"protocol_bytes\":{}}}",
        if progress.live_head == 0 { "complete_empty" } else if progress.lower_bound == 1 { "complete" } else { "partial" },
        progress.live_head,
        progress.lower_bound,
        counters.payload_bytes,
        counters.protocol_bytes + auth.counters().protocol_bytes
    );
    Ok(())
}

fn live(endpoint: &str, db: &Path, receiver: &str, token: &str) -> Result<(), AnyError> {
    let scope = scope(receiver)?;
    let mut store = SqliteReplicaStore::open(db)?;
    let mut auth = client(endpoint, token)?;
    let mut source = client(endpoint, token)?;
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut store).map_err(other)?;
    finish_pass(&mut pass, limits(32)?, &mut auth, &mut source, &mut store).map_err(other)?;
    let progress = store
        .history_progress(&scope)
        .map_err(other)?
        .ok_or("tail not installed")?;
    println!(
        "{{\"live_head\":{},\"lower_bound\":{},\"payload_bytes\":{},\"head_checks\":{}}}",
        progress.live_head,
        progress.lower_bound,
        source.counters().payload_bytes,
        source.counters().head_checks
    );
    Ok(())
}

fn hydrate(
    endpoint: &str,
    db: &Path,
    receiver: &str,
    token: &str,
    target: u64,
    waiters: usize,
    count: usize,
) -> Result<(), AnyError> {
    let scope = scope(receiver)?;
    let mut store = SqliteReplicaStore::open(db)?;
    let mut auth = client(endpoint, token)?;
    let mut source = client(endpoint, token)?;
    let mut queue = HydrationQueue::new(waiters).map_err(other)?;
    for index in 0..waiters {
        queue
            .request(
                target
                    .checked_add(u64::from(index % 2 != 0))
                    .ok_or("target overflow")?,
            )
            .map_err(other)?;
    }
    let mut pages = 0;
    let mut resolved = 0;
    loop {
        let progress = store
            .history_progress(&scope)
            .map_err(other)?
            .ok_or("tail not installed")?;
        if queue.next_target(progress.lower_bound).is_none() {
            break;
        }
        let request = older(
            scope.clone(),
            progress.generation,
            progress.lower_bound,
            count,
        );
        let plan = fetch_older(&request, limits(count)?, &mut auth, &mut source).map_err(other)?;
        let updated = store.install_older(plan).map_err(other)?;
        pages += 1;
        resolved += queue.resolve(updated.lower_bound);
    }
    let progress = store
        .history_progress(&scope)
        .map_err(other)?
        .ok_or("tail not installed")?;
    println!(
        "{{\"live_head\":{},\"lower_bound\":{},\"pages\":{pages},\"resolved\":{resolved},\"payload_bytes\":{},\"protocol_bytes\":{}}}",
        progress.live_head,
        progress.lower_bound,
        source.counters().payload_bytes,
        source.counters().protocol_bytes + auth.counters().protocol_bytes
    );
    Ok(())
}

fn run() -> Result<(), AnyError> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("show") if args.len() == 4 => show(Path::new(&args[2]), &args[3])?,
        Some("bootstrap") if args.len() == 8 => bootstrap(
            &args[2], Path::new(&args[3]), &args[4], &args[5], args[6].parse()?, args[7].parse()?,
        )?,
        Some("live") if args.len() == 6 => {
            live(&args[2], Path::new(&args[3]), &args[4], &args[5])?
        }
        Some("hydrate") if args.len() == 9 => hydrate(
            &args[2], Path::new(&args[3]), &args[4], &args[5], args[6].parse()?,
            args[7].parse()?, args[8].parse()?,
        )?,
        Some("fence") if args.len() == 4 => {
            let mut store = SqliteReplicaStore::open(&args[2])?;
            store.fence_deletion(&scope(&args[3])?).map_err(other)?;
            println!("{{\"fenced\":true}}");
        }
        Some("stats") if args.len() == 4 => {
            let (tails, older, bytes) = client(&args[2], &args[3])?.history_counters().map_err(other)?;
            println!("{{\"tail_reads\":{tails},\"older_reads\":{older},\"payload_bytes\":{bytes}}}");
        }
        Some("prune") if args.len() == 5 => {
            let mut client = client(&args[2], &args[3])?;
            let position: u64 = args[4].parse()?;
            client.prune_through(position).map_err(other)?;
            println!("{{\"pruned_through\":{position}}}");
        }
        _ => return Err("usage: history_lab show DB RECEIVER | bootstrap ADDR DB RECEIVER READ_TOKEN GENERATION COUNT | live ADDR DB RECEIVER READ_TOKEN | hydrate ADDR DB RECEIVER READ_TOKEN TARGET WAITERS PAGE_COUNT | fence DB RECEIVER | stats ADDR WRITE_TOKEN | prune ADDR WRITE_TOKEN POSITION".into()),
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}
