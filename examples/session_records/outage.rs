//! Explicit stdin barriers allow the owned lab to cut a physical connection
//! and gate reconnects without modifying any replication or retry owner.

use super::*;

fn await_phase(expected: &str) -> Result<(), Error> {
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    if line.trim_end() != expected {
        return Err(format!("expected outage fixture barrier {expected}").into());
    }
    Ok(())
}

fn append_through(producer: &mut SqliteReferenceSource, through: u64) -> Result<(), Error> {
    let position = producer.head(&scope("device-a")).map_err(error)?;
    for next in position + 1..=through {
        let (id, payload) = fact(next as usize);
        if producer.append(&id, &payload).map_err(error)? != next {
            return Err("fixture source head moved".into());
        }
    }
    Ok(())
}

fn receipt(
    phase: &str,
    replica: &Path,
    expected: u64,
    reported: WireCounters,
    start: Instant,
) -> Result<(), Error> {
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let (checkpoint, records, payload) = inspect(replica)?;
    if (checkpoint, records, payload) != (expected, expected, expected / 7 * 4126) {
        return Err(
            format!("{phase}: durable replica differs from expected position {expected}").into(),
        );
    }
    println!(
        "{{\"phase\":\"{phase}\",\"checkpoint\":{checkpoint},\"records\":{records},\"cache_payload_bytes\":{payload},\"ordered_complete\":true,\"completed_exchanges\":{},\"reported_protocol_bytes\":{},\"reported_payload_bytes\":{},\"reported_duplicate_bytes\":{},\"elapsed_ms\":{elapsed_ms:.3}}}",
        reported.exchanges, reported.protocol_bytes, reported.payload_bytes, reported.duplicate_bytes,
    );
    io::stdout().flush()?;
    Ok(())
}

fn reported_delta(
    before: WireCounters,
    after: WireCounters,
    auth_bytes: u64,
    auth_exchanges: u64,
) -> WireCounters {
    WireCounters {
        exchanges: after.exchanges - before.exchanges + auth_exchanges,
        protocol_bytes: after.protocol_bytes - before.protocol_bytes + auth_bytes,
        payload_bytes: after.payload_bytes - before.payload_bytes,
        duplicate_bytes: after.duplicate_bytes - before.duplicate_bytes,
        ..WireCounters::default()
    }
}

pub(super) fn run(
    mode: &str,
    endpoint: SocketAddrV4,
    source_path: &Path,
    replica: &Path,
    token: &Id,
) -> Result<(), Error> {
    if !matches!(mode, "loopback" | "session-io") {
        return Err("outage mode must be loopback or session-io".into());
    }
    let mut producer = source(source_path)?;
    if producer.head(&scope("device-a")).map_err(error)? != 0 || inspect(replica)? != (0, 0, 0) {
        return Err("outage fixture requires empty source and replica".into());
    }
    append_through(&mut producer, 7)?;
    let start = Instant::now();
    let (mut active, auth_bytes, auth_exchanges) = Ports::connect(mode, endpoint, token)?;
    {
        let mut store = SqliteReplicaStore::open(replica)?;
        if active.catch_up(&mut store)? != 7 {
            return Err("seed catch-up did not reach seven records".into());
        }
    }
    receipt(
        "seed",
        replica,
        7,
        reported_delta(
            WireCounters::default(),
            active.counters()?,
            auth_bytes,
            auth_exchanges,
        ),
        start,
    )?;

    await_phase("arm")?;
    append_through(&mut producer, 14)?;
    let before = active.counters()?;
    let start = Instant::now();
    {
        let mut store = SqliteReplicaStore::open(replica)?;
        if active.catch_up(&mut store).is_ok() {
            return Err("warm catch-up unexpectedly crossed the cut".into());
        }
    }
    let interrupted = reported_delta(before, active.counters()?, 0, 0);
    active.close()?;
    drop(active);
    receipt("interrupted", replica, 7, interrupted, start)?;

    await_phase("probe")?;
    let start = Instant::now();
    let mut probe_report = WireCounters::default();
    if let Ok((mut probe, auth_bytes, auth_exchanges)) = Ports::connect(mode, endpoint, token) {
        {
            let mut store = SqliteReplicaStore::open(replica)?;
            if probe.catch_up(&mut store).is_ok() {
                return Err("reconnect probe passed inside the outage".into());
            }
        }
        probe_report = reported_delta(
            WireCounters::default(),
            probe.counters()?,
            auth_bytes,
            auth_exchanges,
        );
        let _ = probe.close();
    }
    // Publish while the gate is still closed. The host does not release it
    // until the following receipt proves this append and the blocked probe.
    append_through(&mut producer, 21)?;
    receipt("blocked_probe", replica, 7, probe_report, start)?;

    await_phase("resume")?;
    let start = Instant::now();
    let (mut renewed, auth_bytes, auth_exchanges) = Ports::connect(mode, endpoint, token)?;
    {
        let mut store = SqliteReplicaStore::open(replica)?;
        if renewed.catch_up(&mut store)? != 21 {
            return Err("reconnect did not reach exact live source head".into());
        }
    }
    let resumed = reported_delta(
        WireCounters::default(),
        renewed.counters()?,
        auth_bytes,
        auth_exchanges,
    );
    renewed.close()?;
    receipt("resumed", replica, 21, resumed, start)
}
