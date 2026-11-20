//! Real capability-follow policy qualification on owned TCP/SQLite fixtures.
#[path = "session_freshness/fixture.rs"]
mod fixture;
#[path = "session_freshness/host.rs"]
mod host;
use fixture::{control, err, Error};
use host::{assert_rows, Host};
use nessa_sync::replication::{
    application::*,
    domain::{Id, Scope},
    infrastructure::SqliteReplicaStore,
};
use std::{
    path::Path,
    time::{Duration, Instant},
};
fn sum(v: &[f64]) -> f64 {
    v.iter().sum()
}
fn policy(name: &str) -> Result<FreshnessPolicy, Error> {
    match name {
        "never" => Ok(FreshnessPolicy::Never),
        "always" => Ok(FreshnessPolicy::Always),
        "within" => Ok(FreshnessPolicy::Within(Duration::from_millis(200))),
        _ => Err("policy must be never/always/within".into()),
    }
}
fn settle(host: &mut Host, records: usize) -> Result<(), Error> {
    host.drain(records).map_err(err)?;
    Ok(())
}
fn recover_at_deadline(h: &mut Host) {
    let at = h.owner.metadata().unwrap().retry_at.unwrap();
    let now = h.time.now();
    h.time.advance(at.saturating_sub(now));
}
fn cached(h: &mut Host, n: usize) {
    let read = h.read(ReadIntent::Cached, 2).unwrap();
    assert_rows(&read.value, n);
}
fn contracts(h: &mut Host, ctl: &str, name: &str) -> Result<(), Error> {
    assert!(h.time.logical());
    let before = h.owner.metadata().map_err(err)?;
    let cp = h.checkpoint();
    let reads = h.projections;
    assert_eq!(control(ctl, 1, 1)?, 8); // Cache remains at7 while the source advances.
    control(ctl, 5, 0)?;
    if name == "always" {
        let projections = h.projections;
        assert!(matches!(
            h.read(ReadIntent::Fresh, 2),
            Err(FreshnessError::Sync(SyncError::Source(
                SourceError::Unavailable
            )))
        ));
        assert_eq!(h.projections, projections);
    } else {
        if name == "within" {
            h.time.advance(Duration::from_millis(250));
        }
        let calls = h.link.borrow().authorizations;
        let read = h.read(ReadIntent::Fresh, 2).map_err(err)?;
        assert_rows(&read.value, 7);
        assert_eq!(h.link.borrow().authorizations, calls);
        if name == "never" {
            h.owner.request(FreshnessWake::Explicit);
        }
        assert!(matches!(
            h.drive(2),
            Err(FreshnessError::Sync(SyncError::Source(
                SourceError::Unavailable
            )))
        ));
    }
    assert_eq!(h.checkpoint(), cp);
    let failed = h.owner.metadata().map_err(err)?;
    assert!(failed.check_failed);
    assert_eq!(failed.last_applied_at, before.last_applied_at);
    assert_eq!(failed.last_head_checked_at, before.last_head_checked_at);
    cached(h, 7);
    let dials = h.link.borrow().dials;
    let attempts = h.link.borrow().authorizations;
    let projections = h.projections;
    let response = h.read(ReadIntent::Fresh, 2);
    if name == "always" {
        assert_eq!(response, Err(FreshnessError::Deferred));
        assert_eq!(h.projections, projections);
    } else {
        let response = response.map_err(err)?;
        assert_rows(&response.value, 7);
        assert!(response.freshness.check_failed);
        assert_eq!(
            response.freshness.last_head_checked_at,
            before.last_head_checked_at
        );
    }
    assert_eq!(h.link.borrow().dials, dials);
    assert_eq!(h.link.borrow().authorizations, attempts);
    for _ in 0..16 {
        h.owner.request(FreshnessWake::Hint);
        h.owner.request(FreshnessWake::Reconnect);
    }
    let at = failed.retry_at.unwrap();
    h.time.advance(
        at.saturating_sub(h.time.now())
            .saturating_sub(Duration::from_nanos(1)),
    );
    assert_eq!(h.drive(2), Ok(FreshnessWork::Waiting));
    assert_eq!(h.link.borrow().dials, dials);
    assert_eq!(h.link.borrow().authorizations, attempts);
    control(ctl, 2, 1)?;
    h.time.advance(Duration::from_nanos(1));
    assert_eq!(
        h.drive(2),
        Err(FreshnessError::Sync(SyncError::Unverifiable))
    );
    assert_eq!(h.jitter.get(), 2);
    assert_eq!(h.link.borrow().dials, dials + 1);
    let second = h.owner.metadata().map_err(err)?;
    assert_eq!(
        second.retry_at.unwrap().saturating_sub(h.time.now()),
        Duration::from_millis(100)
    );
    control(ctl, 2, 0)?;
    recover_at_deadline(h);
    assert_eq!(h.drive(2), Ok(FreshnessWork::Checked));
    assert_eq!(h.checkpoint(), 7); // An observed head8 is not applied progress.
    let recovered = h.owner.metadata().map_err(err)?;
    assert!(!recovered.check_failed);
    assert_eq!(recovered.last_applied_at, before.last_applied_at);
    assert!(recovered.last_head_checked_at > before.last_head_checked_at);
    assert_eq!(recovered.clock_generation, before.clock_generation);
    assert_eq!(h.jitter.get(), 2);
    h.time.advance(Duration::from_millis(1));
    settle(h, 2)?;
    cached(h, 8);
    assert_eq!(control(ctl, 1, 6)?, 14);
    h.owner.request(FreshnessWake::Explicit);
    assert_eq!(h.drive(2), Ok(FreshnessWork::Checked));
    assert_eq!(h.checkpoint(), 8);
    h.time.advance(Duration::from_millis(1));
    assert_eq!(h.drive(2), Ok(FreshnessWork::Applied));
    assert_eq!(h.checkpoint(), 10);
    assert_eq!(control(ctl, 1, 1)?, 15);
    control(ctl, 4, 1)?;
    h.link.borrow().pending(15)?;
    assert_eq!(control(ctl, 1, 1)?, 16);
    control(ctl, 4, 16)?;
    h.link.borrow().pending(16)?;
    assert!(h.link.borrow().hint()?);
    h.owner.request(FreshnessWake::Hint);
    let retained = h.owner.metadata().map_err(err)?;
    let traces = h.link.borrow().pages.len();
    let draws = h.jitter.get();
    h.link.borrow_mut().retire();
    let dials = h.link.borrow().dials;
    assert_eq!(
        h.s.source_head(&fixture::scope()),
        Err(SourceError::Unavailable)
    );
    assert_eq!(h.link.borrow().dials, dials);
    h.owner.request(FreshnessWake::Reconnect);
    assert_eq!(
        h.owner.metadata().map_err(err)?.last_applied_at,
        retained.last_applied_at
    );
    settle(h, 2)?;
    assert_eq!(h.checkpoint(), 16);
    assert_eq!(h.jitter.get(), draws);
    assert_eq!(h.link.borrow().pages[traces], (10, 14));
    assert_eq!(
        &h.link.borrow().pages[traces..],
        &[(10, 14), (12, 14), (14, 16)]
    );
    cached(h, 16);
    assert_eq!(h.owner.metadata().map_err(err)?.phase, FreshnessPhase::Idle);
    assert_eq!(control(ctl, 1, 1)?, 17);
    h.owner.request(FreshnessWake::Explicit);
    assert_eq!(h.drive(2), Ok(FreshnessWork::Checked));
    let stamp = h.owner.metadata().map_err(err)?.last_applied_at;
    h.store.fail = true;
    assert_eq!(
        h.drive(2),
        Err(FreshnessError::Sync(SyncError::Store(StoreError::Failed)))
    );
    assert_eq!(h.checkpoint(), 16);
    assert_eq!(h.owner.metadata().map_err(err)?.last_applied_at, stamp);
    recover_at_deadline(h);
    h.store.lose = true;
    assert_eq!(
        h.drive(2),
        Err(FreshnessError::Sync(SyncError::Store(
            StoreError::Uncertain
        )))
    );
    assert_eq!(h.checkpoint(), 17);
    let m = h.owner.metadata().map_err(err)?;
    assert_eq!(m.position, None);
    assert_eq!(m.last_applied_at, None);
    let loads = h.store.loads;
    let pages = h.link.borrow().pages.len();
    h.owner.request(FreshnessWake::Reconnect);
    assert_eq!(h.drive(2), Err(FreshnessError::ReloadRequired));
    assert_eq!(h.store.loads, loads);
    assert_eq!(h.link.borrow().pages.len(), pages);
    control(ctl, 3, 1)?;
    h.owner.resume();
    assert_eq!(h.drive(2), Err(FreshnessError::Sync(SyncError::Denied)));
    assert_eq!(h.store.loads, loads);
    h.owner.request(FreshnessWake::Hint);
    assert_eq!(h.drive(2), Err(FreshnessError::Stopped));
    control(ctl, 3, 0)?;
    h.owner.resume();
    settle(h, 2)?;
    assert_eq!(h.link.borrow().pages.len(), pages);
    assert_eq!(h.owner.metadata().map_err(err)?.last_applied_at, None);
    cached(h, 17);
    for (mode, expected) in [
        (3, SyncError::WrongAccessScope),
        (4, SyncError::InvalidAccessResponse),
    ] {
        control(ctl, 3, mode)?;
        h.owner.request(FreshnessWake::Explicit);
        let loads = h.store.loads;
        assert_eq!(h.drive(2), Err(FreshnessError::Sync(expected)));
        assert_eq!(h.store.loads, loads);
        let d = h.link.borrow().dials;
        h.owner.request(FreshnessWake::Reconnect);
        assert_eq!(h.drive(2), Err(FreshnessError::Stopped));
        assert_eq!(h.link.borrow().dials, d);
        control(ctl, 3, 0)?;
        h.owner.resume();
        settle(h, 2)?;
    }
    assert!(h.projections > reads);
    let s = fixture::scope();
    h.link.borrow_mut().retire();
    h.owner.reset_scope(Scope::new(
        s.receiver().clone(),
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        s.schema().clone(),
        Id::new("next-epoch").unwrap(),
    ));
    let reset = h.owner.metadata().map_err(err)?;
    assert_eq!(reset.last_applied_at, None);
    assert_eq!(reset.last_head_checked_at, None);
    assert_eq!(reset.position, None);
    assert!(!reset.check_pending);
    assert_eq!(h.checkpoint(), 17);
    assert!(matches!(
        h.read(ReadIntent::Cached, 2),
        Err(FreshnessError::Sync(SyncError::Store(
            StoreError::ScopeMismatch { .. }
        )))
    ));
    let loads = h.store.loads;
    let dials = h.link.borrow().dials;
    h.owner.request(FreshnessWake::Reconnect);
    assert_eq!(
        h.drive(2),
        Err(FreshnessError::Sync(SyncError::WrongAccessScope))
    );
    assert_eq!(h.store.loads, loads);
    assert_eq!(h.link.borrow().dials, dials);
    Ok(())
}
struct FailureTiming {
    check_ms: f64,
    cached_ms: f64,
    backoff_ms: f64,
    recovery_ms: f64,
}
fn measured_failure(h: &mut Host, ctl: &str, name: &str) -> Result<FailureTiming, Error> {
    assert_eq!(control(ctl, 1, 1)?, 8);
    h.time.advance(Duration::from_millis(250));
    let before = h.owner.metadata().map_err(err)?;
    control(ctl, 5, 0)?;
    if name != "always" {
        let calls = h.link.borrow().authorizations;
        let read = h.read(ReadIntent::Fresh, 32).map_err(err)?;
        assert_rows(&read.value, 7);
        assert_eq!(h.link.borrow().authorizations, calls);
        if name == "never" {
            h.owner.request(FreshnessWake::Explicit);
        }
    }
    let started = Instant::now();
    if name == "always" {
        let projections = h.projections;
        assert!(matches!(
            h.read(ReadIntent::Fresh, 32),
            Err(FreshnessError::Sync(SyncError::Source(
                SourceError::Unavailable
            )))
        ));
        assert_eq!(h.projections, projections);
    } else {
        assert!(matches!(
            h.drive(32),
            Err(FreshnessError::Sync(SyncError::Source(
                SourceError::Unavailable
            )))
        ));
    }
    let check_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(h.checkpoint(), 7);
    let failed = h.owner.metadata().map_err(err)?;
    assert!(failed.check_failed);
    assert_eq!(failed.last_head_checked_at, before.last_head_checked_at);
    assert_eq!(failed.last_applied_at, before.last_applied_at);
    let calls = h.link.borrow().authorizations;
    let dials = h.link.borrow().dials;
    let started = Instant::now();
    let cached = h.read(ReadIntent::Cached, 32).map_err(err)?;
    assert_rows(&cached.value, 7);
    assert!(cached.freshness.check_failed);
    let cached_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(h.link.borrow().authorizations, calls);
    assert_eq!(h.link.borrow().dials, dials);
    let started = Instant::now();
    recover_at_deadline(h);
    let backoff_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    settle(h, 32)?;
    let recovery_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(h.checkpoint(), 8);
    assert_rows(&h.read(ReadIntent::Cached, 32).map_err(err)?.value, 8);
    assert!(!h.owner.metadata().map_err(err)?.check_failed);
    Ok(FailureTiming {
        check_ms,
        cached_ms,
        backoff_ms,
        recovery_ms,
    })
}
fn bench(
    name: &str,
    logical: bool,
    adverse: bool,
    address: String,
    ctl: &str,
    path: &Path,
) -> Result<(), Error> {
    let mut h = Host::new(address.clone(), path, policy(name)?, logical)?;
    assert_eq!(h.checkpoint(), 0);
    assert_eq!(control(ctl, 0, 0)?, 0);
    assert_eq!(control(ctl, 1, 7)?, 7);
    let start = Instant::now();
    h.owner.request(FreshnessWake::Explicit);
    settle(&mut h, 32)?;
    let cold_ms = start.elapsed().as_secs_f64() * 1000.0;
    cached(&mut h, 7);
    let cold_apply_ms = sum(&h.store.apply_ms);
    let before = h.link.borrow().counters();
    let a = h.link.borrow().authorizations;
    let head = h.link.borrow().heads;
    let p = h.link.borrow().pages.len();
    let readbase = h.local_ms.len();
    let checkbase = h.check_ms.len();
    let applybase = h.store.apply_ms.len();
    let mut read_ms = 0.0;
    let mut inline_check_ms = 0.0;
    let mut pending = 0;
    let stable_start = Instant::now();
    for i in 0..6 {
        h.time.advance(if i == 2 || i == 5 {
            Duration::from_millis(250)
        } else {
            Duration::from_millis(1)
        });
        let calls = h.link.borrow().authorizations;
        let heads = h.link.borrow().heads;
        let local_before = h.local_ms.len();
        let t = Instant::now();
        let read = h.read(ReadIntent::Fresh, 32).map_err(err)?;
        let elapsed = t.elapsed().as_secs_f64() * 1000.0;
        read_ms += elapsed;
        assert_rows(&read.value, 7);
        if read.freshness.check_pending {
            pending += 1;
        }
        if name != "always" {
            assert_eq!(h.link.borrow().authorizations, calls);
            assert_eq!(h.link.borrow().heads, heads);
        } else {
            assert_eq!(h.link.borrow().heads, heads + 1);
            inline_check_ms += elapsed - sum(&h.local_ms[local_before..]);
        }
        settle(&mut h, 32)?;
        assert_eq!(h.checkpoint(), 7);
    }
    let stable_ms = stable_start.elapsed().as_secs_f64() * 1000.0;
    let after = h.link.borrow().counters();
    let checks = h.link.borrow().heads - head;
    assert_eq!(h.link.borrow().pages.len(), p);
    if logical {
        assert_eq!(
            checks,
            match name {
                "always" => 6,
                "within" => 2,
                _ => 0,
            }
        );
    }
    if name == "never" {
        assert_eq!(checks, 0);
    }
    if name == "always" {
        assert_eq!(checks, 6);
    }
    let local_ms = sum(&h.local_ms[readbase..]);
    let deferred_check_ms = sum(&h.check_ms[checkbase..]);
    let apply_ms = sum(&h.store.apply_ms[applybase..]);
    let stable_auth = h.link.borrow().authorizations - a;
    let m = h.owner.metadata().map_err(err)?;
    h.link.borrow_mut().retire();
    h.owner.request(FreshnessWake::Reconnect);
    assert_eq!(
        h.owner.metadata().map_err(err)?.last_applied_at,
        m.last_applied_at
    );
    assert_eq!(
        h.owner.metadata().map_err(err)?.last_head_checked_at,
        m.last_head_checked_at
    );
    let r = Instant::now();
    settle(&mut h, 32)?;
    let reconnect_ms = r.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(
        h.owner.metadata().map_err(err)?.last_applied_at,
        m.last_applied_at
    );
    assert_eq!(
        h.owner.metadata().map_err(err)?.clock_generation,
        m.clock_generation
    );
    let failure = if adverse {
        contracts(&mut h, ctl, name)?;
        None
    } else {
        Some(measured_failure(&mut h, ctl, name)?)
    };
    let failure_json=match failure {Some(t)=>format!("{{\"check_ms\":{:.6},\"cached_read_ms\":{:.6},\"backoff_wait_ms\":{:.6},\"recovery_ms\":{:.6},\"cached_rows_before_recovery\":7,\"recovered_rows\":8}}",t.check_ms,t.cached_ms,t.backoff_ms,t.recovery_ms),None=>"null".into()};
    let final_rows = if adverse { 17 } else { 8 };
    let total_apply_ms = sum(&h.store.apply_ms);
    let totals = h.link.borrow().counters();
    let dials = h.link.borrow().dials;
    let connections = h.link.borrow().connections;
    let generations = h.owner.metadata().map_err(err)?.clock_generation;
    drop(h);
    let mut reopened = SqliteReplicaStore::open(path)?;
    let rows = reopened
        .read_after(&fixture::scope(), 0, 64, MAX_RECORD_BYTES)
        .map_err(err)?;
    assert_rows(&rows, final_rows);
    assert_eq!(
        reopened
            .checkpoint(&fixture::scope())
            .map_err(err)?
            .unwrap()
            .position(),
        final_rows as u64
    );
    let mut restart = Host::new(address, path, policy(name)?, logical)?;
    let view = restart.read(ReadIntent::Cached, 32).map_err(err)?;
    assert_rows(&view.value, final_rows);
    assert_eq!(view.freshness.last_applied_at, None);
    assert_eq!(view.freshness.last_head_checked_at, None);
    assert_eq!(restart.link.borrow().dials, 0);
    drop(restart);
    println!("{{\"policy\":\"{name}\",\"clock\":\"{}\",\"contracts\":{adverse},\"useful_reads\":6,\"stable_head_attempts\":{checks},\"stable_authorizations\":{stable_auth},\"stable_completed_exchanges\":{},\"stable_protocol_bytes\":{},\"stable_payload_bytes\":{},\"responses_with_check_pending\":{pending},\"cold_ms\":{cold_ms:.6},\"cold_apply_ms\":{cold_apply_ms:.6},\"total_apply_ms\":{total_apply_ms:.6},\"read_call_ms\":{read_ms:.6},\"local_projection_ms\":{local_ms:.6},\"inline_check_path_ms\":{inline_check_ms:.6},\"deferred_check_ms\":{deferred_check_ms:.6},\"apply_ms_during_stable\":{apply_ms:.6},\"stable_elapsed_including_think_ms\":{stable_ms:.6},\"reconnect_ms\":{reconnect_ms:.6},\"dial_attempts\":{dials},\"connections\":{connections},\"completed_client_exchanges\":{},\"client_protocol_bytes\":{},\"client_payload_bytes\":{},\"reopened_rows\":{final_rows},\"restart_times_unknown\":true,\"clock_generation\":{generations},\"failure_timing\":{failure_json},\"ordered_complete\":true}}",if logical{"logical"}else{"real"},after.exchanges-before.exchanges,after.protocol_bytes-before.protocol_bytes,after.payload_bytes-before.payload_bytes,totals.exchanges,totals.protocol_bytes,totals.payload_bytes);
    Ok(())
}
fn inspect(path: &Path) -> Result<(), Error> {
    let mut h = Host::new("127.0.0.1:1".into(), path, FreshnessPolicy::Never, false)?;
    let read = h.read(ReadIntent::Cached, 32).map_err(err)?;
    let n = read.value.len();
    assert_rows(&read.value, n);
    assert_eq!(h.checkpoint(), n as u64);
    assert_eq!(read.freshness.last_applied_at, None);
    assert_eq!(read.freshness.last_head_checked_at, None);
    assert_eq!(read.freshness.position, None);
    assert_eq!(read.freshness.observed_head, None);
    assert_eq!(h.link.borrow().dials, 0);
    println!("{{\"rows\":{n},\"checkpoint\":{n},\"last_applied_unknown\":true,\"last_head_unknown\":true,\"dials\":0,\"ordered_complete\":true}}");
    Ok(())
}
fn run() -> Result<(), Error> {
    let a = std::env::args().collect::<Vec<_>>();
    match a.get(1).map(String::as_str) {
        Some("inspect") if a.len()==3 => inspect(Path::new(&a[2])),
        Some("serve") if a.len()==4 => fixture::serve(a[2].clone().into(),a[3].parse()?),
        Some("bench") if a.len()==9 => {
            if a[8]!="owned" || !matches!(a[3].as_str(),"logical"|"real") || !matches!(a[4].as_str(),"contracts"|"stable") || (a[4]=="contracts" && a[3]!="logical") {
                return Err("owned marker and logical contracts or logical/real stable required".into());
            }
            bench(&a[2],a[3]=="logical",a[4]=="contracts",a[5].clone(),&a[6],Path::new(&a[7]))
        },
        _=>Err("usage: session_freshness serve SOURCE PORT | bench POLICY logical|real contracts|stable DATA CONTROL CACHE owned".into()),
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
