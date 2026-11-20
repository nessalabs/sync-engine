//! Owned native same-scope recovery lab. No transport or collection API.
use nessa_sync::replication::{
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
};
use std::{path::Path, time::Instant};
#[path = "catalogue_reset/metrics.rs"]
mod metrics;
use metrics::MeasuredStore;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("phone"),
        id("gateway"),
        id("catalogue"),
        id("first"),
        id("opaque"),
        id("epoch"),
    )
}
fn source(path: &Path) -> SqliteCatalogueSource {
    let s = scope();
    SqliteCatalogueSource::open(
        path,
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        s.schema().clone(),
    )
    .unwrap()
}
fn main() {
    let a = std::env::args().collect::<Vec<_>>();
    assert!(
        a.len() == 5 && a[4] == "owned",
        "catalogue_reset prepare|start|step SOURCE CACHE owned"
    );
    let horizon = a[1].starts_with("horizon-");
    let command = a[1].strip_prefix("horizon-").unwrap_or(&a[1]);
    let sp = Path::new(&a[2]);
    let cp = Path::new(&a[3]);
    if command == "prepare" {
        assert!(!sp.exists() && !cp.exists(), "owned fresh paths required");
    }
    let mut s = source(sp);
    let mut d = MeasuredStore::new(SqliteCatalogueStore::open(cp).unwrap());
    let mut auth = MemoryAuthorizer::allowed(scope());
    if command == "prepare" {
        for i in 0..620 {
            s.upsert(
                &id(&format!("entry-{i:04}")),
                format!("session-{i:04}:hello").as_bytes(),
            )
            .unwrap();
        }
        while let Some(p) = begin_or_resume(&scope(), &mut auth, &mut s, &mut d).unwrap() {
            apply_next_page(&p, 40, 100000, &mut auth, &mut s, &mut d).unwrap();
        }
        for i in 0..160 {
            s.delete(&id(&format!("entry-{i:04}"))).unwrap();
        }
        // Owned fixture only; no production activation/collection API exists.
        let mut fixture = rusqlite::Connection::open(sp).unwrap();
        let tx = fixture.transaction().unwrap();
        if horizon {
            tx.execute("UPDATE catalogue_source_meta SET horizon=780", [])
                .unwrap();
            tx.execute("UPDATE catalogue_source_entries SET manifest_visible=0 WHERE deleted=1 AND revision<780",[]).unwrap();
        } else {
            tx.execute(
                "UPDATE catalogue_source_entries SET manifest_visible=0 WHERE deleted=1",
                [],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        println!("{{\"prepared\":620,\"retired\":160,\"head\":780}}");
        return;
    }
    let timer = Instant::now();
    let (done, state) = if horizon {
        match command {
            "start" => {
                let expected = d
                    .recovery(&scope())
                    .unwrap()
                    .map(|r| r.progress)
                    .or_else(|| d.progress(&scope()).unwrap());
                (
                    false,
                    Some(
                        start_catalogue_recovery_with_horizon(
                            &scope(),
                            expected,
                            CapabilitySupport::current(),
                            &mut auth,
                            &mut s,
                            &mut d,
                        )
                        .unwrap(),
                    ),
                )
            }
            "step" => {
                let r = d.recovery(&scope()).unwrap().unwrap();
                match step_catalogue_recovery_with_horizon(
                    &r,
                    CapabilitySupport::current(),
                    40,
                    100000,
                    &mut auth,
                    &mut s,
                    &mut d,
                )
                .unwrap()
                {
                    HorizonRecoveryStep::Pending(r) => (false, Some(r)),
                    HorizonRecoveryStep::Finished(h) => {
                        assert_eq!((h.source.position(), h.horizon), (780, 780));
                        (true, None)
                    }
                }
            }
            _ => panic!("unknown horizon command"),
        }
    } else {
        match command {
            "start" => {
                let p = d.progress(&scope()).unwrap().unwrap();
                (
                    false,
                    Some(
                        start_catalogue_recovery(
                            &scope(),
                            p,
                            CapabilitySupport::current(),
                            &mut auth,
                            &mut s,
                            &mut d,
                        )
                        .unwrap(),
                    ),
                )
            }
            "step" => {
                let r = d.recovery(&scope()).unwrap().unwrap();
                match step_catalogue_recovery(
                    &r,
                    CapabilitySupport::current(),
                    40,
                    100000,
                    &mut auth,
                    &mut s,
                    &mut d,
                )
                .unwrap()
                {
                    RecoveryStep::Pending(r) => (false, Some(r)),
                    RecoveryStep::Finished(h) => {
                        assert_eq!(h.position(), 780);
                        (true, None)
                    }
                }
            }
            _ => panic!("unknown command"),
        }
    };
    let work_ms = timer.elapsed().as_secs_f64() * 1000.;
    let db = rusqlite::Connection::open(cp).unwrap();
    let pending: i64 = db
        .query_row("SELECT count(*) FROM catalogue_candidates", [], |r| {
            r.get(0)
        })
        .unwrap();
    let (phase, completed, generation) = state.map_or_else(
        || {
            let p = d.progress(&scope()).unwrap().unwrap();
            ("Idle", p.completed, p.generation)
        },
        |r| {
            (
                match r.phase {
                    RecoveryPhase::Enumerate => "Enumerate",
                    RecoveryPhase::Reconcile => "Reconcile",
                    RecoveryPhase::Finish => "Finish",
                },
                r.progress.completed,
                r.progress.generation,
            )
        },
    );
    println!("{{\"observation_calls\":{},\"journal_effect_calls\":{},\"candidate_read_calls\":{},\"done\":{done},\"phase\":\"{phase}\",\"completed\":{completed},\"generation\":{generation},\"pending\":{pending},\"head_reads\":{},\"manifest_reads\":{},\"resolve_reads\":{},\"manifest_bytes\":{},\"payload_bytes\":{},\"authorizations\":{},\"work_ms\":{work_ms:.6}}}",d.observations,d.journal_effects,d.candidate_reads,s.head_reads(),s.manifest_reads(),s.resolve_reads(),s.manifest_bytes(),s.payload_bytes(),auth.checks);
}
