#![cfg(feature = "sqlite")]
#[cfg(feature = "transport")]
use nessa_sync::replication::infrastructure::{
    CapabilityCatalogueClient, LoopbackClient, LoopbackConfig, LoopbackServer,
};
use nessa_sync::replication::{
    application::Access,
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope, SourceHead},
    infrastructure::{
        CataloguePublicationError, MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore,
    },
};
use rusqlite::{types::Value, Connection};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-collection-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self, n: &str) -> PathBuf {
        self.0.join(n)
    }
    fn source(&self) -> SqliteCatalogueSource {
        SqliteCatalogueSource::open(
            self.path("source"),
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
        )
        .unwrap()
    }
    fn store(&self) -> SqliteCatalogueStore {
        SqliteCatalogueStore::open(self.path("cache")).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn item(n: usize) -> Id {
    id(&format!("item-{n}"))
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn support() -> CapabilitySupport {
    CapabilitySupport::current()
}
fn auth() -> MemoryAuthorizer {
    MemoryAuthorizer::allowed(scope())
}
fn head(s: &mut SqliteCatalogueSource) -> CatalogueHorizonHead {
    s.horizon_head(&scope(), support()).unwrap()
}
fn source_snapshot(p: &Path) -> Vec<Vec<Vec<Value>>> {
    let c = Connection::open(p).unwrap();
    ["catalogue_source_meta", "catalogue_source_entries"]
        .into_iter()
        .map(|table| {
            let mut q = c
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                .unwrap();
            let n = q.column_count();
            q.query_map([], |r| (0..n).map(|i| r.get(i)).collect())
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        })
        .collect()
}
fn cache_snapshot(p: &Path) -> Vec<Vec<Vec<Value>>> {
    let c = Connection::open(p).unwrap();
    [
        "catalogue_progress",
        "catalogue_entries",
        "catalogue_candidates",
    ]
    .into_iter()
    .map(|table| {
        let mut q = c
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2,3,4"))
            .unwrap();
        let n = q.column_count();
        q.query_map([], |r| (0..n).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    })
    .collect()
}
fn populate(s: &mut SqliteCatalogueSource) {
    for i in 0..6 {
        s.upsert(&item(i), b"live").unwrap();
    }
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    s.upsert(&item(5), b"new").unwrap();
}
fn sync(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore) {
    let mut a = auth();
    while let Some(p) = begin_or_resume_with_horizon(&scope(), support(), &mut a, s, d).unwrap() {
        let mut next = Some(p);
        while let Some(p) = next {
            next = apply_next_page_with_horizon(&p, 2, 999, support(), &mut a, s, d)
                .unwrap()
                .active;
        }
    }
}
#[test]
fn zero_activation_strict_collection_preserves_rows_and_equal_marker_and_owned_resolve() {
    let f = Fixture::new();
    let mut s = f.source();
    let h = head(&mut s);
    let r = s.compare_collect(&h, 0).unwrap();
    assert_eq!(r.head, h);
    assert_eq!(r.hidden_markers, 0);
    assert_eq!(s.head(&scope()), Ok(0));
    populate(&mut s);
    let h = head(&mut s);
    let before = source_snapshot(&f.path("source"));
    let r = s.compare_collect(&h, 8).unwrap();
    assert_eq!(r.hidden_markers, 1);
    assert_eq!((r.head.source.position(), r.head.horizon), (9, 8));
    let after = source_snapshot(&f.path("source"));
    assert_eq!(before[1].len(), after[1].len());
    for (old, new) in before[1].iter().zip(&after[1]) {
        assert_eq!(&old[..5], &new[..5]);
    }
    let c = Connection::open(f.path("source")).unwrap();
    assert_eq!(
        c.query_row(
            "SELECT manifest_visible FROM catalogue_source_entries WHERE entry_id='item-0'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        c.query_row(
            "SELECT manifest_visible FROM catalogue_source_entries WHERE entry_id='item-1'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM catalogue_source_entries WHERE deleted=0 AND manifest_visible=1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        4
    );
    let p = CataloguePass {
        scope: scope(),
        completed: 8,
        boundary: 9,
        cursor: None,
        generation: 1,
    };
    let page = s
        .horizon_manifest(
            &ManifestRequest {
                pass: p.clone(),
                max_entries: 9,
            },
            support(),
            CatalogueReadKind::Incremental,
        )
        .unwrap();
    assert_eq!(page.page.entries.len(), 1);
    assert_eq!(page.page.entries[0].key.id, item(5));
    let reset = CataloguePass { completed: 0, ..p };
    let owned = s
        .horizon_value(
            &reset,
            &item(0),
            999,
            support(),
            CatalogueReadKind::FullReset,
        )
        .unwrap()
        .value;
    assert!(owned.manifest.deleted);
    assert_eq!(
        (owned.manifest.key.creation, owned.manifest.revision),
        (1, 7)
    );
    assert!(owned.payload.is_empty());
    assert_eq!(
        s.upsert(&item(0), b"again"),
        Err(CatalogueStoreError::Fenced)
    );
    assert_eq!(s.delete(&item(0)), Err(CatalogueStoreError::Fenced));
    assert_eq!(head(&mut s), r.head);
    let unchanged = source_snapshot(&f.path("source"));
    let again = s.compare_collect(&r.head, 8).unwrap();
    assert_eq!(again.hidden_markers, 0);
    assert_eq!(source_snapshot(&f.path("source")), unchanged);
}
#[test]
fn full_publication_cas_bounds_writer_support_and_rollback_are_checked_before_effects() {
    let f = Fixture::new();
    let mut s = f.source();
    populate(&mut s);
    let h = head(&mut s);
    let before = source_snapshot(&f.path("source"));
    assert_eq!(
        s.compare_collect(&h, 10),
        Err(CataloguePublicationError::InvalidHorizon {
            previous: 0,
            requested: 10,
            head: 9
        })
    );
    assert_eq!(source_snapshot(&f.path("source")), before);
    let c = Connection::open(f.path("source")).unwrap();
    c.execute_batch("CREATE TRIGGER fixture_collection_fail BEFORE UPDATE OF manifest_visible ON catalogue_source_entries BEGIN SELECT RAISE(ABORT,'owned mutation failure'); END;").unwrap();
    assert_eq!(
        s.compare_collect(&h, 8),
        Err(CataloguePublicationError::Store(
            CatalogueStoreError::Failed
        ))
    );
    assert_eq!(source_snapshot(&f.path("source")), before);
    c.execute_batch("DROP TRIGGER fixture_collection_fail")
        .unwrap();
    let receipt = s.compare_collect(&h, 8).unwrap();
    let committed = source_snapshot(&f.path("source"));
    assert_eq!(
        s.compare_collect(&h, 9),
        Err(CataloguePublicationError::Stale)
    );
    assert_eq!(source_snapshot(&f.path("source")), committed);
    assert_eq!(
        s.compare_collect(&receipt.head, 7),
        Err(CataloguePublicationError::InvalidHorizon {
            previous: 8,
            requested: 7,
            head: 9
        })
    );
    assert_eq!(source_snapshot(&f.path("source")), committed);
    c.execute(
        "UPDATE catalogue_source_meta SET writer_required=?1",
        [u64::MAX.to_be_bytes().as_slice()],
    )
    .unwrap();
    let current = head(&mut s);
    let before = source_snapshot(&f.path("source"));
    assert!(matches!(
        s.compare_collect(&current, 8),
        Err(CataloguePublicationError::Incompatible(_))
    ));
    assert_eq!(source_snapshot(&f.path("source")), before);
}
#[test]
fn competing_collectors_have_one_full_cas_winner_and_lost_result_requires_reload() {
    let f = Fixture::new();
    let mut s = f.source();
    populate(&mut s);
    let expected = head(&mut s);
    let a = f.source();
    let b = f.source();
    let gate = Arc::new(Barrier::new(2));
    let threads = [(a, 8), (b, 9)]
        .into_iter()
        .map(|(mut s, h)| {
            let g = gate.clone();
            let expected = expected.clone();
            std::thread::spawn(move || {
                g.wait();
                s.compare_collect(&expected, h)
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(CataloguePublicationError::Stale))
            .count(),
        1
    );
    drop(results);
    drop(s);
    let mut s = f.source();
    let current = head(&mut s);
    assert!([8, 9].contains(&current.horizon));
    let before = source_snapshot(&f.path("source"));
    assert_eq!(
        s.compare_collect(&expected, 9),
        Err(CataloguePublicationError::Stale)
    );
    assert_eq!(source_snapshot(&f.path("source")), before);
    assert_eq!(
        s.compare_collect(&current, current.horizon)
            .unwrap()
            .hidden_markers,
        0
    );
    assert_eq!(source_snapshot(&f.path("source")), before);
}
#[test]
fn actual_collection_expires_a_resumed_pass_then_authorized_reset_preserves_fences() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    for i in 0..6 {
        s.upsert(&item(i), b"live").unwrap();
    }
    sync(&mut s, &mut d);
    s.upsert(&item(4), b"new").unwrap();
    s.upsert(&item(5), b"new").unwrap();
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d)
        .unwrap()
        .unwrap();
    let partial =
        apply_next_page_with_horizon(&p, 1, 999, support(), &mut auth(), &mut s, &mut d).unwrap();
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    let h = head(&mut s);
    assert_eq!(s.compare_collect(&h, 10).unwrap().hidden_markers, 1);
    let before = cache_snapshot(&f.path("cache"));
    assert!(matches!(
        begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired { completed: 6, .. }
        ))
    ));
    assert_eq!(cache_snapshot(&f.path("cache")), before);
    drop(d);
    let mut d = f.store();
    let mut denied = auth();
    denied.decision = Access::Denied;
    assert_eq!(
        start_catalogue_recovery_with_horizon(
            &scope(),
            Some(partial.clone()),
            support(),
            &mut denied,
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Denied)
    );
    assert_eq!(cache_snapshot(&f.path("cache")), before);
    let mut r = start_catalogue_recovery_with_horizon(
        &scope(),
        Some(partial),
        support(),
        &mut auth(),
        &mut s,
        &mut d,
    )
    .unwrap();
    for _ in 0..30 {
        drop(d);
        d = f.store();
        assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
        match step_catalogue_recovery_with_horizon(
            &r,
            support(),
            2,
            999,
            &mut auth(),
            &mut s,
            &mut d,
        )
        .unwrap()
        {
            HorizonRecoveryStep::Pending(next) => r = next,
            HorizonRecoveryStep::Finished(h) => {
                assert_eq!((h.source.position(), h.horizon), (10, 10));
                assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 10);
                assert_eq!(d.count(&scope()).unwrap(), 6);
                assert!(
                    d.cached_entry(&scope(), &item(0))
                        .unwrap()
                        .unwrap()
                        .manifest
                        .deleted
                );
                assert!(
                    d.cached_entry(&scope(), &item(1))
                        .unwrap()
                        .unwrap()
                        .manifest
                        .deleted
                );
                assert_eq!(
                    s.upsert(&item(0), b"again"),
                    Err(CatalogueStoreError::Fenced)
                );
                return;
            }
        }
    }
    panic!("finite reset did not finish");
}

fn finish(
    mut r: CatalogueRecovery,
    s: &mut SqliteCatalogueSource,
    d: &mut SqliteCatalogueStore,
) -> CatalogueHorizonHead {
    for _ in 0..40 {
        match step_catalogue_recovery_with_horizon(&r, support(), 2, 999, &mut auth(), s, d)
            .unwrap()
        {
            HorizonRecoveryStep::Pending(next) => r = next,
            HorizonRecoveryStep::Finished(h) => return h,
        }
    }
    panic!("finite reset did not finish")
}
#[test]
fn completed_equal_horizon_continues_incrementally_after_real_collection() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    for i in 0..6 {
        s.upsert(&item(i), b"live").unwrap();
    }
    sync(&mut s, &mut d);
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    sync(&mut s, &mut d);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 8);
    s.upsert(&item(5), b"new").unwrap();
    let h = head(&mut s);
    s.compare_collect(&h, 8).unwrap();
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d)
        .unwrap()
        .unwrap();
    assert_eq!((p.completed, p.boundary), (8, 9));
    apply_next_page_with_horizon(&p, 2, 999, support(), &mut auth(), &mut s, &mut d).unwrap();
    drop(d);
    let mut d = f.store();
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 9);
    assert!(
        d.cached_entry(&scope(), &item(0))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    assert_eq!(
        d.cached_entry(&scope(), &item(5)).unwrap().unwrap().payload,
        b"new"
    );
}
#[test]
fn collection_observed_after_source_snapshot_fences_the_staged_receiver_commit() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    for i in 0..6 {
        s.upsert(&item(i), b"live").unwrap();
    }
    sync(&mut s, &mut d);
    s.upsert(&item(4), b"new").unwrap();
    s.upsert(&item(5), b"new").unwrap();
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d)
        .unwrap()
        .unwrap();
    let page = s
        .horizon_manifest(
            &ManifestRequest {
                pass: p.clone(),
                max_entries: 2,
            },
            support(),
            CatalogueReadKind::Incremental,
        )
        .unwrap()
        .page;
    let values = page
        .entries
        .iter()
        .map(|e| {
            s.horizon_value(
                &p,
                &e.key.id,
                999,
                support(),
                CatalogueReadKind::Incremental,
            )
            .unwrap()
            .value
        })
        .collect();
    let plan = CataloguePagePlan::new(page, values, vec![]).unwrap();
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    let h = head(&mut s);
    s.compare_collect(&h, 10).unwrap();
    let mut other = f.store();
    assert!(
        begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut other).is_err()
    );
    let before = cache_snapshot(&f.path("cache"));
    assert_eq!(
        d.apply_page(plan),
        Err(CatalogueStoreError::HorizonExpired {
            completed: 6,
            horizon: 10
        })
    );
    assert_eq!(cache_snapshot(&f.path("cache")), before);
    assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(10));
}
#[test]
fn collection_racing_published_finish_requires_explicit_new_generation() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    for i in 0..6 {
        s.upsert(&item(i), b"live").unwrap();
    }
    sync(&mut s, &mut d);
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    let expected = d.progress(&scope()).unwrap();
    let mut r = start_catalogue_recovery_with_horizon(
        &scope(),
        expected,
        support(),
        &mut auth(),
        &mut s,
        &mut d,
    )
    .unwrap();
    while r.phase != RecoveryPhase::Finish || r.progress.completed != r.boundary {
        r = match step_catalogue_recovery_with_horizon(
            &r,
            support(),
            2,
            999,
            &mut auth(),
            &mut s,
            &mut d,
        )
        .unwrap()
        {
            HorizonRecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    assert_eq!(r.progress.completed, 8);
    s.upsert(&item(5), b"new").unwrap();
    let h = head(&mut s);
    assert_eq!(s.compare_collect(&h, 9).unwrap().hidden_markers, 2);
    let before = cache_snapshot(&f.path("cache"));
    assert!(matches!(
        step_catalogue_recovery_with_horizon(&r, support(), 2, 999, &mut auth(), &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired { completed: 8, .. }
        ))
    ));
    assert_eq!(cache_snapshot(&f.path("cache")), before);
    drop(d);
    let mut d = f.store();
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    let next = start_catalogue_recovery_with_horizon(
        &scope(),
        Some(r.progress.clone()),
        support(),
        &mut auth(),
        &mut s,
        &mut d,
    )
    .unwrap();
    assert_eq!(next.progress.generation, r.progress.generation + 1);
    finish(next, &mut s, &mut d);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 9);
    assert_eq!(d.count(&scope()).unwrap(), 6);
    assert!(
        d.cached_entry(&scope(), &item(0))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
}
#[test]
fn cold_reset_after_reopen_never_infers_unknown_absence_or_reuses_retired_identity() {
    let f = Fixture::new();
    let mut s = f.source();
    populate(&mut s);
    let h = head(&mut s);
    s.compare_collect(&h, 8).unwrap();
    drop(s);
    let mut s = f.source();
    let mut d = f.store();
    assert!(
        begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d).is_err()
    );
    assert_eq!(d.progress(&scope()).unwrap(), None);
    let r = start_catalogue_recovery_with_horizon(
        &scope(),
        None,
        support(),
        &mut auth(),
        &mut s,
        &mut d,
    )
    .unwrap();
    finish(r, &mut s, &mut d);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 9);
    assert_eq!(d.count(&scope()).unwrap(), 5);
    assert!(d.cached_entry(&scope(), &item(0)).unwrap().is_none());
    assert!(
        d.cached_entry(&scope(), &item(1))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    assert_eq!(
        s.upsert(&item(0), b"again"),
        Err(CatalogueStoreError::Fenced)
    );
    let p = CataloguePass {
        scope: scope(),
        completed: 0,
        boundary: 9,
        cursor: None,
        generation: 1,
    };
    assert!(
        s.horizon_value(&p, &item(0), 999, support(), CatalogueReadKind::FullReset)
            .unwrap()
            .value
            .manifest
            .deleted
    );
}
#[cfg(feature = "transport")]
#[test]
fn actual_activation_refuses_unaware_native_and_wire_before_data_without_downgrade() {
    let f = Fixture::new();
    let mut s = f.source();
    populate(&mut s);
    let h = head(&mut s);
    s.compare_collect(&h, 8).unwrap();
    let r = ManifestRequest {
        pass: CataloguePass {
            scope: scope(),
            completed: 8,
            boundary: 9,
            cursor: None,
            generation: 1,
        },
        max_entries: 2,
    };
    assert_eq!(s.head(&scope()), Err(CatalogueSourceError::HorizonRequired));
    assert_eq!(
        s.source_head(&scope()),
        Err(CatalogueSourceError::HorizonRequired)
    );
    assert_eq!(s.manifest(&r), Err(CatalogueSourceError::HorizonRequired));
    assert_eq!(
        s.resolve(&r.pass, &item(5), 999),
        Err(CatalogueSourceError::HorizonRequired)
    );
    assert_eq!((s.manifest_reads(), s.resolve_reads()), (0, 0));
    let server = LoopbackServer::bind(
        0,
        LoopbackConfig {
            source_path: f.path("records"),
            catalogue_source_path: Some(f.path("source")),
            artifact_source_path: None,
            catalogue_fault: None,
            origin: id("origin"),
            stream: id("stream"),
            incarnation: id("incarnation"),
            schema: id("schema"),
            access_epoch: id("epoch"),
            read_token: id("read"),
            write_token: id("write"),
            allowed_receivers: vec![id("receiver")],
        },
    )
    .unwrap();
    let address = match server.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    std::thread::spawn(move || server.serve().unwrap());
    let mut a = LoopbackClient::new(address, id("read")).unwrap();
    let mut b = CapabilityCatalogueClient::new(address, id("read"), support()).unwrap();
    assert_eq!(a.head(&scope()), Err(CatalogueSourceError::InvalidRequest));
    assert_eq!(a.manifest(&r), Err(CatalogueSourceError::InvalidRequest));
    assert_eq!(
        a.resolve(&r.pass, &item(5), 999),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        b.source_head(&scope()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        b.manifest_with_head(&r, support()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        b.resolve_with_head(&r.pass, &item(5), 999, support()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        (
            a.counters().catalogue_manifest_bytes,
            a.counters().catalogue_payload_bytes,
            b.counters().catalogue_manifest_bytes,
            b.counters().catalogue_payload_bytes
        ),
        (0, 0, 0, 0)
    );
}

#[test]
fn collection_compares_complete_v2_publication_and_source_identity_even_for_zero() {
    let f = Fixture::new();
    let mut s = f.source();
    populate(&mut s);
    let h = head(&mut s);
    let before = source_snapshot(&f.path("source"));
    for change in 0..6 {
        let mut expected = h.clone();
        match change {
            0 => expected.version = 1,
            1 => expected.source = SourceHead::advertised(scope(), 2, 9, 0, 0),
            2 => expected.source = SourceHead::new(scope(), 8, 0, 0),
            3 => expected.source = SourceHead::new(scope(), 9, 1, 0),
            4 => expected.source = SourceHead::new(scope(), 9, 0, 1),
            _ => expected.horizon = 1,
        };
        assert_eq!(
            s.compare_collect(&expected, 0),
            Err(CataloguePublicationError::Stale)
        );
        assert_eq!(source_snapshot(&f.path("source")), before);
    }
    let wrong = Scope::new(
        id("receiver"),
        id("other-origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    );
    let expected = CatalogueHorizonHead::new(SourceHead::new(wrong, 9, 0, 0), 0);
    assert_eq!(
        s.compare_collect(&expected, 0),
        Err(CataloguePublicationError::IdentityChanged)
    );
    assert_eq!(source_snapshot(&f.path("source")), before);
}
