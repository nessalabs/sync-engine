#![cfg(feature = "sqlite")]
use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer},
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope, SourceHead},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
};
use rusqlite::{types::Value, Connection};
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
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
            "owned-v2-reset-{}-{}",
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
        let s = scope();
        SqliteCatalogueSource::open(
            self.path("source"),
            s.origin().clone(),
            s.stream().clone(),
            s.incarnation().clone(),
            s.schema().clone(),
        )
        .unwrap()
    }
    fn store(&self) -> SqliteCatalogueStore {
        SqliteCatalogueStore::open(self.path("cache")).unwrap()
    }
    // Fixture-only future collection: there is no production activation operation.
    fn floor(&self, h: u64) {
        let mut c = Connection::open(self.path("source")).unwrap();
        let t = c.transaction().unwrap();
        t.execute("UPDATE catalogue_source_meta SET horizon=?1", [h as i64])
            .unwrap();
        t.execute("UPDATE catalogue_source_entries SET manifest_visible=0 WHERE deleted=1 AND revision<?1",[h as i64]).unwrap();
        t.commit().unwrap();
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
fn item(n: usize) -> Id {
    id(&format!("item-{n:04}"))
}
fn support() -> CapabilitySupport {
    CapabilitySupport::current()
}
fn auth() -> MemoryAuthorizer {
    MemoryAuthorizer::allowed(scope())
}
fn seed(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore, n: usize) {
    for i in 0..n {
        s.upsert(&item(i), b"old").unwrap();
    }
    let mut a = auth();
    while let Some(p) = begin_or_resume_with_horizon(&scope(), support(), &mut a, s, d).unwrap() {
        apply_next_page_with_horizon(&p, 40, 10000, support(), &mut a, s, d).unwrap();
    }
}
fn start(s: &mut impl CatalogueHorizonSource, d: &mut SqliteCatalogueStore) -> CatalogueRecovery {
    let expected = d
        .recovery(&scope())
        .unwrap()
        .map(|r| r.progress)
        .or_else(|| d.progress(&scope()).unwrap());
    start_catalogue_recovery_with_horizon(&scope(), expected, support(), &mut auth(), s, d).unwrap()
}
fn step(
    r: &CatalogueRecovery,
    s: &mut impl CatalogueHorizonSource,
    d: &mut (impl CatalogueRecoveryStore + CatalogueHorizonStore),
) -> Result<HorizonRecoveryStep, CatalogueError> {
    step_catalogue_recovery_with_horizon(r, support(), 2, 10000, &mut auth(), s, d)
}
fn settle(
    mut r: CatalogueRecovery,
    s: &mut impl CatalogueHorizonSource,
    d: &mut SqliteCatalogueStore,
) -> CatalogueHorizonHead {
    for _ in 0..40 {
        match step(&r, s, d).unwrap() {
            HorizonRecoveryStep::Pending(next) => r = next,
            HorizonRecoveryStep::Finished(h) => return h,
        }
    }
    panic!("finite reset did not finish")
}
fn snapshot(p: &Path) -> Vec<Vec<Vec<Value>>> {
    let c = Connection::open(p).unwrap();
    [
        "catalogue_progress",
        "catalogue_entries",
        "catalogue_candidates",
    ]
    .into_iter()
    .map(|name| {
        let mut q = c
            .prepare(&format!("SELECT * FROM {name} ORDER BY 1,2,3,4"))
            .unwrap();
        let count = q.column_count();
        q.query_map([], |r| (0..count).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    })
    .collect()
}
fn pending(f: &Fixture) -> i64 {
    Connection::open(f.path("cache"))
        .unwrap()
        .query_row("SELECT count(*) FROM catalogue_candidates", [], |r| {
            r.get(0)
        })
        .unwrap()
}
#[test]
fn expired_resume_keeps_state_then_v2_reset_reopens_and_fences_hidden_owned_deletions() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    seed(&mut s, &mut d, 6);
    s.upsert(&item(4), b"new").unwrap();
    s.upsert(&item(5), b"new").unwrap();
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d)
        .unwrap()
        .unwrap();
    let partial =
        apply_next_page_with_horizon(&p, 1, 999, support(), &mut auth(), &mut s, &mut d).unwrap();
    let old = partial.active.as_ref().unwrap();
    let page = s
        .horizon_manifest(
            &ManifestRequest {
                pass: old.clone(),
                max_entries: 2,
            },
            support(),
            CatalogueReadKind::Incremental,
        )
        .unwrap()
        .page;
    let v = s
        .horizon_value(
            old,
            &item(5),
            999,
            support(),
            CatalogueReadKind::Incremental,
        )
        .unwrap()
        .value;
    let stale = CataloguePagePlan::new(page, vec![v], vec![]).unwrap();
    s.delete(&item(0)).unwrap();
    s.delete(&item(1)).unwrap();
    s.upsert(&item(5), b"latest").unwrap();
    f.floor(10);
    let before = snapshot(&f.path("cache"));
    assert!(matches!(
        begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired { completed: 6, .. }
        ))
    ));
    assert_eq!(snapshot(&f.path("cache")), before);
    assert!(matches!(
        apply_next_page_with_horizon(old, 2, 999, support(), &mut auth(), &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired { .. }
        ))
    ));
    assert_eq!(snapshot(&f.path("cache")), before);
    let r = start(&mut s, &mut d);
    assert!(r.progress.generation > old.generation);
    assert_eq!(r.progress.completed, 0);
    assert_eq!(pending(&f), 6);
    assert_eq!(d.count(&scope()).unwrap(), 0);
    let r = match step(&r, &mut s, &mut d).unwrap() {
        HorizonRecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    drop(d);
    let mut d = f.store();
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    let h = settle(r, &mut s, &mut d);
    assert_eq!((h.source.position(), h.horizon), (11, 10));
    assert_eq!(pending(&f), 0);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 11);
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
        d.cached_entry(&scope(), &item(5)).unwrap().unwrap().payload,
        b"latest"
    );
    let before = snapshot(&f.path("cache"));
    assert_eq!(d.apply_page(stale), Err(CatalogueStoreError::Stale));
    assert_eq!(snapshot(&f.path("cache")), before);
    assert_eq!(
        s.upsert(&item(0), b"again"),
        Err(CatalogueStoreError::Fenced)
    );
    let raw = Connection::open(f.path("source")).unwrap();
    assert_eq!(
        raw.query_row("SELECT count(*) FROM catalogue_source_entries", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        6
    );
}
#[test]
fn fresh_parent_creation_is_atomic_cas_and_does_not_accept_existing_progress() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&item(0), b"old").unwrap();
    s.upsert(&item(1), b"old").unwrap();
    s.delete(&item(0)).unwrap();
    f.floor(3);
    let mut d = f.store();
    assert!(
        begin_or_resume_with_horizon(&scope(), support(), &mut auth(), &mut s, &mut d).is_err()
    );
    assert_eq!(d.progress(&scope()).unwrap(), None);
    let absent = CatalogueProgress {
        scope: scope(),
        completed: 0,
        generation: 0,
        active: None,
    };
    assert_eq!(
        start_catalogue_recovery_with_horizon(
            &scope(),
            Some(absent),
            support(),
            &mut auth(),
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Store(CatalogueStoreError::Stale))
    );
    assert_eq!(d.progress(&scope()).unwrap(), None);
    let raw = Connection::open(f.path("cache")).unwrap();
    raw.execute_batch("CREATE TRIGGER fixture_start_fail BEFORE UPDATE OF reset_phase ON catalogue_progress BEGIN SELECT RAISE(ABORT,'owned fault'); END;").unwrap();
    assert_eq!(
        d.start_fresh_recovery(&scope(), 3),
        Err(CatalogueStoreError::Failed)
    );
    assert_eq!(d.progress(&scope()).unwrap(), None);
    assert_eq!(d.recovery(&scope()).unwrap(), None);
    assert_eq!(pending(&f), 0);
    raw.execute_batch("DROP TRIGGER fixture_start_fail")
        .unwrap();
    let a = f.store();
    let b = f.store();
    let gate = Arc::new(Barrier::new(2));
    let threads = [a, b]
        .into_iter()
        .map(|mut d| {
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                d.start_fresh_recovery(&scope(), 3)
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
            .filter(|r| **r == Err(CatalogueStoreError::Stale))
            .count(),
        1
    );
    let r = d.recovery(&scope()).unwrap().unwrap();
    assert_eq!(r.progress.generation, 1);
    let before = snapshot(&f.path("cache"));
    assert_eq!(
        start_catalogue_recovery_with_horizon(
            &scope(),
            None,
            support(),
            &mut auth(),
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Store(CatalogueStoreError::Stale))
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    settle(r, &mut s, &mut d);
    assert!(
        d.cached_entry(&scope(), &item(0))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
}
#[test]
fn cold_reset_uses_explicit_v2_without_an_ordinary_parent_checkpoint() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&item(0), b"one").unwrap();
    let mut d = f.store();
    f.floor(1);
    let r = start_catalogue_recovery_with_horizon(
        &scope(),
        None,
        support(),
        &mut auth(),
        &mut s,
        &mut d,
    )
    .unwrap();
    assert_eq!((r.progress.generation, r.progress.completed), (1, 0));
    assert!(d.progress(&scope()).is_err());
    settle(r, &mut s, &mut d);
    assert_eq!(d.count(&scope()).unwrap(), 1);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 1);
}

struct AdvanceBeforeRead<'a> {
    fixture: &'a Fixture,
    source: SqliteCatalogueSource,
    manifest: bool,
}
impl CatalogueHorizonSource for AdvanceBeforeRead<'_> {
    fn horizon_head(
        &mut self,
        s: &Scope,
        c: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        self.source.horizon_head(s, c)
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        if self.manifest {
            self.fixture.floor(4);
        }
        self.source.horizon_manifest(r, c, k)
    }
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        if !self.manifest {
            self.fixture.floor(4);
        }
        self.source.horizon_value(p, id, b, c, k)
    }
}
struct HeadOverride {
    source: SqliteCatalogueSource,
    head: CatalogueHorizonHead,
}
impl CatalogueHorizonSource for HeadOverride {
    fn horizon_head(
        &mut self,
        _: &Scope,
        _: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        Ok(self.head.clone())
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        self.source.horizon_manifest(r, c, k)
    }
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        self.source.horizon_value(p, id, b, c, k)
    }
}
#[test]
fn full_reset_uses_boundary_and_refused_reads_preserve_journal_until_explicit_supersession() {
    for manifest in [true, false] {
        let f = Fixture::new();
        let mut s = f.source();
        let mut d = f.store();
        seed(&mut s, &mut d, 3);
        f.floor(3);
        let r = start(&mut s, &mut d);
        s.upsert(&item(2), b"advanced").unwrap();
        let delayed = s.horizon_head(&scope(), support()).unwrap();
        let mut source = AdvanceBeforeRead {
            fixture: &f,
            source: s,
            manifest,
        };
        let before = snapshot(&f.path("cache"));
        let reads = source.source.resolve_reads();
        assert!(matches!(
            step(&r, &mut source, &mut d),
            Err(CatalogueError::Source(
                CatalogueSourceError::HorizonExpired { completed: 0, .. }
            ))
        ));
        assert_eq!(source.source.resolve_reads(), reads);
        assert_eq!(snapshot(&f.path("cache")), before);
        drop(d);
        let mut d = f.store();
        assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(4));
        assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
        let mut delayed = HeadOverride {
            source: source.source,
            head: delayed,
        };
        assert_eq!(
            start_catalogue_recovery_with_horizon(
                &scope(),
                Some(r.progress.clone()),
                support(),
                &mut auth(),
                &mut delayed,
                &mut d
            ),
            Err(CatalogueError::Store(
                CatalogueStoreError::HorizonRegressed {
                    previous: 4,
                    received: 3
                }
            ))
        );
        assert_eq!(snapshot(&f.path("cache")), before);
        let next = start(&mut delayed.source, &mut d);
        assert_eq!(next.boundary, 4);
        assert_eq!(next.progress.generation, r.progress.generation + 1);
        assert_eq!(pending(&f), 3);
        let h = settle(next, &mut delayed.source, &mut d);
        assert_eq!((h.source.position(), h.horizon), (4, 4));
        assert_eq!(
            d.cached_entry(&scope(), &item(2)).unwrap().unwrap().payload,
            b"advanced"
        );
    }
}
fn fixture_with_hidden_candidate() -> (
    Fixture,
    SqliteCatalogueSource,
    SqliteCatalogueStore,
    CatalogueRecovery,
) {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    seed(&mut s, &mut d, 3);
    s.delete(&item(0)).unwrap();
    s.upsert(&item(2), b"new").unwrap();
    f.floor(5);
    let r = start(&mut s, &mut d);
    (f, s, d, r)
}
#[test]
fn every_reset_effect_rechecks_competing_observation_before_cache_cursor_or_completion() {
    for stage in ["page", "values", "empty", "publish", "finish"] {
        let (f, mut s, mut d, mut r) = fixture_with_hidden_candidate();
        if stage != "page" {
            while r.phase == RecoveryPhase::Enumerate {
                r = match step(&r, &mut s, &mut d).unwrap() {
                    HorizonRecoveryStep::Pending(r) => r,
                    _ => panic!(),
                };
            }
            assert_eq!(pending(&f), 1);
        }
        let mut plan = None;
        let mut value = None;
        if stage == "page" {
            let p = r.progress.active.as_ref().unwrap();
            let page = s
                .horizon_manifest(
                    &ManifestRequest {
                        pass: p.clone(),
                        max_entries: 2,
                    },
                    support(),
                    CatalogueReadKind::FullReset,
                )
                .unwrap()
                .page;
            let values = page
                .entries
                .iter()
                .map(|e| {
                    s.horizon_value(p, &e.key.id, 999, support(), CatalogueReadKind::FullReset)
                        .unwrap()
                        .value
                })
                .collect();
            plan = Some(CataloguePagePlan::new(page, values, vec![]).unwrap());
        }
        if stage == "values" {
            let p = CataloguePass {
                scope: scope(),
                completed: 0,
                boundary: r.boundary,
                cursor: None,
                generation: r.progress.generation,
            };
            value = Some(
                s.horizon_value(&p, &item(0), 999, support(), CatalogueReadKind::FullReset)
                    .unwrap()
                    .value,
            );
        }
        if ["empty", "publish", "finish"].contains(&stage) {
            r = match step(&r, &mut s, &mut d).unwrap() {
                HorizonRecoveryStep::Pending(r) => r,
                _ => panic!(),
            };
            assert_eq!(pending(&f), 0);
        }
        if ["publish", "finish"].contains(&stage) {
            r = match step(&r, &mut s, &mut d).unwrap() {
                HorizonRecoveryStep::Pending(r) => r,
                _ => panic!(),
            };
            assert_eq!(r.phase, RecoveryPhase::Finish);
            assert_eq!(r.progress.completed, 0);
        }
        if stage == "finish" {
            r = match step(&r, &mut s, &mut d).unwrap() {
                HorizonRecoveryStep::Pending(r) => r,
                _ => panic!(),
            };
            assert_eq!(r.progress.completed, 5);
        }
        s.upsert(&item(2), b"later").unwrap();
        f.floor(6);
        let mut other = f.store();
        other.observe_horizon(&scope(), 6).unwrap();
        let before = snapshot(&f.path("cache"));
        let result = match stage {
            "page" => d.recovery_page(&r, plan.unwrap()).map(|_| ()),
            "values" => d.recovery_values(&r, vec![value.unwrap()]).map(|_| ()),
            "empty" | "publish" => d.advance_recovery(&r).map(|_| ()),
            _ => d.finish_recovery(&r),
        };
        assert_eq!(
            result,
            Err(if stage == "finish" {
                CatalogueStoreError::HorizonExpired {
                    completed: 5,
                    horizon: 6,
                }
            } else {
                CatalogueStoreError::ResetRequired
            })
        );
        assert_eq!(snapshot(&f.path("cache")), before);
        drop(d);
        let mut d = f.store();
        assert_eq!(d.recovery(&scope()).unwrap(), Some(r));
        assert_eq!(snapshot(&f.path("cache")), before);
    }
}
#[test]
fn final_fresh_v2_check_keeps_finish_fenced_on_expiry_lower_metadata_and_denial() {
    let (f, mut s, mut d, mut r) = fixture_with_hidden_candidate();
    while r.phase != RecoveryPhase::Finish || r.progress.completed != r.boundary {
        r = match step(&r, &mut s, &mut d).unwrap() {
            HorizonRecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    s.upsert(&item(2), b"later").unwrap();
    let lower = s.horizon_head(&scope(), support()).unwrap();
    f.floor(6);
    let before = snapshot(&f.path("cache"));
    assert!(matches!(
        step(&r, &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired { completed: 5, .. }
        ))
    ));
    assert_eq!(snapshot(&f.path("cache")), before);
    drop(d);
    let mut d = f.store();
    let mut lower = HeadOverride {
        source: s,
        head: lower,
    };
    assert_eq!(
        step(&r, &mut lower, &mut d),
        Err(CatalogueError::Store(
            CatalogueStoreError::HorizonRegressed {
                previous: 6,
                received: 5
            }
        ))
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    let mut denied = auth();
    denied.decision = Access::Denied;
    assert_eq!(
        step_catalogue_recovery_with_horizon(
            &r,
            support(),
            2,
            999,
            &mut denied,
            &mut lower.source,
            &mut d
        ),
        Err(CatalogueError::Denied)
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    let next = start(&mut lower.source, &mut d);
    assert_eq!(next.progress.generation, r.progress.generation + 1);
    settle(next, &mut lower.source, &mut d);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 6);
}

struct DenyAt {
    checks: usize,
    at: usize,
}
impl ScopeAuthorizer for DenyAt {
    fn authorize(&mut self, s: &Scope) -> Access {
        self.checks += 1;
        if self.checks == self.at {
            Access::Denied
        } else {
            Access::Allowed(s.clone())
        }
    }
}
#[test]
fn current_authority_is_checked_after_metadata_after_value_and_before_finish() {
    for at in [3, 4] {
        let (f, mut s, mut d, r) = fixture_with_hidden_candidate();
        let before = snapshot(&f.path("cache"));
        let count = s.resolve_reads();
        let mut a = DenyAt { checks: 0, at };
        assert_eq!(
            step_catalogue_recovery_with_horizon(&r, support(), 2, 999, &mut a, &mut s, &mut d),
            Err(CatalogueError::Denied)
        );
        assert_eq!(s.resolve_reads() - count, if at == 3 { 0 } else { 1 });
        assert_eq!(snapshot(&f.path("cache")), before);
    }
    let (f, mut s, mut d, mut r) = fixture_with_hidden_candidate();
    while r.phase != RecoveryPhase::Finish || r.progress.completed != r.boundary {
        r = match step(&r, &mut s, &mut d).unwrap() {
            HorizonRecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    let before = snapshot(&f.path("cache"));
    let mut a = DenyAt { checks: 0, at: 3 };
    assert_eq!(
        step_catalogue_recovery_with_horizon(&r, support(), 2, 999, &mut a, &mut s, &mut d),
        Err(CatalogueError::Denied)
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r));
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    seed(&mut s, &mut d, 2);
    let p = d.progress(&scope()).unwrap();
    let before = snapshot(&f.path("cache"));
    let mut a = DenyAt { checks: 0, at: 3 };
    assert_eq!(
        start_catalogue_recovery_with_horizon(&scope(), p, support(), &mut a, &mut s, &mut d),
        Err(CatalogueError::Denied)
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    assert_eq!(pending(&f), 0);
}
struct TemporarilyMissing {
    source: SqliteCatalogueSource,
    fail: bool,
}
impl CatalogueHorizonSource for TemporarilyMissing {
    fn horizon_head(
        &mut self,
        s: &Scope,
        c: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        self.source.horizon_head(s, c)
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        self.source.horizon_manifest(r, c, k)
    }
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        c: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        if self.fail {
            Err(CatalogueSourceError::Unavailable)
        } else {
            self.source.horizon_value(p, id, b, c, k)
        }
    }
}
#[test]
fn transient_classification_failure_never_infers_absence_or_discards_pending_floors() {
    let (f, mut s, mut d, mut r) = fixture_with_hidden_candidate();
    while r.phase == RecoveryPhase::Enumerate {
        r = match step(&r, &mut s, &mut d).unwrap() {
            HorizonRecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    assert_eq!(pending(&f), 1);
    let before = snapshot(&f.path("cache"));
    let mut source = TemporarilyMissing {
        source: s,
        fail: true,
    };
    assert_eq!(
        step(&r, &mut source, &mut d),
        Err(CatalogueError::Source(CatalogueSourceError::Unavailable))
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    assert!(d.cached_entry(&scope(), &item(0)).unwrap().is_none());
    drop(d);
    let mut d = f.store();
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    assert_eq!(pending(&f), 1);
    source.fail = false;
    settle(r, &mut source, &mut d);
    assert!(
        d.cached_entry(&scope(), &item(0))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    assert_eq!(pending(&f), 0);
}
struct LostResult {
    inner: SqliteCatalogueStore,
    lose_start: bool,
    lose_page: bool,
}
macro_rules! forward {($($name:ident($($arg:ident:$ty:ty),*)->$ret:ty;)*)=>{$(fn $name(&mut self,$($arg:$ty),*)->$ret{self.inner.$name($($arg),*)})*};}
impl CatalogueStore for LostResult {
    forward! {progress(s:&Scope)->Result<Option<CatalogueProgress>,CatalogueStoreError>;begin(s:&Scope,p:Option<CatalogueProgress>,h:u64)->Result<CatalogueProgress,CatalogueStoreError>;cached_revision(s:&Scope,i:&Id)->Result<Option<u64>,CatalogueStoreError>;apply_page(p:CataloguePagePlan)->Result<CatalogueProgress,CatalogueStoreError>;reset(s:&Scope,p:CatalogueProgress)->Result<CatalogueProgress,CatalogueStoreError>;}
}
impl CatalogueHorizonStore for LostResult {
    forward! {observe_horizon(s:&Scope,h:u64)->Result<(),CatalogueStoreError>;observed_horizon(s:&Scope)->Result<Option<u64>,CatalogueStoreError>;}
}
impl CatalogueRecoveryStore for LostResult {
    forward! {recovery(s:&Scope)->Result<Option<CatalogueRecovery>,CatalogueStoreError>;start_recovery(s:&Scope,p:CatalogueProgress,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;recovery_candidates(r:&CatalogueRecovery,n:usize)->Result<Vec<ManifestEntry>,CatalogueStoreError>;recovery_values(r:&CatalogueRecovery,v:Vec<ResolvedEntry>)->Result<CatalogueRecovery,CatalogueStoreError>;advance_recovery(r:&CatalogueRecovery)->Result<CatalogueRecovery,CatalogueStoreError>;finish_recovery(r:&CatalogueRecovery)->Result<(),CatalogueStoreError>;}
    fn start_fresh_recovery(
        &mut self,
        s: &Scope,
        h: u64,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        let r = self.inner.start_fresh_recovery(s, h)?;
        if self.lose_start {
            Err(CatalogueStoreError::Uncertain)
        } else {
            Ok(r)
        }
    }
    fn recovery_page(
        &mut self,
        r: &CatalogueRecovery,
        p: CataloguePagePlan,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        let r = self.inner.recovery_page(r, p)?;
        if self.lose_page {
            Err(CatalogueStoreError::Uncertain)
        } else {
            Ok(r)
        }
    }
}
#[test]
fn uncertain_fresh_start_and_page_stop_without_replay_and_reload_exact_committed_state() {
    let f = Fixture::new();
    let mut s = f.source();
    for i in 0..3 {
        s.upsert(&item(i), b"old").unwrap();
    }
    f.floor(3);
    let mut d = LostResult {
        inner: f.store(),
        lose_start: true,
        lose_page: false,
    };
    assert_eq!(
        start_catalogue_recovery_with_horizon(
            &scope(),
            None,
            support(),
            &mut auth(),
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Store(CatalogueStoreError::Uncertain))
    );
    let r = d.recovery(&scope()).unwrap().unwrap();
    assert_eq!(r.progress.generation, 1);
    assert_eq!(d.inner.count(&scope()).unwrap(), 0);
    let before = snapshot(&f.path("cache"));
    assert_eq!(
        start_catalogue_recovery_with_horizon(
            &scope(),
            None,
            support(),
            &mut auth(),
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Store(CatalogueStoreError::Stale))
    );
    assert_eq!(snapshot(&f.path("cache")), before);
    d.lose_page = true;
    let reads = s.manifest_reads();
    assert_eq!(
        step(&r, &mut s, &mut d),
        Err(CatalogueError::Store(CatalogueStoreError::Uncertain))
    );
    assert_eq!(s.manifest_reads(), reads + 1);
    drop(d);
    let mut d = f.store();
    let loaded = d.recovery(&scope()).unwrap().unwrap();
    assert!(loaded.progress.active.as_ref().unwrap().cursor.is_some());
    assert_eq!(loaded.progress.generation, 1);
    assert_eq!(d.count(&scope()).unwrap(), 2);
    settle(loaded, &mut s, &mut d);
    assert_eq!(d.count(&scope()).unwrap(), 3);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().generation, 1);
}

struct IncorrectExpiry {
    source: SqliteCatalogueSource,
}
impl CatalogueHorizonSource for IncorrectExpiry {
    fn horizon_head(
        &mut self,
        s: &Scope,
        c: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        self.source.horizon_head(s, c)
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
        _: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        Err(CatalogueSourceError::HorizonExpired {
            completed: r.pass.completed,
            head: Box::new(self.source.horizon_head(&r.pass.scope, c)?),
        })
    }
    fn horizon_value(
        &mut self,
        _: &CataloguePass,
        _: &Id,
        _: usize,
        _: CapabilitySupport,
        _: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        panic!("invalid refusal must not reach payload")
    }
}
#[test]
fn reset_refusal_using_zero_c_instead_of_covered_boundary_is_invalid_and_not_observed() {
    let f = Fixture::new();
    let mut s = f.source();
    let mut d = f.store();
    seed(&mut s, &mut d, 3);
    f.floor(1);
    let r = start(&mut s, &mut d);
    assert_eq!(r.boundary, 3);
    f.floor(2);
    let before = snapshot(&f.path("cache"));
    let mut s = IncorrectExpiry { source: s };
    assert_eq!(
        step(&r, &mut s, &mut d),
        Err(CatalogueError::Source(CatalogueSourceError::InvalidRequest))
    );
    assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(1));
    assert_eq!(snapshot(&f.path("cache")), before);
    settle(r, &mut s.source, &mut d);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 3);
}
#[test]
fn reset_admits_writer_only_requirements_and_refuses_reader_or_version_before_effects() {
    for case in ["version", "scope", "reader", "writer"] {
        let f = Fixture::new();
        let mut s = f.source();
        let mut d = f.store();
        seed(&mut s, &mut d, 3);
        f.floor(2);
        let p = d.progress(&scope()).unwrap();
        let before = snapshot(&f.path("cache"));
        if case == "reader" || case == "writer" {
            Connection::open(f.path("source"))
                .unwrap()
                .execute(
                    &format!(
                        "UPDATE catalogue_source_meta SET {}_required=?1",
                        if case == "reader" { "reader" } else { "writer" }
                    ),
                    [u64::MAX.to_be_bytes().as_slice()],
                )
                .unwrap();
        }
        if case == "writer" {
            let r = start_catalogue_recovery_with_horizon(
                &scope(),
                p,
                support(),
                &mut auth(),
                &mut s,
                &mut d,
            )
            .unwrap();
            let h = settle(r, &mut s, &mut d);
            assert_eq!(h.source.writer_required(), u64::MAX);
            assert_eq!(d.count(&scope()).unwrap(), 3);
            continue;
        }
        if case == "reader" {
            assert!(matches!(
                start_catalogue_recovery_with_horizon(
                    &scope(),
                    p,
                    support(),
                    &mut auth(),
                    &mut s,
                    &mut d
                ),
                Err(CatalogueError::Incompatible(_))
            ));
        } else {
            let mut h = s.horizon_head(&scope(), support()).unwrap();
            if case == "version" {
                h.version = 1;
            } else {
                let other = Scope::new(
                    id("receiver"),
                    id("origin"),
                    id("stream"),
                    id("other-incarnation"),
                    id("schema"),
                    id("epoch"),
                );
                h.source = SourceHead::new(other, 3, 0, 0);
            }
            let mut source = HeadOverride { source: s, head: h };
            assert!(start_catalogue_recovery_with_horizon(
                &scope(),
                p,
                support(),
                &mut auth(),
                &mut source,
                &mut d
            )
            .is_err());
        }
        assert_eq!(snapshot(&f.path("cache")), before);
        assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(0));
        assert_eq!(pending(&f), 0);
    }
}

struct SharedPolicy(Rc<RefCell<Access>>);
impl ScopeAuthorizer for SharedPolicy {
    fn authorize(&mut self, _: &Scope) -> Access {
        self.0.borrow().clone()
    }
}
struct ChangeOnCandidates {
    inner: SqliteCatalogueStore,
    decision: Rc<RefCell<Access>>,
    after: Access,
}
impl CatalogueStore for ChangeOnCandidates {
    forward! {progress(s:&Scope)->Result<Option<CatalogueProgress>,CatalogueStoreError>;begin(s:&Scope,p:Option<CatalogueProgress>,h:u64)->Result<CatalogueProgress,CatalogueStoreError>;cached_revision(s:&Scope,i:&Id)->Result<Option<u64>,CatalogueStoreError>;apply_page(p:CataloguePagePlan)->Result<CatalogueProgress,CatalogueStoreError>;reset(s:&Scope,p:CatalogueProgress)->Result<CatalogueProgress,CatalogueStoreError>;}
}
impl CatalogueHorizonStore for ChangeOnCandidates {
    forward! {observe_horizon(s:&Scope,h:u64)->Result<(),CatalogueStoreError>;observed_horizon(s:&Scope)->Result<Option<u64>,CatalogueStoreError>;}
}
impl CatalogueRecoveryStore for ChangeOnCandidates {
    forward! {recovery(s:&Scope)->Result<Option<CatalogueRecovery>,CatalogueStoreError>;start_recovery(s:&Scope,p:CatalogueProgress,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;start_fresh_recovery(s:&Scope,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;recovery_page(r:&CatalogueRecovery,p:CataloguePagePlan)->Result<CatalogueRecovery,CatalogueStoreError>;recovery_values(r:&CatalogueRecovery,v:Vec<ResolvedEntry>)->Result<CatalogueRecovery,CatalogueStoreError>;advance_recovery(r:&CatalogueRecovery)->Result<CatalogueRecovery,CatalogueStoreError>;finish_recovery(r:&CatalogueRecovery)->Result<(),CatalogueStoreError>;}
    fn recovery_candidates(
        &mut self,
        r: &CatalogueRecovery,
        n: usize,
    ) -> Result<Vec<ManifestEntry>, CatalogueStoreError> {
        let result = self.inner.recovery_candidates(r, n)?;
        *self.decision.borrow_mut() = self.after.clone();
        Ok(result)
    }
}
#[test]
fn empty_candidate_read_reauthorizes_before_advancing_for_both_current_policy_refusals() {
    for horizon in [false, true] {
        for after in [Access::Denied, Access::Unverifiable] {
            let (f, mut s, mut d, mut r) = fixture_with_hidden_candidate();
            while r.phase == RecoveryPhase::Enumerate || pending(&f) > 0 {
                r = match step(&r, &mut s, &mut d).unwrap() {
                    HorizonRecoveryStep::Pending(r) => r,
                    _ => panic!(),
                };
            }
            assert_eq!(r.phase, RecoveryPhase::Reconcile);
            assert_eq!(pending(&f), 0);
            let before = snapshot(&f.path("cache"));
            let decision = Rc::new(RefCell::new(Access::Allowed(scope())));
            let expected = if after == Access::Denied {
                CatalogueError::Denied
            } else {
                CatalogueError::Unverifiable
            };
            let mut d = ChangeOnCandidates {
                inner: d,
                decision: decision.clone(),
                after,
            };
            let mut a = SharedPolicy(decision);
            let result = if horizon {
                step_catalogue_recovery_with_horizon(&r, support(), 2, 999, &mut a, &mut s, &mut d)
                    .map(|_| ())
            } else {
                step_catalogue_recovery(&r, support(), 2, 999, &mut a, &mut s, &mut d).map(|_| ())
            };
            assert_eq!(result, Err(expected));
            assert_eq!(snapshot(&f.path("cache")), before);
            assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
            drop(d);
            let mut d = f.store();
            assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
            settle(r, &mut s, &mut d);
        }
    }
}

#[test]
fn staged_start_boundary_refuses_newer_observation_before_creation_or_live_erasure() {
    for fresh in [true, false] {
        let f = Fixture::new();
        let mut s = f.source();
        let mut d = f.store();
        if fresh {
            for i in 0..3 {
                s.upsert(&item(i), b"old").unwrap();
            }
        } else {
            seed(&mut s, &mut d, 3);
        }
        let expected = d.progress(&scope()).unwrap();
        let held = s.horizon_head(&scope(), support()).unwrap();
        assert_eq!(held.source.position(), 3);
        s.upsert(&item(2), b"new").unwrap();
        f.floor(4);
        let mut other = f.store();
        other.observe_horizon(&scope(), 4).unwrap();
        let before = snapshot(&f.path("cache"));
        let result = match expected {
            Some(p) => d.start_recovery(&scope(), p, held.source.position()),
            None => d.start_fresh_recovery(&scope(), held.source.position()),
        };
        assert_eq!(result, Err(CatalogueStoreError::ResetRequired));
        assert_eq!(snapshot(&f.path("cache")), before);
        drop(d);
        let mut d = f.store();
        assert_eq!(snapshot(&f.path("cache")), before);
        assert_eq!(d.recovery(&scope()).unwrap(), None);
        assert_eq!(d.count(&scope()).unwrap(), if fresh { 0 } else { 3 });
    }
}
