#![cfg(feature = "sqlite")]
use nessa_sync::replication::{
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-horizon-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn source(&self) -> SqliteCatalogueSource {
        SqliteCatalogueSource::open(
            self.0.join("source"),
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
        )
        .unwrap()
    }
    fn store(&self) -> SqliteCatalogueStore {
        SqliteCatalogueStore::open(self.0.join("cache")).unwrap()
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
fn support() -> CapabilitySupport {
    CapabilitySupport::current()
}
fn pass(c: u64, h: u64) -> CataloguePass {
    CataloguePass {
        scope: scope(),
        completed: c,
        boundary: h,
        cursor: None,
        generation: 1,
    }
}
// Fixture-only activation: no production publisher or collector is exposed.
fn raise(f: &Fixture, h: u64) {
    rusqlite::Connection::open(f.0.join("source"))
        .unwrap()
        .execute("UPDATE catalogue_source_meta SET horizon=?1", [h as i64])
        .unwrap();
}
#[test]
fn source_v2_equality_expiry_restart_and_legacy_fail_closed() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&id("one"), b"one").unwrap();
    s.upsert(&id("two"), b"two").unwrap();
    raise(&f, 1);
    drop(s);
    let mut s = f.source();
    assert_eq!(s.horizon_head(&scope(), support()).unwrap().horizon, 1);
    assert_eq!(s.head(&scope()), Err(CatalogueSourceError::HorizonRequired));
    assert_eq!(
        s.source_head(&scope()),
        Err(CatalogueSourceError::HorizonRequired)
    );
    let r = ManifestRequest {
        pass: pass(1, 2),
        max_entries: 4,
    };
    assert_eq!(s.manifest(&r), Err(CatalogueSourceError::HorizonRequired));
    assert_eq!(
        s.resolve(&r.pass, &id("two"), 99),
        Err(CatalogueSourceError::HorizonRequired)
    );
    assert_eq!(s.manifest_reads(), 0);
    assert_eq!(s.resolve_reads(), 0);
    assert!(s
        .horizon_manifest(&r, support(), CatalogueReadKind::Incremental)
        .is_ok());
    assert!(s
        .horizon_value(
            &r.pass,
            &id("two"),
            99,
            support(),
            CatalogueReadKind::Incremental
        )
        .is_ok());
    let expired = ManifestRequest {
        pass: pass(0, 2),
        max_entries: 4,
    };
    let before = (s.manifest_reads(), s.resolve_reads());
    assert_eq!(
        s.horizon_manifest(&expired, support(), CatalogueReadKind::Incremental),
        Err(CatalogueSourceError::HorizonExpired {
            completed: 0,
            head: Box::new(s.horizon_head(&scope(), support()).unwrap())
        })
    );
    assert_eq!(
        s.horizon_value(
            &expired.pass,
            &id("one"),
            99,
            support(),
            CatalogueReadKind::Incremental
        ),
        Err(CatalogueSourceError::HorizonExpired {
            completed: 0,
            head: Box::new(s.horizon_head(&scope(), support()).unwrap())
        })
    );
    assert_eq!(before, (s.manifest_reads(), s.resolve_reads()));
    assert!(s
        .horizon_manifest(&expired, support(), CatalogueReadKind::FullReset)
        .is_ok());
    assert!(s
        .horizon_manifest(&r, support(), CatalogueReadKind::FullReset)
        .is_err());
}
#[test]
fn receiver_observation_is_exact_scope_restart_durable_and_not_progress() {
    let f = Fixture::new();
    let mut d = f.store();
    let s = scope();
    d.observe_horizon(&s, 5).unwrap();
    assert_eq!(d.progress(&s).unwrap(), None);
    drop(d);
    let mut d = f.store();
    assert_eq!(d.observed_horizon(&s).unwrap(), Some(5));
    assert_eq!(
        d.observe_horizon(&s, 4),
        Err(CatalogueStoreError::HorizonRegressed {
            previous: 5,
            received: 4
        })
    );
    assert_eq!(d.observed_horizon(&s).unwrap(), Some(5));
    let different = Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("next-epoch"),
    );
    assert_eq!(d.observed_horizon(&different).unwrap(), None);
    d.observe_horizon(&different, 0).unwrap();
    assert_eq!(d.observed_horizon(&s).unwrap(), Some(5));
    assert_eq!(
        d.begin(&s, None, 10),
        Err(CatalogueStoreError::HorizonExpired {
            completed: 0,
            horizon: 5
        })
    );
    assert_eq!(d.progress(&s).unwrap(), None);
}
#[test]
fn cold_and_resumed_expiry_preserve_reopened_cache_and_cursor() {
    let f = Fixture::new();
    let mut s = f.source();
    for n in 0..4 {
        s.upsert(&id(&format!("item-{n}")), b"value").unwrap();
    }
    let mut d = f.store();
    let mut a = MemoryAuthorizer::allowed(scope());
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
        .unwrap()
        .unwrap();
    let after =
        apply_next_page_with_horizon(&p, 1, 999, support(), &mut a, &mut s, &mut d).unwrap();
    assert_eq!(after.completed, 0);
    assert!(after.active.as_ref().unwrap().cursor.is_some());
    raise(&f, 1);
    assert_eq!(
        begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d),
        Err(CatalogueError::Source(
            CatalogueSourceError::HorizonExpired {
                completed: 0,
                head: Box::new(s.horizon_head(&scope(), support()).unwrap())
            }
        ))
    );
    assert_eq!(d.progress(&scope()).unwrap(), Some(after.clone()));
    assert_eq!(d.count(&scope()).unwrap(), 1);
    drop(d);
    let mut d = f.store();
    assert_eq!(d.progress(&scope()).unwrap(), Some(after));
    assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(1));
    assert_eq!(d.count(&scope()).unwrap(), 1);
    let mut fresh = SqliteCatalogueStore::open(f.0.join("fresh")).unwrap();
    assert!(begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut fresh).is_err());
    assert_eq!(fresh.progress(&scope()).unwrap(), None);
    assert_eq!(fresh.count(&scope()).unwrap(), 0);
}
#[test]
fn concurrent_observation_blocks_staged_page_before_cache_or_completion() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&id("one"), b"one").unwrap();
    let mut d = f.store();
    let mut a = MemoryAuthorizer::allowed(scope());
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
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
    let value = s
        .horizon_value(
            &p,
            &id("one"),
            99,
            support(),
            CatalogueReadKind::Incremental,
        )
        .unwrap()
        .value;
    let plan = CataloguePagePlan::new(page, vec![value], vec![]).unwrap();
    let before = d.progress(&scope()).unwrap();
    let mut other = f.store();
    other.observe_horizon(&scope(), 1).unwrap();
    assert_eq!(
        d.apply_page(plan),
        Err(CatalogueStoreError::HorizonExpired {
            completed: 0,
            horizon: 1
        })
    );
    drop(d);
    let mut d = f.store();
    assert_eq!(d.progress(&scope()).unwrap(), before);
    assert_eq!(d.count(&scope()).unwrap(), 0);
}

struct RaiseOnRead<'a> {
    fixture: &'a Fixture,
    source: SqliteCatalogueSource,
    on_manifest: bool,
    on_value: bool,
}
impl CatalogueHorizonSource for RaiseOnRead<'_> {
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
        if self.on_manifest {
            raise(self.fixture, 1);
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
        if self.on_value {
            raise(self.fixture, 1);
        }
        self.source.horizon_value(p, id, b, c, k)
    }
}
#[test]
fn manifest_and_value_expiry_observe_publication_before_reopen_and_delayed_lower_head() {
    for during_manifest in [true, false] {
        let f = Fixture::new();
        let mut s = f.source();
        s.upsert(&id("one"), b"one").unwrap();
        let mut a = MemoryAuthorizer::allowed(scope());
        let mut d = f.store();
        let p = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
            .unwrap()
            .unwrap();
        let before = d.progress(&scope()).unwrap();
        let mut source = RaiseOnRead {
            fixture: &f,
            source: s,
            on_manifest: during_manifest,
            on_value: !during_manifest,
        };
        assert!(matches!(
            apply_next_page_with_horizon(&p, 2, 99, support(), &mut a, &mut source, &mut d),
            Err(CatalogueError::Source(
                CatalogueSourceError::HorizonExpired { completed: 0, .. }
            ))
        ));
        assert_eq!(source.source.resolve_reads(), 0);
        assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(1));
        assert_eq!(d.progress(&scope()).unwrap(), before);
        assert_eq!(d.count(&scope()).unwrap(), 0);
        drop(d);
        let mut d = f.store();
        raise(&f, 0);
        let mut s = f.source();
        assert_eq!(
            begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d),
            Err(CatalogueError::Store(
                CatalogueStoreError::HorizonRegressed {
                    previous: 1,
                    received: 0
                }
            ))
        );
        assert_eq!(d.progress(&scope()).unwrap(), before);
        assert_eq!(d.count(&scope()).unwrap(), 0);
        assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(1));
    }
}
#[test]
fn completed_equal_horizon_reads_and_mandatory_fresh_recheck_work() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&id("one"), b"old").unwrap();
    let mut d = f.store();
    let mut a = MemoryAuthorizer::allowed(scope());
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
        .unwrap()
        .unwrap();
    let done = apply_next_page_with_horizon(&p, 2, 99, support(), &mut a, &mut s, &mut d).unwrap();
    assert_eq!(done.completed, 1);
    raise(&f, 1);
    s.upsert(&id("one"), b"new").unwrap();
    let p = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
        .unwrap()
        .unwrap();
    assert_eq!(p.completed, 1);
    assert_eq!(p.boundary, 2);
    let done = apply_next_page_with_horizon(&p, 2, 99, support(), &mut a, &mut s, &mut d).unwrap();
    assert_eq!(done.completed, 2);
    s.upsert(&id("two"), b"later").unwrap();
    let next = begin_or_resume_with_horizon(&scope(), support(), &mut a, &mut s, &mut d)
        .unwrap()
        .unwrap();
    assert_eq!(next.boundary, 3);
    drop(d);
    let mut d = f.store();
    assert_eq!(
        d.cached_entry(&scope(), &id("one"))
            .unwrap()
            .unwrap()
            .payload,
        b"new"
    );
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 2);
}

#[test]
fn observation_failure_rolls_back_and_source_migration_keeps_populated_identity() {
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&id("live"), b"live").unwrap();
    s.upsert(&id("deleted"), b"old").unwrap();
    s.delete(&id("deleted")).unwrap();
    drop(s);
    let raw = rusqlite::Connection::open(f.0.join("source")).unwrap();
    raw.execute_batch(
        "ALTER TABLE catalogue_source_meta DROP COLUMN horizon; PRAGMA user_version=3;",
    )
    .unwrap();
    let mut s = f.source();
    assert_eq!(s.horizon_head(&scope(), support()).unwrap().horizon, 0);
    assert_eq!(s.head(&scope()).unwrap(), 3);
    assert_eq!(
        s.upsert(&id("deleted"), b"again"),
        Err(CatalogueStoreError::Fenced)
    );
    let mut d = f.store();
    d.observe_horizon(&scope(), 0).unwrap();
    let raw = rusqlite::Connection::open(f.0.join("cache")).unwrap();
    raw.execute_batch("CREATE TRIGGER fixture_fail BEFORE UPDATE ON catalogue_horizons BEGIN SELECT RAISE(ABORT,'owned fixture failure'); END;").unwrap();
    assert_eq!(
        d.observe_horizon(&scope(), 1),
        Err(CatalogueStoreError::Failed)
    );
    drop(d);
    let mut d = f.store();
    assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(0));
    assert_eq!(d.progress(&scope()).unwrap(), None);
    raw.execute_batch("DROP TRIGGER fixture_fail").unwrap();
    d.observe_horizon(&scope(), 1).unwrap();
    assert_eq!(d.observed_horizon(&scope()).unwrap(), Some(1));
}

struct HeadOverride {
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
        _: &ManifestRequest,
        _: CapabilitySupport,
        _: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        panic!("invalid admission must not read manifest")
    }
    fn horizon_value(
        &mut self,
        _: &CataloguePass,
        _: &Id,
        _: usize,
        _: CapabilitySupport,
        _: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        panic!("invalid admission must not read value")
    }
}
#[test]
fn incompatible_versions_readers_and_wrong_scope_do_not_observe_or_create_progress() {
    use nessa_sync::replication::domain::SourceHead;
    for case in 0..4 {
        let f = Fixture::new();
        let mut d = f.store();
        let mut a = MemoryAuthorizer::allowed(scope());
        let mut h = CatalogueHorizonHead::new(SourceHead::new(scope(), 10, 0, 0), 1);
        match case {
            0 => h.version = 1,
            1 => h.source = SourceHead::new(scope(), 10, 1, 0),
            2 => h.horizon = 11,
            _ => {
                h.source = SourceHead::new(
                    Scope::new(
                        id("different"),
                        id("origin"),
                        id("stream"),
                        id("incarnation"),
                        id("schema"),
                        id("epoch"),
                    ),
                    10,
                    0,
                    0,
                )
            }
        }
        assert!(begin_or_resume_with_horizon(
            &scope(),
            support(),
            &mut a,
            &mut HeadOverride { head: h },
            &mut d
        )
        .is_err());
        assert_eq!(d.observed_horizon(&scope()).unwrap(), None);
        assert_eq!(d.progress(&scope()).unwrap(), None);
        assert_eq!(d.count(&scope()).unwrap(), 0);
    }
}

#[cfg(feature = "transport")]
#[test]
fn legacy_and_capability_wire_refuse_positive_fixture_without_payload_or_downgrade() {
    use nessa_sync::replication::infrastructure::{
        CapabilityCatalogueClient, LoopbackClient, LoopbackConfig, LoopbackServer,
    };
    let f = Fixture::new();
    let mut s = f.source();
    s.upsert(&id("one"), b"one").unwrap();
    raise(&f, 1);
    let server = LoopbackServer::bind(
        0,
        LoopbackConfig {
            source_path: f.0.join("records"),
            catalogue_source_path: Some(f.0.join("source")),
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
    let addr = match server.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    std::thread::spawn(move || server.serve().unwrap());
    let mut legacy = LoopbackClient::new(addr, id("read")).unwrap();
    let r = ManifestRequest {
        pass: pass(0, 1),
        max_entries: 2,
    };
    assert_eq!(
        legacy.head(&scope()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        legacy.manifest(&r),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        legacy.resolve(&r.pass, &id("one"), 99),
        Err(CatalogueSourceError::InvalidRequest)
    );
    let mut cap = CapabilityCatalogueClient::new(addr, id("read"), support()).unwrap();
    assert_eq!(
        cap.source_head(&scope()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        cap.manifest_with_head(&r, support()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(
        cap.resolve_with_head(&r.pass, &id("one"), 99, support()),
        Err(CatalogueSourceError::InvalidRequest)
    );
    assert_eq!(legacy.counters().catalogue_manifest_bytes, 0);
    assert_eq!(cap.counters().catalogue_manifest_bytes, 0);
    assert_eq!(legacy.counters().catalogue_payload_bytes, 0);
    assert_eq!(cap.counters().catalogue_payload_bytes, 0);
}
