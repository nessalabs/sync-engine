#![cfg(feature = "sqlite")]
use nessa_sync::replication::{
    application::Access,
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
};
use rusqlite::Connection;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-reset-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn p(&self, n: &str) -> PathBuf {
        self.0.join(n)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
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
fn source(p: &Path) -> SqliteCatalogueSource {
    let s = scope();
    SqliteCatalogueSource::open(
        p,
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        s.schema().clone(),
    )
    .unwrap()
}
fn seed(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore, n: usize) {
    for i in 0..n {
        s.upsert(&id(&format!("id-{i:04}")), format!("value-{i}").as_bytes())
            .unwrap();
    }
    let mut a = MemoryAuthorizer::allowed(scope());
    while let Some(p) = begin_or_resume(&scope(), &mut a, s, d).unwrap() {
        apply_next_page(&p, 40, 100000, &mut a, s, d).unwrap();
    }
}
fn start(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore) -> CatalogueRecovery {
    let expected = if let Some(r) = d.recovery(&scope()).unwrap() {
        r.progress
    } else {
        d.progress(&scope()).unwrap().unwrap()
    };
    start_catalogue_recovery(
        &scope(),
        expected,
        CapabilitySupport::current(),
        &mut MemoryAuthorizer::allowed(scope()),
        s,
        d,
    )
    .unwrap()
}
fn step(
    s: &mut SqliteCatalogueSource,
    d: &mut SqliteCatalogueStore,
    r: &CatalogueRecovery,
) -> RecoveryStep {
    step_catalogue_recovery(
        r,
        CapabilitySupport::current(),
        40,
        100000,
        &mut MemoryAuthorizer::allowed(scope()),
        s,
        d,
    )
    .unwrap()
}
fn settle(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore, mut r: CatalogueRecovery) {
    for _ in 0..100 {
        match step(s, d, &r) {
            RecoveryStep::Pending(next) => r = next,
            RecoveryStep::Finished(_) => return,
        }
    }
    panic!("finite reset");
}
fn pending(path: &Path) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row("SELECT count(*) FROM catalogue_candidates", [], |r| {
            r.get(0)
        })
        .unwrap()
}
#[test]
fn full_reset_reopens_classifies_hidden_deletions_and_freshly_rechecks() {
    let f = Dir::new();
    let p = f.p("cache.db");
    let mut s = source(&f.p("source.db"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 80);
    s.delete(&id("id-0000")).unwrap();
    s.upsert(&id("id-0001"), b"new").unwrap();
    Connection::open(f.p("source.db"))
        .unwrap()
        .execute(
            "UPDATE catalogue_source_entries SET manifest_visible=0 WHERE entry_id='id-0000'",
            [],
        )
        .unwrap();
    let mut r = start(&mut s, &mut d);
    assert_eq!(r.boundary, 82);
    assert_eq!(pending(&p), 80);
    assert_eq!(d.count(&scope()).unwrap(), 0);
    assert_eq!(
        d.progress(&scope()),
        Err(CatalogueStoreError::ResetRequired)
    );
    while r.phase == RecoveryPhase::Enumerate {
        r = match step(&mut s, &mut d, &r) {
            RecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
        drop(d);
        d = SqliteCatalogueStore::open(&p).unwrap();
        assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    }
    assert_eq!(r.progress.completed, 0);
    assert!(r.progress.active.is_none());
    let before = s.head_reads();
    settle(&mut s, &mut d, r);
    assert!(s.head_reads() > before);
    assert_eq!(pending(&p), 0);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 82);
    assert_eq!(d.count(&scope()).unwrap(), 80);
    let dead = d.cached_entry(&scope(), &id("id-0000")).unwrap().unwrap();
    assert!(dead.manifest.deleted);
    assert!(dead.payload.is_empty());
    assert_eq!(
        d.cached_entry(&scope(), &id("id-0001"))
            .unwrap()
            .unwrap()
            .payload,
        b"new"
    );
}
#[test]
fn superseding_reset_keeps_candidates_and_rejects_old_generation() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 3);
    let old = start(&mut s, &mut d);
    let pass = old.progress.active.as_ref().unwrap().clone();
    let req = ManifestRequest {
        pass,
        max_entries: 40,
    };
    let page = s.manifest(&req).unwrap();
    let vals = page
        .entries
        .iter()
        .map(|e| s.resolve(&req.pass, &e.key.id, 1000).unwrap())
        .collect();
    let plan = CataloguePagePlan::new(page, vals, vec![]).unwrap();
    let next = start(&mut s, &mut d);
    assert_eq!(pending(&p), 3);
    assert!(next.progress.generation > old.progress.generation);
    assert_eq!(d.recovery_page(&old, plan), Err(CatalogueStoreError::Stale));
    assert_eq!(d.advance_recovery(&old), Err(CatalogueStoreError::Stale));
    assert_eq!(pending(&p), 3);
    settle(&mut s, &mut d, next);
}
#[test]
fn missing_row_and_unavailable_are_not_deletion_or_completion() {
    let f = Dir::new();
    let p = f.p("cache");
    let sp = f.p("source");
    let mut s = source(&sp);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    let mut r = start(&mut s, &mut d);
    // Ordinary unavailable/corrupt owned fixture; no authenticated absence proof.
    Connection::open(&sp)
        .unwrap()
        .execute(
            "DELETE FROM catalogue_source_entries WHERE entry_id='id-0000'",
            [],
        )
        .unwrap();
    r = match step(&mut s, &mut d, &r) {
        RecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    assert_eq!(r.phase, RecoveryPhase::Reconcile);
    let result = step_catalogue_recovery(
        &r,
        CapabilitySupport::current(),
        40,
        1000,
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut d,
    );
    assert_eq!(
        result,
        Err(CatalogueError::Source(CatalogueSourceError::Unavailable))
    );
    assert_eq!(pending(&p), 1);
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    assert_eq!(r.progress.completed, 0);
    assert_eq!(d.count(&scope()).unwrap(), 0);
}
#[test]
fn denial_preserves_live_cache_and_candidate_capture_rolls_back() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 2);
    let expected = d.progress(&scope()).unwrap().unwrap();
    let mut denied = MemoryAuthorizer::allowed(scope());
    denied.decision = Access::Denied;
    assert_eq!(
        start_catalogue_recovery(
            &scope(),
            expected.clone(),
            CapabilitySupport::current(),
            &mut denied,
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Denied)
    );
    assert_eq!(d.count(&scope()).unwrap(), 2);
    let db = Connection::open(&p).unwrap();
    db.execute_batch("CREATE TRIGGER fail_reset BEFORE DELETE ON catalogue_entries BEGIN SELECT RAISE(ABORT,'owned rollback'); END").unwrap();
    assert!(d.start_recovery(&scope(), expected.clone(), 2).is_err());
    assert_eq!(pending(&p), 0);
    assert_eq!(d.count(&scope()).unwrap(), 2);
    assert_eq!(d.progress(&scope()).unwrap(), Some(expected));
}
#[test]
fn classification_rollback_and_post_commit_check_keep_recovery_fenced() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 2);
    s.delete(&id("id-0000")).unwrap();
    Connection::open(f.p("source"))
        .unwrap()
        .execute(
            "UPDATE catalogue_source_entries SET manifest_visible=0 WHERE deleted=1",
            [],
        )
        .unwrap();
    let mut r = start(&mut s, &mut d);
    while r.phase == RecoveryPhase::Enumerate {
        r = match step(&mut s, &mut d, &r) {
            RecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    let db = Connection::open(&p).unwrap();
    db.execute_batch("CREATE TRIGGER fail_classify BEFORE DELETE ON catalogue_candidates BEGIN SELECT RAISE(ABORT,'owned classification rollback'); END").unwrap();
    let count = d.count(&scope()).unwrap();
    let candidates = pending(&p);
    assert!(step_catalogue_recovery(
        &r,
        CapabilitySupport::current(),
        40,
        100000,
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut d
    )
    .is_err());
    assert_eq!(d.count(&scope()).unwrap(), count);
    assert_eq!(pending(&p), candidates);
    assert!(d.cached_entry(&scope(), &id("id-0000")).unwrap().is_none());
    db.execute_batch("DROP TRIGGER fail_classify").unwrap();
    while r.phase != RecoveryPhase::Finish {
        r = match step(&mut s, &mut d, &r) {
            RecoveryStep::Pending(r) => r,
            _ => panic!(),
        };
    }
    assert_eq!(r.progress.completed, 0);
    r = match step(&mut s, &mut d, &r) {
        RecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    assert_eq!(r.progress.completed, 3);
    drop(d);
    d = SqliteCatalogueStore::open(&p).unwrap();
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    let mut a = MemoryAuthorizer::allowed(scope());
    a.decision = Access::Denied;
    let reads = s.head_reads();
    assert_eq!(
        step_catalogue_recovery(
            &r,
            CapabilitySupport::current(),
            40,
            1000,
            &mut a,
            &mut s,
            &mut d
        ),
        Err(CatalogueError::Denied)
    );
    assert_eq!(s.head_reads(), reads);
    assert_eq!(
        d.progress(&scope()),
        Err(CatalogueStoreError::ResetRequired)
    );
    assert!(matches!(
        step(&mut s, &mut d, &r),
        RecoveryStep::Finished(_)
    ));
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 3);
    // A delayed ordinary live page from the reset cannot replace the permanent fence.
    let dead = d.cached_entry(&scope(), &id("id-0000")).unwrap().unwrap();
    assert!(dead.manifest.deleted);
    assert!(dead.payload.is_empty());
}
#[test]
fn repeated_batch_and_competing_starts_do_not_skip_pending_work() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    s.delete(&id("id-0000")).unwrap();
    Connection::open(f.p("source"))
        .unwrap()
        .execute("UPDATE catalogue_source_entries SET manifest_visible=0", [])
        .unwrap();
    let expected = d.progress(&scope()).unwrap().unwrap();
    let mut other = SqliteCatalogueStore::open(&p).unwrap();
    let mut r = start(&mut s, &mut d);
    assert_eq!(
        other.start_recovery(&scope(), expected, 2),
        Err(CatalogueStoreError::Stale)
    );
    r = match step(&mut s, &mut d, &r) {
        RecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    let floors = d.recovery_candidates(&r, 1).unwrap();
    let pass = CataloguePass {
        scope: scope(),
        completed: 0,
        boundary: r.boundary,
        cursor: None,
        generation: r.progress.generation,
    };
    let value = s.resolve(&pass, &floors[0].key.id, 1000).unwrap();
    let confirmed = d.recovery_values(&r, vec![value.clone()]).unwrap();
    assert_eq!(pending(&p), 0);
    // Simulate loss of the successful result; explicit reload discovers the durable batch.
    drop(confirmed);
    drop(d);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    assert_eq!(
        d.recovery_values(&r, vec![value]),
        Err(CatalogueStoreError::Stale)
    );
    let loaded = d.recovery(&scope()).unwrap().unwrap();
    assert_eq!(loaded, r);
    assert!(d.recovery_candidates(&loaded, 1).unwrap().is_empty());
    settle(&mut s, &mut d, loaded);
}
#[test]
fn empty_reset_and_source_revision_regression_are_explicit() {
    let f = Dir::new();
    let p = f.p("cache");
    let sp = f.p("source");
    let mut s = source(&sp);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 0);
    let r = start(&mut s, &mut d);
    assert_eq!(r.phase, RecoveryPhase::Reconcile);
    settle(&mut s, &mut d, r);
    seed(&mut s, &mut d, 1);
    let saved = d.progress(&scope()).unwrap().unwrap();
    Connection::open(&sp)
        .unwrap()
        .execute("UPDATE catalogue_source_meta SET head=0", [])
        .unwrap();
    assert!(start_catalogue_recovery(
        &scope(),
        saved.clone(),
        CapabilitySupport::current(),
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut d
    )
    .is_err());
    assert_eq!(d.progress(&scope()).unwrap(), Some(saved));
    assert_eq!(d.count(&scope()).unwrap(), 1);
}
struct Lost {
    inner: SqliteCatalogueStore,
}
macro_rules! forward {($($name:ident($($arg:ident:$ty:ty),*)->$ret:ty;)*)=>{$(fn $name(&mut self,$($arg:$ty),*)->$ret{self.inner.$name($($arg),*)})*};}
impl CatalogueStore for Lost {
    forward! {progress(s:&Scope)->Result<Option<CatalogueProgress>,CatalogueStoreError>;
    begin(s:&Scope,e:Option<CatalogueProgress>,h:u64)->Result<CatalogueProgress,CatalogueStoreError>;
    cached_revision(s:&Scope,i:&Id)->Result<Option<u64>,CatalogueStoreError>;
    apply_page(p:CataloguePagePlan)->Result<CatalogueProgress,CatalogueStoreError>;
    reset(s:&Scope,p:CatalogueProgress)->Result<CatalogueProgress,CatalogueStoreError>;}
}
impl CatalogueRecoveryStore for Lost {
    forward! {recovery(s:&Scope)->Result<Option<CatalogueRecovery>,CatalogueStoreError>;
    start_recovery(s:&Scope,p:CatalogueProgress,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;
    recovery_page(r:&CatalogueRecovery,p:CataloguePagePlan)->Result<CatalogueRecovery,CatalogueStoreError>;
    recovery_candidates(r:&CatalogueRecovery,n:usize)->Result<Vec<ManifestEntry>,CatalogueStoreError>;
    advance_recovery(r:&CatalogueRecovery)->Result<CatalogueRecovery,CatalogueStoreError>;
    finish_recovery(r:&CatalogueRecovery)->Result<(),CatalogueStoreError>;}
    fn recovery_values(
        &mut self,
        r: &CatalogueRecovery,
        v: Vec<ResolvedEntry>,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        self.inner.recovery_values(r, v)?;
        Err(CatalogueStoreError::Uncertain)
    }
}
#[test]
fn uncertain_batch_returns_without_replay_and_explicit_reload_observes_commit() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    s.delete(&id("id-0000")).unwrap();
    Connection::open(f.p("source"))
        .unwrap()
        .execute("UPDATE catalogue_source_entries SET manifest_visible=0", [])
        .unwrap();
    let mut r = start(&mut s, &mut d);
    r = match step(&mut s, &mut d, &r) {
        RecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    let before = s.resolve_reads();
    let mut lost = Lost { inner: d };
    assert_eq!(
        step_catalogue_recovery(
            &r,
            CapabilitySupport::current(),
            40,
            1000,
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut lost
        ),
        Err(CatalogueError::Store(CatalogueStoreError::Uncertain))
    );
    assert_eq!(s.resolve_reads(), before + 1);
    assert_eq!(pending(&p), 0);
    drop(lost);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    let loaded = d.recovery(&scope()).unwrap().unwrap();
    settle(&mut s, &mut d, loaded);
    assert_eq!(s.resolve_reads(), before + 1);
    assert!(
        d.cached_entry(&scope(), &id("id-0000"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
}
#[test]
fn populated_receiver_migration_preserves_cache_progress_and_refuses_partial_formats() {
    let f = Dir::new();
    let p = f.p("old");
    let db = Connection::open(&p).unwrap();
    db.execute_batch("PRAGMA application_id=1314083652; PRAGMA user_version=1;
 CREATE TABLE catalogue_progress(receiver TEXT NOT NULL,origin TEXT NOT NULL,stream TEXT NOT NULL,incarnation TEXT NOT NULL,schema_id TEXT NOT NULL,access_epoch TEXT NOT NULL,completed INTEGER NOT NULL,generation INTEGER NOT NULL,active_boundary INTEGER,cursor_creation INTEGER,cursor_id TEXT,PRIMARY KEY(receiver,origin,stream)) WITHOUT ROWID;
 CREATE TABLE catalogue_entries(receiver TEXT NOT NULL,origin TEXT NOT NULL,stream TEXT NOT NULL,entry_id TEXT NOT NULL,creation INTEGER NOT NULL,revision INTEGER NOT NULL,deleted INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(receiver,origin,stream,entry_id)) WITHOUT ROWID;
 INSERT INTO catalogue_progress VALUES('phone','gateway','catalogue','first','opaque','epoch',1,1,NULL,NULL,NULL);
 INSERT INTO catalogue_entries VALUES('phone','gateway','catalogue','id-0000',1,1,0,X'61');").unwrap();
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    assert!(d.recovery(&scope()).unwrap().is_none());
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 1);
    assert_eq!(
        d.cached_entry(&scope(), &id("id-0000"))
            .unwrap()
            .unwrap()
            .payload,
        b"a"
    );
    assert_eq!(pending(&p), 0);
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    drop(d);
    db.execute_batch("DROP TABLE catalogue_candidates").unwrap();
    assert!(SqliteCatalogueStore::open(&p).is_err());
}
#[test]
fn receiver_migration_failure_rolls_back_both_journal_fields_and_version() {
    let f = Dir::new();
    let p = f.p("partial");
    let db = Connection::open(&p).unwrap();
    db.execute_batch("PRAGMA application_id=1314083652; PRAGMA user_version=1;
 CREATE TABLE catalogue_progress(receiver TEXT,origin TEXT,stream TEXT,incarnation TEXT,schema_id TEXT,access_epoch TEXT,completed INTEGER,generation INTEGER,active_boundary INTEGER,cursor_creation INTEGER,cursor_id TEXT,unexpected INTEGER);").unwrap();
    assert!(SqliteCatalogueStore::open(&p).is_err());
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM pragma_table_info('catalogue_progress') WHERE name='reset_phase'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='catalogue_candidates'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
#[test]
fn candidate_revision_floor_refuses_a_correlated_older_page_without_cursor_movement() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    s.upsert(&id("id-0000"), b"new").unwrap();
    while let Some(pass) = begin_or_resume(
        &scope(),
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut d,
    )
    .unwrap()
    {
        apply_next_page(
            &pass,
            40,
            10000,
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut d,
        )
        .unwrap();
    }
    let r = start(&mut s, &mut d);
    let pass = r.progress.active.as_ref().unwrap();
    let mut page = s
        .manifest(&ManifestRequest {
            pass: pass.clone(),
            max_entries: 40,
        })
        .unwrap();
    let mut value = s.resolve(pass, &id("id-0000"), 1000).unwrap();
    // Ordinary contradictory adapter-plan contract: numerical/page correlation is
    // valid, but it is older than this receiver's captured durable floor.
    page.entries[0].revision = 1;
    value.manifest.revision = 1;
    value.payload = b"old".to_vec();
    let plan = CataloguePagePlan::new(page, vec![value], vec![]).unwrap();
    assert_eq!(
        d.recovery_page(&r, plan),
        Err(CatalogueStoreError::Conflict)
    );
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r));
    assert_eq!(pending(&p), 1);
    assert_eq!(d.count(&scope()).unwrap(), 0);
}
#[test]
fn unfinished_candidates_cannot_advance_and_late_live_page_cannot_restore_fenced_content() {
    let f = Dir::new();
    let p = f.p("cache");
    let sp = f.p("source");
    let mut s = source(&sp);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    let old = start(&mut s, &mut d);
    let pass = old.progress.active.as_ref().unwrap();
    let page = s
        .manifest(&ManifestRequest {
            pass: pass.clone(),
            max_entries: 40,
        })
        .unwrap();
    let value = s.resolve(pass, &id("id-0000"), 1000).unwrap();
    let plan = CataloguePagePlan::new(page, vec![value], vec![]).unwrap();
    s.delete(&id("id-0000")).unwrap();
    Connection::open(&sp)
        .unwrap()
        .execute("UPDATE catalogue_source_entries SET manifest_visible=0", [])
        .unwrap();
    let mut r = start(&mut s, &mut d);
    r = match step(&mut s, &mut d, &r) {
        RecoveryStep::Pending(r) => r,
        _ => panic!(),
    };
    assert_eq!(pending(&p), 1);
    assert_eq!(d.advance_recovery(&r), Err(CatalogueStoreError::Stale));
    settle(&mut s, &mut d, r);
    assert_eq!(d.recovery_page(&old, plan), Err(CatalogueStoreError::Stale));
    assert!(
        d.cached_entry(&scope(), &id("id-0000"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
}
#[derive(Clone)]
struct Policy(std::rc::Rc<std::cell::Cell<bool>>);
impl nessa_sync::replication::application::ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        if self.0.get() {
            Access::Denied
        } else {
            Access::Allowed(s.clone())
        }
    }
}
struct ChangingPolicy {
    inner: SqliteCatalogueSource,
    policy: Policy,
    after_manifest: bool,
    after_value: bool,
    reads: usize,
}
impl CatalogueSource for ChangingPolicy {
    fn head(&mut self, s: &Scope) -> Result<u64, CatalogueSourceError> {
        self.inner.head(s)
    }
    fn manifest(&mut self, r: &ManifestRequest) -> Result<ManifestPage, CatalogueSourceError> {
        self.inner.manifest(r)
    }
    fn resolve(
        &mut self,
        p: &CataloguePass,
        i: &Id,
        n: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        self.inner.resolve(p, i, n)
    }
}
impl CatalogueHeadSource for ChangingPolicy {
    fn source_head(
        &mut self,
        s: &Scope,
    ) -> Result<nessa_sync::replication::domain::SourceHead, CatalogueSourceError> {
        self.inner.source_head(s)
    }
}
impl CatalogueReadSource for ChangingPolicy {
    fn manifest_with_head(
        &mut self,
        r: &ManifestRequest,
        c: CapabilitySupport,
    ) -> Result<CatalogueManifestRead, CatalogueSourceError> {
        let v = self.inner.manifest_with_head(r, c)?;
        if self.after_manifest {
            self.policy.0.set(true);
        }
        Ok(v)
    }
    fn resolve_with_head(
        &mut self,
        p: &CataloguePass,
        i: &Id,
        n: usize,
        c: CapabilitySupport,
    ) -> Result<CatalogueValueRead, CatalogueSourceError> {
        let v = self.inner.resolve_with_head(p, i, n, c)?;
        self.reads += 1;
        if self.after_value {
            self.policy.0.set(true);
        }
        Ok(v)
    }
}
#[test]
fn policy_change_after_manifest_stops_before_the_first_payload_read() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 2);
    let r = start(&mut s, &mut d);
    let mut policy = Policy(std::rc::Rc::new(std::cell::Cell::new(false)));
    let mut source = ChangingPolicy {
        inner: s,
        policy: policy.clone(),
        after_manifest: true,
        after_value: false,
        reads: 0,
    };
    assert_eq!(
        step_catalogue_recovery(
            &r,
            CapabilitySupport::current(),
            40,
            10000,
            &mut policy,
            &mut source,
            &mut d
        ),
        Err(CatalogueError::Denied)
    );
    assert_eq!(source.reads, 0);
    assert_eq!(d.count(&scope()).unwrap(), 0);
    assert_eq!(pending(&p), 2);
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r));
}
#[test]
fn policy_change_after_a_value_stops_the_next_read_in_each_phase() {
    for reconcile in [false, true] {
        for n in [1, 2] {
            let f = Dir::new();
            let p = f.p("cache");
            let sp = f.p("source");
            let mut s = source(&sp);
            let mut d = SqliteCatalogueStore::open(&p).unwrap();
            seed(&mut s, &mut d, n);
            if reconcile {
                for i in 0..n {
                    s.delete(&id(&format!("id-{i:04}"))).unwrap();
                }
                Connection::open(&sp)
                    .unwrap()
                    .execute("UPDATE catalogue_source_entries SET manifest_visible=0", [])
                    .unwrap();
            }
            let mut r = start(&mut s, &mut d);
            if reconcile {
                r = match step(&mut s, &mut d, &r) {
                    RecoveryStep::Pending(r) => r,
                    _ => panic!(),
                };
                assert_eq!(r.phase, RecoveryPhase::Reconcile);
            }
            let mut policy = Policy(std::rc::Rc::new(std::cell::Cell::new(false)));
            let mut source = ChangingPolicy {
                inner: s,
                policy: policy.clone(),
                after_manifest: false,
                after_value: true,
                reads: 0,
            };
            assert_eq!(
                step_catalogue_recovery(
                    &r,
                    CapabilitySupport::current(),
                    40,
                    10000,
                    &mut policy,
                    &mut source,
                    &mut d
                ),
                Err(CatalogueError::Denied)
            );
            assert_eq!(source.reads, 1);
            assert_eq!(d.count(&scope()).unwrap(), 0);
            assert_eq!(pending(&p), n as i64);
            assert_eq!(d.recovery(&scope()).unwrap(), Some(r));
        }
    }
}
#[test]
fn missing_or_partial_cache_table_is_refused_without_migration_side_effects() {
    for version in [1, 2] {
        for partial in [false, true] {
            let f = Dir::new();
            let p = f.p("cache");
            drop(SqliteCatalogueStore::open(&p).unwrap());
            let db = Connection::open(&p).unwrap();
            if version == 1 {
                db.execute_batch("DROP TABLE catalogue_candidates; ALTER TABLE catalogue_progress DROP COLUMN reset_phase; ALTER TABLE catalogue_progress DROP COLUMN reset_boundary; PRAGMA user_version=1;").unwrap();
            }
            db.execute_batch("DROP TABLE catalogue_entries").unwrap();
            if partial {
                db.execute_batch("CREATE TABLE catalogue_entries(receiver TEXT,origin TEXT,stream TEXT,entry_id TEXT,creation INTEGER,revision INTEGER,deleted INTEGER)").unwrap();
            }
            assert!(SqliteCatalogueStore::open(&p).is_err());
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                version
            );
            if version == 1 {
                assert_eq!(db.query_row("SELECT count(*) FROM pragma_table_info('catalogue_progress') WHERE name='reset_phase'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
                assert_eq!(
                    db.query_row(
                        "SELECT count(*) FROM sqlite_master WHERE name='catalogue_candidates'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                    0
                );
            }
        }
    }
}
#[test]
fn authorized_changed_epoch_reset_cancels_old_recovery_atomically_and_retains_fences() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 3);
    s.delete(&id("id-0000")).unwrap();
    while let Some(pass) = begin_or_resume(
        &scope(),
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut d,
    )
    .unwrap()
    {
        apply_next_page(
            &pass,
            40,
            10000,
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut d,
        )
        .unwrap();
    }
    let mut r = start(&mut s, &mut d);
    let request = ManifestRequest {
        pass: r.progress.active.as_ref().unwrap().clone(),
        max_entries: 2,
    };
    let page = s.manifest(&request).unwrap();
    let values = page
        .entries
        .iter()
        .map(|e| s.resolve(&request.pass, &e.key.id, 1000).unwrap())
        .collect();
    r = d
        .recovery_page(&r, CataloguePagePlan::new(page, values, vec![]).unwrap())
        .unwrap();
    let remaining = pending(&p);
    assert!(remaining > 0);
    assert_eq!(d.count(&scope()).unwrap(), 2);
    let late_req = ManifestRequest {
        pass: r.progress.active.as_ref().unwrap().clone(),
        max_entries: 2,
    };
    let late_page = s.manifest(&late_req).unwrap();
    let late_values = late_page
        .entries
        .iter()
        .map(|e| s.resolve(&late_req.pass, &e.key.id, 1000).unwrap())
        .collect();
    let late = CataloguePagePlan::new(late_page, late_values, vec![]).unwrap();
    assert_eq!(
        reset_catalogue(
            &scope(),
            r.progress.clone(),
            &mut MemoryAuthorizer::allowed(scope()),
            &mut d
        ),
        Err(CatalogueError::Store(CatalogueStoreError::ResetRequired))
    );
    let old = scope();
    let replacement = Scope::new(
        old.receiver().clone(),
        old.origin().clone(),
        old.stream().clone(),
        old.incarnation().clone(),
        old.schema().clone(),
        id("new-epoch"),
    );
    let mut denied = MemoryAuthorizer::allowed(replacement.clone());
    denied.decision = Access::Denied;
    assert_eq!(
        reset_catalogue(&replacement, r.progress.clone(), &mut denied, &mut d),
        Err(CatalogueError::Denied)
    );
    assert_eq!(pending(&p), remaining);
    let db = Connection::open(&p).unwrap();
    db.execute_batch("CREATE TRIGGER fail_cancel BEFORE UPDATE OF access_epoch ON catalogue_progress WHEN NEW.access_epoch!=OLD.access_epoch BEGIN SELECT RAISE(ABORT,'owned cancel rollback'); END").unwrap();
    assert!(reset_catalogue(
        &replacement,
        r.progress.clone(),
        &mut MemoryAuthorizer::allowed(replacement.clone()),
        &mut d
    )
    .is_err());
    assert_eq!(pending(&p), remaining);
    assert_eq!(d.recovery(&scope()).unwrap(), Some(r.clone()));
    assert_eq!(d.count(&scope()).unwrap(), 2);
    db.execute_batch("DROP TRIGGER fail_cancel").unwrap();
    let result = reset_catalogue(
        &replacement,
        r.progress.clone(),
        &mut MemoryAuthorizer::allowed(replacement.clone()),
        &mut d,
    )
    .unwrap();
    assert_eq!(result.completed, 0);
    assert!(result.generation > r.progress.generation);
    assert!(result.active.is_none());
    assert_eq!(pending(&p), 0);
    assert_eq!(d.recovery(&replacement).unwrap(), None);
    assert_eq!(d.progress(&replacement).unwrap(), Some(result));
    assert_eq!(d.count(&replacement).unwrap(), 1);
    assert!(
        d.cached_entry(&replacement, &id("id-0000"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    assert!(d
        .cached_entry(&replacement, &id("id-0001"))
        .unwrap()
        .is_none());
    assert_eq!(d.recovery_page(&r, late), Err(CatalogueStoreError::Stale));
    drop(d);
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    assert!(d.recovery(&replacement).unwrap().is_none());
    assert_eq!(d.count(&replacement).unwrap(), 1);
}
#[test]
fn newer_values_keep_the_reset_boundary_and_fresh_check_returns_followup_head() {
    let f = Dir::new();
    let p = f.p("cache");
    let mut s = source(&f.p("source"));
    let mut d = SqliteCatalogueStore::open(&p).unwrap();
    seed(&mut s, &mut d, 1);
    let mut r = start(&mut s, &mut d);
    assert_eq!(r.boundary, 1);
    s.upsert(&id("id-0000"), b"newer").unwrap();
    let mut finished = false;
    for _ in 0..20 {
        match step(&mut s, &mut d, &r) {
            RecoveryStep::Pending(next) => r = next,
            RecoveryStep::Finished(head) => {
                assert_eq!(head.position(), 2);
                finished = true;
                break;
            }
        }
    }
    assert!(finished);
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 1);
    let v = d.cached_entry(&scope(), &id("id-0000")).unwrap().unwrap();
    assert_eq!(v.manifest.revision, 2);
    assert_eq!(v.payload, b"newer");
    let mut a = MemoryAuthorizer::allowed(scope());
    let pass = begin_or_resume(&scope(), &mut a, &mut s, &mut d)
        .unwrap()
        .unwrap();
    assert_eq!(pass.boundary, 2);
    apply_next_page(&pass, 40, 1000, &mut a, &mut s, &mut d).unwrap();
    assert_eq!(d.progress(&scope()).unwrap().unwrap().completed, 2);
}
