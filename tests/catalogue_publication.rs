#![cfg(feature = "sqlite")]
use nessa_sync::replication::{
    application::RecordHeadSource,
    catalogue::{
        apply_next_page, begin_or_resume, begin_or_resume_with_capabilities, CatalogueError,
        CatalogueHeadSource, CataloguePass, CatalogueSource, CatalogueSourceError, CatalogueStore,
        CatalogueStoreError, ManifestPage, ManifestRequest, ResolvedEntry,
    },
    domain::{CapabilityError, CapabilitySupport, Id, Scope, SourceHead},
    infrastructure::{
        CataloguePublicationError, MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore,
        SqliteOpenError, SqliteReferenceSource,
    },
};
use rusqlite::{params, Connection};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Folder(PathBuf);
impl Folder {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-catalogue-publication-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self, n: &str) -> PathBuf {
        self.0.join(n)
    }
}
impl Drop for Folder {
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
        id("catalogue"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn source(p: &Path) -> SqliteCatalogueSource {
    SqliteCatalogueSource::open(
        p,
        id("origin"),
        id("catalogue"),
        id("incarnation"),
        id("schema"),
    )
    .unwrap()
}
fn head(s: &mut SqliteCatalogueSource) -> SourceHead {
    s.source_head(&scope()).unwrap()
}
fn legacy(p: &Path) {
    Connection::open(p).unwrap().execute_batch("PRAGMA application_id=1314083651; PRAGMA user_version=1;
 CREATE TABLE catalogue_source_meta(singleton INTEGER PRIMARY KEY,origin TEXT,stream TEXT,incarnation TEXT,schema_id TEXT,head INTEGER);
 INSERT INTO catalogue_source_meta VALUES(1,'origin','catalogue','incarnation','schema',2);
 CREATE TABLE catalogue_source_entries(entry_id TEXT PRIMARY KEY,creation INTEGER,revision INTEGER,deleted INTEGER,payload BLOB);
 INSERT INTO catalogue_source_entries VALUES('a',1,1,0,X'61'),('b',2,2,1,X'');
 CREATE INDEX catalogue_source_page ON catalogue_source_entries(creation,entry_id);").unwrap();
}
// Future-format compatibility fixture only; no public encoder can assign these bits.
fn future(p: &Path, version: u16, reader: u64, writer: u64) {
    Connection::open(p).unwrap().execute("UPDATE catalogue_source_meta SET metadata_version=?1,reader_required=?2,writer_required=?3",params![version,reader.to_be_bytes().as_slice(),writer.to_be_bytes().as_slice()]).unwrap();
}
fn begin(s: &mut SqliteCatalogueSource, d: &mut SqliteCatalogueStore) -> CataloguePass {
    begin_or_resume_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut MemoryAuthorizer::allowed(scope()),
        s,
        d,
    )
    .unwrap()
    .unwrap()
}
#[test]
fn populated_migration_preserves_current_values_creation_deletion_and_reopen() {
    let d = Folder::new();
    let p = d.path("source.db");
    legacy(&p);
    let mut s = source(&p);
    assert_eq!(head(&mut s), SourceHead::new(scope(), 2, 0, 0));
    assert_eq!(s.upsert(&id("a"), b"a"), Ok(1));
    assert_eq!(
        s.upsert(&id("b"), b"live"),
        Err(CatalogueStoreError::Fenced)
    );
    assert_eq!(s.delete(&id("b")), Err(CatalogueStoreError::Fenced));
    assert_eq!(
        s.compare_upsert(&head(&mut source(&p)), &id("a"), b"new", 0, 0),
        Ok(3)
    );
    drop(s);
    let mut s = source(&p);
    let mut cache = SqliteCatalogueStore::open(d.path("cache.db")).unwrap();
    let pass = begin(&mut s, &mut cache);
    let a = s.resolve(&pass, &id("a"), 100).unwrap();
    assert_eq!(
        (a.manifest.key.creation, a.manifest.revision, a.payload),
        (1, 3, b"new".to_vec())
    );
    assert!(s.resolve(&pass, &id("b"), 100).unwrap().manifest.deleted);
    assert_eq!(
        Connection::open(&p)
            .unwrap()
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}
#[test]
fn migration_identity_partial_future_and_rollback_never_default_requirements() {
    let d = Folder::new();
    let p = d.path("source.db");
    legacy(&p);
    assert!(matches!(
        SqliteCatalogueSource::open(
            &p,
            id("other"),
            id("catalogue"),
            id("incarnation"),
            id("schema")
        ),
        Err(SqliteOpenError::SourceIdentityMismatch)
    ));
    let db = Connection::open(&p).unwrap();
    db.execute("UPDATE catalogue_source_meta SET head='bad'", [])
        .unwrap();
    assert!(SqliteCatalogueSource::open(
        &p,
        id("origin"),
        id("catalogue"),
        id("incarnation"),
        id("schema")
    )
    .is_err());
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM pragma_table_info('catalogue_source_meta')",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        6
    );
    let p = d.path("partial.db");
    legacy(&p);
    Connection::open(&p)
        .unwrap()
        .execute_batch(
            "ALTER TABLE catalogue_source_meta ADD COLUMN metadata_version INTEGER DEFAULT 1",
        )
        .unwrap();
    assert!(SqliteCatalogueSource::open(
        &p,
        id("origin"),
        id("catalogue"),
        id("incarnation"),
        id("schema")
    )
    .is_err());
    let p = d.path("future.db");
    drop(source(&p));
    Connection::open(&p)
        .unwrap()
        .pragma_update(None, "user_version", 3)
        .unwrap();
    assert!(matches!(
        SqliteCatalogueSource::open(
            &p,
            id("origin"),
            id("catalogue"),
            id("incarnation"),
            id("schema")
        ),
        Err(SqliteOpenError::UnexpectedDatabase)
    ));
    let p = d.path("masks.db");
    drop(source(&p));
    Connection::open(&p).unwrap().execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE catalogue_source_meta SET reader_required=X'00';").unwrap();
    assert!(SqliteCatalogueSource::open(
        &p,
        id("origin"),
        id("catalogue"),
        id("incarnation"),
        id("schema")
    )
    .is_err());
}
#[test]
fn compare_upsert_cas_scope_idempotence_and_lost_result_reload() {
    let d = Folder::new();
    let p = d.path("source.db");
    let mut s = source(&p);
    let expected = head(&mut s);
    for stale in [
        SourceHead::advertised(scope(), 2, 0, 0, 0),
        SourceHead::new(scope(), 0, 1, 0),
        SourceHead::new(scope(), 0, 0, 1),
    ] {
        assert_eq!(
            s.compare_upsert(&stale, &id("a"), b"a", 0, 0),
            Err(CataloguePublicationError::Stale)
        );
    }
    let _lost = s.compare_upsert(&expected, &id("a"), b"a", 0, 0);
    drop(s);
    let mut s = source(&p);
    assert_eq!(
        s.compare_upsert(&expected, &id("a"), b"a", 0, 0),
        Err(CataloguePublicationError::Stale)
    );
    let actual = head(&mut s);
    assert_eq!(s.compare_upsert(&actual, &id("a"), b"a", 0, 0), Ok(1));
    let other = Scope::new(
        id("other-device"),
        id("origin"),
        id("catalogue"),
        id("incarnation"),
        id("schema"),
        id("new-epoch"),
    );
    assert_eq!(
        s.compare_upsert(&SourceHead::new(other, 1, 0, 0), &id("a"), b"b", 0, 0),
        Ok(2)
    );
    let foreign = Scope::new(
        id("receiver"),
        id("origin"),
        id("catalogue"),
        id("new-incarnation"),
        id("schema"),
        id("epoch"),
    );
    assert_eq!(
        s.compare_upsert(&SourceHead::new(foreign, 2, 0, 0), &id("a"), b"c", 0, 0),
        Err(CataloguePublicationError::IdentityChanged)
    );
    for (reader, writer, error) in [
        (1, 0, CapabilityError::MissingReader(1)),
        (0, 1, CapabilityError::MissingWriter(1)),
    ] {
        let actual = head(&mut s);
        assert_eq!(
            s.compare_upsert(&actual, &id("a"), b"b", reader, writer),
            Err(CataloguePublicationError::Incompatible(error))
        );
        assert_eq!(head(&mut s), actual);
    }
}
#[test]
fn competing_catalogue_handles_have_one_cas_winner() {
    let d = Folder::new();
    let p = d.path("source.db");
    let expected = head(&mut source(&p));
    let b = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|n| {
            let p = p.clone();
            let expected = expected.clone();
            let b = b.clone();
            std::thread::spawn(move || {
                let mut s = source(&p);
                b.wait();
                s.compare_upsert(&expected, &id(&format!("entry{n}")), b"value", 0, 0)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| **r == Ok(1)).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(CataloguePublicationError::Stale))
            .count(),
        1
    );
}
#[test]
fn publication_and_delete_faults_rollback_head_masks_and_current_value() {
    for boundary in [
        "BEFORE UPDATE ON catalogue_source_meta",
        "BEFORE INSERT ON catalogue_source_entries",
        "AFTER INSERT ON catalogue_source_entries",
    ] {
        let d = Folder::new();
        let p = d.path("source.db");
        let mut s = source(&p);
        let expected = head(&mut s);
        let db = Connection::open(&p).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_publish {boundary} BEGIN SELECT RAISE(ABORT,'owned fault'); END;"
        ))
        .unwrap();
        assert_eq!(
            s.compare_upsert(&expected, &id("a"), b"value", 0, 0),
            Err(CataloguePublicationError::Store(
                CatalogueStoreError::Failed
            ))
        );
        drop(s);
        assert_eq!(head(&mut source(&p)), expected);
        assert_eq!(
            db.query_row("SELECT count(*) FROM catalogue_source_entries", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
    }
    let d = Folder::new();
    let p = d.path("delete.db");
    let mut s = source(&p);
    s.upsert(&id("a"), b"value").unwrap();
    Connection::open(&p).unwrap().execute_batch("CREATE TRIGGER fail_delete AFTER UPDATE ON catalogue_source_entries BEGIN SELECT RAISE(ABORT,'owned fault'); END;").unwrap();
    assert_eq!(s.delete(&id("a")), Err(CatalogueStoreError::Failed));
    drop(s);
    assert_eq!(head(&mut source(&p)).position(), 1);
}
#[test]
fn future_masks_are_lossless_and_existing_writers_recheck_before_noop_or_delete() {
    let d = Folder::new();
    let p = d.path("source.db");
    let mut s = source(&p);
    s.upsert(&id("a"), b"value").unwrap();
    let expected = head(&mut s);
    future(&p, 1, 0, u64::MAX);
    let h = head(&mut source(&p));
    assert_eq!((h.reader_required(), h.writer_required()), (0, u64::MAX));
    assert_eq!(
        s.upsert(&id("a"), b"value"),
        Err(CatalogueStoreError::Failed)
    );
    assert_eq!(s.upsert(&id("b"), b"new"), Err(CatalogueStoreError::Failed));
    assert_eq!(s.delete(&id("a")), Err(CatalogueStoreError::Failed));
    assert_eq!(
        s.compare_upsert(&expected, &id("a"), b"value", 0, 0),
        Err(CataloguePublicationError::Incompatible(
            CapabilityError::MissingWriter(u64::MAX)
        ))
    );
    let mut cache = SqliteCatalogueStore::open(d.path("cache.db")).unwrap();
    let pass = begin(&mut s, &mut cache);
    let result = apply_next_page(
        &pass,
        10,
        100,
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut cache,
    )
    .unwrap();
    assert_eq!(result.completed, 1);
    assert_eq!(cache.count(&scope()).unwrap(), 1);
    future(&p, 2, u64::MAX, u64::MAX);
    let h = head(&mut source(&p));
    assert_eq!(
        (h.version(), h.reader_required(), h.writer_required()),
        (2, u64::MAX, u64::MAX)
    );
    assert_eq!(
        s.manifest(&ManifestRequest {
            pass,
            max_entries: 10
        }),
        Err(CatalogueSourceError::Incompatible(
            CapabilityError::UnsupportedVersion(2)
        ))
    );
}
#[test]
fn reader_refusal_before_initial_or_resume_preserves_reopened_cursor_and_cache() {
    let d = Folder::new();
    let p = d.path("source.db");
    let cache = d.path("cache.db");
    let mut s = source(&p);
    for name in ["a", "b", "c"] {
        s.upsert(&id(name), b"value").unwrap();
    }
    let mut store = SqliteCatalogueStore::open(&cache).unwrap();
    let pass = begin(&mut s, &mut store);
    let saved = apply_next_page(
        &pass,
        1,
        100,
        &mut MemoryAuthorizer::allowed(scope()),
        &mut s,
        &mut store,
    )
    .unwrap();
    let active = saved.active.clone().unwrap();
    future(&p, 1, 1, 0);
    drop(store);
    let mut store = SqliteCatalogueStore::open(&cache).unwrap();
    assert_eq!(
        begin_or_resume_with_capabilities(
            &scope(),
            CapabilitySupport::current(),
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut store
        ),
        Err(CatalogueError::Incompatible(
            CapabilityError::MissingReader(1)
        ))
    );
    assert_eq!(store.progress(&scope()).unwrap(), Some(saved.clone()));
    assert_eq!(store.count(&scope()).unwrap(), 1);
    assert_eq!(
        s.manifest(&ManifestRequest {
            pass: active.clone(),
            max_entries: 10
        }),
        Err(CatalogueSourceError::Incompatible(
            CapabilityError::MissingReader(1)
        ))
    );
    assert_eq!(
        s.resolve(&active, &id("b"), 100),
        Err(CatalogueSourceError::Incompatible(
            CapabilityError::MissingReader(1)
        ))
    );
    let mut empty = SqliteCatalogueStore::open(d.path("empty.db")).unwrap();
    assert!(matches!(
        begin_or_resume_with_capabilities(
            &scope(),
            CapabilitySupport::current(),
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut empty
        ),
        Err(CatalogueError::Incompatible(_))
    ));
    assert!(empty.progress(&scope()).unwrap().is_none());
    assert_eq!(
        begin_or_resume(
            &scope(),
            &mut MemoryAuthorizer::allowed(scope()),
            &mut s,
            &mut empty
        ),
        Err(CatalogueError::Incompatible(
            CapabilityError::MissingReader(1)
        ))
    );
    assert!(empty.progress(&scope()).unwrap().is_none());
    drop(store);
    assert_eq!(
        SqliteCatalogueStore::open(cache)
            .unwrap()
            .progress(&scope())
            .unwrap(),
        Some(saved)
    );
}
struct Racing {
    inner: SqliteCatalogueSource,
    path: PathBuf,
    at: usize,
    calls: usize,
    incompatible: bool,
}
impl CatalogueSource for Racing {
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
        b: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        self.calls += 1;
        if self.calls == self.at {
            if self.incompatible {
                let mut db = Connection::open(&self.path).unwrap();
                let tx = db.transaction().unwrap();
                tx.execute(
                    "UPDATE catalogue_source_meta SET head=head+1,reader_required=?1",
                    [1u64.to_be_bytes().as_slice()],
                )
                .unwrap();
                tx.execute("UPDATE catalogue_source_entries SET revision=(SELECT head FROM catalogue_source_meta),payload=X'6e6577' WHERE entry_id='b'",[]).unwrap();
                tx.commit().unwrap();
            } else {
                self.inner.upsert(&id("b"), b"new").unwrap();
                self.inner.delete(&id("c")).unwrap();
            }
        }
        self.inner.resolve(p, i, b)
    }
}
#[test]
fn newer_value_and_requirements_are_checked_together_and_failed_page_is_not_published() {
    for at in [1, 2] {
        let d = Folder::new();
        let p = d.path("source.db");
        let cache = d.path("cache.db");
        let mut s = source(&p);
        for name in ["a", "b", "c"] {
            s.upsert(&id(name), b"v").unwrap();
        }
        let mut store = SqliteCatalogueStore::open(&cache).unwrap();
        let pass = begin(&mut s, &mut store);
        let saved = store.progress(&scope()).unwrap();
        let mut race = Racing {
            inner: s,
            path: p,
            at,
            calls: 0,
            incompatible: true,
        };
        assert_eq!(
            apply_next_page(
                &pass,
                3,
                100,
                &mut MemoryAuthorizer::allowed(scope()),
                &mut race,
                &mut store
            ),
            Err(CatalogueError::Incompatible(
                CapabilityError::MissingReader(1)
            ))
        );
        assert_eq!(store.progress(&scope()).unwrap(), saved);
        assert_eq!(store.count(&scope()).unwrap(), 0);
        assert_eq!(race.inner.payload_bytes(), (at - 1) as u64);
        drop(store);
        let mut store = SqliteCatalogueStore::open(cache).unwrap();
        assert_eq!(store.progress(&scope()).unwrap(), saved);
        assert_eq!(store.count(&scope()).unwrap(), 0);
    }
}
#[test]
fn supported_newer_values_and_deletion_keep_captured_boundary_then_recheck() {
    let d = Folder::new();
    let p = d.path("source.db");
    let mut s = source(&p);
    for name in ["a", "b", "c"] {
        s.upsert(&id(name), b"v").unwrap();
    }
    let mut store = SqliteCatalogueStore::open(d.path("cache.db")).unwrap();
    let pass = begin(&mut s, &mut store);
    let mut race = Racing {
        inner: s,
        path: p,
        at: 1,
        calls: 0,
        incompatible: false,
    };
    let progress = apply_next_page(
        &pass,
        3,
        100,
        &mut MemoryAuthorizer::allowed(scope()),
        &mut race,
        &mut store,
    )
    .unwrap();
    assert_eq!(progress.completed, 3);
    assert!(progress.active.is_none());
    assert_eq!(
        store
            .cached_entry(&scope(), &id("b"))
            .unwrap()
            .unwrap()
            .payload,
        b"new"
    );
    assert!(
        store
            .cached_entry(&scope(), &id("c"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    let next = begin(&mut race.inner, &mut store);
    assert_eq!(next.boundary, 5);
    // Shared row decoder did not change the separate record metadata owner.
    let mut record = SqliteReferenceSource::open(
        d.path("records.db"),
        id("origin"),
        id("records"),
        id("incarnation"),
        id("schema"),
    )
    .unwrap();
    let selected = Scope::new(
        id("receiver"),
        id("origin"),
        id("records"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    );
    assert_eq!(
        record.source_head(&selected).unwrap(),
        SourceHead::new(selected, 0, 0, 0)
    );
}
