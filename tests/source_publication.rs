#![cfg(feature = "sqlite")]
use nessa_sync::replication::{
    application::{
        begin_pass_with_capabilities, finish_pass, RecordHeadSource, RecordReadView, RecordSource,
        SourceError, StoreError, SyncError,
    },
    domain::{CapabilityError, CapabilitySupport, Id, Limits, PageRequest, Scope, SourceHead},
    history::{HistorySource, HistorySourceError, OlderRequest, TailRequest},
    infrastructure::{
        MemoryAuthorizer, SourcePublicationError, SqliteOpenError, SqliteReferenceSource,
        SqliteReplicaStore,
    },
};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Folder(PathBuf);
impl Folder {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "owned-publication-{}-{}",
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
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn source(p: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        p,
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
    )
    .unwrap()
}
fn page() -> PageRequest {
    PageRequest {
        scope: scope(),
        after: 0,
        target: 2,
        max_records: 10,
        max_payload_bytes: 100,
        max_record_bytes: 100,
    }
}
fn legacy(p: &Path) {
    Connection::open(p).unwrap().execute_batch("PRAGMA application_id=1314083667; PRAGMA user_version=1;
 CREATE TABLE source_meta(singleton INTEGER PRIMARY KEY,origin TEXT,stream TEXT,incarnation TEXT,schema_id TEXT,head INTEGER);
 INSERT INTO source_meta VALUES(1,'origin','stream','incarnation','schema',2);
 CREATE TABLE source_records(position INTEGER PRIMARY KEY,record_id TEXT UNIQUE,payload BLOB);
 INSERT INTO source_records VALUES(1,'one',X'61'),(2,'two',X'62');
 CREATE TABLE source_history(singleton INTEGER PRIMARY KEY,pruned_through INTEGER);
 INSERT INTO source_history VALUES(1,1);").unwrap();
}
// Owned forward-format fixture only. No current public writer assigns these bits.
fn future(p: &Path, version: u16, reader: u64, writer: u64) {
    Connection::open(p)
        .unwrap()
        .execute(
            "UPDATE source_meta SET metadata_version=?1,reader_required=?2,writer_required=?3",
            params![
                version,
                reader.to_be_bytes().as_slice(),
                writer.to_be_bytes().as_slice()
            ],
        )
        .unwrap();
}
fn head(s: &mut SqliteReferenceSource) -> SourceHead {
    s.source_head(&scope()).unwrap()
}
#[test]
fn populated_v1_migration_preserves_identity_rows_floor_and_dedup_after_restart() {
    let d = Folder::new();
    let p = d.path("source.db");
    legacy(&p);
    let mut s = source(&p);
    assert_eq!(head(&mut s), SourceHead::new(scope(), 2, 0, 0));
    assert_eq!(s.append(&id("one"), b"a"), Ok(1));
    assert_eq!(s.page(&page()), Err(SourceError::Pruned));
    let mut r = page();
    r.after = 1;
    assert_eq!(s.page(&r).unwrap().records[0].payload, b"b");
    drop(s);
    let mut s = source(&p);
    assert_eq!(head(&mut s).position(), 2);
    let db = Connection::open(&p).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row("SELECT pruned_through FROM source_history", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        s.append(&id("one"), b"different"),
        Err(StoreError::ConflictingRecord)
    );
}
#[test]
fn wrong_identity_and_failed_migration_do_not_change_old_file() {
    let d = Folder::new();
    let p = d.path("source.db");
    legacy(&p);
    assert!(matches!(
        SqliteReferenceSource::open(
            &p,
            id("origin"),
            id("stream"),
            id("new-incarnation"),
            id("schema")
        ),
        Err(SqliteOpenError::SourceIdentityMismatch)
    ));
    let db = Connection::open(&p).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    db.execute_batch("DROP TABLE source_history; CREATE TABLE source_history(broken TEXT);")
        .unwrap();
    assert!(SqliteReferenceSource::open(
        &p,
        id("origin"),
        id("stream"),
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
            "SELECT count(*) FROM pragma_table_info('source_meta')",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        6
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM source_records", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}
#[test]
fn atomic_compare_append_is_restartable_idempotent_and_source_owned() {
    let d = Folder::new();
    let p = d.path("source.db");
    let mut s = source(&p);
    let expected = head(&mut s);
    // CAS covers the full publication, not just the integer head.
    for stale in [
        SourceHead::advertised(scope(), 2, 0, 0, 0),
        SourceHead::new(scope(), 0, 1, 0),
        SourceHead::new(scope(), 0, 0, 1),
    ] {
        assert_eq!(
            s.compare_append(&stale, &id("stale"), b"value", 0, 0),
            Err(SourcePublicationError::Stale)
        );
        assert_eq!(head(&mut s), expected);
    }
    assert_eq!(
        s.compare_append(&expected, &id("one"), b"value", 0, 0),
        Ok(1)
    );
    drop(s);
    let mut s = source(&p);
    let actual = head(&mut s);
    assert_eq!(actual.position(), 1);
    assert_eq!(
        s.compare_append(&expected, &id("one"), b"value", 0, 0),
        Err(SourcePublicationError::Stale)
    );
    assert_eq!(s.compare_append(&actual, &id("one"), b"value", 0, 0), Ok(1));
    let other = Scope::new(
        id("other-device"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("new-epoch"),
    );
    let expected = SourceHead::new(other, 1, 0, 0);
    assert_eq!(
        s.compare_append(&expected, &id("two"), b"value", 0, 0),
        Ok(2)
    );
    let foreign = SourceHead::new(
        Scope::new(
            id("receiver"),
            id("origin"),
            id("stream"),
            id("new-incarnation"),
            id("schema"),
            id("epoch"),
        ),
        2,
        0,
        0,
    );
    assert_eq!(
        s.compare_append(&foreign, &id("three"), b"value", 0, 0),
        Err(SourcePublicationError::IdentityChanged)
    );
    for (reader, writer, error) in [
        (1, 0, CapabilityError::MissingReader(1)),
        (0, 1, CapabilityError::MissingWriter(1)),
    ] {
        let expected = head(&mut s);
        assert_eq!(
            s.compare_append(&expected, &id("future"), b"value", reader, writer),
            Err(SourcePublicationError::Incompatible(error))
        );
        assert_eq!(head(&mut s), expected);
    }
}
#[test]
fn competing_handles_have_one_publication_winner() {
    let d = Folder::new();
    let p = d.path("source.db");
    let expected = head(&mut source(&p));
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|n| {
            let p = p.clone();
            let expected = expected.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let mut s = source(&p);
                b.wait();
                s.compare_append(&expected, &id(&format!("record{n}")), b"value", 0, 0)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| **r == Ok(1)).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(SourcePublicationError::Stale))
            .count(),
        1
    );
    assert_eq!(head(&mut source(&p)).position(), 1);
}
#[test]
fn transaction_faults_rollback_metadata_and_record_and_lost_reply_reloads() {
    for boundary in [
        "BEFORE UPDATE ON source_meta",
        "BEFORE INSERT ON source_records",
        "AFTER INSERT ON source_records",
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
            s.compare_append(&expected, &id("one"), b"value", 0, 0),
            Err(SourcePublicationError::Store(StoreError::Failed))
        );
        drop(s);
        let mut s = source(&p);
        assert_eq!(head(&mut s), expected);
        assert_eq!(
            db.query_row("SELECT count(*) FROM source_records", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        db.execute_batch("DROP TRIGGER fail_publish;").unwrap();
        let _lost_result = s.compare_append(&expected, &id("one"), b"value", 0, 0);
        drop(s);
        assert_eq!(head(&mut source(&p)).position(), 1);
    }
}
#[test]
fn full_width_future_requirements_survive_restart_without_downgrade() {
    let d = Folder::new();
    let p = d.path("source.db");
    drop(source(&p));
    future(&p, 2, u64::MAX, u64::MAX);
    let mut s = source(&p);
    let actual = head(&mut s);
    assert_eq!(
        (
            actual.version(),
            actual.reader_required(),
            actual.writer_required()
        ),
        (2, u64::MAX, u64::MAX)
    );
    assert_eq!(s.append(&id("one"), b"value"), Err(StoreError::Failed));
    assert_eq!(s.prune_through(0), Err(StoreError::Failed));
    assert_eq!(
        s.compare_append(&actual, &id("one"), b"value", 0, 0),
        Err(SourcePublicationError::Incompatible(
            CapabilityError::UnsupportedVersion(2)
        ))
    );
}
#[test]
fn fresh_requirements_guard_all_payload_routes_and_existing_writer_handles() {
    for reader in [0, 1] {
        let d = Folder::new();
        let p = d.path("source.db");
        let mut s = source(&p);
        s.append(&id("one"), b"a").unwrap();
        s.append(&id("two"), b"b").unwrap();
        let before = s.view_head(&scope()).unwrap();
        future(&p, 1, reader, u64::MAX);
        let actual = head(&mut source(&p));
        assert_eq!(
            (actual.reader_required(), actual.writer_required()),
            (reader, u64::MAX)
        );
        assert_eq!(s.append(&id("one"), b"a"), Err(StoreError::Failed));
        assert_eq!(s.append(&id("three"), b"c"), Err(StoreError::Failed));
        assert_eq!(s.prune_through(1), Err(StoreError::Failed));
        assert!(matches!(
            s.compare_append(&before, &id("three"), b"c", 0, 0),
            Err(SourcePublicationError::Incompatible(_))
        ));
        let tail = TailRequest {
            scope: scope(),
            generation: 1,
            max_records: 10,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        };
        let older = OlderRequest {
            scope: scope(),
            generation: 1,
            before: 3,
            max_records: 10,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        };
        if reader == 0 {
            assert_eq!(s.page(&page()).unwrap().records.len(), 2);
            assert_eq!(s.tail(&tail).unwrap().records.len(), 2);
            assert_eq!(s.older(&older).unwrap().records.len(), 2);
        } else {
            assert_eq!(
                s.page(&page()),
                Err(SourceError::Incompatible(CapabilityError::MissingReader(1)))
            );
            assert_eq!(
                s.tail(&tail),
                Err(HistorySourceError::Incompatible(
                    CapabilityError::MissingReader(1)
                ))
            );
            assert_eq!(
                s.older(&older),
                Err(HistorySourceError::Incompatible(
                    CapabilityError::MissingReader(1)
                ))
            );
            assert_eq!(s.payload_bytes(), 0);
            assert_eq!(s.history_payload_bytes(), 0);
        }
    }
}
#[test]
fn reader_refusal_preserves_reopened_cache_and_writer_only_reads_continue() {
    let d = Folder::new();
    let p = d.path("source.db");
    let cache = d.path("cache.db");
    let mut s = source(&p);
    s.append(&id("one"), b"value").unwrap();
    future(&p, 1, 0, u64::MAX);
    let mut store = SqliteReplicaStore::open(&cache).unwrap();
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut pass = begin_pass_with_capabilities(
        &scope(),
        CapabilitySupport::current(),
        &mut a,
        &mut s,
        &mut store,
    )
    .unwrap();
    finish_pass(
        &mut pass,
        Limits::new(10, 100, 100).unwrap(),
        &mut a,
        &mut s,
        &mut store,
    )
    .unwrap();
    let saved = store.checkpoint(&scope()).unwrap();
    future(&p, 1, 1, u64::MAX);
    assert_eq!(
        begin_pass_with_capabilities(
            &scope(),
            CapabilitySupport::current(),
            &mut a,
            &mut s,
            &mut store
        ),
        Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
    );
    drop(store);
    let store = SqliteReplicaStore::open(cache).unwrap();
    assert_eq!(store.checkpoint(&scope()).unwrap(), saved);
    assert_eq!(store.count(&scope()).unwrap(), 1);
}
#[test]
fn partial_future_and_malformed_storage_are_refused_instead_of_defaulted() {
    let d = Folder::new();
    let p = d.path("source.db");
    legacy(&p);
    let db = Connection::open(&p).unwrap();
    db.execute_batch("ALTER TABLE source_meta ADD COLUMN metadata_version INTEGER DEFAULT 1;")
        .unwrap();
    assert!(SqliteReferenceSource::open(
        &p,
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema")
    )
    .is_err());
    let p = d.path("future.db");
    drop(source(&p));
    let db = Connection::open(&p).unwrap();
    db.pragma_update(None, "user_version", 3).unwrap();
    assert!(matches!(
        SqliteReferenceSource::open(
            &p,
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema")
        ),
        Err(SqliteOpenError::UnexpectedDatabase)
    ));
    let p = d.path("malformed.db");
    drop(source(&p));
    let db = Connection::open(&p).unwrap();
    db.execute_batch(
        "PRAGMA ignore_check_constraints=ON; UPDATE source_meta SET reader_required=X'00';",
    )
    .unwrap();
    assert!(SqliteReferenceSource::open(
        &p,
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema")
    )
    .is_err());
}
