#![cfg(feature = "sqlite")]

use nessa_sync::replication::{
    catalogue::{
        apply_next_page_with_horizon, begin_or_resume_with_horizon, CatalogueHorizonHead,
        CatalogueHorizonSource, CatalogueProgress, CatalogueReadKind, CatalogueSourceError,
        CatalogueStore, ManifestRequest,
    },
    domain::{CapabilitySupport, Id, RetentionError, RetentionPosition, Scope},
    infrastructure::{
        CataloguePublicationError, CatalogueRetentionError as E, MemoryAuthorizer,
        SqliteCatalogueSource, SqliteCatalogueStore,
    },
};
use rusqlite::{types::Value, Connection};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "catalogue-retention-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(receiver: &str, incarnation: &str, grant: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("origin"),
        id("catalogue"),
        id(incarnation),
        id("schema"),
        id(grant),
    )
}
fn source(path: &Path) -> SqliteCatalogueSource {
    SqliteCatalogueSource::open(path, id("origin"), id("catalogue"), id("v1"), id("schema"))
        .unwrap()
}
fn head(s: &mut SqliteCatalogueSource, scope: &Scope) -> CatalogueHorizonHead {
    s.horizon_head(scope, CapabilitySupport::current()).unwrap()
}
fn support() -> CapabilitySupport {
    CapabilitySupport::current()
}
fn position(p: u64) -> RetentionPosition {
    RetentionPosition::new(p)
}
fn catch_up(
    scope: &Scope,
    source: &mut SqliteCatalogueSource,
    store: &mut SqliteCatalogueStore,
) -> CatalogueProgress {
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut active =
        begin_or_resume_with_horizon(scope, support(), &mut auth, source, store).unwrap();
    while let Some(pass) = active {
        active = apply_next_page_with_horizon(&pass, 1, 16, support(), &mut auth, source, store)
            .unwrap()
            .active;
    }
    store.progress(scope).unwrap().unwrap()
}
fn rows(db: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut q = db.prepare(sql).unwrap();
    let n = q.column_count();
    q.query_map([], |r| (0..n).map(|i| r.get(i)).collect())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn durable_completed_pins_expire_and_pressure_can_advance_strict_collection() {
    let tmp = Fixture::new();
    let db = tmp.0.join("source");
    let cache = tmp.0.join("cache");
    let mut s = source(&db);
    let mut receiver = SqliteCatalogueStore::open(&cache).unwrap();
    let slow = scope("phone", "v1", "old");
    let fast = scope("desktop", "v1", "active");
    let phone = id("phone-hold");
    let desktop = id("desktop-hold");
    assert_eq!(s.upsert(&id("a"), b"a"), Ok(1));
    let completed = catch_up(&slow, &mut s, &mut receiver);
    assert_eq!(completed.completed, 1);
    assert_eq!(s.delete(&id("a")), Ok(2));
    assert_eq!(s.upsert(&id("b"), b"b"), Ok(3));
    assert_eq!(s.delete(&id("b")), Ok(4));
    let fast_progress = catch_up(&fast, &mut s, &mut receiver);
    assert_eq!(fast_progress.completed, 4);
    assert_eq!(s.upsert(&id("c"), b"c"), Ok(5));
    assert_eq!(s.upsert(&id("d"), b"d"), Ok(6));
    let mut auth = MemoryAuthorizer::allowed(slow.clone());
    let active = begin_or_resume_with_horizon(&slow, support(), &mut auth, &mut s, &mut receiver)
        .unwrap()
        .unwrap();
    let active =
        apply_next_page_with_horizon(&active, 1, 16, support(), &mut auth, &mut s, &mut receiver)
            .unwrap();
    let active_pass = active.active.clone().unwrap();
    assert_eq!((active.completed, active_pass.boundary), (1, 6));
    assert!(active_pass.cursor.is_some());
    let first = s
        .register_catalogue_retention_pin(&phone, &active, 10, 19)
        .unwrap();
    let current = s
        .refresh_catalogue_retention_pin(&first, &active, 11, 20)
        .unwrap();
    assert_eq!(
        s.refresh_catalogue_retention_pin(&first, &active, 11, 21),
        Err(E::StaleReceipt)
    );
    let fast_receipt = s
        .register_catalogue_retention_pin(&desktop, &fast_progress, 11, 30)
        .unwrap();
    let mut wrong = fast_progress.clone();
    wrong.scope = scope("desktop", "v1", "new-grant");
    assert_eq!(
        s.refresh_catalogue_retention_pin(&fast_receipt, &wrong, 11, 31),
        Err(E::ScopeMismatch)
    );
    wrong.scope = scope("desktop", "other-incarnation", "active");
    assert_eq!(
        s.register_catalogue_retention_pin(&id("other"), &wrong, 11, 31),
        Err(E::ScopeMismatch)
    );
    let expected = head(&mut s, &fast);
    assert_eq!(
        s.compare_collect(&expected, 6),
        Err(CataloguePublicationError::RetentionPolicyRequired)
    );
    assert_eq!(
        s.collect_with_retention(&expected, position(7), None, 11),
        Err(E::Policy(RetentionError::PreferredAboveHead))
    );

    let retained = s
        .collect_with_retention(&expected, position(6), None, 19)
        .unwrap();
    assert_eq!((retained.head.horizon, retained.hidden_markers), (1, 0));
    drop(s);
    let mut s = source(&db);
    assert_eq!(
        s.catalogue_retention_pin_receipt(&phone, &slow),
        Ok(Some(current.clone()))
    );
    let expiry = s
        .collect_with_retention(&retained.head, position(6), None, 20)
        .unwrap();
    assert_eq!((expiry.head.horizon, expiry.hidden_markers), (4, 1));
    assert_eq!(
        s.refresh_catalogue_retention_pin(&current, &active, 20, 31),
        Err(E::Expired)
    );
    assert_eq!(
        s.refresh_catalogue_retention_pin(&current, &active, 19, 31),
        Err(E::ClockRegressed)
    );
    assert_eq!(
        s.register_catalogue_retention_pin(&phone, &active, 20, 31),
        Err(E::PositionOutsideHistory)
    );
    let source_db = Connection::open(&db).unwrap();
    assert_eq!(
        source_db
            .query_row(
                "SELECT manifest_visible FROM catalogue_source_entries WHERE entry_id='b'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let immutable_columns = "SELECT entry_id, creation, revision, deleted, payload FROM catalogue_source_entries ORDER BY entry_id";
    let identities = rows(&source_db, immutable_columns);
    let receiver_db = Connection::open(&cache).unwrap();
    let receiver_sql = "SELECT * FROM catalogue_entries ORDER BY receiver,origin,stream,entry_id";
    let saved_rows = rows(&receiver_db, receiver_sql);
    let forced = s
        .collect_with_retention(&expiry.head, position(6), Some(position(5)), 21)
        .unwrap();
    assert_eq!((forced.head.horizon, forced.hidden_markers), (5, 1));
    assert_eq!(rows(&source_db, immutable_columns), identities);
    assert_eq!(rows(&receiver_db, receiver_sql), saved_rows);
    assert_eq!(receiver.progress(&slow), Ok(Some(active.clone())));
    assert_eq!(receiver.progress(&fast), Ok(Some(fast_progress.clone())));
    let expired = s.horizon_manifest(
        &ManifestRequest {
            pass: active_pass,
            max_entries: 1,
        },
        support(),
        CatalogueReadKind::Incremental,
    );
    assert!(
        matches!(expired, Err(CatalogueSourceError::HorizonExpired { completed: 1, head }) if head.horizon == 5)
    );
    assert_eq!(
        s.refresh_catalogue_retention_pin(&fast_receipt, &fast_progress, 21, 33),
        Err(E::PositionOutsideHistory)
    );
    drop(s);
    let mut s = source(&db);
    assert_eq!(head(&mut s, &fast), forced.head);
    assert_eq!(
        s.compare_collect(&forced.head, 6),
        Err(CataloguePublicationError::RetentionPolicyRequired)
    );
    let expired = s
        .collect_with_retention(&forced.head, position(6), None, 30)
        .unwrap();
    assert_eq!(expired.head.horizon, 6);
    assert_eq!(
        s.compare_collect(&expired.head, 6),
        Err(CataloguePublicationError::RetentionPolicyRequired)
    );
}

#[test]
fn competing_registration_and_collection_have_only_serial_history_outcomes() {
    let tmp = Fixture::new();
    let db = tmp.0.join("source");
    let mut s = source(&db);
    let scope = scope("peer", "v1", "grant");
    let mut receiver = SqliteCatalogueStore::open(tmp.0.join("receiver")).unwrap();
    assert_eq!(s.upsert(&id("a"), b"live"), Ok(1));
    let completed = catch_up(&scope, &mut s, &mut receiver);
    assert_eq!(s.delete(&id("a")), Ok(2));
    assert_eq!(s.upsert(&id("b"), b"new"), Ok(3));
    let expected = head(&mut s, &scope);
    let mut collector = source(&db);
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let worker = thread::spawn(move || {
        other.wait();
        collector
            .collect_with_retention(&expected, position(3), None, 1)
            .unwrap()
    });
    barrier.wait();
    let holder = id("peer");
    let registration = s.register_catalogue_retention_pin(&holder, &completed, 1, 10);
    let collection = worker.join().unwrap();
    match registration {
        Ok(receipt) => {
            assert_eq!((collection.head.horizon, collection.hidden_markers), (1, 0));
            assert_eq!(
                s.catalogue_retention_pin_receipt(&holder, &scope),
                Ok(Some(receipt))
            );
        }
        Err(E::PositionOutsideHistory) => {
            assert_eq!((collection.head.horizon, collection.hidden_markers), (3, 1));
            assert_eq!(s.catalogue_retention_pin_receipt(&holder, &scope), Ok(None));
        }
        Err(other) => panic!("unexpected registration: {other:?}"),
    }
    assert_eq!(head(&mut s, &scope), collection.head);
    assert_eq!(receiver.progress(&scope), Ok(Some(completed)));
}

#[test]
fn v4_migration_preserves_existing_horizon_and_refuses_partial_v5() {
    let tmp = Fixture::new();
    let file = tmp.0.join("source");
    let scope = scope("peer", "v1", "grant");
    let mut s = source(&file);
    assert_eq!(s.upsert(&id("a"), b"live"), Ok(1));
    assert_eq!(s.delete(&id("a")), Ok(2));
    let expected = head(&mut s, &scope);
    let persisted = s.compare_collect(&expected, 2).unwrap().head;
    drop(s);
    let db = Connection::open(&file).unwrap();
    db.execute_batch(
        "DROP TRIGGER catalogue_retired_no_insert;
        DROP INDEX catalogue_compaction_candidates;
        DROP TABLE catalogue_retired_proof;
        DROP TABLE catalogue_retention_pins;
        ALTER TABLE catalogue_source_meta DROP COLUMN retention_now_ms;
        PRAGMA user_version=4",
    )
    .unwrap();
    let mut migrated = source(&file);
    assert_eq!(head(&mut migrated, &scope), persisted);
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        6
    );
    drop(migrated);
    db.execute_batch("DROP TABLE catalogue_retention_pins")
        .unwrap();
    assert!(SqliteCatalogueSource::open(
        &file,
        id("origin"),
        id("catalogue"),
        id("v1"),
        id("schema")
    )
    .is_err());
}

#[test]
fn v5_compaction_preserves_proof_and_rolls_back_atomically() {
    use nessa_sync::replication::catalogue::CatalogueStoreError;
    let tmp = Fixture::new();
    let file = tmp.0.join("source");
    let scope = scope("peer", "v1", "grant");
    let mut s = source(&file);
    s.upsert(&id("a"), b"a").unwrap();
    s.delete(&id("a")).unwrap();
    s.upsert(&id("b"), b"b").unwrap();
    s.delete(&id("b")).unwrap();
    let h = head(&mut s, &scope);
    s.compare_collect(&h, 4).unwrap();
    drop(s);
    let db = Connection::open(&file).unwrap();
    // Recreate the exact v5 layout with populated retained rows.
    db.execute_batch("DROP TRIGGER catalogue_retired_no_insert; DROP INDEX catalogue_compaction_candidates; DROP TABLE catalogue_retired_proof; PRAGMA user_version=5;").unwrap();
    let mut s = source(&file);
    assert_eq!(head(&mut s, &scope).horizon, 4);
    assert_eq!(s.compact_retired(0), Err(CatalogueStoreError::Failed));
    assert_eq!(s.compact_retired(257), Err(CatalogueStoreError::Failed));
    let plan: String = db.query_row("EXPLAIN QUERY PLAN SELECT entry_id FROM catalogue_source_entries INDEXED BY catalogue_compaction_candidates WHERE manifest_visible=0 AND deleted=1 AND length(payload)=0 AND revision<4 ORDER BY revision,entry_id LIMIT 1", [], |r| r.get(3)).unwrap();
    assert!(plan.contains("catalogue_compaction_candidates"), "{plan}");
    // Exercise the compactor boundary even for a pre-existing hidden equality row.
    db.execute(
        "UPDATE catalogue_source_entries SET manifest_visible=0 WHERE entry_id='b'",
        [],
    )
    .unwrap();
    db.execute_batch("CREATE TRIGGER fail_move BEFORE DELETE ON catalogue_source_entries BEGIN SELECT RAISE(ABORT, 'fault'); END;").unwrap();
    assert_eq!(s.compact_retired(1), Err(CatalogueStoreError::Failed));
    assert_eq!(
        db.query_row("SELECT count(*) FROM catalogue_retired_proof", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER fail_move").unwrap();
    assert_eq!(s.compact_retired(1), Ok(1));
    assert_eq!(s.compact_retired(1), Ok(0));
    let pass = nessa_sync::replication::catalogue::CataloguePass {
        scope: scope.clone(),
        completed: 0,
        boundary: 4,
        cursor: None,
        generation: 1,
    };
    let proof = s
        .horizon_value(&pass, &id("a"), 16, support(), CatalogueReadKind::FullReset)
        .unwrap()
        .value;
    assert_eq!(
        (
            proof.manifest.key.creation,
            proof.manifest.revision,
            proof.manifest.deleted,
            proof.payload
        ),
        (1, 2, true, vec![])
    );
    assert_eq!(
        s.horizon_value(
            &pass,
            &id("unknown"),
            16,
            support(),
            CatalogueReadKind::FullReset
        ),
        Err(CatalogueSourceError::Unavailable)
    );
    assert_eq!(
        s.upsert(&id("a"), b"resurrect"),
        Err(CatalogueStoreError::Fenced)
    );
    assert_eq!(s.delete(&id("a")), Err(CatalogueStoreError::Fenced));
    assert!(db.execute("INSERT INTO catalogue_source_entries(entry_id,creation,revision,deleted,payload) VALUES('a',5,5,0,x'')", []).is_err());
    assert_eq!(db.query_row("SELECT count(*) FROM catalogue_source_entries WHERE entry_id='b' AND revision=4 AND manifest_visible=0", [], |r| r.get::<_,i64>(0)).unwrap(), 1);
    drop(s);
    let mut s = source(&file);
    assert_eq!(
        s.upsert(&id("a"), b"again"),
        Err(CatalogueStoreError::Fenced)
    );
    assert_eq!(
        db.query_row(
            "SELECT creation,revision FROM catalogue_retired_proof WHERE entry_id='a'",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
        )
        .unwrap(),
        (1, 2)
    );
}
