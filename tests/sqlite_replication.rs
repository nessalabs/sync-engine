#![cfg(feature = "sqlite")]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use nessa_sync::replication::application::{
    begin_pass, finish_pass, RecordSource, ReplicaStore, SourceError, StoreError,
};
use nessa_sync::replication::domain::{
    validate_page, Checkpoint, CommitPlan, Id, Limits, Page, PageRequest, Record, Scope,
    MAX_ID_BYTES,
};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        loop {
            let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "nessa-sync-sqlite-test-{}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("temporary directory: {error}"),
            }
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("only this test's new temporary directory is removed");
    }
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("first"),
        id("opaque-v1"),
        id("grant-1"),
    )
}

fn open_source(path: PathBuf) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        path,
        id("origin"),
        id("stream"),
        id("first"),
        id("opaque-v1"),
    )
    .unwrap()
}

fn limits() -> Limits {
    Limits::new(2, 32, 32).unwrap()
}

fn plan(scope: &Scope, after: u64, records: &[(&str, &[u8])]) -> CommitPlan {
    let target = after + records.len() as u64;
    let request = PageRequest {
        scope: scope.clone(),
        after,
        target,
        max_records: 4,
        max_payload_bytes: 64,
        max_record_bytes: 64,
    };
    let page = Page {
        request: request.clone(),
        records: records
            .iter()
            .enumerate()
            .map(|(offset, (record_id, payload))| Record {
                position: after + offset as u64 + 1,
                id: id(record_id),
                scope: scope.clone(),
                payload: payload.to_vec(),
            })
            .collect(),
    };
    validate_page(
        &Checkpoint::new(scope.clone(), after),
        &request,
        page,
        Limits::new(4, 64, 64).unwrap(),
    )
    .unwrap()
}

#[test]
fn restart_reads_saved_transcript_without_source_and_resumes_from_checkpoint() {
    let dir = TestDirectory::new();
    let source_path = dir.path("source.db");
    let replica_path = dir.path("replica.db");
    let scope = scope();
    let mut source = open_source(source_path.clone());
    source.append(&id("one"), b"hello").unwrap();
    source.append(&id("two"), b"world").unwrap();
    let mut store = SqliteReplicaStore::open(&replica_path).unwrap();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(pass.position(), 2);
    drop(source);
    drop(store);

    let mut reopened = SqliteReplicaStore::open(&replica_path).unwrap();
    let cached = reopened.read_after(&scope, 0, 2, 32).unwrap();
    assert_eq!(
        cached
            .iter()
            .map(|record| &record.payload)
            .collect::<Vec<_>>(),
        vec![b"hello", b"world"]
    );
    assert_eq!(reopened.load(&scope).unwrap().unwrap().position(), 2);
    drop(reopened);

    let mut source = open_source(source_path);
    source.append(&id("three"), b"again").unwrap();
    let mut reopened = SqliteReplicaStore::open(&replica_path).unwrap();
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut reopened).unwrap();
    assert_eq!(pass.position(), 2);
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut reopened).unwrap();
    assert_eq!(
        (
            pass.position(),
            reopened.count(&scope).unwrap(),
            source.payload_bytes()
        ),
        (3, 3, 5)
    );
}

#[test]
fn second_insert_failure_rolls_back_first_record_and_checkpoint() {
    let dir = TestDirectory::new();
    let path = dir.path("replica.db");
    let scope = scope();
    let batch = plan(&scope, 0, &[("one", b"a"), ("two", b"b")]);
    let mut store = SqliteReplicaStore::open(&path).unwrap();
    let trigger = rusqlite::Connection::open(&path).unwrap();
    trigger.execute_batch("CREATE TRIGGER abort_second BEFORE INSERT ON replica_records WHEN NEW.position = 2 BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert_eq!(store.apply(batch.clone()), Err(StoreError::Failed));
    drop(store);
    let mut reopened = SqliteReplicaStore::open(&path).unwrap();
    assert_eq!(reopened.load(&scope).unwrap(), None);
    assert_eq!(reopened.count(&scope).unwrap(), 0);
    trigger.execute_batch("DROP TRIGGER abort_second;").unwrap();
    reopened.apply(batch).unwrap();
    assert_eq!(reopened.load(&scope).unwrap().unwrap().position(), 2);
    assert_eq!(reopened.count(&scope).unwrap(), 2);
}

#[test]
fn lost_reply_replay_and_conflicting_fact_identity_after_restart() {
    let dir = TestDirectory::new();
    let path = dir.path("replica.db");
    let scope = scope();
    let original = plan(&scope, 0, &[("one", b"original")]);
    let mut first_process = SqliteReplicaStore::open(&path).unwrap();
    let _discarded_reply = first_process.apply(original.clone());
    drop(first_process);
    let mut restarted = SqliteReplicaStore::open(&path).unwrap();
    assert_eq!(restarted.load(&scope).unwrap().unwrap().position(), 1);
    assert_eq!(restarted.count(&scope).unwrap(), 1);
    assert_eq!(restarted.apply(original), Ok(()));
    assert_eq!(restarted.count(&scope).unwrap(), 1);
    assert_eq!(
        restarted.apply(plan(&scope, 0, &[("one", b"different")])),
        Err(StoreError::ConflictingRecord)
    );
    assert_eq!(
        restarted.read_after(&scope, 0, 1, 64).unwrap()[0].payload,
        b"original"
    );
    assert_eq!(restarted.load(&scope).unwrap().unwrap().position(), 1);
    assert_eq!(
        restarted.apply(plan(&scope, 1, &[("one", b"reused later")])),
        Err(StoreError::ConflictingRecord)
    );
    assert_eq!(restarted.count(&scope).unwrap(), 1);
}

#[test]
fn two_database_connections_cannot_both_commit_different_position_one() {
    let dir = TestDirectory::new();
    let path = dir.path("replica.db");
    let scope = scope();
    let first = SqliteReplicaStore::open(&path).unwrap();
    let second = SqliteReplicaStore::open(&path).unwrap();
    let gate = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for (mut store, record_id, gate) in [
        (first, "first", gate.clone()),
        (second, "second", gate.clone()),
    ] {
        let scope = scope.clone();
        threads.push(std::thread::spawn(move || {
            let candidate = plan(&scope, 0, &[(record_id, b"payload")]);
            gate.wait();
            store.apply(candidate)
        }));
    }
    gate.wait();
    let mut outcomes = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    outcomes.sort_by_key(|outcome| outcome.is_err());
    assert_eq!(outcomes, [Ok(()), Err(StoreError::Stale)]);
    let mut final_store = SqliteReplicaStore::open(&path).unwrap();
    assert_eq!(final_store.load(&scope).unwrap().unwrap().position(), 1);
    assert_eq!(final_store.count(&scope).unwrap(), 1);
    assert_eq!(final_store.read_after(&scope, 0, 2, 32).unwrap().len(), 1);
}

#[test]
fn source_checks_single_record_size_before_reading_payload_and_uses_indexed_range() {
    let dir = TestDirectory::new();
    let path = dir.path("source.db");
    let scope = scope();
    let mut source = open_source(path.clone());
    source.append(&id("one"), b"123456").unwrap();
    let request = PageRequest {
        scope: scope.clone(),
        after: 0,
        target: 1,
        max_records: 10,
        max_payload_bytes: 1000,
        max_record_bytes: 4,
    };
    assert_eq!(source.page(&request), Err(SourceError::OversizedRecord));
    assert_eq!(source.payload_bytes(), 0);
    assert_eq!(source.append(&id("one"), b"123456"), Ok(1));
    assert_eq!(
        source.append(&id("one"), b"changed"),
        Err(StoreError::ConflictingRecord)
    );
    let db = rusqlite::Connection::open(&path).unwrap();
    let detail: String = db.query_row(
        "EXPLAIN QUERY PLAN SELECT position, octet_length(record_id) <= ?4, length(payload) FROM source_records WHERE position > ?1 AND position <= ?2 ORDER BY position LIMIT ?3",
        rusqlite::params![0, 1, 10, 2 * MAX_ID_BYTES],
        |row| row.get(3),
    ).unwrap();
    assert!(
        detail.contains("SEARCH source_records USING INTEGER PRIMARY KEY"),
        "{detail}"
    );
}

#[test]
fn missing_saved_or_source_position_is_refused_instead_of_displayed_as_complete() {
    let dir = TestDirectory::new();
    let source_path = dir.path("source.db");
    let replica_path = dir.path("replica.db");
    let scope = scope();
    let mut source = open_source(source_path.clone());
    source.append(&id("one"), b"a").unwrap();
    source.append(&id("two"), b"b").unwrap();
    let mut store = SqliteReplicaStore::open(&replica_path).unwrap();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    let db = rusqlite::Connection::open(&replica_path).unwrap();
    db.execute("DELETE FROM replica_records WHERE position = 1", [])
        .unwrap();
    assert_eq!(store.read_after(&scope, 0, 2, 32), Err(StoreError::Failed));
    assert_eq!(store.load(&scope).unwrap().unwrap().position(), 2);

    let db = rusqlite::Connection::open(&source_path).unwrap();
    db.execute("DELETE FROM source_records WHERE position = 1", [])
        .unwrap();
    let request = PageRequest {
        scope,
        after: 0,
        target: 2,
        max_records: 2,
        max_payload_bytes: 32,
        max_record_bytes: 32,
    };
    assert_eq!(source.page(&request), Err(SourceError::Pruned));
}

#[test]
fn cached_read_stops_at_the_committed_checkpoint() {
    let dir = TestDirectory::new();
    let path = dir.path("replica.db");
    let scope = scope();
    let mut store = SqliteReplicaStore::open(&path).unwrap();
    store
        .apply(plan(&scope, 0, &[("one", b"visible")]))
        .unwrap();
    // A foreign writer has inserted data without advancing the checkpoint.
    // The read API cannot treat that row as accepted transcript content.
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "INSERT INTO replica_records (receiver, origin, stream, position, record_id, payload) VALUES (?1, ?2, ?3, 2, 'uncommitted', X'78')",
        rusqlite::params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()],
    ).unwrap();
    assert_eq!(store.read_after(&scope, 0, 10, 32).unwrap().len(), 1);
    assert_eq!(store.read_after(&scope, 1, 10, 32).unwrap(), Vec::new());
    assert_eq!(store.load(&scope).unwrap().unwrap().position(), 1);
}

#[test]
fn file_kind_and_source_identity_are_not_silently_replaced() {
    let dir = TestDirectory::new();
    let path = dir.path("source.db");
    let source = open_source(path.clone());
    drop(source);
    let original_source = rusqlite::Connection::open(&path).unwrap();
    original_source
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    drop(original_source);
    assert!(matches!(
        SqliteReplicaStore::open(&path),
        Err(nessa_sync::replication::infrastructure::SqliteOpenError::UnexpectedDatabase)
    ));
    assert!(matches!(
        SqliteReferenceSource::open(
            &path,
            id("origin"),
            id("stream"),
            id("second"),
            id("opaque-v1")
        ),
        Err(nessa_sync::replication::infrastructure::SqliteOpenError::SourceIdentityMismatch)
    ));
    let unchanged_source = rusqlite::Connection::open(&path).unwrap();
    let source_mode: String = unchanged_source
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(source_mode, "delete");
    drop(unchanged_source);
    let mut original_source = open_source(path.clone());
    original_source.append(&id("saved"), b"keep").unwrap();
    drop(original_source);
    let damaged = rusqlite::Connection::open(&path).unwrap();
    damaged.execute("DELETE FROM source_meta", []).unwrap();
    assert!(matches!(
        SqliteReferenceSource::open(
            &path,
            id("origin"),
            id("stream"),
            id("first"),
            id("opaque-v1")
        ),
        Err(nessa_sync::replication::infrastructure::SqliteOpenError::UnexpectedDatabase)
    ));
    let retained_count: i64 = damaged
        .query_row("SELECT COUNT(*) FROM source_records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained_count, 1);

    let foreign_path = dir.path("foreign.db");
    let foreign = rusqlite::Connection::open(&foreign_path).unwrap();
    foreign
        .execute_batch(
            "CREATE TABLE user_data (value TEXT NOT NULL); INSERT INTO user_data VALUES ('keep');",
        )
        .unwrap();
    let before: String = foreign
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(before, "delete");
    assert!(matches!(
        SqliteReplicaStore::open(&foreign_path),
        Err(nessa_sync::replication::infrastructure::SqliteOpenError::UnexpectedDatabase)
    ));
    let after: String = foreign
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let retained: String = foreign
        .query_row("SELECT value FROM user_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!((after, retained), (before, "keep".to_string()));
}

#[test]
fn one_new_record_reads_one_indexed_range_and_scope_change_retains_progress() {
    let dir = TestDirectory::new();
    let source_path = dir.path("source.db");
    let replica_path = dir.path("replica.db");
    let scope = scope();
    let mut source = open_source(source_path.clone());
    for position in 0..32 {
        source
            .append(&id(&format!("fact-{position}")), b"x")
            .unwrap();
    }
    let mut store = SqliteReplicaStore::open(&replica_path).unwrap();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(store.count(&scope).unwrap(), 32);
    drop(store);
    drop(source);

    let mut source = open_source(source_path);
    source.append(&id("fact-32"), b"y").unwrap();
    let mut store = SqliteReplicaStore::open(&replica_path).unwrap();
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(pass.position(), 32);
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    assert_eq!((source.page_reads(), source.payload_bytes()), (1, 1));
    assert_eq!(store.count(&scope).unwrap(), 33);
    assert_eq!(
        store.read_after(&scope, 32, 2, 32).unwrap()[0].payload,
        b"y"
    );

    let changed_scope = Scope::new(
        scope.receiver().clone(),
        scope.origin().clone(),
        scope.stream().clone(),
        scope.incarnation().clone(),
        scope.schema().clone(),
        id("grant-2"),
    );
    assert_eq!(
        store.load(&changed_scope),
        Err(StoreError::ScopeMismatch {
            saved: Box::new(scope.clone()),
            requested: Box::new(changed_scope.clone()),
        })
    );
    assert_eq!(
        store.read_after(&changed_scope, 0, 1, 32),
        Err(StoreError::ScopeMismatch {
            saved: Box::new(scope),
            requested: Box::new(changed_scope),
        })
    );
}

#[path = "replication/infrastructure/request.rs"]
mod request_contract;
