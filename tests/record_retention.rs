#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::application::{RecordSource, SourceError, StoreError};
use nessa_sync::replication::domain::{Checkpoint, Id, PageRequest, RetentionPosition, Scope};
use nessa_sync::replication::infrastructure::{
    RecordRetentionError as Error, SqliteReferenceSource,
};
use rusqlite::Connection;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("nessa-retain-{}-{t}", std::process::id()));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> PathBuf {
        self.0.join("source.sqlite")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope(receiver: &str, epoch: &str, incarnation: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("origin"),
        id("stream"),
        id(incarnation),
        id("schema"),
        id(epoch),
    )
}
fn open(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(path, id("origin"), id("stream"), id("first"), id("schema"))
        .unwrap()
}
fn populated(path: &Path) -> SqliteReferenceSource {
    let mut src = open(path);
    for i in 1..=6 {
        assert_eq!(src.append(&id(&format!("r{i}")), &[i]), Ok(u64::from(i)));
    }
    src
}
fn page(scope: &Scope, after: u64) -> PageRequest {
    PageRequest {
        scope: scope.clone(),
        after,
        target: 6,
        max_records: 8,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    }
}
fn pos(n: u64) -> RetentionPosition {
    RetentionPosition::new(n)
}

#[test]
fn floor_obeys_pins_and_explicit_hard_pressure_across_reopen() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let mut src = populated(&path);
    let phone = scope("phone", "current", "first");
    let backup = scope("backup", "current", "first");
    let phone_id = id("phone-holder");
    let phone_pin = src
        .register_retention_pin(&phone_id, &Checkpoint::new(phone.clone(), 2), 10, 20)
        .unwrap();
    src.register_retention_pin(
        &id("backup-holder"),
        &Checkpoint::new(backup.clone(), 3),
        10,
        30,
    )
    .unwrap();
    assert_eq!(src.prune_through(5), Err(StoreError::Failed));
    assert_eq!(
        src.prune_with_retention(&phone, pos(5), pos(5), None, 10),
        Err(Error::HeadChanged)
    );
    assert_eq!(
        src.prune_with_retention(&phone, pos(6), pos(5), None, 11),
        Ok(pos(2))
    );
    assert_eq!(src.page(&page(&phone, 1)), Err(SourceError::Pruned));
    assert_eq!(src.page(&page(&phone, 2)).unwrap().records[0].position, 3);
    drop(src);

    let mut src = open(&path);
    assert_eq!(
        src.retention_pin_receipt(&phone_id, &phone).unwrap(),
        Some(phone_pin)
    );
    assert_eq!(
        src.prune_with_retention(&phone, pos(6), pos(5), Some(pos(4)), 12),
        Ok(pos(4))
    );
    assert_eq!(src.page(&page(&phone, 2)), Err(SourceError::Pruned));
    assert_eq!(
        src.prune_with_retention(&phone, pos(6), pos(3), None, 13),
        Ok(pos(4))
    );
    assert_eq!(
        src.prune_with_retention(&phone, pos(6), pos(5), None, 30),
        Ok(pos(5))
    );
    assert_eq!(src.prune_through(6), Err(StoreError::Failed));
    drop(src);

    let mut src = open(&path);
    assert_eq!(src.page(&page(&phone, 4)), Err(SourceError::Pruned));
    assert_eq!(src.page(&page(&phone, 5)).unwrap().records[0].position, 6);
    assert_eq!(src.append(&id("r1"), &[1]), Ok(1));
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM source_records", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        6
    );
}

#[test]
fn expiry_cannot_resurrect_an_old_receipt_after_restart_or_new_registration() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let mut source = populated(&path);
    let scope = scope("phone", "current", "first");
    let holder = id("phone-holder");
    let checkpoint = Checkpoint::new(scope.clone(), 2);
    let first = source
        .register_retention_pin(&holder, &checkpoint, 10, 20)
        .unwrap();
    assert_eq!(
        source.register_retention_pin(&holder, &checkpoint, 11, 25),
        Err(Error::AlreadyRegistered)
    );
    assert_eq!(
        source.refresh_retention_pin(&first, &checkpoint, 20, 30),
        Err(Error::Expired)
    );
    drop(source);

    let mut source = open(&path);
    assert_eq!(
        source.refresh_retention_pin(&first, &checkpoint, 19, 30),
        Err(Error::ClockRegressed)
    );
    let replacement = source
        .register_retention_pin(&holder, &checkpoint, 20, 30)
        .unwrap();
    assert_ne!(replacement, first);
    assert_eq!(
        source.refresh_retention_pin(&first, &checkpoint, 20, 30),
        Err(Error::StaleReceipt)
    );
    assert_eq!(
        source.refresh_retention_pin(&replacement, &Checkpoint::new(scope, 1), 21, 30),
        Err(Error::PositionRegressed)
    );
}

#[test]
fn refresh_uses_exact_scope_and_only_one_competing_handle_can_apply() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let mut first = populated(&path);
    let second = open(&path);
    let active = scope("phone", "current", "first");
    let holder = id("holder");
    let checkpoint = Checkpoint::new(active.clone(), 2);
    let receipt = first
        .register_retention_pin(&holder, &checkpoint, 10, 20)
        .unwrap();
    for mismatched in [
        scope("phone", "old", "first"),
        scope("tablet", "current", "first"),
        scope("phone", "current", "new"),
    ] {
        assert_eq!(
            first.refresh_retention_pin(&receipt, &Checkpoint::new(mismatched, 3), 11, 30),
            Err(Error::ScopeMismatch)
        );
    }
    assert_eq!(
        first.register_retention_pin(&id("future"), &Checkpoint::new(active.clone(), 7), 11, 30),
        Err(Error::PositionOutsideHistory)
    );
    assert_eq!(
        first.register_retention_pin(
            &id("wrong-source"),
            &Checkpoint::new(scope("phone", "current", "wrong"), 2),
            11,
            30
        ),
        Err(Error::ScopeMismatch)
    );

    let barrier = Arc::new(Barrier::new(2));
    let race = |mut src: SqliteReferenceSource| {
        let gate = barrier.clone();
        let receipt = receipt.clone();
        let newer = Checkpoint::new(active.clone(), 4);
        thread::spawn(move || {
            gate.wait();
            src.refresh_retention_pin(&receipt, &newer, 12, 30)
        })
    };
    let (a, b) = (race(first), race(second));
    let results = [a.join().unwrap(), b.join().unwrap()];
    let winner = results.iter().find_map(|r| r.as_ref().ok()).unwrap();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(Error::StaleReceipt))
            .count(),
        1
    );
    let mut src = open(&path);
    assert_eq!(
        src.retention_pin_receipt(&holder, &active)
            .as_ref()
            .unwrap()
            .as_ref(),
        Some(winner)
    );
    assert_eq!(
        src.prune_with_retention(&active, pos(6), pos(6), None, 13),
        Ok(pos(4))
    );
}

#[test]
fn the_version_two_migration_preserves_history_and_opens_empty_pin_state() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let mut src = populated(&path);
    src.prune_through(2).unwrap();
    drop(src);
    let db = Connection::open(&path).unwrap();
    db.execute_batch(
        "DROP TABLE source_retention_pins;
         ALTER TABLE source_history DROP COLUMN retention_now_ms;
         PRAGMA user_version=2;",
    )
    .unwrap();
    drop(db);

    let mut src = open(&path);
    let scope = scope("phone", "current", "first");
    let holder = id("holder");
    assert_eq!(src.page(&page(&scope, 1)), Err(SourceError::Pruned));
    assert_eq!(src.page(&page(&scope, 2)).unwrap().records[0].position, 3);
    assert_eq!(src.retention_pin_receipt(&holder, &scope), Ok(None));
    assert_eq!(
        src.register_retention_pin(&holder, &Checkpoint::new(scope.clone(), 1), 5, 10),
        Err(Error::PositionOutsideHistory)
    );
    src.register_retention_pin(&holder, &Checkpoint::new(scope, 2), 5, 10)
        .unwrap();
    assert_eq!(src.append(&id("r1"), &[1]), Ok(1));
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
}
