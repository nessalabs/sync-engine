#![cfg(feature = "sqlite")]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::domain::Id;
use nessa_sync::replication::infrastructure::{
    OriginClaimError, OriginProvisionError, SqliteOriginAuthority,
};
use nessa_sync::replication::origin_epoch::{OriginAuthorityKey, OriginEpoch};
use rusqlite::Connection;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let count = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nessa-origin-authority-{}-{time}-{count}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn file(&self) -> PathBuf {
        self.0.join("authority.sqlite")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn key(value: &str) -> OriginAuthorityKey {
    OriginAuthorityKey::new(Id::new(value).unwrap())
}

#[test]
fn reopen_never_bootstraps_missing_or_unrecognized_authority() {
    let fixture = Fixture::new();
    let file = fixture.file();
    assert!(SqliteOriginAuthority::open(&file).is_err());
    assert!(!file.exists());
    fs::write(&file, []).unwrap();
    assert!(SqliteOriginAuthority::open(&file).is_err());
    match SqliteOriginAuthority::provision(&file) {
        Err(OriginProvisionError::Create(error)) => {
            assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists)
        }
        _ => panic!("did not refuse an existing empty file"),
    }
    assert_eq!(fs::metadata(file).unwrap().len(), 0);
}

#[test]
fn independent_handles_serialize_competing_initial_claims_and_reopen() {
    let fixture = Fixture::new();
    let file = fixture.file();
    let writer_a = SqliteOriginAuthority::provision(&file).unwrap();
    let writer_b = SqliteOriginAuthority::open(&file).unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let race = |mut writer: SqliteOriginAuthority| {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let key = key("origin");
            let expected = writer.current(&key);
            barrier.wait();
            let expected = expected.unwrap();
            assert_eq!(expected, None);
            writer.compare_and_claim(&key, expected)
        })
    };
    let (a, b) = (race(writer_a), race(writer_b));
    let results = [a.join().unwrap(), b.join().unwrap()];
    let first = OriginEpoch::FIRST;
    assert_eq!(results.iter().filter(|r| **r == Ok(first)).count(), 1);
    let stale = Err(OriginClaimError::Stale {
        actual: Some(first),
    });
    assert_eq!(results.iter().filter(|r| **r == stale).count(), 1);

    let mut reopened = SqliteOriginAuthority::open(&file).unwrap();
    let origin = key("origin");
    let other = key("independent-origin");
    assert_eq!(reopened.current(&origin), Ok(Some(first)));
    assert_eq!(reopened.compare_and_claim(&origin, None), stale);
    assert_eq!(
        reopened.compare_and_claim(&origin, Some(first)).ok(),
        OriginEpoch::new(2)
    );
    assert_eq!(reopened.current(&other), Ok(None));
    assert_eq!(reopened.compare_and_claim(&other, None), Ok(first));
    drop(reopened);
    let reopened = SqliteOriginAuthority::open(&file).unwrap();
    assert_eq!(reopened.current(&origin), Ok(OriginEpoch::new(2)));
    assert_eq!(reopened.current(&other), Ok(Some(first)));
}

#[test]
fn the_reference_adapter_refuses_epoch_exhaustion_without_writing() {
    let fixture = Fixture::new();
    let file = fixture.file();
    drop(SqliteOriginAuthority::provision(&file).unwrap());
    let max = OriginEpoch::new(u64::MAX).unwrap();
    let never_wrap = key("never-wrap");
    let conn = Connection::open(&file).unwrap();
    conn.execute(
        "INSERT INTO origin_epochs (authority, epoch) VALUES (?1, ?2)",
        (never_wrap.id().as_str(), max.get().to_be_bytes()),
    )
    .unwrap();
    drop(conn);
    let mut authority = SqliteOriginAuthority::open(&file).unwrap();
    assert_eq!(
        authority.compare_and_claim(&never_wrap, Some(max)),
        Err(OriginClaimError::Exhausted)
    );
    drop(authority);
    let authority = SqliteOriginAuthority::open(&file).unwrap();
    assert_eq!(authority.current(&never_wrap), Ok(Some(max)));
}
