#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::application::{RecordHeadSource, SourceError};
use nessa_sync::replication::domain::{Id, Scope, SourceHead};
use nessa_sync::replication::infrastructure::{
    EpochHeadError, OriginClaimError, SqliteEpochHeadSource, SqliteOriginAuthority,
    SqliteReferenceSource,
};
use nessa_sync::replication::origin_epoch::{OriginAuthorityKey, OriginEpoch};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nessa-epoch-src-{}-{nanos}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(stream: &str) -> Scope {
    Scope::new(
        id("device"),
        id("origin"),
        id(stream),
        id("first"),
        id("schema"),
        id("grant"),
    )
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(path, id("origin"), id("stream"), id("first"), id("schema"))
        .unwrap()
}
fn binding() -> OriginAuthorityKey {
    OriginAuthorityKey::new(id("trusted-origin"))
}

#[test]
fn old_producer_refuses_fresh_head_after_new_claim_and_handed_result_keeps_stamp() {
    let f = Fixture::new();
    let auth_file = f.file("authority");
    let source_file = f.file("source");
    let mut ordinary = source(&source_file);
    assert_eq!(ordinary.append(&id("first"), b"transcript"), Ok(1));
    let scope = scope("stream");
    let mut first = SqliteEpochHeadSource::claim(
        source(&source_file),
        SqliteOriginAuthority::provision(&auth_file).unwrap(),
        binding(),
        None,
    )
    .unwrap();
    assert_eq!(first.epoch(), OriginEpoch::FIRST);
    let first_capture = first.capture_head(&scope).unwrap();
    let handed_before_handoff = first.capture_head(&scope).unwrap();
    let (key, epoch, head) = first_capture.into_parts();
    assert_eq!(key, binding());
    assert_eq!(epoch, OriginEpoch::FIRST);
    assert_eq!(head, ordinary.source_head(&scope).unwrap());

    let mut second = SqliteEpochHeadSource::claim(
        source(&source_file),
        SqliteOriginAuthority::open(&auth_file).unwrap(),
        binding(),
        Some(first.epoch()),
    )
    .unwrap();
    let next = OriginEpoch::new(2).unwrap();
    assert!(matches!(
        first.capture_head(&scope),
        Err(EpochHeadError::Authority(OriginClaimError::Stale { actual: Some(epoch) }))
        if epoch == next
    ));
    assert_eq!(ordinary.append(&id("second"), b"next"), Ok(2));
    let (key, epoch, head) = second.capture_head(&scope).unwrap().into_parts();
    assert_eq!(key, binding());
    assert_eq!(epoch, next);
    assert_eq!(head, SourceHead::new(scope.clone(), 2, 0, 0));
    assert_eq!(head, ordinary.source_head(&scope).unwrap());

    let (_, epoch, head) = handed_before_handoff.into_parts();
    assert_eq!(epoch, OriginEpoch::FIRST);
    assert_eq!(head.position(), 1);
}

#[test]
fn stale_claim_makes_no_producer_and_scope_mismatch_has_no_claim_effect() {
    let f = Fixture::new();
    let auth_file = f.file("authority");
    let src_file = f.file("source");
    let mut one = SqliteOriginAuthority::provision(&auth_file).unwrap();
    let first = one.compare_and_claim(&binding(), None).unwrap();
    assert!(matches!(
        SqliteEpochHeadSource::claim(source(&src_file), SqliteOriginAuthority::open(&auth_file).unwrap(), binding(), None),
        Err(OriginClaimError::Stale { actual: Some(saved) }) if saved == first
    ));
    let mut producer = SqliteEpochHeadSource::claim(
        source(&src_file),
        SqliteOriginAuthority::open(&auth_file).unwrap(),
        binding(),
        Some(first),
    )
    .unwrap();
    assert!(matches!(
        producer.capture_head(&scope("different-stream")),
        Err(EpochHeadError::Source(SourceError::IdentityChanged))
    ));
    assert_eq!(one.current(&binding()), Ok(Some(producer.epoch())));
    let (_, _, head) = producer
        .capture_head(&scope("stream"))
        .unwrap()
        .into_parts();
    assert_eq!(head.position(), 0);
    assert_eq!(head.scope(), &scope("stream"));
}
