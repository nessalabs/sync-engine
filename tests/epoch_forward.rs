#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::application::SourceError;
use nessa_sync::replication::domain::{
    Checkpoint, CommitPlan, Id, Limits, PageRequest, Scope, SourceHead,
};
use nessa_sync::replication::infrastructure::{
    ClaimedNativeHead, ClaimedNativePlan, EpochBinding, EpochReplicaError, EpochSourceError,
    OriginClaimError, SqliteEpochRecordSource, SqliteEpochReplicaStore, SqliteOriginAuthority,
    SqliteReferenceSource,
};
use nessa_sync::replication::origin_epoch::{
    AdmittedEpochRead, NativeEpochAdmission, OriginAuthorityKey, OriginEpoch,
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let tag = NEXT.fetch_add(1, Ordering::Relaxed);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nessa-epoch-forward-{}-{t}-{tag}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
        id("incarnation"),
        id("schema"),
        id("grant"),
    )
}
fn key() -> OriginAuthorityKey {
    OriginAuthorityKey::new(id("trusted-origin"))
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        path,
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
    )
    .unwrap()
}
fn expected(scope: &Scope, after: u64, target: u64) -> PageRequest {
    PageRequest {
        scope: scope.clone(),
        after,
        target,
        max_records: 1,
        max_payload_bytes: 2,
        max_record_bytes: 2,
    }
}

// A local composition-owned operation selector. Its evidence is a typed result
// from the producer, not network bytes or an arbitrary claimed epoch.
#[derive(Clone)]
struct FixtureAdmission {
    binding: OriginAuthorityKey,
    scope: Scope,
    page: Arc<Mutex<Option<PageRequest>>>,
}
impl FixtureAdmission {
    fn select_page(&self, page: PageRequest) {
        *self.page.lock().unwrap() = Some(page);
    }
}
impl NativeEpochAdmission for FixtureAdmission {
    type HeadEvidence = ClaimedNativeHead;
    type PlanEvidence = ClaimedNativePlan;

    fn admit_head(&mut self, evidence: ClaimedNativeHead) -> Option<AdmittedEpochRead<SourceHead>> {
        let (key, epoch, head) = evidence.into_parts();
        (key == self.binding && head.scope() == &self.scope)
            .then(|| AdmittedEpochRead::new(key, self.scope.clone(), epoch, head))
    }

    fn admit_plan(&mut self, evidence: ClaimedNativePlan) -> Option<AdmittedEpochRead<CommitPlan>> {
        let (key, epoch, request, plan) = evidence.into_parts();
        let accepted = key == self.binding
            && request.scope == self.scope
            && self.page.lock().unwrap().as_ref() == Some(&request);
        accepted.then(|| AdmittedEpochRead::new(key, request.scope, epoch, plan))
    }
}

#[test]
fn claimed_forward_results_keep_epoch_and_request_through_native_apply_and_reopen() {
    let files = Files::new();
    let source_file = files.path("source");
    let authority_file = files.path("authority");
    let receiver_file = files.path("receiver");
    let scope = scope();
    let key = key();
    let mut writer = source(&source_file);
    for (name, payload) in [("one", b"a"), ("two", b"b"), ("three", b"c")] {
        writer.append(&id(name), payload).unwrap();
    }
    let mut old = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::provision(&authority_file).unwrap(),
        key.clone(),
        None,
    )
    .unwrap();

    let binding = EpochBinding::new(
        scope.receiver().clone(),
        scope.origin().clone(),
        key.clone(),
    );
    let admission = FixtureAdmission {
        binding: key.clone(),
        scope: scope.clone(),
        page: Arc::new(Mutex::new(None)),
    };
    let mut receiver =
        SqliteEpochReplicaStore::provision(&receiver_file, binding.clone(), admission.clone())
            .unwrap();
    let limits = Limits::new(1, 2, 2).unwrap();
    let first = expected(&scope, 0, 3);
    let next = expected(&scope, 1, 3);
    let c0 = Checkpoint::new(scope.clone(), 0);
    let c1 = Checkpoint::new(scope.clone(), 1);
    let c2 = Checkpoint::new(scope.clone(), 2);
    let c3 = Checkpoint::new(scope.clone(), 3);
    assert_eq!(
        receiver.observe_head(old.capture_head(&scope).unwrap()),
        Ok(OriginEpoch::FIRST)
    );
    let initial = old
        .capture_page(&first)
        .unwrap()
        .validate(&c0, &first, limits)
        .unwrap();
    assert_eq!(initial.request(), &first);
    admission.select_page(first);
    assert_eq!(receiver.apply(initial), Ok(()));
    let captured = old.capture_page(&next).unwrap();

    let mut new = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::open(&authority_file).unwrap(),
        key,
        Some(old.epoch()),
    )
    .unwrap();
    let next_epoch = new.epoch();
    assert!(matches!(
        old.capture_page(&next),
        Err(EpochSourceError::Authority(OriginClaimError::Stale { actual: Some(epoch) }))
        if epoch == next_epoch
    ));
    assert_eq!(
        receiver.observe_head(new.capture_head(&scope).unwrap()),
        Ok(next_epoch)
    );
    admission.select_page(next.clone());
    assert_eq!(
        receiver.apply(captured.validate(&c1, &next, limits).unwrap()),
        Err(EpochReplicaError::Fenced)
    );
    assert_eq!(receiver.checkpoint(&scope), Ok(Some(c1.clone())));
    assert_eq!(receiver.read_after(&scope, 0, 3, 8).unwrap().len(), 1);
    drop(receiver);

    let mut receiver =
        SqliteEpochReplicaStore::open(&receiver_file, binding.clone(), admission.clone()).unwrap();
    assert_eq!(receiver.observed_epoch(), Ok(Some(next_epoch)));
    let mut differently_requested = next.clone();
    differently_requested.target = 2;
    assert!(new
        .capture_page(&next)
        .unwrap()
        .validate(&c1, &differently_requested, limits)
        .is_err());
    admission.select_page(differently_requested);
    let valid = new
        .capture_page(&next)
        .unwrap()
        .validate(&c1, &next, limits)
        .unwrap();
    assert_eq!(receiver.apply(valid), Err(EpochReplicaError::Unadmitted));
    assert_eq!(receiver.checkpoint(&scope), Ok(Some(c1.clone())));

    admission.select_page(next.clone());
    let valid = new
        .capture_page(&next)
        .unwrap()
        .validate(&c1, &next, limits)
        .unwrap();
    assert_eq!(receiver.apply(valid), Ok(()));
    assert_eq!(receiver.checkpoint(&scope), Ok(Some(c2.clone())));
    let final_request = expected(&scope, 2, 3);
    admission.select_page(final_request.clone());
    let final_plan = new
        .capture_page(&final_request)
        .unwrap()
        .validate(&c2, &final_request, limits)
        .unwrap();
    assert_eq!(receiver.apply(final_plan), Ok(()));
    drop(receiver);
    let mut receiver = SqliteEpochReplicaStore::open(&receiver_file, binding, admission).unwrap();
    assert_eq!(receiver.checkpoint(&scope), Ok(Some(c3)));
    let payloads = receiver
        .read_after(&scope, 0, 3, 8)
        .unwrap()
        .into_iter()
        .map(|row| row.payload)
        .collect::<Vec<_>>();
    assert_eq!(payloads, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn native_claim_does_not_skip_fresh_source_capabilities_or_page_budgets() {
    let files = Files::new();
    let source_file = files.path("source");
    let scope = scope();
    let mut writer = source(&source_file);
    writer.append(&id("one"), b"ab").unwrap();
    let mut producer = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::provision(files.path("authority")).unwrap(),
        key(),
        None,
    )
    .unwrap();
    let head = producer.capture_head(&scope).unwrap();
    let (_, _, original) = head.into_parts();
    assert_eq!(original.reader_required(), 0);
    let mut page = expected(&scope, 0, 1);
    page.max_record_bytes = 1;
    assert!(matches!(
        producer.capture_page(&page),
        Err(EpochSourceError::Source(SourceError::OversizedRecord))
    ));

    // Native test fixture for metadata published by a future reader dialect,
    // independent of the old compatible head. The current public writer does
    // not have a capability that can create this publication.
    let conn = rusqlite::Connection::open(&source_file).unwrap();
    conn.execute(
        "UPDATE source_meta SET reader_required = ?1 WHERE singleton = 1",
        [1u64.to_be_bytes()],
    )
    .unwrap();
    let page = expected(&scope, 0, 1);
    assert!(matches!(
        producer.capture_page(&page),
        Err(EpochSourceError::Source(SourceError::Incompatible(_)))
    ));
}
