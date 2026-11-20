#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::domain::{Checkpoint, CommitPlan, Id, Limits, Scope, SourceHead};
use nessa_sync::replication::history::{
    HistorySourceError, HistoryStoreError, HistoryValidationError, OlderPlan, OlderRequest,
    TailPlan, TailRequest,
};
use nessa_sync::replication::infrastructure::{
    ClaimedNativeHead, ClaimedNativeOlderPlan, ClaimedNativePlan, ClaimedNativeTailPlan,
    EpochBinding, EpochHistoryError, EpochReplicaError, EpochSourceError, EpochTailError,
    OriginClaimError, SqliteEpochRecordSource, SqliteEpochReplicaStore, SqliteOriginAuthority,
    SqliteReferenceSource,
};
use nessa_sync::replication::origin_epoch::{
    AdmittedEpochRead, NativeEpochAdmission, NativeEpochHistoryAdmission, NativeEpochTailAdmission,
    OriginAuthorityKey, OriginEpoch,
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
        let path =
            std::env::temp_dir().join(format!("nessa-epoch-tail-{}-{t}-{tag}", std::process::id()));
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
fn key(value: &str) -> OriginAuthorityKey {
    OriginAuthorityKey::new(id(value))
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
fn request(scope: &Scope, generation: u64) -> TailRequest {
    TailRequest {
        scope: scope.clone(),
        generation,
        max_records: 2,
        max_payload_bytes: 4,
        max_record_bytes: 2,
    }
}

// This fixture's trusted operation selector consumes only opaque receipts
// from its local claim producer; it accepts no transport-supplied stamps.
#[derive(Clone)]
struct Admission {
    key: OriginAuthorityKey,
    scope: Scope,
    tail: Arc<Mutex<Option<TailRequest>>>,
    older: Arc<Mutex<Option<OlderRequest>>>,
}

impl Admission {
    fn select_older(&self, request: OlderRequest) {
        *self.older.lock().unwrap() = Some(request);
    }

    fn select(&self, request: TailRequest) {
        *self.tail.lock().unwrap() = Some(request);
    }
}
impl NativeEpochAdmission for Admission {
    type HeadEvidence = ClaimedNativeHead;
    type PlanEvidence = ClaimedNativePlan;

    fn admit_head(&mut self, evidence: ClaimedNativeHead) -> Option<AdmittedEpochRead<SourceHead>> {
        let (key, epoch, head) = evidence.into_parts();
        (key == self.key && head.scope() == &self.scope)
            .then(|| AdmittedEpochRead::new(key, self.scope.clone(), epoch, head))
    }
    fn admit_plan(&mut self, _: ClaimedNativePlan) -> Option<AdmittedEpochRead<CommitPlan>> {
        None
    }
}
impl NativeEpochTailAdmission for Admission {
    type TailEvidence = Option<ClaimedNativeTailPlan>;

    fn admit_tail(&mut self, evidence: Self::TailEvidence) -> Option<AdmittedEpochRead<TailPlan>> {
        let (key, epoch, plan) = evidence?.into_parts();
        let request = &plan.snapshot().request;
        let allowed = key == self.key
            && request.scope == self.scope
            && self.tail.lock().unwrap().as_ref() == Some(request);
        allowed.then(|| AdmittedEpochRead::new(key, self.scope.clone(), epoch, plan))
    }
}

impl NativeEpochHistoryAdmission for Admission {
    type OlderEvidence = Option<ClaimedNativeOlderPlan>;

    fn admit_older(
        &mut self,
        evidence: Self::OlderEvidence,
    ) -> Option<AdmittedEpochRead<OlderPlan>> {
        let (key, epoch, plan) = evidence?.into_parts();
        let request = &plan.page().request;
        let allowed = key == self.key
            && request.scope == self.scope
            && self.older.lock().unwrap().as_ref() == Some(request);
        allowed.then(|| AdmittedEpochRead::new(key, self.scope.clone(), epoch, plan))
    }
}

fn older(scope: &Scope, generation: u64, before: u64) -> OlderRequest {
    OlderRequest {
        scope: scope.clone(),
        generation,
        before,
        max_records: 1,
        max_payload_bytes: 2,
        max_record_bytes: 2,
    }
}

#[test]
fn qualified_tail_handoff_preserves_saved_generation_until_authorized_install() {
    let files = Files::new();
    let source_file = files.path("source");
    let authority_file = files.path("authority");
    let receiver_file = files.path("receiver");
    let scope = scope();
    let key = key("trusted-origin");
    let mut writer = source(&source_file);
    writer.append(&id("one"), b"a").unwrap();
    writer.append(&id("two"), b"b").unwrap();
    let mut first = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::provision(&authority_file).unwrap(),
        key.clone(),
        None,
    )
    .unwrap();
    let first_epoch = first.epoch();
    let mut alias_producer = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::open(&authority_file).unwrap(),
        OriginAuthorityKey::new(id("unbound")),
        None,
    )
    .unwrap();
    let binding = EpochBinding::new(
        scope.receiver().clone(),
        scope.origin().clone(),
        key.clone(),
    );
    let admission = Admission {
        key: key.clone(),
        scope: scope.clone(),
        tail: Arc::new(Mutex::new(None)),
        older: Arc::new(Mutex::new(None)),
    };
    let mut receiver =
        SqliteEpochReplicaStore::provision(&receiver_file, binding.clone(), admission.clone())
            .unwrap();
    let initial = request(&scope, 1);
    let later = request(&scope, 2);
    let limits = Limits::new(2, 4, 2).unwrap();
    admission.select(initial.clone());
    assert_eq!(receiver.history_progress(&scope), Ok(None));
    assert_eq!(
        receiver.observe_head(first.capture_head(&scope).unwrap()),
        Ok(OriginEpoch::FIRST)
    );
    assert_eq!(receiver.history_progress(&scope), Ok(None));
    let initial_plan = first
        .capture_tail(&initial, limits)
        .unwrap()
        .validate(&initial, limits)
        .unwrap();
    assert_eq!(initial_plan.request(), &initial);
    let saved = receiver.install_tail(Some(initial_plan)).unwrap();
    assert_eq!(
        (saved.live_head, saved.generation, saved.lower_bound),
        (2, 1, 1)
    );
    let old_duplicate = first.capture_tail(&initial, limits).unwrap();
    writer.append(&id("three"), b"c").unwrap();
    let delayed = first.capture_tail(&later, limits).unwrap();

    let mut second = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::open(&authority_file).unwrap(),
        key,
        Some(first_epoch),
    )
    .unwrap();
    let next_epoch = second.epoch();
    assert!(matches!(
        first.capture_tail(&later, limits),
        Err(EpochSourceError::Authority(OriginClaimError::Stale { actual: Some(epoch) }))
        if epoch == next_epoch
    ));
    writer.append(&id("four"), b"d").unwrap();
    admission.select(later.clone());
    assert_eq!(
        receiver.install_tail(None),
        Err(EpochTailError::Epoch(EpochReplicaError::Unadmitted))
    );
    let wrong_key = alias_producer
        .capture_tail(&later, limits)
        .unwrap()
        .validate(&later, limits)
        .unwrap();
    assert_eq!(
        receiver.install_tail(Some(wrong_key)),
        Err(EpochTailError::Epoch(EpochReplicaError::Unadmitted))
    );
    let not_seen = second
        .capture_tail(&later, limits)
        .unwrap()
        .validate(&later, limits)
        .unwrap();
    assert_eq!(
        receiver.install_tail(Some(not_seen)),
        Err(EpochTailError::Epoch(EpochReplicaError::Unobserved))
    );
    assert_eq!(
        receiver.observe_head(second.capture_head(&scope).unwrap()),
        Ok(next_epoch)
    );
    let stale = delayed.validate(&later, limits).unwrap();
    assert_eq!(
        receiver.install_tail(Some(stale)),
        Err(EpochTailError::Epoch(EpochReplicaError::Fenced))
    );
    assert_eq!(receiver.history_progress(&scope), Ok(Some(saved.clone())));
    let preserved = receiver.read_after(&scope, 0, 2, 4).unwrap();
    assert_eq!(
        preserved
            .iter()
            .map(|r| r.payload.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice(), b"b".as_slice()]
    );
    drop(receiver);

    let mut receiver =
        SqliteEpochReplicaStore::open(&receiver_file, binding.clone(), admission.clone()).unwrap();
    assert_eq!(receiver.observed_epoch(), Ok(Some(next_epoch)));
    assert_eq!(
        receiver.checkpoint(&scope),
        Ok(Some(Checkpoint::new(scope.clone(), 2)))
    );
    admission.select(initial.clone());
    let old_repeat = old_duplicate.validate(&initial, limits).unwrap();
    assert_eq!(
        receiver.install_tail(Some(old_repeat)),
        Err(EpochTailError::Epoch(EpochReplicaError::Fenced))
    );
    let wrong_request = second.capture_tail(&later, limits).unwrap();
    assert!(matches!(
        wrong_request.validate(&initial, limits),
        Err(HistoryValidationError::WrongRequest)
    ));
    let valid = second
        .capture_tail(&later, limits)
        .unwrap()
        .validate(&later, limits)
        .unwrap();
    assert_eq!(
        receiver.install_tail(Some(valid)),
        Err(EpochTailError::Epoch(EpochReplicaError::Unadmitted))
    );
    assert_eq!(receiver.history_progress(&scope), Ok(Some(saved)));

    admission.select(later.clone());
    let valid = second
        .capture_tail(&later, limits)
        .unwrap()
        .validate(&later, limits)
        .unwrap();
    let new = receiver.install_tail(Some(valid)).unwrap();
    assert_eq!((new.live_head, new.generation, new.lower_bound), (4, 2, 3));
    let repeat = second
        .capture_tail(&later, limits)
        .unwrap()
        .validate(&later, limits)
        .unwrap();
    assert_eq!(
        receiver.install_tail(Some(repeat)),
        Err(EpochTailError::History(HistoryStoreError::Stale))
    );
    drop(receiver);

    let mut receiver = SqliteEpochReplicaStore::open(&receiver_file, binding, admission).unwrap();
    assert_eq!(receiver.history_progress(&scope), Ok(Some(new)));
    assert_eq!(
        receiver.checkpoint(&scope),
        Ok(Some(Checkpoint::new(scope.clone(), 4)))
    );
    let recent = receiver.read_after(&scope, 2, 2, 4).unwrap();
    assert_eq!(
        recent
            .iter()
            .map(|r| r.payload.as_slice())
            .collect::<Vec<_>>(),
        [b"c".as_slice(), b"d".as_slice()]
    );
}

#[test]
fn native_tail_preflights_limits_and_rechecks_publication_in_its_snapshot() {
    let files = Files::new();
    let source_file = files.path("source");
    let scope = scope();
    let mut writer = source(&source_file);
    writer.append(&id("one"), b"ab").unwrap();
    let mut producer = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::provision(files.path("authority")).unwrap(),
        key("trusted-origin"),
        None,
    )
    .unwrap();
    let limits = Limits::new(2, 4, 2).unwrap();
    let (_, _, before) = producer.capture_head(&scope).unwrap().into_parts();
    assert_eq!(before.reader_required(), 0);
    assert!(matches!(
        producer.capture_tail(&request(&scope, 0), limits),
        Err(EpochSourceError::TailRequest(
            HistoryValidationError::InvalidRequest
        ))
    ));
    let mut oversized = request(&scope, 1);
    oversized.max_records = 3;
    assert!(matches!(
        producer.capture_tail(&oversized, limits),
        Err(EpochSourceError::TailRequest(
            HistoryValidationError::InvalidRequest
        ))
    ));

    // Future native publication fixture, not a current writable capability.
    assert!(matches!(
        producer.capture_older(&older(&scope, 1, 1), limits),
        Err(EpochSourceError::OlderRequest(
            HistoryValidationError::InvalidRequest
        ))
    ));
    let db = rusqlite::Connection::open(&source_file).unwrap();
    db.execute(
        "UPDATE source_meta SET reader_required=?1 WHERE singleton=1",
        [1u64.to_be_bytes()],
    )
    .unwrap();
    assert!(matches!(
        producer.capture_tail(&request(&scope, 1), limits),
        Err(EpochSourceError::History(HistorySourceError::Incompatible(
            _
        )))
    ));
    assert!(matches!(
        producer.capture_older(&older(&scope, 1, 2), limits),
        Err(EpochSourceError::History(HistorySourceError::Incompatible(
            _
        )))
    ));
}

#[test]
fn guarded_older_pages_extend_only_matching_history_after_epoch_handoff() {
    let files = Files::new();
    let source_file = files.path("older-source");
    let authority_file = files.path("older-authority");
    let receiver_file = files.path("older-receiver");
    let scope = scope();
    let key = key("trusted-origin");
    let mut ordinary = source(&source_file);
    for (name, payload) in [
        ("one", b"a"),
        ("two", b"b"),
        ("three", b"c"),
        ("four", b"d"),
    ] {
        ordinary.append(&id(name), payload).unwrap();
    }
    let mut first = SqliteEpochRecordSource::claim(
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
    let admission = Admission {
        key: key.clone(),
        scope: scope.clone(),
        tail: Arc::new(Mutex::new(None)),
        older: Arc::new(Mutex::new(None)),
    };
    let limits = Limits::new(1, 2, 2).unwrap();
    let mut receiver =
        SqliteEpochReplicaStore::provision(&receiver_file, binding.clone(), admission.clone())
            .unwrap();
    let mut start = request(&scope, 1);
    start.max_records = 1;
    start.max_payload_bytes = 2;
    admission.select(start.clone());
    receiver
        .observe_head(first.capture_head(&scope).unwrap())
        .unwrap();
    let tail = first
        .capture_tail(&start, limits)
        .unwrap()
        .validate(&start, limits)
        .unwrap();
    let initial = receiver.install_tail(Some(tail)).unwrap();
    assert_eq!(
        (initial.live_head, initial.generation, initial.lower_bound),
        (4, 1, 4)
    );
    let range = older(&scope, 1, 4);
    admission.select_older(range.clone());
    let delayed = first.capture_older(&range, limits).unwrap();
    let old_repeat = first.capture_older(&range, limits).unwrap();

    let mut second = SqliteEpochRecordSource::claim(
        source(&source_file),
        SqliteOriginAuthority::open(&authority_file).unwrap(),
        key,
        Some(first.epoch()),
    )
    .unwrap();
    assert!(matches!(
        first.capture_older(&range, limits),
        Err(EpochSourceError::Authority(OriginClaimError::Stale { actual: Some(epoch) }))
        if epoch == second.epoch()
    ));
    let future = second
        .capture_older(&range, limits)
        .unwrap()
        .validate(&range, limits)
        .unwrap();
    assert_eq!(
        receiver.install_older(Some(future)),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Unobserved))
    );
    receiver
        .observe_head(second.capture_head(&scope).unwrap())
        .unwrap();
    let delayed = delayed.validate(&range, limits).unwrap();
    assert_eq!(
        receiver.install_older(Some(delayed)),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
    );
    assert_eq!(receiver.history_progress(&scope), Ok(Some(initial.clone())));
    assert_eq!(
        receiver.read_after(&scope, 3, 1, 2).unwrap()[0].payload,
        b"d"
    );
    drop(receiver);

    let mut receiver =
        SqliteEpochReplicaStore::open(&receiver_file, binding.clone(), admission.clone()).unwrap();
    let different = older(&scope, 1, 3);
    assert!(matches!(
        second
            .capture_older(&range, limits)
            .unwrap()
            .validate(&different, limits),
        Err(HistoryValidationError::WrongRequest)
    ));
    admission.select_older(different.clone());
    let wrong_operation = second
        .capture_older(&range, limits)
        .unwrap()
        .validate(&range, limits)
        .unwrap();
    assert_eq!(
        receiver.install_older(Some(wrong_operation)),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Unadmitted))
    );
    assert_eq!(receiver.history_progress(&scope), Ok(Some(initial)));
    admission.select_older(range.clone());
    let qualified = second
        .capture_older(&range, limits)
        .unwrap()
        .validate(&range, limits)
        .unwrap();
    assert_eq!(qualified.request(), &range);
    let extended = receiver.install_older(Some(qualified)).unwrap();
    assert_eq!(
        (
            extended.live_head,
            extended.generation,
            extended.lower_bound
        ),
        (4, 1, 3)
    );

    let old_repeat = old_repeat.validate(&range, limits).unwrap();
    assert_eq!(
        receiver.install_older(Some(old_repeat)),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
    );
    let repeat = second
        .capture_older(&range, limits)
        .unwrap()
        .validate(&range, limits)
        .unwrap();
    assert_eq!(receiver.install_older(Some(repeat)), Ok(extended));
    let stale_generation = older(&scope, 2, 3);
    admission.select_older(stale_generation.clone());
    let stale_generation = second
        .capture_older(&stale_generation, limits)
        .unwrap()
        .validate(&stale_generation, limits)
        .unwrap();
    assert_eq!(
        receiver.install_older(Some(stale_generation)),
        Err(EpochHistoryError::History(HistoryStoreError::Stale))
    );

    admission.select_older(different.clone());
    let next = second
        .capture_older(&different, limits)
        .unwrap()
        .validate(&different, limits)
        .unwrap();
    let final_progress = receiver.install_older(Some(next)).unwrap();
    assert_eq!(
        (
            final_progress.live_head,
            final_progress.generation,
            final_progress.lower_bound
        ),
        (4, 1, 2)
    );
    drop(receiver);
    let mut reopened = SqliteEpochReplicaStore::open(&receiver_file, binding, admission).unwrap();
    assert_eq!(reopened.history_progress(&scope), Ok(Some(final_progress)));
    assert_eq!(
        reopened.checkpoint(&scope),
        Ok(Some(Checkpoint::new(scope.clone(), 4)))
    );
    let payloads = reopened
        .read_after(&scope, 1, 3, 6)
        .unwrap()
        .into_iter()
        .map(|r| r.payload)
        .collect::<Vec<_>>();
    assert_eq!(payloads, [b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
}
