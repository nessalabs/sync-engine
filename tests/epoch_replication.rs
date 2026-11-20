#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::application::{RecordHeadSource, RecordSource, StoreError};
use nessa_sync::replication::domain::{
    validate_page, Checkpoint, CommitPlan, Id, Limits, PageRequest, Scope, SourceHead,
};
use nessa_sync::replication::infrastructure::{
    EpochBinding, EpochReplicaError as Error, SqliteEpochReplicaStore, SqliteOpenError,
    SqliteOriginAuthority, SqliteReferenceSource, SqliteReplicaStore,
};
use nessa_sync::replication::origin_epoch::{
    AdmittedEpochRead, NativeEpochAdmission, OriginAuthorityKey, OriginEpoch,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nessa-epoch-{}-{t}-{n}", std::process::id()));
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
fn scope(grant: &str) -> Scope {
    Scope::new(
        id("device"),
        id("origin"),
        id("s"),
        id("initial"),
        id("schema"),
        id(grant),
    )
}
fn authority_key(name: &str) -> OriginAuthorityKey {
    OriginAuthorityKey::new(id(name))
}
fn binding(name: &str) -> EpochBinding {
    EpochBinding::new(id("device"), id("origin"), authority_key(name))
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(path, id("origin"), id("s"), id("initial"), id("schema")).unwrap()
}

// The test owns this local admission boundary. Evidence is not decoded from
// request bytes. It ties the writer claim to the exact result it returns.
struct Handed<T> {
    key: OriginAuthorityKey,
    scope: Scope,
    epoch: OriginEpoch,
    value: T,
}
#[derive(Clone)]
struct FixtureAdmission(Arc<Mutex<Scope>>);
impl FixtureAdmission {
    fn new(scope: Scope) -> Self {
        Self(Arc::new(Mutex::new(scope)))
    }
    fn select(&self, scope: Scope) {
        *self.0.lock().unwrap() = scope;
    }
    fn verify<T>(&self, h: Handed<T>) -> Option<AdmittedEpochRead<T>> {
        (h.scope == *self.0.lock().unwrap())
            .then(|| AdmittedEpochRead::new(h.key, h.scope, h.epoch, h.value))
    }
}
impl NativeEpochAdmission for FixtureAdmission {
    type HeadEvidence = Handed<SourceHead>;
    type PlanEvidence = Handed<CommitPlan>;

    fn admit_head(&mut self, e: Self::HeadEvidence) -> Option<AdmittedEpochRead<SourceHead>> {
        self.verify(e)
    }
    fn admit_plan(&mut self, e: Self::PlanEvidence) -> Option<AdmittedEpochRead<CommitPlan>> {
        self.verify(e)
    }
}

struct Writer {
    source: SqliteReferenceSource,
    key: OriginAuthorityKey,
    epoch: OriginEpoch,
}
impl Writer {
    fn head(&mut self, ledger: &SqliteOriginAuthority, s: &Scope) -> Option<Handed<SourceHead>> {
        if ledger.current(&self.key).unwrap() != Some(self.epoch) {
            return None;
        }
        Some(Handed {
            key: self.key.clone(),
            scope: s.clone(),
            epoch: self.epoch,
            value: self.source.source_head(s).unwrap(),
        })
    }
    fn plan(
        &mut self,
        ledger: &SqliteOriginAuthority,
        s: &Scope,
        after: u64,
        target: u64,
    ) -> Option<Handed<CommitPlan>> {
        if ledger.current(&self.key).unwrap() != Some(self.epoch) {
            return None;
        }
        let limits = Limits::new(8, 16, 8).unwrap();
        let request = PageRequest {
            scope: s.clone(),
            after,
            target,
            max_records: 8,
            max_payload_bytes: 16,
            max_record_bytes: 8,
        };
        let checkpoint = Checkpoint::new(s.clone(), after);
        let page = self.source.page(&request).unwrap();
        let value = validate_page(&checkpoint, &request, page, limits).unwrap();
        Some(Handed {
            key: self.key.clone(),
            scope: s.clone(),
            epoch: self.epoch,
            value,
        })
    }
}

#[test]
fn committed_floor_fences_delayed_plans_even_after_reopen() {
    let f = Fixture::new();
    let authority = f.file("authority");
    let source_file = f.file("source");
    let receiver = f.file("receiver");
    let key = authority_key("binding");
    let s = scope("grant");
    let mut claims = SqliteOriginAuthority::provision(&authority).unwrap();
    let mut new_claims = SqliteOriginAuthority::open(&authority).unwrap();
    let e1 = claims.compare_and_claim(&key, None).unwrap();
    let mut first = Writer {
        source: source(&source_file),
        key: key.clone(),
        epoch: e1,
    };
    assert_eq!(first.source.append(&id("one"), b"a"), Ok(1));
    assert_eq!(first.source.append(&id("two"), b"b"), Ok(2));

    let mut store = SqliteEpochReplicaStore::provision(
        &receiver,
        binding("binding"),
        FixtureAdmission::new(s.clone()),
    )
    .unwrap();
    assert!(matches!(
        SqliteReplicaStore::open(&receiver),
        Err(SqliteOpenError::UnexpectedDatabase)
    ));
    assert_eq!(
        store.apply(first.plan(&claims, &s, 0, 1).unwrap()),
        Err(Error::Unobserved)
    );
    assert_eq!(store.observe_head(first.head(&claims, &s).unwrap()), Ok(e1));
    assert_eq!(store.observed_epoch(), Ok(Some(e1)));
    assert_eq!(store.checkpoint(&s), Ok(None)); // head position deliberately nonzero
    store.apply(first.plan(&claims, &s, 0, 1).unwrap()).unwrap();
    let delayed = first.plan(&claims, &s, 1, 2).unwrap();
    let old_repeat = first.plan(&claims, &s, 0, 1).unwrap();
    let old_head = first.head(&claims, &s).unwrap();

    let e2 = new_claims.compare_and_claim(&key, Some(e1)).unwrap();
    assert!(first.head(&claims, &s).is_none());
    let mut second = Writer {
        source: source(&source_file),
        key: key.clone(),
        epoch: e2,
    };
    let mut observer = SqliteEpochReplicaStore::open(
        &receiver,
        binding("binding"),
        FixtureAdmission::new(s.clone()),
    )
    .unwrap();
    assert_eq!(
        observer.apply(second.plan(&claims, &s, 1, 2).unwrap()),
        Err(Error::Unobserved)
    );
    assert_eq!(
        observer.observe_head(second.head(&claims, &s).unwrap()),
        Ok(e2)
    );

    // A competing handle that predates observation reads the same current floor.
    assert_eq!(store.apply(delayed), Err(Error::Fenced));
    drop((store, observer));
    let mut store = SqliteEpochReplicaStore::open(
        &receiver,
        binding("binding"),
        FixtureAdmission::new(s.clone()),
    )
    .unwrap();
    assert_eq!(store.apply(old_repeat), Err(Error::Fenced)); // not even idempotent success
    assert_eq!(store.observe_head(old_head), Err(Error::Fenced));
    assert_eq!(store.observed_epoch(), Ok(Some(e2)));
    assert_eq!(
        store.checkpoint(&s),
        Ok(Some(Checkpoint::new(s.clone(), 1)))
    );
    let rows = store.read_after(&s, 0, 8, 16).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.payload.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice()]
    );
    assert_eq!(store.apply(second.plan(&claims, &s, 1, 2).unwrap()), Ok(()));
    assert_eq!(store.apply(second.plan(&claims, &s, 1, 2).unwrap()), Ok(()));
    assert_eq!(
        store.checkpoint(&s),
        Ok(Some(Checkpoint::new(s.clone(), 2)))
    );
    let rows = store.read_after(&s, 0, 8, 16).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.payload.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice(), b"b".as_slice()]
    );
}

#[test]
fn file_binding_and_exact_current_scope_do_not_admit_fallback_or_reset() {
    let f = Fixture::new();
    let receiver = f.file("receiver");
    let s = scope("grant");
    let next = scope("later-grant");
    let admission = FixtureAdmission::new(s.clone());
    let key = authority_key("binding");
    let mut ledger = SqliteOriginAuthority::provision(f.file("authority")).unwrap();
    let e1 = ledger.compare_and_claim(&key, None).unwrap();
    let e2 = ledger.compare_and_claim(&key, Some(e1)).unwrap();
    let mut writer = Writer {
        source: source(&f.file("source")),
        key,
        epoch: e2,
    };
    writer.source.append(&id("one"), b"a").unwrap();
    assert!(
        SqliteEpochReplicaStore::open(&receiver, binding("binding"), admission.clone()).is_err()
    );
    assert!(!receiver.exists());
    let mut store =
        SqliteEpochReplicaStore::provision(&receiver, binding("binding"), admission.clone())
            .unwrap();
    assert!(
        SqliteEpochReplicaStore::provision(&receiver, binding("binding"), admission.clone())
            .is_err()
    );
    assert!(matches!(
        SqliteEpochReplicaStore::open(&receiver, binding("unbound"), admission.clone()),
        Err(SqliteOpenError::SourceIdentityMismatch)
    ));
    let ordinary = f.file("ordinary");
    drop(SqliteReplicaStore::open(&ordinary).unwrap());
    assert!(matches!(
        SqliteEpochReplicaStore::open(&ordinary, binding("binding"), admission.clone()),
        Err(SqliteOpenError::UnexpectedDatabase)
    ));
    let mut wrong = writer.head(&ledger, &s).unwrap();
    wrong.key = authority_key("unbound");
    assert_eq!(store.observe_head(wrong), Err(Error::WrongBinding));
    let mut wrong = writer.head(&ledger, &next).unwrap();
    wrong.scope = s.clone();
    assert_eq!(store.observe_head(wrong), Err(Error::WrongBinding));
    assert_eq!(
        store.observe_head(writer.head(&ledger, &next).unwrap()),
        Err(Error::Unadmitted)
    );
    assert_eq!(store.checkpoint(&s), Ok(None));
    assert_eq!(store.observed_epoch(), Ok(None));
    store
        .observe_head(writer.head(&ledger, &s).unwrap())
        .unwrap();
    store
        .apply(writer.plan(&ledger, &s, 0, 1).unwrap())
        .unwrap();
    admission.select(next.clone());
    assert_eq!(
        store.apply(writer.plan(&ledger, &s, 0, 1).unwrap()),
        Err(Error::Unadmitted),
    );
    drop(store);

    let mut store =
        SqliteEpochReplicaStore::open(&receiver, binding("binding"), admission).unwrap();
    let mut old_head = writer.head(&ledger, &next).unwrap();
    old_head.epoch = e1;
    assert_eq!(store.observe_head(old_head), Err(Error::Fenced));
    assert_eq!(store.observed_epoch(), Ok(Some(e2)));
    assert_eq!(
        store.apply(writer.plan(&ledger, &s, 0, 1).unwrap()),
        Err(Error::Unadmitted)
    );
    assert_eq!(
        store.observe_head(writer.head(&ledger, &next).unwrap()),
        Ok(e2)
    );
    assert!(matches!(
        store.apply(writer.plan(&ledger, &next, 0, 1).unwrap()),
        Err(Error::Store(StoreError::ScopeMismatch { .. }))
    ));
    assert_eq!(
        store.checkpoint(&s),
        Ok(Some(Checkpoint::new(s.clone(), 1)))
    );
    assert_eq!(store.read_after(&s, 0, 8, 16).unwrap().len(), 1);
}
