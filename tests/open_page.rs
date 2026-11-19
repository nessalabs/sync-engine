//! Optional open-target domain/application contract; durable TCP evidence is separate.
use nessa_sync::replication::{
    application::{
        begin_pass_with_page, finish_pass, Access, OpenRecordSource, RecordSource, ReplicaStore,
        ScopeAuthorizer, SourceError, StoreError, SyncError,
    },
    domain::{
        validate_open_page, Checkpoint, CommitPlan, Id, Limits, OpenPageRequest, OpenedPage, Page,
        PageRequest, Record, Scope, ValidationError,
    },
    infrastructure::{MemoryAuthorizer, MemorySource, MemoryStore, SourceFact},
};
use std::sync::{Arc, Mutex};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(id("r"), id("o"), id("s"), id("i"), id("v"), id("e"))
}
fn limits() -> Limits {
    Limits::new(2, 16, 16).unwrap()
}
fn request(after: u64) -> OpenPageRequest {
    OpenPageRequest {
        scope: scope(),
        after,
        max_records: 2,
        max_payload_bytes: 16,
        max_record_bytes: 16,
    }
}
fn record(position: u64) -> Record {
    Record {
        scope: scope(),
        position,
        id: id(&format!("f{position}")),
        payload: vec![1],
    }
}
fn opened(after: u64, target: u64) -> OpenedPage {
    OpenedPage {
        request: request(after),
        target,
        records: ((after + 1)..=target).take(2).map(record).collect(),
    }
}
struct Source {
    inner: MemorySource,
    opens: usize,
}
impl Source {
    fn new(count: u64) -> Self {
        let mut inner = MemorySource::new(id("o"), id("s"), id("i"), id("v"));
        for n in 1..=count {
            inner
                .append(SourceFact {
                    id: id(&format!("f{n}")),
                    payload: vec![1],
                })
                .unwrap();
        }
        Self { inner, opens: 0 }
    }
}
impl RecordSource for Source {
    fn head(&mut self, s: &Scope) -> Result<u64, SourceError> {
        self.inner.head(s)
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        self.inner.page(r)
    }
}
impl OpenRecordSource for Source {
    fn open_page(&mut self, r: &OpenPageRequest) -> Result<OpenedPage, SourceError> {
        self.opens += 1;
        let target = self.inner.head(&r.scope)?;
        let records = if target > r.after {
            self.inner.page(&r.for_target(target))?.records
        } else {
            Vec::new()
        };
        Ok(OpenedPage {
            request: r.clone(),
            target,
            records,
        })
    }
}
#[test]
fn first_page_is_committed_and_remaining_pages_retain_the_finite_target() {
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = Source::new(5);
    let mut store = MemoryStore::new();
    let mut pass = begin_pass_with_page(&scope(), limits(), &mut a, &mut s, &mut store).unwrap();
    assert_eq!((pass.position(), pass.target(), a.checks), (2, 5, 2));
    s.inner
        .append(SourceFact {
            id: id("later"),
            payload: vec![2],
        })
        .unwrap();
    finish_pass(&mut pass, limits(), &mut a, &mut s, &mut store).unwrap();
    assert_eq!((pass.position(), pass.target()), (5, 5));
    assert_eq!(store.records(&scope()).unwrap().len(), 5);
}
#[test]
fn empty_capture_is_complete_without_apply_and_behind_capture_never_resets() {
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut source = Source::new(0);
    let mut store = MemoryStore::new();
    let pass = begin_pass_with_page(&scope(), limits(), &mut a, &mut source, &mut store).unwrap();
    assert!(pass.is_complete());
    assert_eq!(store.apply_calls().unwrap(), 0);
    assert_eq!(a.checks, 2);
    let (target, plan) = validate_open_page(
        &Checkpoint::new(scope(), 4),
        &request(4),
        OpenedPage {
            request: request(4),
            target: 4,
            records: vec![],
        },
        limits(),
    )
    .unwrap();
    assert_eq!(target, 4);
    assert!(plan.is_none());
    assert_eq!(
        validate_open_page(
            &Checkpoint::new(scope(), 4),
            &request(4),
            OpenedPage {
                request: request(4),
                target: 3,
                records: vec![]
            },
            limits()
        ),
        Err(ValidationError::HeadBehindProgress)
    );
}
#[test]
fn exact_echo_and_canonical_page_validation_refuse_invalid_first_pages() {
    for case in 0..9 {
        let mut reply = opened(0, 3);
        match case {
            0 => {
                reply.request.scope =
                    Scope::new(id("foreign"), id("o"), id("s"), id("i"), id("v"), id("e"))
            }
            1 => reply.request.after = 1,
            2 => reply.request.max_records = 3,
            3 => reply.records.clear(),
            4 => reply.target = 0,
            5 => reply.records[0].position = 2,
            6 => reply.records[1].id = reply.records[0].id.clone(),
            7 => reply.records[0].payload = vec![0; 17],
            _ => reply.records.push(record(3)),
        }
        assert!(
            validate_open_page(&Checkpoint::new(scope(), 0), &request(0), reply, limits()).is_err(),
            "case {case}"
        );
    }
}
#[test]
fn failed_stale_and_uncertain_apply_return_no_pass_and_retry_actual_progress() {
    for error in [StoreError::Failed, StoreError::Stale, StoreError::Uncertain] {
        let mut a = MemoryAuthorizer::allowed(scope());
        let mut source = Source::new(2);
        let mut store = MemoryStore::new();
        store.fail_next_apply(error.clone());
        assert_eq!(
            begin_pass_with_page(&scope(), limits(), &mut a, &mut source, &mut store),
            Err(SyncError::Store(error.clone()))
        );
        let saved = store
            .checkpoint(&scope())
            .unwrap()
            .map_or(0, |c| c.position());
        assert_eq!(saved, if error == StoreError::Uncertain { 2 } else { 0 });
        let pass =
            begin_pass_with_page(&scope(), limits(), &mut a, &mut source, &mut store).unwrap();
        assert_eq!(pass.position(), 2);
        assert!(pass.is_complete());
        assert_eq!(store.records(&scope()).unwrap().len(), 2);
    }
}
struct ChangingPolicy {
    denied: Arc<Mutex<bool>>,
    calls: usize,
}
impl ScopeAuthorizer for ChangingPolicy {
    fn authorize(&mut self, s: &Scope) -> Access {
        self.calls += 1;
        if *self.denied.lock().unwrap() {
            Access::Denied
        } else {
            Access::Allowed(s.clone())
        }
    }
}
struct ChangingLoad {
    denied: Arc<Mutex<bool>>,
}
impl ReplicaStore for ChangingLoad {
    fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        *self.denied.lock().unwrap() = true;
        Ok(None)
    }
    fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
        panic!("denied apply")
    }
}
#[test]
fn policy_change_during_load_is_refused_before_first_page_read() {
    let denied = Arc::new(Mutex::new(false));
    let mut a = ChangingPolicy {
        denied: denied.clone(),
        calls: 0,
    };
    let mut store = ChangingLoad { denied };
    let mut source = Source::new(2);
    assert_eq!(
        begin_pass_with_page(&scope(), limits(), &mut a, &mut source, &mut store),
        Err(SyncError::Denied)
    );
    assert_eq!(a.calls, 2);
    assert_eq!(source.opens, 0);
    assert_eq!(source.inner.head_reads, 0);
}
#[test]
fn initial_refusal_hides_saved_progress_and_source() {
    struct NoProgress;
    impl ReplicaStore for NoProgress {
        fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
            panic!("unauthorized load")
        }
        fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
            panic!("unauthorized apply")
        }
    }
    for (decision, error) in [
        (Access::Denied, SyncError::Denied),
        (Access::Unverifiable, SyncError::Unverifiable),
        (Access::InvalidResponse, SyncError::InvalidAccessResponse),
        (Access::IdentityChanged, SyncError::WrongAccessScope),
    ] {
        let mut a = MemoryAuthorizer::allowed(scope());
        a.decision = decision;
        let mut source = Source::new(2);
        assert_eq!(
            begin_pass_with_page(&scope(), limits(), &mut a, &mut source, &mut NoProgress),
            Err(error)
        );
        assert_eq!(source.opens, 0);
    }
}
