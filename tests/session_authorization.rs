#![cfg(feature = "transport")]
use nessa_sync::replication::{
    application::{
        begin_pass, finish_pass, step, Access, RecordSource, ReplicaStore, ScopeAuthorizer,
        SourceError, StoreError, SyncError,
    },
    domain::{Checkpoint, CommitPlan, Id, Limits, Page, PageRequest, Scope},
    infrastructure::session::{
        BindingMetadata, ControlFrame, ScopeIdentity, ScopedSessionAuthorizer, SessionLifecycle,
        SessionLimits,
    },
    infrastructure::{MemoryAuthorizer, MemorySource, MemoryStore, SourceFact},
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

type Trace = Rc<RefCell<Vec<&'static str>>>;
fn id(text: &str) -> Id {
    Id::new(text).unwrap()
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
fn context() -> (SessionLifecycle, ScopeIdentity) {
    let limits = SessionLimits {
        control_bytes: 8192,
        bindings: 2,
    };
    let mut owner = SessionLifecycle::new(limits, 1).unwrap();
    let key = owner.connection_id();
    owner
        .accept_welcome(
            &key,
            &ControlFrame::Welcome {
                session: 1,
                limits,
                record_bytes: 16,
            },
        )
        .unwrap();
    let (request, _) = owner.begin_bind(scope()).unwrap();
    let identity = owner
        .accept_bound(
            &key,
            &ControlFrame::Bound {
                correlation: request.correlation(),
                binding: BindingMetadata {
                    handle: 1,
                    generation: 1,
                    scope: scope(),
                    head: 999,
                },
            },
        )
        .unwrap();
    (owner, identity)
}
fn source() -> MemorySource {
    let mut source = MemorySource::new(id("origin"), id("stream"), id("incarnation"), id("schema"));
    source
        .append(SourceFact {
            id: id("fact-1"),
            payload: b"one".to_vec(),
        })
        .unwrap();
    source
}
fn limits() -> Limits {
    Limits::new(2, 16, 16).unwrap()
}

struct CurrentPolicy {
    decisions: VecDeque<Access>,
    calls: usize,
    trace: Trace,
}
impl CurrentPolicy {
    fn new(decisions: Vec<Access>, trace: &Trace) -> Self {
        Self {
            decisions: decisions.into(),
            calls: 0,
            trace: trace.clone(),
        }
    }
}
impl ScopeAuthorizer for CurrentPolicy {
    fn authorize(&mut self, _: &Scope) -> Access {
        self.calls += 1;
        self.trace.borrow_mut().push("policy");
        self.decisions.pop_front().unwrap_or(Access::Unverifiable)
    }
}
struct ObservedSource {
    inner: MemorySource,
    trace: Trace,
}
impl RecordSource for ObservedSource {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.trace.borrow_mut().push("head");
        self.inner.head(scope)
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.trace.borrow_mut().push("page");
        self.inner.page(request)
    }
}
struct ObservedStore {
    inner: MemoryStore,
    trace: Trace,
    loads: usize,
    applies: usize,
    load_failure: Option<StoreError>,
}
impl ObservedStore {
    fn new(trace: &Trace) -> Self {
        Self {
            inner: MemoryStore::new(),
            trace: trace.clone(),
            loads: 0,
            applies: 0,
            load_failure: None,
        }
    }
}
impl ReplicaStore for ObservedStore {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.loads += 1;
        self.trace.borrow_mut().push("load");
        if let Some(error) = &self.load_failure {
            return Err(error.clone());
        }
        self.inner.load(scope)
    }
    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        self.applies += 1;
        self.trace.borrow_mut().push("apply");
        self.inner.apply(plan)
    }
}

#[test]
fn exact_context_delegates_current_policy_every_time_without_caching_allowed() {
    let (owner, identity) = context();
    let trace = Trace::default();
    let mut current = CurrentPolicy::new(
        vec![
            Access::Denied,
            Access::Allowed(scope()),
            Access::Unverifiable,
            Access::IdentityChanged,
            Access::InvalidResponse,
        ],
        &trace,
    );
    {
        let dynamic: &mut dyn ScopeAuthorizer = &mut current;
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, dynamic);
        assert!(trace.borrow().is_empty()); // Construction grants nothing.
        for expected in [
            Access::Denied,
            Access::Allowed(scope()),
            Access::Unverifiable,
            Access::IdentityChanged,
            Access::InvalidResponse,
            Access::Unverifiable,
        ] {
            assert_eq!(gate.authorize(&scope()), expected);
        }
    }
    assert_eq!(current.calls, 6);
    assert_eq!(*trace.borrow(), vec!["policy"; 6]);
}

#[test]
fn current_policy_precedes_real_progress_and_source_boundaries() {
    let (owner, identity) = context();
    let trace = Trace::default();
    let mut current = CurrentPolicy::new(
        vec![Access::Allowed(scope()), Access::Allowed(scope())],
        &trace,
    );
    let mut source = ObservedSource {
        inner: source(),
        trace: trace.clone(),
    };
    let mut store = ObservedStore::new(&trace);
    {
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
        let mut pass = begin_pass(&scope(), &mut gate, &mut source, &mut store).unwrap();
        assert_eq!(pass.target(), 1); // The Bound advisory head 999 is not authority.
        finish_pass(&mut pass, limits(), &mut gate, &mut source, &mut store).unwrap();
    }
    assert_eq!(
        *trace.borrow(),
        ["policy", "load", "head", "policy", "page", "apply"]
    );
    assert_eq!(current.calls, 2);
    assert_eq!(
        store
            .inner
            .checkpoint(&scope())
            .unwrap()
            .unwrap()
            .position(),
        1
    );
    assert_eq!(store.inner.records(&scope()).unwrap()[0].payload, b"one");
}

#[test]
fn denied_or_unverifiable_policy_cannot_disclose_saved_scope_mismatch() {
    let (owner, identity) = context();
    let other = Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("old"),
        id("schema"),
        id("epoch"),
    );
    let mismatch = StoreError::ScopeMismatch {
        saved: Box::new(other),
        requested: Box::new(scope()),
    };
    for (decision, expected) in [
        (Access::Denied, SyncError::Denied),
        (Access::Unverifiable, SyncError::Unverifiable),
        (Access::IdentityChanged, SyncError::WrongAccessScope),
        (Access::InvalidResponse, SyncError::InvalidAccessResponse),
    ] {
        let trace = Trace::default();
        let mut current = CurrentPolicy::new(vec![decision], &trace);
        let mut source = ObservedSource {
            inner: source(),
            trace: trace.clone(),
        };
        let mut store = ObservedStore::new(&trace);
        store.load_failure = Some(mismatch.clone());
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
        assert_eq!(
            begin_pass(&scope(), &mut gate, &mut source, &mut store),
            Err(expected)
        );
        assert_eq!(*trace.borrow(), ["policy"]);
        assert_eq!(store.loads, 0);
        assert_eq!(source.inner.head_reads, 0);
        assert_eq!(store.applies, 0);
    }
    let trace = Trace::default();
    let mut current = CurrentPolicy::new(vec![Access::Allowed(scope())], &trace);
    let mut source = ObservedSource {
        inner: source(),
        trace: trace.clone(),
    };
    let mut store = ObservedStore::new(&trace);
    store.load_failure = Some(mismatch.clone());
    let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
    assert_eq!(
        begin_pass(&scope(), &mut gate, &mut source, &mut store),
        Err(SyncError::Store(mismatch))
    );
    assert_eq!(*trace.borrow(), ["policy", "load"]);
}

#[test]
fn all_six_requested_scope_mismatches_stop_before_policy_and_progress() {
    let (owner, identity) = context();
    let fields = [
        "receiver",
        "origin",
        "stream",
        "incarnation",
        "schema",
        "epoch",
    ];
    for changed in 0..6 {
        let mut values = fields.map(id);
        values[changed] = id("other");
        let [r, o, s, i, f, e] = values;
        let requested = Scope::new(r, o, s, i, f, e);
        let trace = Trace::default();
        let mut current = CurrentPolicy::new(vec![Access::Allowed(requested.clone())], &trace);
        let mut source = ObservedSource {
            inner: source(),
            trace: trace.clone(),
        };
        let mut store = ObservedStore::new(&trace);
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
        assert_eq!(
            begin_pass(&requested, &mut gate, &mut source, &mut store),
            Err(SyncError::WrongAccessScope)
        );
        assert!(trace.borrow().is_empty());
        assert_eq!(store.loads, 0);
    }
}

#[test]
fn mismatched_allowed_echo_is_not_promoted_to_current_scope() {
    let (owner, identity) = context();
    let changed = Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("new-epoch"),
    );
    let trace = Trace::default();
    let mut current = CurrentPolicy::new(
        vec![Access::Allowed(changed.clone()), Access::Allowed(changed)],
        &trace,
    );
    let mut source = ObservedSource {
        inner: source(),
        trace: trace.clone(),
    };
    let mut store = ObservedStore::new(&trace);
    let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
    assert_eq!(gate.authorize(&scope()), Access::IdentityChanged);
    assert_eq!(
        begin_pass(&scope(), &mut gate, &mut source, &mut store),
        Err(SyncError::WrongAccessScope)
    );
    assert_eq!(*trace.borrow(), ["policy", "policy"]);
    assert_eq!(store.loads, 0);
}

#[test]
fn known_lifecycle_stops_cannot_be_overridden_by_an_allowed_policy_port() {
    for state in ["invalid", "closed", "other-connection"] {
        let (mut owner, identity) = context();
        let (other_owner, _) = context();
        match state {
            "invalid" => owner.invalidate_scope(&identity).unwrap(),
            "closed" => owner.close(),
            _ => {}
        }
        let selected = if state == "other-connection" {
            &other_owner
        } else {
            &owner
        };
        let trace = Trace::default();
        let mut current = CurrentPolicy::new(vec![Access::Allowed(scope())], &trace);
        let mut source = ObservedSource {
            inner: source(),
            trace: trace.clone(),
        };
        let mut store = ObservedStore::new(&trace);
        let mut gate = ScopedSessionAuthorizer::new(selected, &identity, &mut current);
        let expected = if state == "closed" {
            SyncError::Unverifiable
        } else {
            SyncError::WrongAccessScope
        };
        assert_eq!(
            begin_pass(&scope(), &mut gate, &mut source, &mut store),
            Err(expected)
        );
        assert!(trace.borrow().is_empty());
        assert_eq!(store.loads, 0);
        assert_eq!(source.inner.head_reads, 0);
    }
}

#[test]
fn policy_change_before_next_page_preserves_existing_cache_and_checkpoint() {
    for (changed, expected) in [
        (Access::Denied, SyncError::Denied),
        (Access::Unverifiable, SyncError::Unverifiable),
        (Access::IdentityChanged, SyncError::WrongAccessScope),
        (Access::InvalidResponse, SyncError::InvalidAccessResponse),
    ] {
        // Seed committed fixture progress at one using existing application ports.
        let mut initial_source = source();
        let mut initial_store = MemoryStore::new();
        let mut allowed = MemoryAuthorizer::allowed(scope());
        let mut initial = begin_pass(
            &scope(),
            &mut allowed,
            &mut initial_source,
            &mut initial_store,
        )
        .unwrap();
        finish_pass(
            &mut initial,
            limits(),
            &mut allowed,
            &mut initial_source,
            &mut initial_store,
        )
        .unwrap();
        initial_source
            .append(SourceFact {
                id: id("fact-2"),
                payload: b"two".to_vec(),
            })
            .unwrap();
        initial_source.head_reads = 0;
        initial_source.page_reads = 0;
        let (owner, identity) = context();
        let trace = Trace::default();
        let mut current = CurrentPolicy::new(vec![Access::Allowed(scope()), changed], &trace);
        let mut source = ObservedSource {
            inner: initial_source,
            trace: trace.clone(),
        };
        let mut store = ObservedStore::new(&trace);
        store.inner = initial_store;
        {
            let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
            let mut pass = begin_pass(&scope(), &mut gate, &mut source, &mut store).unwrap();
            assert_eq!(pass.target(), 2);
            assert_eq!(
                step(&mut pass, limits(), &mut gate, &mut source, &mut store),
                Err(expected)
            );
            assert_eq!(pass.position(), 1);
        }
        assert_eq!(current.calls, 2);
        assert_eq!(source.inner.page_reads, 0);
        assert_eq!(store.applies, 0);
        assert_eq!(
            store
                .inner
                .checkpoint(&scope())
                .unwrap()
                .unwrap()
                .position(),
            1
        );
        assert_eq!(store.inner.records(&scope()).unwrap().len(), 1);
        assert_eq!(*trace.borrow(), ["policy", "load", "head", "policy"]);
    }
}

#[test]
fn lifecycle_transition_between_operations_stops_a_captured_pass() {
    for close in [false, true] {
        let (mut owner, identity) = context();
        let trace = Trace::default();
        let mut current = CurrentPolicy::new(
            vec![Access::Allowed(scope()), Access::Allowed(scope())],
            &trace,
        );
        let mut source = ObservedSource {
            inner: source(),
            trace: trace.clone(),
        };
        let mut store = ObservedStore::new(&trace);
        let mut pass = {
            let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
            begin_pass(&scope(), &mut gate, &mut source, &mut store).unwrap()
        };
        if close {
            owner.close();
        } else {
            owner.invalidate_scope(&identity).unwrap();
        }
        {
            let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
            assert_eq!(
                step(&mut pass, limits(), &mut gate, &mut source, &mut store),
                Err(if close {
                    SyncError::Unverifiable
                } else {
                    SyncError::WrongAccessScope
                })
            );
        }
        assert_eq!(current.calls, 1);
        assert_eq!(source.inner.page_reads, 0);
        assert_eq!(store.applies, 0);
        assert_eq!(pass.position(), 0);
        assert!(store.inner.checkpoint(&scope()).unwrap().is_none());
    }
}

#[test]
fn explicit_rebind_and_fresh_policy_resume_saved_progress_without_reset() {
    let mut initial_source = source();
    let mut initial_store = MemoryStore::new();
    let mut allowed = MemoryAuthorizer::allowed(scope());
    let mut initial = begin_pass(
        &scope(),
        &mut allowed,
        &mut initial_source,
        &mut initial_store,
    )
    .unwrap();
    finish_pass(
        &mut initial,
        limits(),
        &mut allowed,
        &mut initial_source,
        &mut initial_store,
    )
    .unwrap();
    initial_source
        .append(SourceFact {
            id: id("fact-2"),
            payload: b"two".to_vec(),
        })
        .unwrap();
    initial_source.head_reads = 0;
    initial_source.page_reads = 0;
    let (mut old_owner, old_identity) = context();
    let trace = Trace::default();
    let mut current = CurrentPolicy::new(
        vec![
            Access::Denied,
            Access::Allowed(scope()),
            Access::Allowed(scope()),
        ],
        &trace,
    );
    let mut source = ObservedSource {
        inner: initial_source,
        trace: trace.clone(),
    };
    let mut store = ObservedStore::new(&trace);
    store.inner = initial_store;
    {
        let mut gate = ScopedSessionAuthorizer::new(&old_owner, &old_identity, &mut current);
        assert_eq!(
            begin_pass(&scope(), &mut gate, &mut source, &mut store),
            Err(SyncError::Denied)
        );
    }
    old_owner.close();
    let (new_owner, new_identity) = context();
    {
        let mut stale = ScopedSessionAuthorizer::new(&new_owner, &old_identity, &mut current);
        assert_eq!(
            begin_pass(&scope(), &mut stale, &mut source, &mut store),
            Err(SyncError::WrongAccessScope)
        );
    }
    assert_eq!(store.loads, 0);
    assert_eq!(
        store
            .inner
            .checkpoint(&scope())
            .unwrap()
            .unwrap()
            .position(),
        1
    );
    {
        let mut gate = ScopedSessionAuthorizer::new(&new_owner, &new_identity, &mut current);
        let mut pass = begin_pass(&scope(), &mut gate, &mut source, &mut store).unwrap();
        assert_eq!(pass.position(), 1);
        assert_eq!(pass.target(), 2);
        finish_pass(&mut pass, limits(), &mut gate, &mut source, &mut store).unwrap();
    }
    assert_eq!(current.calls, 3);
    assert_eq!(store.applies, 1);
    assert_eq!(
        store
            .inner
            .checkpoint(&scope())
            .unwrap()
            .unwrap()
            .position(),
        2
    );
    assert_eq!(store.inner.records(&scope()).unwrap().len(), 2);
}
