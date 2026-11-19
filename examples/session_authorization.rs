//! Current-policy prerequisite lab using existing in-memory application ports.
//! This does not route data over a session or authenticate a production peer.
use nessa_sync::replication::{
    application::{begin_pass, step, Access, ReplicaStore, ScopeAuthorizer, StoreError, SyncError},
    domain::{Checkpoint, CommitPlan, Id, Limits, Scope},
    infrastructure::session::{
        BindingMetadata, ControlFrame, ScopedSessionAuthorizer, SessionLifecycle, SessionLimits,
    },
    infrastructure::{MemorySource, MemoryStore, SourceFact},
};
use std::collections::VecDeque;

struct FixturePolicy {
    decisions: VecDeque<Access>,
    calls: usize,
}
impl ScopeAuthorizer for FixturePolicy {
    fn authorize(&mut self, _: &Scope) -> Access {
        self.calls += 1;
        self.decisions.pop_front().unwrap_or(Access::Unverifiable)
    }
}
struct FixtureStore {
    inner: MemoryStore,
    loads: usize,
    applies: usize,
}
impl ReplicaStore for FixtureStore {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.loads += 1;
        self.inner.load(scope)
    }
    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        self.applies += 1;
        self.inner.apply(plan)
    }
}
fn main() {
    let id = || Id::new("fixture").unwrap();
    let scope = Scope::new(id(), id(), id(), id(), id(), id());
    let limits = SessionLimits {
        control_bytes: 8192,
        bindings: 1,
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
    let (request, _) = owner.begin_bind(scope.clone()).unwrap();
    let identity = owner
        .accept_bound(
            &key,
            &ControlFrame::Bound {
                correlation: request.correlation(),
                binding: BindingMetadata {
                    handle: 1,
                    generation: 1,
                    scope: scope.clone(),
                    head: 999,
                },
            },
        )
        .unwrap();
    let mut current = FixturePolicy {
        decisions: vec![
            Access::Denied,
            Access::Allowed(scope.clone()),
            Access::Denied,
        ]
        .into(),
        calls: 0,
    };
    let mut source = MemorySource::new(id(), id(), id(), id());
    source
        .append(SourceFact {
            id: id(),
            payload: b"one".to_vec(),
        })
        .unwrap();
    let mut store = FixtureStore {
        inner: MemoryStore::new(),
        loads: 0,
        applies: 0,
    };
    {
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
        assert_eq!(
            begin_pass(&scope, &mut gate, &mut source, &mut store),
            Err(SyncError::Denied)
        );
        assert_eq!(store.loads, 0);
        assert_eq!(source.head_reads, 0);
        let mut pass = begin_pass(&scope, &mut gate, &mut source, &mut store).unwrap();
        assert_eq!(pass.target(), 1); // Never use the advisory Bound head as progress.
        assert_eq!(
            step(
                &mut pass,
                Limits::new(2, 16, 16).unwrap(),
                &mut gate,
                &mut source,
                &mut store
            ),
            Err(SyncError::Denied)
        );
    }
    owner.close();
    {
        let mut gate = ScopedSessionAuthorizer::new(&owner, &identity, &mut current);
        assert_eq!(
            begin_pass(&scope, &mut gate, &mut source, &mut store),
            Err(SyncError::Unverifiable)
        );
    }
    assert!(store.inner.checkpoint(&scope).unwrap().is_none());
    assert_eq!(current.calls, 3);
    assert_eq!(store.loads, 1);
    assert_eq!(source.head_reads, 1);
    assert_eq!(source.page_reads, 0);
    assert_eq!(store.applies, 0);
    println!("{{\"policy_calls\":{},\"progress_loads\":{},\"head_reads\":{},\"page_reads\":{},\"apply_calls\":{},\"closed_owner_refused\":true}}", current.calls, store.loads, source.head_reads, source.page_reads, store.applies);
}
