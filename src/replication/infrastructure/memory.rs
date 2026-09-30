//! Illustrative in-memory adapters. Their transactional guarantees hold only
//! within a live process; durable restart and real transport belong to later slices.
//! Cloned stores share one lock, so competing handles exercise atomic CAS.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::replication::application::{
    Access, RecordSource, ReplicaStore, ScopeAuthorizer, SourceError, StoreError,
};
use crate::replication::domain::{
    validate_page_request, Checkpoint, CommitPlan, Id, Limits, Page, PageRequest, Record, Scope,
};

/// A canonical source fact before receiver-specific delivery scope is attached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFact {
    /// Immutable fact ID.
    pub id: Id,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

/// A bounded in-memory source with observable read counts.
#[derive(Clone, Debug)]
pub struct MemorySource {
    /// Origin identity.
    pub origin: Id,
    /// Stream identity.
    pub stream: Id,
    /// Source incarnation.
    pub incarnation: Id,
    /// Required schema identity.
    pub schema: Id,
    facts: Vec<SourceFact>,
    head: u64,
    /// Number of head reads.
    pub head_reads: usize,
    /// Number of page reads.
    pub page_reads: usize,
    /// Record payload bytes returned by page reads.
    pub payload_bytes: usize,
}

impl MemorySource {
    /// Creates an empty source under host-selected identities.
    pub fn new(origin: Id, stream: Id, incarnation: Id, schema: Id) -> Self {
        Self {
            origin,
            stream,
            incarnation,
            schema,
            facts: Vec::new(),
            head: 0,
            head_reads: 0,
            page_reads: 0,
            payload_bytes: 0,
        }
    }
    /// Appends an already committed source fact; duplicate IDs are refused.
    pub fn append(&mut self, fact: SourceFact) -> Result<u64, StoreError> {
        if self.facts.iter().any(|existing| existing.id == fact.id) {
            return Err(StoreError::ConflictingRecord);
        }
        let next = self.head.checked_add(1).ok_or(StoreError::Failed)?;
        self.facts.push(fact);
        self.head = next;
        Ok(next)
    }
    /// Returns the current head without a source-port read.
    pub fn committed_head(&self) -> u64 {
        self.head
    }
}

impl RecordSource for MemorySource {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.head_reads += 1;
        if scope.origin() != &self.origin
            || scope.stream() != &self.stream
            || scope.incarnation() != &self.incarnation
            || scope.schema() != &self.schema
        {
            return Err(SourceError::IdentityChanged);
        }
        Ok(self.committed_head())
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.page_reads += 1;
        if request.scope.origin() != &self.origin
            || request.scope.stream() != &self.stream
            || request.scope.incarnation() != &self.incarnation
            || request.scope.schema() != &self.schema
        {
            return Err(SourceError::IdentityChanged);
        }
        // Generic admissibility has one domain owner. These adapters preserve
        // their existing representable request budgets; physical limits below
        // may return a smaller page without imposing a receiver policy.
        let limits = Limits::new(usize::MAX, usize::MAX, usize::MAX)
            .map_err(|_| SourceError::InvalidRequest)?;
        validate_page_request(request, limits).map_err(|_| SourceError::InvalidRequest)?;
        if request.target > self.committed_head() {
            return Err(SourceError::InvalidRequest);
        }
        let start = usize::try_from(request.after).map_err(|_| SourceError::InvalidRequest)?;
        let target = usize::try_from(request.target).map_err(|_| SourceError::InvalidRequest)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (index, fact) in self.facts[start..target].iter().enumerate() {
            if records.len() == request.max_records {
                break;
            }
            if fact.payload.len() > request.max_record_bytes {
                if records.is_empty() {
                    return Err(SourceError::OversizedRecord);
                }
                break;
            }
            let next = bytes
                .checked_add(fact.payload.len())
                .ok_or(SourceError::OversizedRecord)?;
            if next > request.max_payload_bytes {
                if records.is_empty() {
                    return Err(SourceError::OversizedRecord);
                }
                break;
            }
            bytes = next;
            let offset = u64::try_from(index).map_err(|_| SourceError::InvalidRequest)?;
            let position = request
                .after
                .checked_add(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or(SourceError::InvalidRequest)?;
            records.push(Record {
                position,
                id: fact.id.clone(),
                scope: request.scope.clone(),
                payload: fact.payload.clone(),
            });
        }
        self.payload_bytes = self.payload_bytes.saturating_add(bytes);
        Ok(Page {
            request: request.clone(),
            records,
        })
    }
}

/// Mutable host policy fixture; policy is checked on every read.
#[derive(Clone, Debug)]
pub struct MemoryAuthorizer {
    /// Current decision returned to the application.
    pub decision: Access,
    /// Number of policy checks.
    pub checks: usize,
    /// Exact scopes presented to the policy port, in call order.
    pub observed_scopes: Vec<Scope>,
}

impl MemoryAuthorizer {
    /// Authorizes the supplied exact scope initially.
    pub fn allowed(scope: Scope) -> Self {
        Self {
            decision: Access::Allowed(scope),
            checks: 0,
            observed_scopes: Vec::new(),
        }
    }
}

impl ScopeAuthorizer for MemoryAuthorizer {
    fn authorize(&mut self, scope: &Scope) -> Access {
        self.checks += 1;
        self.observed_scopes.push(scope.clone());
        self.decision.clone()
    }
}

type StoreKey = (Id, Id, Id);

fn store_key(scope: &Scope) -> StoreKey {
    (
        scope.receiver().clone(),
        scope.origin().clone(),
        scope.stream().clone(),
    )
}

#[derive(Default)]
struct StoreState {
    checkpoints: HashMap<StoreKey, Checkpoint>,
    records: HashMap<StoreKey, Vec<Record>>,
    ids: HashMap<(StoreKey, Id), Record>,
    apply_calls: usize,
    fail_next: Option<StoreError>,
}

/// Shared in-memory replica store. Clones are competing handles over one state.
#[derive(Clone, Default)]
pub struct MemoryStore {
    state: Arc<Mutex<StoreState>>,
}

impl MemoryStore {
    /// Creates an empty replica store.
    pub fn new() -> Self {
        Self::default()
    }
    /// Injects one failure at the next apply. `Uncertain` commits first and loses its reply.
    pub fn fail_next_apply(&self, failure: StoreError) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_next = Some(failure);
        }
    }
    /// Returns saved records for one receiver and source stream.
    pub fn records(&self, scope: &Scope) -> Result<Vec<Record>, StoreError> {
        self.state
            .lock()
            .map(|state| {
                state
                    .records
                    .get(&store_key(scope))
                    .cloned()
                    .unwrap_or_default()
            })
            .map_err(|_| StoreError::Failed)
    }
    /// Returns progress for one receiver and source stream, even if its saved
    /// incarnation or access epoch differs from the requested scope.
    pub fn checkpoint(&self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.state
            .lock()
            .map(|state| state.checkpoints.get(&store_key(scope)).cloned())
            .map_err(|_| StoreError::Failed)
    }
    /// Number of attempted apply calls, including refusals.
    pub fn apply_calls(&self) -> Result<usize, StoreError> {
        self.state
            .lock()
            .map(|state| state.apply_calls)
            .map_err(|_| StoreError::Failed)
    }
}

impl ReplicaStore for MemoryStore {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        let saved = self.checkpoint(scope)?;
        if let Some(checkpoint) = &saved {
            if checkpoint.scope() != scope {
                return Err(StoreError::ScopeMismatch {
                    saved: Box::new(checkpoint.scope().clone()),
                    requested: Box::new(scope.clone()),
                });
            }
        }
        Ok(saved)
    }
    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Failed)?;
        state.apply_calls += 1;
        let key = store_key(plan.expected().scope());
        let current = state
            .checkpoints
            .get(&key)
            .cloned()
            .unwrap_or(Checkpoint::new(plan.expected().scope().clone(), 0));
        if current.scope() != plan.expected().scope() {
            return Err(StoreError::ScopeMismatch {
                saved: Box::new(current.scope().clone()),
                requested: Box::new(plan.expected().scope().clone()),
            });
        }
        // A lost reply may be replayed after another handle has advanced further.
        // A conflicting reuse of an already-saved fact ID is distinct from a
        // stale position and must preserve the original committed meaning.
        if current.position() >= plan.next().position() {
            let start =
                usize::try_from(plan.expected().position()).map_err(|_| StoreError::Stale)?;
            let end = usize::try_from(plan.next().position()).map_err(|_| StoreError::Stale)?;
            let saved = state.records.get(&key).ok_or(StoreError::Stale)?;
            if saved
                .get(start..end)
                .is_some_and(|records| records == plan.records())
            {
                return Ok(());
            }
            if plan.records().iter().any(|record| {
                state
                    .ids
                    .get(&(key.clone(), record.id.clone()))
                    .is_some_and(|existing| existing != record)
            }) {
                return Err(StoreError::ConflictingRecord);
            }
            return Err(StoreError::Stale);
        }
        if current != *plan.expected() {
            return Err(StoreError::Stale);
        }
        for record in plan.records() {
            if state
                .ids
                .get(&(key.clone(), record.id.clone()))
                .is_some_and(|existing| existing != record)
            {
                return Err(StoreError::ConflictingRecord);
            }
        }
        // A fault belongs to the next otherwise valid commit attempt. A stale
        // plan or duplicate reply must not consume it.
        let failure = state.fail_next.take();
        if let Some(error) = &failure {
            if *error != StoreError::Uncertain {
                return Err(error.clone());
            }
        }
        for record in plan.records() {
            state
                .ids
                .insert((key.clone(), record.id.clone()), record.clone());
        }
        let (_, next, records) = plan.into_parts();
        state
            .records
            .entry(key.clone())
            .or_default()
            .extend(records);
        state.checkpoints.insert(key, next);
        if failure == Some(StoreError::Uncertain) {
            Err(StoreError::Uncertain)
        } else {
            Ok(())
        }
    }
}
