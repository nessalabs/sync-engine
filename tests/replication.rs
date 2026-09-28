//! Domain/application/adapter state and ordering tests for issue #2.
use nessa_sync::replication::application::{
    begin_pass, finish_pass, step, Access, RecordSource, ReplicaStore, SourceError, StoreError,
    SyncError,
};
use nessa_sync::replication::domain::{
    validate_page, Checkpoint, Id, Limits, Page, PageRequest, Record, Scope, ValidationError,
};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, MemorySource, MemoryStore, SourceFact,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema-v1"),
        id("epoch-1"),
    )
}
fn limits() -> Limits {
    Limits::new(2, 8, 8).unwrap()
}
fn source(values: &[&str]) -> MemorySource {
    let mut source = MemorySource::new(
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema-v1"),
    );
    for (index, value) in values.iter().enumerate() {
        source
            .append(SourceFact {
                id: id(&format!("fact-{index}")),
                payload: value.as_bytes().to_vec(),
            })
            .unwrap();
    }
    source
}
fn records(store: &MemoryStore, receiver: &str) -> Vec<Vec<u8>> {
    store
        .records(&scope(receiver))
        .unwrap()
        .iter()
        .map(|r| r.payload.clone())
        .collect()
}
fn position(store: &MemoryStore, receiver: &str) -> u64 {
    store
        .checkpoint(&scope(receiver))
        .unwrap()
        .map_or(0, |c| c.position())
}

#[test]
fn two_receivers_are_independent_and_idle_is_quiet() {
    let mut source = source(&["one", "two", "three"]);
    let (mut a, mut b) = (MemoryStore::new(), MemoryStore::new());
    let (sa, sb) = (scope("a"), scope("b"));
    let (mut aa, mut ab) = (
        MemoryAuthorizer::allowed(sa.clone()),
        MemoryAuthorizer::allowed(sb.clone()),
    );
    let mut pa = begin_pass(&sa, &mut aa, &mut source, &mut a).unwrap();
    let mut pb = begin_pass(&sb, &mut ab, &mut source, &mut b).unwrap();
    finish_pass(&mut pa, limits(), &mut aa, &mut source, &mut a).unwrap();
    finish_pass(&mut pb, limits(), &mut ab, &mut source, &mut b).unwrap();
    assert_eq!((position(&a, "a"), position(&b, "b")), (3, 3));
    assert_eq!(records(&a, "a"), records(&b, "b"));
    assert_eq!(
        (source.head_reads, source.page_reads, source.payload_bytes),
        (2, 4, 22)
    );
    source
        .append(SourceFact {
            id: id("fact-3"),
            payload: b"four".to_vec(),
        })
        .unwrap();
    let mut pa = begin_pass(&sa, &mut aa, &mut source, &mut a).unwrap();
    finish_pass(&mut pa, limits(), &mut aa, &mut source, &mut a).unwrap();
    assert_eq!((position(&a, "a"), position(&b, "b")), (4, 3));
    let mut pb = begin_pass(&sb, &mut ab, &mut source, &mut b).unwrap();
    finish_pass(&mut pb, limits(), &mut ab, &mut source, &mut b).unwrap();
    assert_eq!(records(&a, "a"), records(&b, "b"));
    let before = (source.head_reads, source.page_reads, source.payload_bytes);
    assert!(step(&mut pb, limits(), &mut ab, &mut source, &mut b).unwrap());
    assert_eq!(
        (source.head_reads, source.page_reads, source.payload_bytes),
        before
    );
    let idle = begin_pass(&sb, &mut ab, &mut source, &mut b).unwrap();
    assert!(idle.is_complete());
    assert_eq!(source.payload_bytes, before.2);
    assert_eq!(source.page_reads, before.1);
}

#[test]
fn captured_target_finishes_under_churn_and_duplicate_hints() {
    let mut source = source(&["a", "b", "c", "d"]);
    let s = scope("slow");
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(pass.target(), 4);
    for index in 4..8 {
        assert_eq!(
            step(
                &mut pass,
                Limits::new(1, 8, 8).unwrap(),
                &mut auth,
                &mut source,
                &mut store
            )
            .unwrap(),
            index == 7
        );
        source
            .append(SourceFact {
                id: id(&format!("fact-{index}")),
                payload: vec![b'x'],
            })
            .unwrap();
    }
    assert!(pass.is_complete());
    assert_eq!(position(&store, "slow"), 4);
    assert_eq!(source.head_reads, 1);
    let mut next = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(next.target(), 8);
    finish_pass(&mut next, limits(), &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(position(&store, "slow"), 8);
}

#[test]
fn policy_denial_unverifiable_and_wrong_epoch_precede_source_reads() {
    for (decision, expected) in [
        (Access::Denied, SyncError::Denied),
        (Access::Unverifiable, SyncError::Unverifiable),
        (Access::Allowed(scope("other")), SyncError::WrongAccessScope),
    ] {
        let mut source = source(&["a"]);
        let s = scope("r");
        let mut auth = MemoryAuthorizer {
            decision,
            checks: 0,
            observed_scopes: Vec::new(),
        };
        let mut store = MemoryStore::new();
        assert_eq!(
            begin_pass(&s, &mut auth, &mut source, &mut store),
            Err(expected)
        );
        assert_eq!(
            (
                source.head_reads,
                source.page_reads,
                store.apply_calls().unwrap()
            ),
            (0, 0, 0)
        );
    }
    let mut source = source(&["a", "b"]);
    let s = scope("r");
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    auth.decision = Access::Denied;
    assert_eq!(
        step(&mut pass, limits(), &mut auth, &mut source, &mut store),
        Err(SyncError::Denied)
    );
    assert_eq!((source.page_reads, store.apply_calls().unwrap()), (0, 0));
}

struct Malformed {
    inner: MemorySource,
    change: fn(&mut Page),
}
impl RecordSource for Malformed {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.inner.head(scope)
    }
    fn page(&mut self, req: &PageRequest) -> Result<Page, SourceError> {
        let mut page = self.inner.page(req)?;
        (self.change)(&mut page);
        Ok(page)
    }
}
fn altered_scope(scope: &Scope, field: &str) -> Scope {
    let replacement = id("foreign");
    Scope::new(
        if field == "receiver" {
            replacement.clone()
        } else {
            scope.receiver().clone()
        },
        if field == "origin" {
            replacement.clone()
        } else {
            scope.origin().clone()
        },
        if field == "stream" {
            replacement.clone()
        } else {
            scope.stream().clone()
        },
        if field == "incarnation" {
            replacement.clone()
        } else {
            scope.incarnation().clone()
        },
        if field == "schema" {
            replacement.clone()
        } else {
            scope.schema().clone()
        },
        if field == "epoch" {
            replacement
        } else {
            scope.access_epoch().clone()
        },
    )
}
fn wrong_request(page: &mut Page) {
    page.request.scope = altered_scope(&page.request.scope, "receiver");
}
fn wrong_record_limit(page: &mut Page) {
    page.request.max_record_bytes -= 1;
}
fn wrong_origin(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "origin");
}
fn wrong_stream(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "stream");
}
fn wrong_incarnation(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "incarnation");
}
fn wrong_schema(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "schema");
}
fn wrong_receiver(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "receiver");
}
fn wrong_epoch(page: &mut Page) {
    page.records[0].scope = altered_scope(&page.records[0].scope, "epoch");
}
fn gap(page: &mut Page) {
    page.records[0].position += 1;
}
fn duplicate_id(page: &mut Page) {
    page.records[1].id = page.records[0].id.clone();
}
fn too_many(page: &mut Page) {
    page.records.push(page.records[1].clone());
}
fn too_many_bytes(page: &mut Page) {
    page.records[0].payload = vec![b'x'; 8];
}
fn single_oversize(page: &mut Page) {
    page.records[0].payload = vec![b'x'; 9];
}
fn empty(page: &mut Page) {
    page.records.clear();
}
fn overshoot_target(page: &mut Page) {
    page.records[0].position = page.request.target + 1;
}

#[test]
fn malformed_pages_refuse_before_apply() {
    type Case = (fn(&mut Page), ValidationError);
    let cases: &[Case] = &[
        (wrong_request, ValidationError::WrongRequest),
        (wrong_record_limit, ValidationError::WrongRequest),
        (wrong_origin, ValidationError::WrongRecord),
        (wrong_stream, ValidationError::WrongRecord),
        (wrong_incarnation, ValidationError::WrongRecord),
        (wrong_schema, ValidationError::WrongRecord),
        (wrong_receiver, ValidationError::WrongRecord),
        (wrong_epoch, ValidationError::WrongRecord),
        (gap, ValidationError::Noncontiguous),
        (duplicate_id, ValidationError::WrongRecord),
        (too_many, ValidationError::BoundsExceeded),
        (too_many_bytes, ValidationError::BoundsExceeded),
        (single_oversize, ValidationError::OversizedRecord),
        (empty, ValidationError::EmptyPage),
        (overshoot_target, ValidationError::Noncontiguous),
    ];
    for (change, expected) in cases {
        let s = scope("r");
        let mut source = Malformed {
            inner: source(&["a", "b"]),
            change: *change,
        };
        let mut auth = MemoryAuthorizer::allowed(s.clone());
        let mut store = MemoryStore::new();
        let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
        assert_eq!(
            step(&mut pass, limits(), &mut auth, &mut source, &mut store),
            Err(SyncError::Validation(expected.clone()))
        );
        assert_eq!(
            (
                pass.position(),
                position(&store, "r"),
                store.apply_calls().unwrap()
            ),
            (0, 0, 0)
        );
    }
}

#[test]
fn pure_range_and_identity_validation() {
    assert_eq!(Id::new("  "), Err(ValidationError::InvalidId));
    assert_eq!(Id::new("x".repeat(129)), Err(ValidationError::InvalidId));
    assert_eq!(Limits::new(0, 1, 1), Err(ValidationError::InvalidLimits));
    assert_eq!(Limits::new(1, 0, 1), Err(ValidationError::InvalidLimits));
    assert_eq!(Limits::new(1, 1, 0), Err(ValidationError::InvalidLimits));
    let s = scope("r");
    let cp = Checkpoint::new(s.clone(), 1);
    let req = PageRequest {
        scope: s.clone(),
        after: 0,
        target: 2,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let page = Page {
        request: req.clone(),
        records: vec![Record {
            position: 1,
            id: id("x"),
            scope: s,
            payload: vec![],
        }],
    };
    assert_eq!(
        validate_page(&cp, &req, page, limits()),
        Err(ValidationError::InvalidRange)
    );
    // The per-record ceiling may exceed the page ceiling; the page ceiling
    // still refuses a single record that cannot fit.
    let limits = Limits::new(2, 4, 8).unwrap();
    let req = PageRequest {
        scope: cp.scope().clone(),
        after: 1,
        target: 2,
        max_records: 2,
        max_payload_bytes: 4,
        max_record_bytes: 8,
    };
    let page = Page {
        request: req.clone(),
        records: vec![Record {
            position: 2,
            id: id("oversize"),
            scope: cp.scope().clone(),
            payload: vec![0; 5],
        }],
    };
    assert_eq!(
        validate_page(&cp, &req, page, limits),
        Err(ValidationError::OversizedRecord)
    );
}

#[test]
fn failed_apply_and_uncertain_commit_reload_durable_progress() {
    let s = scope("r");
    let mut source = source(&["a", "b"]);
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    store.fail_next_apply(StoreError::Failed);
    assert_eq!(
        step(&mut pass, limits(), &mut auth, &mut source, &mut store),
        Err(SyncError::Store(StoreError::Failed))
    );
    assert_eq!(
        (
            position(&store, "r"),
            records(&store, "r").len(),
            pass.position()
        ),
        (0, 0, 0)
    );
    store.fail_next_apply(StoreError::Uncertain);
    assert_eq!(
        step(&mut pass, limits(), &mut auth, &mut source, &mut store),
        Err(SyncError::Store(StoreError::Uncertain))
    );
    assert_eq!(
        (
            position(&store, "r"),
            records(&store, "r").len(),
            pass.position()
        ),
        (2, 2, 0)
    );
    let next = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    assert!(next.is_complete());
    assert_eq!(store.apply_calls().unwrap(), 2);
}

#[test]
fn competing_plans_and_id_conflict_are_atomic() {
    let s = scope("r");
    let mut source = source(&["a", "b"]);
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut first = MemoryStore::new();
    let mut second = first.clone();
    let mut p1 = begin_pass(&s, &mut auth, &mut source, &mut first).unwrap();
    let mut p2 = begin_pass(&s, &mut auth, &mut source, &mut second).unwrap();
    step(&mut p1, limits(), &mut auth, &mut source, &mut first).unwrap();
    // Exact repeat of one already-committed plan is harmless.
    assert!(step(&mut p2, limits(), &mut auth, &mut source, &mut second).unwrap());
    assert_eq!(records(&first, "r").len(), 2);
    source
        .append(SourceFact {
            id: id("fact-2"),
            payload: b"c".to_vec(),
        })
        .unwrap();
    let mut p3 = begin_pass(&s, &mut auth, &mut source, &mut first).unwrap();
    let mut p4 = begin_pass(&s, &mut auth, &mut source, &mut second).unwrap();
    step(&mut p3, limits(), &mut auth, &mut source, &mut first).unwrap();
    assert_eq!(
        step(&mut p4, limits(), &mut auth, &mut source, &mut second),
        Ok(true)
    );
    // A stale plan with a different next position is refused by CAS.
    let cp = Checkpoint::new(s.clone(), 2);
    let req = PageRequest {
        scope: s.clone(),
        after: 2,
        target: 4,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let page = Page {
        request: req.clone(),
        records: vec![
            Record {
                position: 3,
                id: id("new-id"),
                scope: s.clone(),
                payload: b"c".to_vec(),
            },
            Record {
                position: 4,
                id: id("fact-0"),
                scope: s.clone(),
                payload: b"x".to_vec(),
            },
        ],
    };
    let plan = validate_page(&cp, &req, page, limits()).unwrap();
    assert_eq!(first.apply(plan), Err(StoreError::Stale));
    assert_eq!(position(&first, "r"), 3);
    let cp = Checkpoint::new(s.clone(), 3);
    let req = PageRequest {
        scope: s.clone(),
        after: 3,
        target: 4,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let page = Page {
        request: req.clone(),
        records: vec![Record {
            position: 4,
            id: id("fact-0"),
            scope: s,
            payload: b"x".to_vec(),
        }],
    };
    assert_eq!(
        first.apply(validate_page(&cp, &req, page, limits()).unwrap()),
        Err(StoreError::ConflictingRecord)
    );
    assert_eq!((position(&first, "r"), records(&first, "r").len()), (3, 3));
}

#[test]
fn source_behind_and_unknown_schema_refuse_without_apply() {
    let s = scope("r");
    let mut source = source(&["a"]);
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    let mut shorter = MemorySource::new(
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema-v1"),
    );
    assert_eq!(
        begin_pass(&s, &mut auth, &mut shorter, &mut store),
        Err(SyncError::SourceBehindCheckpoint)
    );
    let unknown = Scope::new(
        s.receiver().clone(),
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        id("schema-unknown"),
        s.access_epoch().clone(),
    );
    let mut empty = MemoryStore::new();
    let mut policy = MemoryAuthorizer::allowed(unknown.clone());
    assert_eq!(
        begin_pass(&unknown, &mut policy, &mut source, &mut empty),
        Err(SyncError::Source(SourceError::IdentityChanged))
    );
    assert_eq!(empty.apply_calls().unwrap(), 0);
}

#[test]
fn oversized_source_record_and_mid_pass_denial_preserve_progress() {
    let s = scope("r");
    let mut source = source(&["a", "too-large"]);
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    assert!(!step(
        &mut pass,
        Limits::new(1, 4, 8).unwrap(),
        &mut auth,
        &mut source,
        &mut store
    )
    .unwrap());
    assert_eq!((position(&store, "r"), records(&store, "r").len()), (1, 1));
    assert_eq!(
        step(
            &mut pass,
            Limits::new(1, 4, 8).unwrap(),
            &mut auth,
            &mut source,
            &mut store
        ),
        Err(SyncError::Source(SourceError::OversizedRecord))
    );
    assert_eq!((position(&store, "r"), records(&store, "r").len()), (1, 1));
    let pages = source.page_reads;
    auth.decision = Access::Unverifiable;
    assert_eq!(
        step(&mut pass, limits(), &mut auth, &mut source, &mut store),
        Err(SyncError::Unverifiable)
    );
    assert_eq!(source.page_reads, pages);
    assert_eq!(position(&store, "r"), 1);
}

#[test]
fn replay_conflicting_id_in_saved_window_is_typed_and_atomic() {
    let s = scope("r");
    let mut source = source(&["a"]);
    let req = PageRequest {
        scope: s.clone(),
        after: 0,
        target: 1,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let cp = Checkpoint::new(s.clone(), 0);
    let original = source.page(&req).unwrap();
    let plan = validate_page(&cp, &req, original.clone(), limits()).unwrap();
    let mut store = MemoryStore::new();
    store.apply(plan.clone()).unwrap();
    store.apply(plan).unwrap();
    let mut conflict = original;
    conflict.records[0].payload = b"wrong".to_vec();
    let conflicting = validate_page(&cp, &req, conflict, limits()).unwrap();
    assert_eq!(store.apply(conflicting), Err(StoreError::ConflictingRecord));
    assert_eq!(
        (position(&store, "r"), records(&store, "r")),
        (1, vec![b"a".to_vec()])
    );
}

#[test]
fn saved_scope_mismatch_preserves_real_progress_and_blocks_source_read() {
    let old = scope("r");
    let mut source = source(&["a"]);
    let mut auth = MemoryAuthorizer::allowed(old.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&old, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    let prior_reads = (source.head_reads, source.page_reads);
    for changed in [
        Scope::new(
            old.receiver().clone(),
            old.origin().clone(),
            old.stream().clone(),
            old.incarnation().clone(),
            old.schema().clone(),
            id("epoch-2"),
        ),
        Scope::new(
            old.receiver().clone(),
            old.origin().clone(),
            old.stream().clone(),
            id("incarnation-2"),
            old.schema().clone(),
            old.access_epoch().clone(),
        ),
        Scope::new(
            old.receiver().clone(),
            old.origin().clone(),
            old.stream().clone(),
            old.incarnation().clone(),
            id("schema-v2"),
            old.access_epoch().clone(),
        ),
    ] {
        auth.decision = Access::Allowed(changed.clone());
        assert_eq!(
            begin_pass(&changed, &mut auth, &mut source, &mut store),
            Err(SyncError::Store(StoreError::ScopeMismatch {
                saved: Box::new(old.clone()),
                requested: Box::new(changed.clone())
            }))
        );
        assert_eq!((source.head_reads, source.page_reads), prior_reads);
        assert_eq!(auth.observed_scopes.last(), Some(&changed));
        let req = PageRequest {
            scope: changed.clone(),
            after: 0,
            target: 1,
            max_records: 1,
            max_payload_bytes: 8,
            max_record_bytes: 8,
        };
        let page = Page {
            request: req.clone(),
            records: vec![Record {
                position: 1,
                id: id("new-epoch-fact"),
                scope: changed.clone(),
                payload: b"z".to_vec(),
            }],
        };
        let plan =
            validate_page(&Checkpoint::new(changed.clone(), 0), &req, page, limits()).unwrap();
        assert_eq!(
            store.apply(plan),
            Err(StoreError::ScopeMismatch {
                saved: Box::new(old.clone()),
                requested: Box::new(changed.clone())
            })
        );
        assert_eq!(
            (position(&store, "r"), records(&store, "r")),
            (1, vec![b"a".to_vec()])
        );
    }
}

#[test]
fn one_receiver_can_track_two_streams_independently() {
    let a = scope("r");
    let b = Scope::new(
        a.receiver().clone(),
        a.origin().clone(),
        id("second-stream"),
        a.incarnation().clone(),
        a.schema().clone(),
        a.access_epoch().clone(),
    );
    let mut first = source(&["a", "b"]);
    let mut second = MemorySource::new(
        a.origin().clone(),
        b.stream().clone(),
        a.incarnation().clone(),
        a.schema().clone(),
    );
    second
        .append(SourceFact {
            id: id("other-id"),
            payload: b"z".to_vec(),
        })
        .unwrap();
    let mut store = MemoryStore::new();
    let mut auth_a = MemoryAuthorizer::allowed(a.clone());
    let mut auth_b = MemoryAuthorizer::allowed(b.clone());
    let mut pass_a = begin_pass(&a, &mut auth_a, &mut first, &mut store).unwrap();
    finish_pass(&mut pass_a, limits(), &mut auth_a, &mut first, &mut store).unwrap();
    let mut pass_b = begin_pass(&b, &mut auth_b, &mut second, &mut store).unwrap();
    finish_pass(&mut pass_b, limits(), &mut auth_b, &mut second, &mut store).unwrap();
    assert_eq!(store.checkpoint(&a).unwrap().unwrap().position(), 2);
    assert_eq!(store.checkpoint(&b).unwrap().unwrap().position(), 1);
    assert_eq!(store.records(&a).unwrap().len(), 2);
    assert_eq!(store.records(&b).unwrap().len(), 1);
}

#[test]
fn invalid_source_request_and_fault_after_stale_plan() {
    let s = scope("r");
    let mut source = source(&["a", "b"]);
    let bad = PageRequest {
        scope: s.clone(),
        after: 1,
        target: 1,
        max_records: 1,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    assert_eq!(source.page(&bad), Err(SourceError::InvalidRequest));
    let req_first = PageRequest {
        scope: s.clone(),
        after: 0,
        target: 1,
        max_records: 1,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let first = validate_page(
        &Checkpoint::new(s.clone(), 0),
        &req_first,
        source.page(&req_first).unwrap(),
        limits(),
    )
    .unwrap();
    let req_stale = PageRequest {
        scope: s.clone(),
        after: 0,
        target: 2,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let stale = validate_page(
        &Checkpoint::new(s.clone(), 0),
        &req_stale,
        source.page(&req_stale).unwrap(),
        limits(),
    )
    .unwrap();
    let mut store = MemoryStore::new();
    store.apply(first.clone()).unwrap();
    let req_second = PageRequest {
        scope: s.clone(),
        after: 1,
        target: 2,
        max_records: 1,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let second = validate_page(
        &Checkpoint::new(s.clone(), 1),
        &req_second,
        source.page(&req_second).unwrap(),
        limits(),
    )
    .unwrap();
    store.fail_next_apply(StoreError::Failed);
    assert_eq!(store.apply(stale), Err(StoreError::Stale));
    store.apply(first).unwrap(); // exact replay also retains the injected fault
    assert_eq!(store.apply(second.clone()), Err(StoreError::Failed));
    assert_eq!(store.checkpoint(&s).unwrap().unwrap().position(), 1);
    store.apply(second).unwrap();
    assert_eq!(store.checkpoint(&s).unwrap().unwrap().position(), 2);
}

#[test]
fn source_refuses_single_record_limit_before_transferring_payload() {
    let s = scope("r");
    let mut source = source(&["sixsix"]);
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&s, &mut auth, &mut source, &mut store).unwrap();
    assert_eq!(
        step(
            &mut pass,
            Limits::new(10, 1000, 4).unwrap(),
            &mut auth,
            &mut source,
            &mut store
        ),
        Err(SyncError::Source(SourceError::OversizedRecord))
    );
    assert_eq!(
        (
            source.page_reads,
            source.payload_bytes,
            store.apply_calls().unwrap(),
            pass.position()
        ),
        (1, 0, 0, 0)
    );
    assert!(store.checkpoint(&s).unwrap().is_none());
}

#[test]
fn dense_page_cannot_cross_captured_target() {
    let s = scope("r");
    let cp = Checkpoint::new(s.clone(), 0);
    let request = PageRequest {
        scope: s.clone(),
        after: 0,
        target: 1,
        max_records: 2,
        max_payload_bytes: 8,
        max_record_bytes: 8,
    };
    let page = Page {
        request: request.clone(),
        records: vec![
            Record {
                position: 1,
                id: id("one"),
                scope: s.clone(),
                payload: b"a".to_vec(),
            },
            Record {
                position: 2,
                id: id("two"),
                scope: s,
                payload: b"b".to_vec(),
            },
        ],
    };
    assert_eq!(
        validate_page(&cp, &request, page, limits()),
        Err(ValidationError::Noncontiguous)
    );
}

struct ForeignCheckpoint(Checkpoint, usize);
impl ReplicaStore for ForeignCheckpoint {
    fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        Ok(Some(self.0.clone()))
    }
    fn apply(&mut self, _: nessa_sync::replication::domain::CommitPlan) -> Result<(), StoreError> {
        self.1 += 1;
        Err(StoreError::Failed)
    }
}

#[test]
fn begin_pass_defensively_refuses_foreign_checkpoint_from_store_port() {
    let requested = scope("r");
    let saved = scope("another-receiver");
    let mut store = ForeignCheckpoint(Checkpoint::new(saved.clone(), 3), 0);
    let mut source = source(&["a"]);
    let mut auth = MemoryAuthorizer::allowed(requested.clone());
    assert_eq!(
        begin_pass(&requested, &mut auth, &mut source, &mut store),
        Err(SyncError::Store(StoreError::ScopeMismatch {
            saved: Box::new(saved),
            requested: Box::new(requested)
        }))
    );
    assert_eq!(
        (source.head_reads, source.page_reads, auth.checks, store.1),
        (0, 0, 1, 0)
    );
}

#[test]
fn revoked_or_unverifiable_access_precedes_saved_scope_mismatch() {
    let saved = scope("r");
    let requested = Scope::new(
        saved.receiver().clone(),
        saved.origin().clone(),
        saved.stream().clone(),
        saved.incarnation().clone(),
        saved.schema().clone(),
        id("epoch-2"),
    );
    let mut source = source(&["a"]);
    let mut auth = MemoryAuthorizer::allowed(saved.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&saved, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut store).unwrap();
    let prior_reads = (source.head_reads, source.page_reads);
    let prior_checks = auth.checks;
    let prior_observations = auth.observed_scopes.len();
    for (decision, expected) in [
        (Access::Denied, SyncError::Denied),
        (Access::Unverifiable, SyncError::Unverifiable),
    ] {
        auth.decision = decision;
        assert_eq!(
            begin_pass(&requested, &mut auth, &mut source, &mut store),
            Err(expected)
        );
        assert_eq!((source.head_reads, source.page_reads), prior_reads);
        assert_eq!(store.checkpoint(&saved).unwrap().unwrap().position(), 1);
        assert_eq!(store.records(&saved).unwrap()[0].payload, b"a");
    }
    assert_eq!(auth.checks, prior_checks + 2);
    assert_eq!(
        auth.observed_scopes[prior_observations..],
        [requested.clone(), requested.clone()]
    );
    auth.decision = Access::Allowed(requested.clone());
    assert_eq!(
        begin_pass(&requested, &mut auth, &mut source, &mut store),
        Err(SyncError::Store(StoreError::ScopeMismatch {
            saved: Box::new(saved),
            requested: Box::new(requested.clone())
        }))
    );
    assert_eq!(auth.observed_scopes.last(), Some(&requested));
    assert_eq!((source.head_reads, source.page_reads), prior_reads);
}

#[test]
fn mid_pass_epoch_change_refuses_next_page_before_source_read() {
    let pass_scope = scope("r");
    let epoch_two = Scope::new(
        pass_scope.receiver().clone(),
        pass_scope.origin().clone(),
        pass_scope.stream().clone(),
        pass_scope.incarnation().clone(),
        pass_scope.schema().clone(),
        id("epoch-2"),
    );
    let mut source = source(&["a", "b"]);
    let mut auth = MemoryAuthorizer::allowed(pass_scope.clone());
    let mut store = MemoryStore::new();
    let mut pass = begin_pass(&pass_scope, &mut auth, &mut source, &mut store).unwrap();
    assert!(!step(
        &mut pass,
        Limits::new(1, 8, 8).unwrap(),
        &mut auth,
        &mut source,
        &mut store
    )
    .unwrap());
    assert_eq!(
        (
            pass.position(),
            store.checkpoint(&pass_scope).unwrap().unwrap().position()
        ),
        (1, 1)
    );
    let page_reads = source.page_reads;
    let apply_calls = store.apply_calls().unwrap();
    let saved_records = store.records(&pass_scope).unwrap();
    auth.decision = Access::Allowed(epoch_two);
    assert_eq!(
        step(&mut pass, limits(), &mut auth, &mut source, &mut store),
        Err(SyncError::WrongAccessScope)
    );
    assert_eq!(source.page_reads, page_reads);
    assert_eq!(store.apply_calls().unwrap(), apply_calls);
    assert_eq!(
        (
            pass.position(),
            store.checkpoint(&pass_scope).unwrap().unwrap().position()
        ),
        (1, 1)
    );
    assert_eq!(store.records(&pass_scope).unwrap(), saved_records);
    assert_eq!(auth.observed_scopes.last(), Some(&pass_scope));
}
