//! Ordinary policy/lifecycle contracts against the actual record follower/store.
use nessa_sync::replication::{
    application::*,
    domain::{CapabilitySupport, Id, Limits, Scope},
    infrastructure::{MemoryAuthorizer, MemorySource, MemoryStore, SourceFact},
};
use std::{cell::Cell, rc::Rc, time::Duration};
#[derive(Clone)]
struct Time(Rc<Cell<Duration>>);
impl Time {
    fn new() -> Self {
        Self(Rc::new(Cell::new(Duration::ZERO)))
    }
    fn set(&self, millis: u64) {
        self.0.set(Duration::from_millis(millis));
    }
}
impl Clock for Time {
    fn now(&self) -> Duration {
        self.0.get()
    }
}
struct Ceiling;
impl RetryJitter for Ceiling {
    fn delay(&mut self, ceiling: Duration) -> Duration {
        ceiling
    }
}
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("reader"),
        id("origin"),
        id("stream"),
        id("first"),
        id("schema"),
        id("epoch"),
    )
}
fn limits() -> Limits {
    Limits::new(1, 100, 100).unwrap()
}
fn source(n: usize) -> MemorySource {
    let mut s = MemorySource::new(id("origin"), id("stream"), id("first"), id("schema"));
    for i in 0..n {
        s.append(SourceFact {
            id: id(&format!("fact-{i}")),
            payload: b"value".to_vec(),
        })
        .unwrap();
    }
    s
}
fn owner(policy: FreshnessPolicy, time: &Time) -> RecordFreshness<Time, Ceiling> {
    RecordFreshness::new(
        scope(),
        policy,
        CapabilitySupport::current(),
        time.clone(),
        Ceiling,
        RetryPolicy::new(Duration::from_millis(10), Duration::from_millis(40)).unwrap(),
    )
}
fn read(
    o: &mut RecordFreshness<Time, Ceiling>,
    intent: ReadIntent,
    a: &mut MemoryAuthorizer,
    s: &mut MemorySource,
    r: &mut MemoryStore,
) -> Result<CachedRead<usize>, FreshnessError> {
    o.read(intent, limits(), a, s, r, |r, scope| {
        Ok(r.records(scope)?.len())
    })
}
fn drain(
    o: &mut RecordFreshness<Time, Ceiling>,
    a: &mut MemoryAuthorizer,
    s: &mut MemorySource,
    r: &mut MemoryStore,
) {
    for _ in 0..20 {
        if o.drive(limits(), a, s, r).unwrap() == FreshnessWork::Waiting {
            return;
        }
    }
    panic!("must finish a bounded fixture");
}
#[test]
fn never_is_passive_and_all_advisory_wakes_coalesce_without_head_progress() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Never, &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(2);
    let mut r = MemoryStore::new();
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .value,
        0
    );
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Ok(FreshnessWork::Waiting)
    );
    assert_eq!((s.head_reads, s.page_reads), (0, 0));
    for wake in [
        FreshnessWake::Explicit,
        FreshnessWake::Hint,
        FreshnessWake::Reconnect,
    ] {
        o.request(wake);
    }
    time.set(1);
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Ok(FreshnessWork::Checked)
    );
    assert!(r.checkpoint(&scope()).unwrap().is_none());
    let m = o.metadata().unwrap();
    assert_eq!(m.last_head_checked_at, Some(Duration::from_millis(1)));
    assert_eq!(m.last_applied_at, None);
    for _ in 0..100 {
        o.request(FreshnessWake::Hint);
    }
    time.set(2);
    o.drive(limits(), &mut a, &mut s, &mut r).unwrap();
    s.append(SourceFact {
        id: id("fact-2"),
        payload: b"next".to_vec(),
    })
    .unwrap();
    time.set(3);
    o.drive(limits(), &mut a, &mut s, &mut r).unwrap();
    assert_eq!(
        (
            s.head_reads,
            r.checkpoint(&scope()).unwrap().unwrap().position()
        ),
        (1, 2)
    );
    assert_eq!(o.metadata().unwrap().phase, FreshnessPhase::Recheck);
    time.set(4);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!((s.head_reads, s.page_reads), (3, 3));
    assert_eq!(r.records(&scope()).unwrap().len(), 3);
    assert_eq!(o.metadata().unwrap().phase, FreshnessPhase::Idle);
}
#[test]
fn within_never_calls_ports_on_read_and_exact_ttl_boundary_reuses_success() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Within(Duration::from_millis(10)), &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(0);
    let mut r = MemoryStore::new();
    let m = read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
        .unwrap()
        .freshness;
    assert!(m.check_pending);
    assert_eq!(s.head_reads, 0);
    assert_eq!(a.checks, 0);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.head_reads, 1);
    time.set(10);
    assert!(
        !read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .freshness
            .check_pending
    );
    time.set(11);
    a.decision = Access::Unverifiable;
    for _ in 0..100 {
        assert!(
            read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
                .unwrap()
                .freshness
                .check_pending
        );
    }
    assert_eq!((s.head_reads, a.checks), (1, 1));
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::Sync(SyncError::Unverifiable))
    );
    let m = read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
        .unwrap()
        .freshness;
    assert!(m.check_failed);
    assert_eq!(m.last_head_checked_at, Some(Duration::ZERO));
    assert_eq!(m.retry_at, Some(Duration::from_millis(21)));
    a.decision = Access::Allowed(scope());
    time.set(20);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.head_reads, 1);
    time.set(21);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.head_reads, 2);
    assert!(!o.metadata().unwrap().check_failed);
}
#[test]
fn always_checks_before_fresh_local_read_and_preserves_refusal_and_backoff() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Always, &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(1);
    let mut r = MemoryStore::new();
    assert_eq!(
        read(&mut o, ReadIntent::Cached, &mut a, &mut s, &mut r)
            .unwrap()
            .value,
        0
    );
    assert_eq!(s.head_reads, 0);
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .value,
        0
    );
    assert_eq!(s.head_reads, 1);
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r),
        Err(FreshnessError::Deferred)
    );
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.head_reads, 2);
    a.decision = Access::Unverifiable;
    let err = o.read(
        ReadIntent::Fresh,
        limits(),
        &mut a,
        &mut s,
        &mut r,
        |_, _| -> Result<(), StoreError> { panic!("failed fresh check cannot deliver") },
    );
    assert_eq!(err, Err(FreshnessError::Sync(SyncError::Unverifiable)));
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r),
        Err(FreshnessError::Deferred)
    );
    time.set(10);
    a.decision = Access::InvalidResponse;
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r),
        Err(FreshnessError::Sync(SyncError::InvalidAccessResponse))
    );
    for wake in [FreshnessWake::Hint, FreshnessWake::Reconnect] {
        o.request(wake);
    }
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::Stopped)
    );
    a.decision = Access::Allowed(scope());
    o.resume();
    assert_eq!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .value,
        1
    );
}
#[test]
fn safe_apply_failure_retries_same_finite_pass_but_uncertainty_requires_reload() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Never, &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(2);
    let mut r = MemoryStore::new();
    o.request(FreshnessWake::Explicit);
    o.drive(limits(), &mut a, &mut s, &mut r).unwrap();
    r.fail_next_apply(StoreError::Failed);
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::Sync(SyncError::Store(StoreError::Failed)))
    );
    assert!(r.checkpoint(&scope()).unwrap().is_none());
    assert_eq!(o.metadata().unwrap().last_applied_at, None);
    time.set(9);
    o.request(FreshnessWake::Reconnect);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.page_reads, 1);
    time.set(10);
    o.drive(limits(), &mut a, &mut s, &mut r).unwrap();
    assert_eq!(
        o.metadata().unwrap().last_applied_at,
        Some(Duration::from_millis(10))
    );
    r.fail_next_apply(StoreError::Uncertain);
    time.set(11);
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::Sync(SyncError::Store(
            StoreError::Uncertain
        )))
    );
    assert_eq!(r.checkpoint(&scope()).unwrap().unwrap().position(), 2);
    let m = o.metadata().unwrap();
    assert_eq!(m.last_applied_at, None);
    assert_eq!(m.position, None);
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::ReloadRequired)
    );
    let pages = s.page_reads;
    a.decision = Access::Denied;
    o.resume();
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::Sync(SyncError::Denied))
    );
    assert_eq!(o.metadata().unwrap().position, None);
    a.decision = Access::Allowed(scope());
    o.resume();
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(s.page_reads, pages);
    assert_eq!(o.metadata().unwrap().position, Some(2));
    assert_eq!(o.metadata().unwrap().last_applied_at, None);
    assert_eq!(r.records(&scope()).unwrap().len(), 2);
}
#[test]
fn scope_restart_and_clock_regression_never_reuse_observation_times_or_escape_retry() {
    let time = Time::new();
    time.set(100);
    let mut o = owner(FreshnessPolicy::Within(Duration::from_millis(5)), &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(1);
    let mut r = MemoryStore::new();
    o.request(FreshnessWake::Explicit);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(
        o.metadata().unwrap().last_applied_at,
        Some(Duration::from_millis(100))
    );
    let mut restarted = owner(FreshnessPolicy::Never, &time);
    let m = read(&mut restarted, ReadIntent::Fresh, &mut a, &mut s, &mut r).unwrap();
    assert_eq!(m.value, 1);
    assert_eq!(m.freshness.last_applied_at, None);
    assert_eq!(m.freshness.last_head_checked_at, None);
    a.decision = Access::Unverifiable;
    o.request(FreshnessWake::Explicit);
    o.drive(limits(), &mut a, &mut s, &mut r).unwrap_err();
    time.set(1);
    let m = o.metadata().unwrap();
    assert_eq!(m.clock_generation, 1);
    assert_eq!(m.last_applied_at, None);
    assert_eq!(m.last_head_checked_at, None);
    assert_eq!(m.retry_at, Some(Duration::from_millis(11)));
    a.decision = Access::Allowed(scope());
    time.set(10);
    drain(&mut o, &mut a, &mut s, &mut r);
    assert_eq!(
        o.metadata().unwrap().retry_at,
        Some(Duration::from_millis(11))
    );
    time.set(11);
    drain(&mut o, &mut a, &mut s, &mut r);
    for s2 in [
        Scope::new(
            id("reader"),
            id("origin"),
            id("stream"),
            id("first"),
            id("schema"),
            id("epoch2"),
        ),
        Scope::new(
            id("reader"),
            id("origin"),
            id("stream"),
            id("second"),
            id("schema"),
            id("epoch"),
        ),
    ] {
        o.reset_scope(s2.clone());
        let m = o.metadata().unwrap();
        assert_eq!(m.scope, s2);
        assert_eq!(m.position, None);
        assert_eq!(m.last_applied_at, None);
        assert_eq!(m.last_head_checked_at, None);
        assert!(!m.check_pending);
    }
}
#[test]
fn retry_window_grows_is_bounded_and_clock_range_is_explicit() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Never, &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    a.decision = Access::Unverifiable;
    let mut s = source(0);
    let mut r = MemoryStore::new();
    o.request(FreshnessWake::Explicit);
    for (now, deadline) in [(0, 10), (10, 30), (30, 70), (70, 110)] {
        time.set(now);
        assert!(o.drive(limits(), &mut a, &mut s, &mut r).is_err());
        assert_eq!(
            o.metadata().unwrap().retry_at,
            Some(Duration::from_millis(deadline))
        );
    }
    time.0.set(Duration::MAX);
    assert_eq!(
        o.drive(limits(), &mut a, &mut s, &mut r),
        Err(FreshnessError::ClockRange)
    );
    assert_eq!(o.metadata().unwrap().phase, FreshnessPhase::Stopped);
    assert_eq!(
        RetryPolicy::new(Duration::ZERO, Duration::from_secs(1)),
        Err(FreshnessError::InvalidRetry)
    );
}
#[test]
fn zero_ttl_is_still_local_and_requires_positive_elapsed_time() {
    let time = Time::new();
    let mut o = owner(FreshnessPolicy::Within(Duration::ZERO), &time);
    let mut a = MemoryAuthorizer::allowed(scope());
    let mut s = source(0);
    let mut r = MemoryStore::new();
    read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r).unwrap();
    drain(&mut o, &mut a, &mut s, &mut r);
    assert!(
        !read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .freshness
            .check_pending
    );
    time.set(1);
    assert!(
        read(&mut o, ReadIntent::Fresh, &mut a, &mut s, &mut r)
            .unwrap()
            .freshness
            .check_pending
    );
    assert_eq!(s.head_reads, 1);
}

struct UnavailableHead(MemorySource);
impl RecordSource for UnavailableHead {
    fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
        Err(SourceError::Unavailable)
    }
    fn page(
        &mut self,
        request: &nessa_sync::replication::domain::PageRequest,
    ) -> Result<nessa_sync::replication::domain::Page, SourceError> {
        self.0.page(request)
    }
}
impl RecordHeadSource for UnavailableHead {
    fn source_head(
        &mut self,
        _: &Scope,
    ) -> Result<nessa_sync::replication::domain::SourceHead, SourceError> {
        self.0.head_reads += 1;
        Err(SourceError::Unavailable)
    }
}
struct Sample(Duration);
impl RetryJitter for Sample {
    fn delay(&mut self, _: Duration) -> Duration {
        self.0
    }
}
#[test]
fn unavailable_source_is_retryable_and_injected_jitter_cannot_escape_bounds() {
    for (sample, expected) in [(Duration::ZERO, 5), (Duration::MAX, 10)] {
        let time = Time::new();
        let mut o = RecordFreshness::new(
            scope(),
            FreshnessPolicy::Always,
            CapabilitySupport::current(),
            time.clone(),
            Sample(sample),
            RetryPolicy::new(Duration::from_millis(10), Duration::from_millis(40)).unwrap(),
        );
        let mut a = MemoryAuthorizer::allowed(scope());
        let mut s = UnavailableHead(source(0));
        let mut r = MemoryStore::new();
        assert_eq!(
            o.read(
                ReadIntent::Fresh,
                limits(),
                &mut a,
                &mut s,
                &mut r,
                |_, _| -> Result<(), StoreError> {
                    panic!("source unavailable must refuse fresh delivery")
                }
            ),
            Err(FreshnessError::Sync(SyncError::Source(
                SourceError::Unavailable
            )))
        );
        let m = o.metadata().unwrap();
        assert!(m.check_failed);
        assert_eq!(m.last_head_checked_at, None);
        assert_eq!(m.retry_at, Some(Duration::from_millis(expected)));
        time.set(expected - 1);
        assert_eq!(
            o.drive(limits(), &mut a, &mut s, &mut r),
            Ok(FreshnessWork::Waiting)
        );
        assert_eq!(s.0.head_reads, 1);
        time.set(expected);
        assert!(o.drive(limits(), &mut a, &mut s, &mut r).is_err());
        assert_eq!(s.0.head_reads, 2);
        assert!(r.checkpoint(&scope()).unwrap().is_none());
    }
}
