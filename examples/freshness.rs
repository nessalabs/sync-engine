//! Native SQLite freshness policy/read comparison. No network latency claim.
use nessa_sync::replication::{
    application::*,
    domain::{CapabilitySupport, Checkpoint, CommitPlan, Id, Limits, Scope},
    infrastructure::{MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore},
};
use std::{
    cell::Cell,
    fs,
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};
#[derive(Clone)]
struct Time(Rc<Cell<Duration>>);
impl Clock for Time {
    fn now(&self) -> Duration {
        self.0.get()
    }
}
struct Half;
impl RetryJitter for Half {
    fn delay(&mut self, ceiling: Duration) -> Duration {
        ceiling / 2
    }
}
type Owner = RecordFreshness<Time, Half>;
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
    Limits::new(2, 100, 100).unwrap()
}
fn owner(policy: FreshnessPolicy, time: &Time) -> Owner {
    RecordFreshness::new(
        scope(),
        policy,
        CapabilitySupport::current(),
        time.clone(),
        Half,
        RetryPolicy::new(Duration::from_millis(10), Duration::from_secs(1)).unwrap(),
    )
}
struct Directory(PathBuf);
impl Directory {
    fn new(label: &str) -> Self {
        let p =
            std::env::temp_dir().join(format!("owned-freshness-{}-{label}", std::process::id()));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn source(&self) -> SqliteReferenceSource {
        SqliteReferenceSource::open(
            self.0.join("source.db"),
            id("origin"),
            id("stream"),
            id("first"),
            id("schema"),
        )
        .unwrap()
    }
    fn store(&self) -> SqliteReplicaStore {
        SqliteReplicaStore::open(self.0.join("cache.db")).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn drain(
    o: &mut Owner,
    a: &mut MemoryAuthorizer,
    s: &mut SqliteReferenceSource,
    r: &mut impl ReplicaStore,
) {
    for _ in 0..30 {
        if o.drive(limits(), a, s, r).unwrap() == FreshnessWork::Waiting {
            return;
        }
    }
    panic!("bounded lab must settle");
}
fn seed(source: &mut SqliteReferenceSource) {
    for n in 1..=7 {
        source.append(&id(&format!("fact-{n}")), b"value").unwrap();
    }
}
fn correct(store: &mut SqliteReplicaStore) {
    let rows = store.read_after(&scope(), 0, 10, 100).unwrap();
    assert_eq!(rows.len(), 7);
    for (n, row) in rows.iter().enumerate() {
        assert_eq!(row.id, id(&format!("fact-{}", n + 1)));
        assert_eq!(row.payload, b"value");
        assert_eq!(row.position, (n + 1) as u64);
    }
    assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 7);
}
fn compare(label: &str, policy: Option<FreshnessPolicy>) -> String {
    let dir = Directory::new(label);
    let mut source = dir.source();
    seed(&mut source);
    let mut store = dir.store();
    let mut auth = MemoryAuthorizer::allowed(scope());
    let time = Time(Rc::new(Cell::new(Duration::ZERO)));
    let mut o = owner(policy.unwrap_or(FreshnessPolicy::Always), &time);
    // Same cold seed, captured target, two-record pages and mandatory final recheck.
    if policy.is_some() {
        o.request(FreshnessWake::Explicit);
        drain(&mut o, &mut auth, &mut source, &mut store);
    } else {
        manual(&mut auth, &mut source, &mut store);
    }
    let cold_heads = source.head_reads();
    let cold_pages = source.page_reads();
    let before_heads = cold_heads;
    let before_pages = cold_pages;
    let start = Instant::now();
    let mut useful_reads = 0;
    for millis in 1..=100 {
        time.0.set(Duration::from_millis(millis));
        if policy.is_some() {
            let read = o
                .read(
                    ReadIntent::Fresh,
                    limits(),
                    &mut auth,
                    &mut source,
                    &mut store,
                    |store, scope| store.read_after(scope, 0, 10, 100),
                )
                .unwrap();
            assert_eq!(read.value.len(), 7);
            useful_reads += 1;
            drain(&mut o, &mut auth, &mut source, &mut store);
        } else {
            manual(&mut auth, &mut source, &mut store);
            assert_eq!(store.read_after(&scope(), 0, 10, 100).unwrap().len(), 7);
            useful_reads += 1;
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let heads = source.head_reads() - before_heads;
    let pages = source.page_reads() - before_pages;
    correct(&mut store);
    drop(store);
    let mut reopened = dir.store();
    correct(&mut reopened);
    let mut restarted = owner(FreshnessPolicy::Never, &time);
    let metadata = restarted
        .read(
            ReadIntent::Fresh,
            limits(),
            &mut auth,
            &mut source,
            &mut reopened,
            |store, scope| store.read_after(scope, 0, 10, 100),
        )
        .unwrap()
        .freshness;
    assert_eq!(metadata.last_applied_at, None);
    assert_eq!(metadata.last_head_checked_at, None);
    format!("{{\"policy\":\"{label}\",\"useful_reads\":{useful_reads},\"cold_head_checks\":{cold_heads},\"cold_pages\":{cold_pages},\"read_head_checks\":{heads},\"read_pages\":{pages},\"elapsed_ms\":{elapsed_ms:.6},\"reopened_rows\":7,\"restart_times_unknown\":true}}")
}
fn manual(a: &mut MemoryAuthorizer, s: &mut SqliteReferenceSource, r: &mut SqliteReplicaStore) {
    let mut p =
        begin_pass_with_capabilities(&scope(), CapabilitySupport::current(), a, s, r).unwrap();
    if !p.is_complete() {
        finish_pass(&mut p, limits(), a, s, r).unwrap();
        assert!(
            begin_pass_with_capabilities(&scope(), CapabilitySupport::current(), a, s, r)
                .unwrap()
                .is_complete()
        );
    }
}
/// Ordinary store result-loss fixture: underlying durable commit is real.
struct LostResult {
    inner: SqliteReplicaStore,
    lose: bool,
}
impl ReplicaStore for LostResult {
    fn load(&mut self, s: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.inner.load(s)
    }
    fn apply(&mut self, p: CommitPlan) -> Result<(), StoreError> {
        self.inner.apply(p)?;
        if self.lose {
            self.lose = false;
            Err(StoreError::Uncertain)
        } else {
            Ok(())
        }
    }
}
fn uncertainty() {
    let dir = Directory::new("uncertainty");
    let mut source = dir.source();
    seed(&mut source);
    let mut store = LostResult {
        inner: dir.store(),
        lose: true,
    };
    let mut auth = MemoryAuthorizer::allowed(scope());
    let time = Time(Rc::new(Cell::new(Duration::ZERO)));
    let mut o = owner(FreshnessPolicy::Never, &time);
    o.request(FreshnessWake::Explicit);
    o.drive(limits(), &mut auth, &mut source, &mut store)
        .unwrap();
    assert_eq!(
        o.drive(limits(), &mut auth, &mut source, &mut store),
        Err(FreshnessError::Sync(SyncError::Store(
            StoreError::Uncertain
        )))
    );
    assert_eq!(
        store
            .inner
            .checkpoint(&scope())
            .unwrap()
            .unwrap()
            .position(),
        2
    );
    assert_eq!(o.metadata().unwrap().last_applied_at, None);
    assert_eq!(o.metadata().unwrap().position, None);
    assert_eq!(
        o.drive(limits(), &mut auth, &mut source, &mut store),
        Err(FreshnessError::ReloadRequired)
    );
    drop(store);
    let mut reopened = dir.store();
    assert_eq!(reopened.count(&scope()).unwrap(), 2);
    // No invented timestamp on reload. Denial must precede progress reload.
    auth.decision = Access::Denied;
    o.resume();
    assert_eq!(
        o.drive(limits(), &mut auth, &mut source, &mut reopened),
        Err(FreshnessError::Sync(SyncError::Denied))
    );
    assert_eq!(o.metadata().unwrap().position, None);
    auth.decision = Access::Allowed(scope());
    o.resume();
    o.drive(limits(), &mut auth, &mut source, &mut reopened)
        .unwrap();
    assert_eq!(o.metadata().unwrap().position, Some(2));
    assert_eq!(o.metadata().unwrap().last_applied_at, None);
    time.0.set(Duration::from_millis(1));
    drain(&mut o, &mut auth, &mut source, &mut reopened);
    correct(&mut reopened);
    assert_eq!(source.page_reads(), 4); // lost-result page, then remaining 3; no duplicate retry.
    drop(reopened);
    correct(&mut dir.store());
}
fn main() {
    uncertainty();
    let rows = [
        compare("manual-every-read", None),
        compare("always", Some(FreshnessPolicy::Always)),
        compare(
            "within-10ms",
            Some(FreshnessPolicy::Within(Duration::from_millis(10))),
        ),
        compare("never", Some(FreshnessPolicy::Never)),
    ];
    println!("{{\"transport\":\"native SQLite; no network\",\"clock\":\"injected 1ms steps; strictly greater than 10ms TTL\",\"workload\":\"seven records; 100 stable useful reads\",\"uncertain_commit_reopen_correct\":true,\"rows\":[{}]}}",rows.join(","));
}
#[cfg(test)]
mod tests {
    #[test]
    fn uncertain_commit_reopens_and_resumes_from_actual_checkpoint() {
        super::uncertainty();
    }
}
