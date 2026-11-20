//! Concrete host composition. One connection lifecycle, no retry/pass/checkpoint state.
use super::fixture::{self, err, Error};
use nessa_sync::replication::{
    application::*,
    domain::{
        CapabilitySupport, Checkpoint, CommitPlan, Page, PageRequest, Record, Scope, SourceHead,
    },
    infrastructure::{
        session::{
            connect_follow, CapabilityFollowSource, FollowAuthorizer, FollowError, FollowSession,
            HintWake, TcpSessionIo, TcpSessionWriter,
        },
        SqliteReplicaStore, WireCounters,
    },
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::{Duration, Instant},
};
#[derive(Clone)]
pub struct Time {
    manual: Option<Rc<Cell<Duration>>>,
    start: Instant,
}
impl Time {
    pub fn new(logical: bool) -> Self {
        Self {
            manual: logical.then(|| Rc::new(Cell::new(Duration::ZERO))),
            start: Instant::now(),
        }
    }
    pub fn advance(&self, d: Duration) {
        if let Some(t) = &self.manual {
            t.set(t.get().saturating_add(d));
        } else {
            std::thread::sleep(d);
        }
    }
    pub fn logical(&self) -> bool {
        self.manual.is_some()
    }
}
impl Clock for Time {
    fn now(&self) -> Duration {
        self.manual
            .as_ref()
            .map_or_else(|| self.start.elapsed(), |t| t.get())
    }
}
pub struct Half(pub Rc<Cell<usize>>);
impl RetryJitter for Half {
    fn delay(&mut self, ceiling: Duration) -> Duration {
        self.0.set(self.0.get() + 1);
        ceiling / 2
    }
}
struct Live {
    root: FollowSession<TcpSessionWriter>,
    authorizer: FollowAuthorizer<TcpSessionWriter>,
    source: CapabilityFollowSource<TcpSessionWriter>,
}
fn add(a: &mut WireCounters, b: WireCounters) {
    a.exchanges += b.exchanges;
    a.protocol_bytes += b.protocol_bytes;
    a.payload_bytes += b.payload_bytes;
    a.duplicate_bytes += b.duplicate_bytes;
    a.head_checks += b.head_checks;
}
pub struct Link {
    address: String,
    live: Option<Live>,
    retired: WireCounters,
    pub dials: usize,
    pub connections: usize,
    pub authorizations: usize,
    pub heads: usize,
    pub pages: Vec<(u64, u64)>,
}
impl Link {
    pub fn new(address: String) -> Self {
        Self {
            address,
            live: None,
            retired: WireCounters::default(),
            dials: 0,
            connections: 0,
            authorizations: 0,
            heads: 0,
            pages: Vec::new(),
        }
    }
    fn ensure(&mut self) -> Result<(), FollowError> {
        if self.live.is_some() {
            return Ok(());
        }
        self.dials += 1;
        let socket = fixture::dial(&self.address, b"dev-read-secret")?;
        let io = TcpSessionIo::new(socket).map_err(|_| FollowError::Unavailable)?;
        let root = connect_follow(
            io,
            fixture::scope(),
            fixture::limits(),
            Duration::from_secs(20),
        )?;
        let (authorizer, source) = root.capability_parts(CapabilitySupport::current());
        self.live = Some(Live {
            root,
            authorizer,
            source,
        });
        self.connections += 1;
        Ok(())
    }
    pub fn retire(&mut self) {
        if let Some(live) = self.live.take() {
            if let Ok(c) = live.root.counters() {
                add(&mut self.retired, c.wire);
            }
            let _ = live.root.close();
        }
    }
    pub fn counters(&self) -> WireCounters {
        let mut total = self.retired;
        if let Some(live) = &self.live {
            if let Ok(c) = live.root.counters() {
                add(&mut total, c.wire);
            }
        }
        total
    }
    pub fn hint(&self) -> Result<bool, Error> {
        match &self.live {
            Some(live) => Ok(matches!(
                live.root.hints().wait(Duration::ZERO).map_err(err)?,
                HintWake::Changed { .. }
            )),
            None => Ok(false),
        }
    }
    pub fn pending(&self, head: u64) -> Result<(), Error> {
        let live = self.live.as_ref().ok_or("no live hint receiver")?;
        let until = Instant::now() + Duration::from_secs(5);
        while live.root.hints().stats().map_err(err)?.pending_head != Some(head) {
            if Instant::now() >= until {
                return Err("owned hint gate".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
}
impl Drop for Link {
    fn drop(&mut self) {
        self.retire();
    }
}
#[derive(Clone)]
pub struct Authorizer(pub Rc<RefCell<Link>>);
impl ScopeAuthorizer for Authorizer {
    fn authorize(&mut self, scope: &Scope) -> Access {
        if scope != &fixture::scope() {
            return Access::IdentityChanged;
        }
        let mut link = self.0.borrow_mut();
        link.authorizations += 1;
        let result = match link.ensure() {
            Ok(()) => link.live.as_mut().unwrap().authorizer.authorize(scope),
            Err(FollowError::Refused(SourceError::Denied)) => Access::Denied,
            Err(FollowError::Refused(SourceError::IdentityChanged)) => Access::IdentityChanged,
            Err(FollowError::Protocol) => Access::InvalidResponse,
            Err(FollowError::Refused(
                SourceError::InvalidRequest
                | SourceError::OversizedRecord
                | SourceError::Pruned
                | SourceError::Incompatible(_),
            )) => Access::InvalidResponse,
            Err(_) => Access::Unverifiable,
        };
        if !matches!(result, Access::Allowed(_)) {
            link.retire();
        }
        result
    }
}
#[derive(Clone)]
pub struct Source(pub Rc<RefCell<Link>>);
impl RecordHeadSource for Source {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        let mut link = self.0.borrow_mut();
        link.heads += 1;
        let result = link
            .live
            .as_mut()
            .ok_or(SourceError::Unavailable)?
            .source
            .source_head(scope);
        if result.is_err() {
            link.retire();
        }
        result
    }
}
impl RecordSource for Source {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.source_head(scope).map(|h| h.position())
    }
    fn page(&mut self, r: &PageRequest) -> Result<Page, SourceError> {
        let mut link = self.0.borrow_mut();
        link.pages.push((r.after, r.target));
        let result = link
            .live
            .as_mut()
            .ok_or(SourceError::Unavailable)?
            .source
            .page(r);
        if result.is_err() {
            link.retire();
        }
        result
    }
}
pub struct Store {
    pub inner: SqliteReplicaStore,
    pub lose: bool,
    pub fail: bool,
    pub apply_ms: Vec<f64>,
    pub loads: usize,
}
impl ReplicaStore for Store {
    fn load(&mut self, s: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.loads += 1;
        self.inner.load(s)
    }
    fn apply(&mut self, p: CommitPlan) -> Result<(), StoreError> {
        if self.fail {
            self.fail = false;
            return Err(StoreError::Failed);
        }
        let started = Instant::now();
        self.inner.apply(p)?;
        self.apply_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        if self.lose {
            self.lose = false;
            Err(StoreError::Uncertain)
        } else {
            Ok(())
        }
    }
}
pub struct Host {
    pub owner: RecordFreshness<Time, Half>,
    pub time: Time,
    pub jitter: Rc<Cell<usize>>,
    pub link: Rc<RefCell<Link>>,
    pub a: Authorizer,
    pub s: Source,
    pub store: Store,
    pub check_ms: Vec<f64>,
    pub drive_ms: Vec<f64>,
    pub projections: usize,
    pub local_ms: Vec<f64>,
}
impl Host {
    pub fn new(
        address: String,
        path: &std::path::Path,
        policy: FreshnessPolicy,
        logical: bool,
    ) -> Result<Self, Error> {
        let time = Time::new(logical);
        let jitter = Rc::new(Cell::new(0));
        let link = Rc::new(RefCell::new(Link::new(address)));
        Ok(Self {
            owner: RecordFreshness::new(
                fixture::scope(),
                policy,
                CapabilitySupport::current(),
                time.clone(),
                Half(jitter.clone()),
                RetryPolicy::new(Duration::from_millis(100), Duration::from_millis(400))
                    .map_err(err)?,
            ),
            time,
            jitter,
            a: Authorizer(link.clone()),
            s: Source(link.clone()),
            link,
            store: Store {
                inner: SqliteReplicaStore::open(path)?,
                lose: false,
                fail: false,
                apply_ms: Vec::new(),
                loads: 0,
            },
            check_ms: Vec::new(),
            drive_ms: Vec::new(),
            projections: 0,
            local_ms: Vec::new(),
        })
    }
    pub fn read(
        &mut self,
        intent: ReadIntent,
        records: usize,
    ) -> Result<CachedRead<Vec<Record>>, FreshnessError> {
        let projections = &mut self.projections;
        let local_ms = &mut self.local_ms;
        self.owner.read(
            intent,
            fixture::budget(records),
            &mut self.a,
            &mut self.s,
            &mut self.store,
            |store, scope| {
                *projections += 1;
                let start = Instant::now();
                let result = store.inner.read_after(scope, 0, 64, MAX_RECORD_BYTES);
                local_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                result
            },
        )
    }
    pub fn drive(&mut self, records: usize) -> Result<FreshnessWork, FreshnessError> {
        let started = Instant::now();
        let r = self.owner.drive(
            fixture::budget(records),
            &mut self.a,
            &mut self.s,
            &mut self.store,
        );
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        if r == Ok(FreshnessWork::Checked) {
            self.check_ms.push(ms);
        }
        self.drive_ms.push(ms);
        r
    }
    pub fn drain(&mut self, records: usize) -> Result<(), FreshnessError> {
        for _ in 0..128 {
            if self.drive(records)? == FreshnessWork::Waiting {
                return Ok(());
            }
        }
        panic!("bounded owned fixture did not settle")
    }
    pub fn checkpoint(&self) -> u64 {
        self.store
            .inner
            .checkpoint(&fixture::scope())
            .unwrap()
            .map_or(0, |p| p.position())
    }
}
pub fn assert_rows(rows: &[Record], n: usize) {
    assert_eq!(rows.len(), n);
    for (i, row) in rows.iter().enumerate() {
        let (id, payload) = fixture::fact(i + 1);
        assert_eq!(row.id, id);
        assert_eq!(row.payload, payload);
        assert_eq!(row.position, (i + 1) as u64);
        assert_eq!(row.scope, fixture::scope());
    }
}
