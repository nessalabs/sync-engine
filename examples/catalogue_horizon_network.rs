//! Owned v2 catalogue process client; existing catalogue_network owns the server.
use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer},
    catalogue::*,
    domain::{CapabilitySupport, Id, RetentionPosition, Scope},
    infrastructure::{
        CataloguePublicationError, HorizonCatalogueClient, MemoryAuthorizer, SqliteCatalogueSource,
        SqliteCatalogueStore, WireCounters,
    },
};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    time::Instant,
};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("gateway"),
        id("transcript-index"),
        id("first"),
        id("opaque-v1"),
        id("epoch-1"),
    )
}
fn source(path: &Path) -> SqliteCatalogueSource {
    let s = scope("phone");
    SqliteCatalogueSource::open(
        path,
        s.origin().clone(),
        s.stream().clone(),
        s.incarnation().clone(),
        s.schema().clone(),
    )
    .unwrap()
}
fn outcome(e: &CatalogueError) -> &'static str {
    match e {
        CatalogueError::Source(CatalogueSourceError::HorizonExpired { .. }) => "expired",
        CatalogueError::Incompatible(_) => "incompatible",
        CatalogueError::Store(CatalogueStoreError::HorizonRegressed { .. }) => "regressed",
        CatalogueError::Denied | CatalogueError::Source(CatalogueSourceError::Denied) => "denied",
        CatalogueError::Source(CatalogueSourceError::Unavailable) => "unavailable",
        CatalogueError::Source(CatalogueSourceError::IdentityChanged) => "identity",
        CatalogueError::Source(CatalogueSourceError::HorizonVersion(_)) => "version",
        _ => "invalid",
    }
}
fn report(
    label: &str,
    wire: WireCounters,
    pages: usize,
    completed: u64,
    active: bool,
    work_ms: f64,
) {
    println!("{{\"outcome\":\"{label}\",\"pages\":{pages},\"completed\":{completed},\"active\":{active},\"exchanges\":{},\"manifest_bytes\":{},\"payload_bytes\":{},\"duplicate_bytes\":{},\"protocol_bytes\":{},\"work_ms\":{work_ms:.6}}}",wire.exchanges,wire.catalogue_manifest_bytes,wire.catalogue_payload_bytes,wire.catalogue_duplicate_bytes,wire.protocol_bytes);
}
fn main() {
    let a = std::env::args().collect::<Vec<_>>();
    assert!(a.len()>=4,"owned v2 client: collect SOURCE H | retire SOURCE N | head PORT TOKEN | pass PORT CACHE RECEIVER TOKEN MAX_PAGES | reset-start/reset-step PORT CACHE RECEIVER TOKEN");
    if a[1] == "compact" {
        let moved = source(Path::new(&a[2]))
            .compact_retired(a[3].parse().unwrap())
            .unwrap();
        println!("{{\"moved\":{moved}}}");
        return;
    }
    if a[1] == "assert-fenced" {
        assert_eq!(
            source(Path::new(&a[2])).upsert(&id(&a[3]), b"ordinary later value"),
            Err(CatalogueStoreError::Fenced)
        );
        println!("{{\"fenced\":true}}");
        return;
    }
    if a[1] == "pin-fixture" {
        let selected = scope("phone");
        let mut auth = MemoryAuthorizer::allowed(selected.clone());
        assert_eq!(auth.authorize(&selected), Access::Allowed(selected.clone()));
        let mut store = SqliteCatalogueStore::open(&a[3]).unwrap();
        let progress = store.progress(&selected).unwrap().unwrap();
        assert_eq!(progress.completed, 620);
        assert_eq!(progress.active.as_ref().unwrap().boundary, 780);
        let mut producer = source(Path::new(&a[2]));
        producer
            .register_catalogue_retention_pin(&id("owned-fixture-holder"), &progress, 1, 10)
            .unwrap();
        let h = producer
            .horizon_head(&selected, CapabilitySupport::current())
            .unwrap();
        assert_eq!(
            producer.compare_collect(&h, 780),
            Err(CataloguePublicationError::RetentionPolicyRequired)
        );
        let receipt = producer
            .collect_with_retention(&h, RetentionPosition::new(780), None, 1)
            .unwrap();
        assert_eq!(receipt.head.horizon, 620);
        println!("{{\"protected_completed\":620,\"active_boundary\":780,\"horizon\":620,\"manual_refused\":true}}");
        return;
    }
    if a[1] == "collect-retention" {
        let mut producer = source(Path::new(&a[2]));
        let h = producer
            .horizon_head(&scope("phone"), CapabilitySupport::current())
            .unwrap();
        let preferred = a[3].parse().unwrap();
        assert_eq!(
            producer.compare_collect(&h, preferred),
            Err(CataloguePublicationError::RetentionPolicyRequired)
        );
        let receipt = producer
            .collect_with_retention(
                &h,
                RetentionPosition::new(preferred),
                None,
                a[4].parse().unwrap(),
            )
            .unwrap();
        assert_eq!(
            producer.compare_collect(&receipt.head, receipt.head.horizon),
            Err(CataloguePublicationError::RetentionPolicyRequired)
        );
        println!(
            "{{\"head\":{},\"horizon\":{},\"hidden\":{}}}",
            receipt.head.source.position(),
            receipt.head.horizon,
            receipt.hidden_markers
        );
        return;
    }
    if a[1] == "collect" {
        let mut s = source(Path::new(&a[2]));
        let h = s
            .horizon_head(&scope("phone"), CapabilitySupport::current())
            .unwrap();
        let receipt = s.compare_collect(&h, a[3].parse().unwrap()).unwrap();
        println!(
            "{{\"head\":{},\"horizon\":{},\"hidden\":{}}}",
            receipt.head.source.position(),
            receipt.head.horizon,
            receipt.hidden_markers
        );
        return;
    }
    if a[1] == "retire" {
        let mut s = source(Path::new(&a[2]));
        let n: usize = a[3].parse().unwrap();
        for i in 0..n {
            s.delete(&id(&format!("entry-{i:04}"))).unwrap();
        }
        println!("{{\"retired\":{n}}}");
        return;
    }
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, a[2].parse().unwrap());
    if a[1] == "head" {
        let mut c = HorizonCatalogueClient::new(address, id(&a[3])).unwrap();
        let now = Instant::now();
        let selected = scope("phone");
        let head_scope = Scope::new(
            selected.receiver().clone(),
            selected.origin().clone(),
            selected.stream().clone(),
            selected.incarnation().clone(),
            selected.schema().clone(),
            id(a.get(4).map_or("epoch-1", String::as_str)),
        );
        match c.horizon_head(&head_scope,CapabilitySupport::current()){Ok(h)=>println!("{{\"outcome\":\"ok\",\"head\":{},\"horizon\":{},\"exchanges\":{},\"protocol_bytes\":{}}}",h.source.position(),h.horizon,c.counters().exchanges,c.counters().protocol_bytes),Err(e)=>report(outcome(&CatalogueError::from(e)),c.counters(),0,0,false,now.elapsed().as_secs_f64()*1000.)}
        return;
    }
    assert!(a.len() >= 6);
    let selected = scope(&a[4]);
    let mut c = HorizonCatalogueClient::new(address, id(&a[5])).unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    assert_eq!(auth.authorize(&selected), Access::Allowed(selected.clone()));
    let mut d = SqliteCatalogueStore::open(&a[3]).unwrap();
    let now = Instant::now();
    let mut pages = 0;
    let result = (|| -> Result<(), CatalogueError> {
        match a[1].as_str() {
            "pass" => {
                let limit: usize = a[6].parse().unwrap();
                let mut p = begin_or_resume_with_horizon(
                    &selected,
                    CapabilitySupport::current(),
                    &mut auth,
                    &mut c,
                    &mut d,
                )?;
                while let Some(active) = p {
                    if pages >= limit {
                        break;
                    }
                    p = apply_next_page_with_horizon(
                        &active,
                        40,
                        512 * 1024,
                        CapabilitySupport::current(),
                        &mut auth,
                        &mut c,
                        &mut d,
                    )?
                    .active;
                    pages += 1;
                }
            }
            "saved-page" => {
                let p = d
                    .progress(&selected)
                    .map_err(CatalogueError::Store)?
                    .and_then(|p| p.active)
                    .expect("active pass required");
                apply_next_page_with_horizon(
                    &p,
                    40,
                    512 * 1024,
                    CapabilitySupport::current(),
                    &mut auth,
                    &mut c,
                    &mut d,
                )?;
                pages = 1;
            }
            "reset-start" => {
                let expected = match d.recovery(&selected).map_err(CatalogueError::Store)? {
                    Some(r) => Some(r.progress),
                    None => d.progress(&selected).map_err(CatalogueError::Store)?,
                };
                start_catalogue_recovery_with_horizon(
                    &selected,
                    expected,
                    CapabilitySupport::current(),
                    &mut auth,
                    &mut c,
                    &mut d,
                )?;
            }
            "reset-step" => {
                let r = d
                    .recovery(&selected)
                    .map_err(CatalogueError::Store)?
                    .expect("explicit reset required");
                step_catalogue_recovery_with_horizon(
                    &r,
                    CapabilitySupport::current(),
                    40,
                    512 * 1024,
                    &mut auth,
                    &mut c,
                    &mut d,
                )?;
                pages = 1;
            }
            _ => panic!("unknown command"),
        }
        Ok(())
    })();
    let work_ms = now.elapsed().as_secs_f64() * 1000.;
    let label = match &result {
        Ok(()) => "ok",
        Err(e) => outcome(e),
    };
    if let Err(e) = result {
        eprintln!("owned v2 operation: {e:?}")
    };
    let p = match d.recovery(&selected).unwrap() {
        Some(r) => Some(r.progress),
        None => d.progress(&selected).unwrap(),
    };
    report(
        label,
        c.counters(),
        pages,
        p.as_ref().map_or(0, |p| p.completed),
        p.as_ref().is_some_and(|p| p.active.is_some()),
        work_ms,
    );
}
