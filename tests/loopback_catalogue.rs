#![cfg(feature = "transport")]

use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use nessa_sync::replication::catalogue::{
    apply_next_page, begin_or_resume, CatalogueSource, CatalogueStore,
};
use nessa_sync::replication::domain::{Id, Scope};
use nessa_sync::replication::infrastructure::{
    LoopbackClient, LoopbackConfig, LoopbackServer, MemoryAuthorizer, SqliteCatalogueSource,
    SqliteCatalogueStore,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("gateway"),
        id("index"),
        id("first"),
        id("opaque"),
        id("epoch"),
    )
}
fn path(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "nessa-sync-{name}-{}-{stamp}.db",
        std::process::id()
    ))
}
fn complete(
    scope: &Scope,
    client: &mut LoopbackClient,
    store: &mut SqliteCatalogueStore,
    max_pages: usize,
) {
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_or_resume(scope, &mut auth, client, store).unwrap();
    for _ in 0..max_pages {
        let Some(active) = pass else {
            return;
        };
        pass = apply_next_page(&active, 40, 512 * 1024, &mut auth, client, store)
            .unwrap()
            .active;
    }
}

#[test]
fn two_durable_receivers_complete_network_catalogue_passes() {
    let source_path = path("catalogue-source");
    let record_path = path("record-source");
    let a_path = path("catalogue-a");
    let b_path = path("catalogue-b");
    let mut source = SqliteCatalogueSource::open(
        &source_path,
        id("gateway"),
        id("index"),
        id("first"),
        id("opaque"),
    )
    .unwrap();
    for number in 0..620 {
        source
            .upsert(
                &id(&format!("entry-{number:04}")),
                format!("value-{number:04}").as_bytes(),
            )
            .unwrap();
    }
    let server = LoopbackServer::bind(
        0,
        LoopbackConfig {
            source_path: record_path.clone(),
            catalogue_source_path: Some(source_path.clone()),
            artifact_source_path: None,
            catalogue_fault: None,
            origin: id("gateway"),
            stream: id("index"),
            incarnation: id("first"),
            schema: id("opaque"),
            access_epoch: id("epoch"),
            read_token: id("read"),
            allowed_receivers: vec![id("a"), id("b")],
            write_token: id("write"),
        },
    )
    .unwrap();
    let addr = match server.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    };
    thread::spawn(move || server.serve().unwrap());
    let mut a = LoopbackClient::new(addr, id("read")).unwrap();
    let mut b = LoopbackClient::new(addr, id("read")).unwrap();
    let mut a_store = SqliteCatalogueStore::open(&a_path).unwrap();
    let mut b_store = SqliteCatalogueStore::open(&b_path).unwrap();
    complete(&scope("a"), &mut a, &mut a_store, 1);
    assert!(a_store
        .progress(&scope("a"))
        .unwrap()
        .unwrap()
        .active
        .is_some());
    drop(a_store);
    let mut a_store = SqliteCatalogueStore::open(&a_path).unwrap();
    source.upsert(&id("entry-0000"), b"changed").unwrap();
    source.delete(&id("entry-0619")).unwrap();
    complete(&scope("a"), &mut a, &mut a_store, 40);
    complete(&scope("a"), &mut a, &mut a_store, 40);
    complete(&scope("b"), &mut b, &mut b_store, 40);
    assert_eq!(a_store.count(&scope("a")).unwrap(), 620);
    assert_eq!(b_store.count(&scope("b")).unwrap(), 620);
    assert_eq!(
        a_store
            .cached_entry(&scope("a"), &id("entry-0000"))
            .unwrap()
            .unwrap()
            .payload,
        b"changed"
    );
    assert!(
        b_store
            .cached_entry(&scope("b"), &id("entry-0619"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    let a_completed = a_store.progress(&scope("a")).unwrap().unwrap().completed;
    assert_eq!(
        a_completed,
        CatalogueSource::head(&mut a, &scope("a")).unwrap()
    );
    let prior_payload = a.counters().catalogue_payload_bytes;
    complete(&scope("a"), &mut a, &mut a_store, 40);
    assert_eq!(a.counters().catalogue_payload_bytes, prior_payload);
    let mut wrong = LoopbackClient::new(addr, id("wrong")).unwrap();
    assert!(CatalogueSource::head(&mut wrong, &scope("a")).is_err());
    for path in [source_path, record_path, a_path, b_path] {
        let _ = std::fs::remove_file(path);
    }
}
