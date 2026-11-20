//! Native SQLite initial admission lab; no transport format or production grant.
#[path = "source_head/fixture.rs"]
mod fixture;
use fixture::{id, scope, Directory};
use nessa_sync::replication::{
    application::{begin_pass_with_capabilities, finish_pass, SyncError},
    catalogue::{
        apply_next_page, begin_or_resume_with_capabilities, CatalogueError, CatalogueStore,
    },
    domain::{CapabilityError, CapabilitySupport, Limits},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueStore, SqliteReplicaStore},
};
fn main() {
    let f = Directory::new();
    let s = scope();
    let supported = CapabilitySupport::current();
    let mut auth = MemoryAuthorizer::allowed(s.clone());
    let mut source = f.records();
    for n in 1..=3 {
        source
            .inner
            .append(&id(&format!("fact-{n}")), b"value")
            .unwrap();
    }
    let path = f.path("record-cache.db");
    let mut store = SqliteReplicaStore::open(&path).unwrap();
    source.meta.reader = 1;
    assert_eq!(
        begin_pass_with_capabilities(&s, supported, &mut auth, &mut source, &mut store),
        Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
    );
    assert_eq!(source.inner.page_reads(), 0);
    assert!(store.checkpoint(&s).unwrap().is_none());
    source.meta.reader = 0;
    source.meta.writer = u64::MAX;
    let mut pass =
        begin_pass_with_capabilities(&s, supported, &mut auth, &mut source, &mut store).unwrap();
    finish_pass(
        &mut pass,
        Limits::new(10, 100, 100).unwrap(),
        &mut auth,
        &mut source,
        &mut store,
    )
    .unwrap();
    assert!(
        begin_pass_with_capabilities(&s, supported, &mut auth, &mut source, &mut store)
            .unwrap()
            .is_complete()
    );
    let saved = store.checkpoint(&s).unwrap();
    source.meta.reader = 1;
    assert!(matches!(
        begin_pass_with_capabilities(&s, supported, &mut auth, &mut source, &mut store),
        Err(SyncError::Incompatible(_))
    ));
    assert_eq!(store.checkpoint(&s).unwrap(), saved);
    drop(store);
    let mut reopened = SqliteReplicaStore::open(&path).unwrap();
    assert_eq!(reopened.count(&s).unwrap(), 3);
    let rows = reopened.read_after(&s, 0, 10, 100).unwrap();
    for (n, row) in rows.iter().enumerate() {
        assert_eq!(row.id, id(&format!("fact-{}", n + 1)));
        assert_eq!(row.payload, b"value");
    }
    let mut catalogue = f.catalogue();
    for n in 1..=3 {
        catalogue
            .inner
            .upsert(&id(&format!("item-{n}")), b"value")
            .unwrap();
    }
    let path = f.path("catalogue-cache.db");
    let mut cache = SqliteCatalogueStore::open(&path).unwrap();
    catalogue.meta.reader = 1;
    assert!(matches!(
        begin_or_resume_with_capabilities(&s, supported, &mut auth, &mut catalogue, &mut cache),
        Err(CatalogueError::Incompatible(_))
    ));
    assert!(cache.progress(&s).unwrap().is_none());
    assert_eq!(catalogue.inner.manifest_reads(), 0);
    catalogue.meta.reader = 0;
    catalogue.meta.writer = u64::MAX;
    let pass =
        begin_or_resume_with_capabilities(&s, supported, &mut auth, &mut catalogue, &mut cache)
            .unwrap()
            .unwrap();
    let saved = apply_next_page(&pass, 1, 100, &mut auth, &mut catalogue, &mut cache).unwrap();
    catalogue.meta.reader = 1;
    assert!(matches!(
        begin_or_resume_with_capabilities(&s, supported, &mut auth, &mut catalogue, &mut cache),
        Err(CatalogueError::Incompatible(_))
    ));
    assert_eq!(cache.progress(&s).unwrap(), Some(saved));
    assert_eq!(catalogue.inner.manifest_reads(), 1);
    catalogue.meta.reader = 0;
    let mut active =
        begin_or_resume_with_capabilities(&s, supported, &mut auth, &mut catalogue, &mut cache)
            .unwrap();
    while let Some(pass) = active {
        active = apply_next_page(&pass, 1, 100, &mut auth, &mut catalogue, &mut cache)
            .unwrap()
            .active;
    }
    assert!(begin_or_resume_with_capabilities(
        &s,
        supported,
        &mut auth,
        &mut catalogue,
        &mut cache
    )
    .unwrap()
    .is_none());
    drop(cache);
    let mut reopened = SqliteCatalogueStore::open(path).unwrap();
    assert_eq!(reopened.count(&s).unwrap(), 3);
    assert_eq!(reopened.progress(&s).unwrap().unwrap().completed, 3);
    for n in 1..=3 {
        assert_eq!(
            reopened
                .cached_entry(&s, &id(&format!("item-{n}")))
                .unwrap()
                .unwrap()
                .payload,
            b"value"
        );
    }
    println!("{{\"scope\":\"initial admission only\",\"record_rows\":3,\"record_head_reads\":{},\"record_page_reads\":{},\"catalogue_rows\":3,\"catalogue_head_reads\":{},\"catalogue_manifest_reads\":{},\"catalogue_resolve_reads\":{},\"unknown_reader_no_fetch_or_mutation\":true,\"unknown_writer_read_only_continues\":true,\"active_resume_rechecked\":true,\"reopened_cache_correct\":true,\"transport_capabilities_implemented\":false}}",source.inner.head_reads(),source.inner.page_reads(),catalogue.inner.head_reads(),catalogue.inner.manifest_reads(),catalogue.inner.resolve_reads());
}
