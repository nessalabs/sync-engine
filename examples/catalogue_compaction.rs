//! Small local source fixture for measuring physical compaction.
use nessa_sync::replication::{
    catalogue::CatalogueHorizonSource,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::{SqliteCatalogueMaintenance, SqliteCatalogueSource},
};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    match args[2].as_str() {
        "seed" => {
            let mut s = SqliteCatalogueSource::open(
                &args[1],
                id("origin"),
                id("catalogue"),
                id("v1"),
                id("schema"),
            )
            .unwrap();
            for i in 0..620 {
                s.upsert(&id(&format!("entry-{i:04}")), b"payload").unwrap();
            }
            for i in 0..160 {
                s.delete(&id(&format!("entry-{i:04}"))).unwrap();
            }
            let scope = Scope::new(
                id("peer"),
                id("origin"),
                id("catalogue"),
                id("v1"),
                id("schema"),
                id("grant"),
            );
            let head = s
                .horizon_head(&scope, CapabilitySupport::current())
                .unwrap();
            assert_eq!(s.compare_collect(&head, 780).unwrap().hidden_markers, 159);
        }
        "open" => {
            drop(
                SqliteCatalogueSource::open(
                    &args[1],
                    id("origin"),
                    id("catalogue"),
                    id("v1"),
                    id("schema"),
                )
                .unwrap(),
            );
            println!("opened");
        }
        "fenced" => {
            let mut s = SqliteCatalogueSource::open(
                &args[1],
                id("origin"),
                id("catalogue"),
                id("v1"),
                id("schema"),
            )
            .unwrap();
            assert_eq!(
                s.upsert(&id("entry-000000"), b"again"),
                Err(nessa_sync::replication::catalogue::CatalogueStoreError::Fenced)
            );
            println!("fenced");
        }
        "migrate" => {
            drop(
                SqliteCatalogueMaintenance::open(
                    &args[1],
                    id("origin"),
                    id("catalogue"),
                    id("v1"),
                    id("schema"),
                )
                .unwrap(),
            );
            println!("migrated");
        }
        "compact" => {
            let mut s = SqliteCatalogueMaintenance::open(
                &args[1],
                id("origin"),
                id("catalogue"),
                id("v1"),
                id("schema"),
            )
            .unwrap();
            println!("{}", s.compact_retired(256).unwrap());
        }
        "compact-all" => {
            let mut s = SqliteCatalogueMaintenance::open(
                &args[1],
                id("origin"),
                id("catalogue"),
                id("v1"),
                id("schema"),
            )
            .unwrap();
            let mut moved = 0;
            loop {
                let batch = s.compact_retired(256).unwrap();
                moved += batch;
                if batch == 0 {
                    break;
                }
            }
            println!("{moved}");
        }
        _ => panic!("expected seed or compact"),
    }
}
