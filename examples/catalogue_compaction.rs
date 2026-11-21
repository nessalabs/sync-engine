//! Small local source fixture for measuring physical compaction.
use nessa_sync::replication::{
    catalogue::CatalogueHorizonSource,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::SqliteCatalogueSource,
};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let mut s = SqliteCatalogueSource::open(
        &args[1],
        id("origin"),
        id("catalogue"),
        id("v1"),
        id("schema"),
    )
    .unwrap();
    match args[2].as_str() {
        "seed" => {
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
        "compact" => println!("{}", s.compact_retired(256).unwrap()),
        "open" => println!("opened"),
        "compact-all" => {
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
