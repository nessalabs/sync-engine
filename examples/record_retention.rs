use nessa_sync::replication::application::{RecordSource, SourceError};
use nessa_sync::replication::domain::{Checkpoint, Id, PageRequest, RetentionPosition, Scope};
use nessa_sync::replication::infrastructure::SqliteReferenceSource;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .expect("pass a fresh disposable record database path");
    assert!(!std::path::Path::new(&path).exists());
    let id = |s: &str| Id::new(s).unwrap();
    let scope = Scope::new(
        id("phone"),
        id("origin"),
        id("stream"),
        id("initial"),
        id("schema"),
        id("access"),
    );
    let open = || {
        SqliteReferenceSource::open(
            &path,
            scope.origin().clone(),
            scope.stream().clone(),
            scope.incarnation().clone(),
            scope.schema().clone(),
        )
        .unwrap()
    };
    let mut source = open();
    for index in 1..=6 {
        source
            .append(&id(&format!("row-{index}")), &[index])
            .unwrap();
    }
    source
        .register_retention_pin(&id("holder"), &Checkpoint::new(scope.clone(), 2), 10, 20)
        .unwrap();
    let p = RetentionPosition::new;
    let protected = source
        .prune_with_retention(&scope, p(6), p(5), None, 11)
        .unwrap();
    let page = PageRequest {
        scope: scope.clone(),
        after: 2,
        target: 6,
        max_records: 6,
        max_payload_bytes: 6,
        max_record_bytes: 6,
    };
    assert_eq!(source.page(&page).unwrap().records.len(), 4);
    let pressured = source
        .prune_with_retention(&scope, p(6), p(5), Some(p(4)), 12)
        .unwrap();
    drop(source);
    let mut source = open();
    assert_eq!(source.page(&page), Err(SourceError::Pruned));
    assert_eq!(source.append(&id("row-1"), &[1]), Ok(1));
    assert_eq!((protected.get(), pressured.get()), (2, 4));
    println!(
        "{{\"initial_rows\":6,\"protected_floor\":{},\"hard_pressure_floor\":{},\"reopened_gap\":true,\"old_dedup_position\":1}}",
        protected.get(),
        pressured.get()
    );
}
