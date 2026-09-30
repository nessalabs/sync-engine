//! Forward source admission consumes the domain rule before database reads.
use super::{id, open_source, scope, TestDirectory};
use nessa_sync::replication::{
    application::{RecordSource, SourceError},
    domain::PageRequest,
};

#[test]
fn sqlite_source_adapter_consumes_generic_request_owner_and_preserves_physical_clamping() {
    let directory = TestDirectory::new();
    let mut source = open_source(directory.path("request-source.sqlite"));
    source.append(&id("fact"), b"a").unwrap();
    let request = PageRequest {
        scope: scope(),
        after: 0,
        target: 1,
        max_records: usize::MAX,
        max_payload_bytes: usize::MAX,
        max_record_bytes: usize::MAX,
    };
    assert_eq!(source.page(&request).unwrap().records.len(), 1);
    let mutations: [fn(&mut PageRequest); 5] = [
        |r| r.target = r.after,
        |r| {
            r.after = 1;
            r.target = 0;
        },
        |r| r.max_records = 0,
        |r| r.max_payload_bytes = 0,
        |r| r.max_record_bytes = 0,
    ];
    for mutate in mutations {
        let mut invalid = request.clone();
        mutate(&mut invalid);
        assert_eq!(source.page(&invalid), Err(SourceError::InvalidRequest));
    }
    let mut beyond_head = request;
    beyond_head.target = 2;
    assert_eq!(source.page(&beyond_head), Err(SourceError::InvalidRequest));
}
