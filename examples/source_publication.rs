//! Native durable publication process lab; source-host authority remains external.
use nessa_sync::replication::{
    application::{begin_pass_with_capabilities, finish_pass, RecordHeadSource, SyncError},
    domain::{CapabilityError, CapabilitySupport, Id, Limits, Scope},
    infrastructure::{MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore},
};
use std::path::Path;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn source(p: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        p,
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
    )
    .unwrap()
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let command = args.get(1).expect("seed|read|inspect SOURCE [CACHE]");
    let mut s = source(Path::new(&args[2]));
    if command == "seed" {
        for n in 1..=7 {
            let expected = s.source_head(&scope()).unwrap();
            s.compare_append(&expected, &id(&format!("fact{n}")), b"value", 0, 0)
                .unwrap();
        }
        println!(
            "{{\"source_head\":{}}}",
            s.source_head(&scope()).unwrap().position()
        );
    } else if command == "read" {
        let mut cache = SqliteReplicaStore::open(&args[3]).unwrap();
        let saved = cache.checkpoint(&scope()).unwrap();
        let mut auth = MemoryAuthorizer::allowed(scope());
        let result = begin_pass_with_capabilities(
            &scope(),
            CapabilitySupport::current(),
            &mut auth,
            &mut s,
            &mut cache,
        );
        let incompatible = match result {
            Ok(mut pass) => {
                finish_pass(
                    &mut pass,
                    Limits::new(10, 100, 100).unwrap(),
                    &mut auth,
                    &mut s,
                    &mut cache,
                )
                .unwrap();
                false
            }
            Err(SyncError::Incompatible(CapabilityError::MissingReader(_))) => {
                assert_eq!(cache.checkpoint(&scope()).unwrap(), saved);
                true
            }
            other => panic!("unexpected admission: {other:?}"),
        };
        println!("{{\"typed_incompatible\":{incompatible},\"checkpoint\":{},\"records\":{},\"page_reads\":{},\"payload_bytes\":{}}}",cache.checkpoint(&scope()).unwrap().map_or(0,|c|c.position()),cache.count(&scope()).unwrap(),s.page_reads(),s.payload_bytes());
    } else if command == "inspect" {
        let head = s.source_head(&scope()).unwrap();
        println!(
            "{{\"head\":{},\"reader\":{},\"writer\":{}}}",
            head.position(),
            head.reader_required(),
            head.writer_required()
        );
    } else {
        panic!("unknown lab command");
    }
}
