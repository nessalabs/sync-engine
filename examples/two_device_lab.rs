//! Executable two-receiver laboratory for the default-feature core.
use nessa_sync::replication::application::{begin_pass, finish_pass};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, MemorySource, MemoryStore, SourceFact,
};

fn id(value: &str) -> Id {
    Id::new(value).expect("fixture ID")
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("lab-origin"),
        id("sample-stream"),
        id("first"),
        id("opaque-v1"),
        id("grant-1"),
    )
}
fn append(source: &mut MemorySource, name: &str, data: &str) {
    source
        .append(SourceFact {
            id: id(name),
            payload: data.as_bytes().to_vec(),
        })
        .expect("distinct fact");
}
fn catch_up(
    source: &mut MemorySource,
    store: &mut MemoryStore,
    auth: &mut MemoryAuthorizer,
    scope: &Scope,
    limits: Limits,
) {
    let mut pass = begin_pass(scope, auth, source, store).expect("authorized head");
    finish_pass(&mut pass, limits, auth, source, store).expect("valid page and atomic apply");
}
fn main() {
    let mut source = MemorySource::new(
        id("lab-origin"),
        id("sample-stream"),
        id("first"),
        id("opaque-v1"),
    );
    append(&mut source, "event-1", "red");
    append(&mut source, "event-2", "blue");
    append(&mut source, "event-3", "green");
    let limits = Limits::new(2, 16, 16).expect("valid limits");
    let (a, b) = (scope("device-a"), scope("device-b"));
    let (mut store_a, mut store_b) = (MemoryStore::new(), MemoryStore::new());
    let (mut auth_a, mut auth_b) = (
        MemoryAuthorizer::allowed(a.clone()),
        MemoryAuthorizer::allowed(b.clone()),
    );
    catch_up(&mut source, &mut store_a, &mut auth_a, &a, limits);
    catch_up(&mut source, &mut store_b, &mut auth_b, &b, limits);
    assert_eq!(store_a.checkpoint(&a).unwrap().unwrap().position(), 3);
    assert_eq!(store_b.checkpoint(&b).unwrap().unwrap().position(), 3);
    // Device B is offline. Only A is explicitly woken for new committed facts.
    append(&mut source, "event-4", "amber");
    append(&mut source, "event-5", "violet");
    catch_up(&mut source, &mut store_a, &mut auth_a, &a, limits);
    assert_eq!(store_b.checkpoint(&b).unwrap().unwrap().position(), 3);
    catch_up(&mut source, &mut store_b, &mut auth_b, &b, limits);
    let expected = ["red", "blue", "green", "amber", "violet"]
        .map(str::as_bytes)
        .map(Vec::from);
    for (store, receiver) in [(&store_a, &a), (&store_b, &b)] {
        let actual: Vec<_> = store
            .records(receiver)
            .unwrap()
            .into_iter()
            .map(|record| record.payload)
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(store.checkpoint(receiver).unwrap().unwrap().position(), 5);
    }
    let prior_pages = source.page_reads;
    let prior_bytes = source.payload_bytes;
    catch_up(&mut source, &mut store_a, &mut auth_a, &a, limits);
    assert_eq!(source.page_reads, prior_pages);
    assert_eq!(source.payload_bytes, prior_bytes);
    assert_eq!(
        (source.head_reads, source.page_reads, source.payload_bytes),
        (5, 6, 46)
    );
    assert_eq!(
        (
            store_a.apply_calls().unwrap(),
            store_b.apply_calls().unwrap()
        ),
        (3, 3)
    );
    let checkpoint_a = store_a.checkpoint(&a).unwrap().unwrap().position();
    let checkpoint_b = store_b.checkpoint(&b).unwrap().unwrap().position();
    let unchanged_bytes = source.payload_bytes - prior_bytes;
    println!("{{\"status\":\"ok\",\"source_head\":{},\"device_a_checkpoint\":{},\"device_b_checkpoint\":{},\"source_head_reads\":{},\"source_page_reads\":{},\"record_payload_bytes\":{},\"device_a_apply_calls\":{},\"device_b_apply_calls\":{},\"unchanged_head_payload_bytes\":{}}}", source.committed_head(), checkpoint_a, checkpoint_b, source.head_reads, source.page_reads, source.payload_bytes, store_a.apply_calls().unwrap(), store_b.apply_calls().unwrap(), unchanged_bytes);
}
