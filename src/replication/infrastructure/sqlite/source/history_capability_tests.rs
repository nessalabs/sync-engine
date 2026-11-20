use super::*;
use crate::replication::domain::CapabilityError;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "history-admission-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn open(&self) -> SqliteReferenceSource {
        SqliteReferenceSource::open(self.0.join("source.db"), id("o"), id("s"), id("i"), id("k"))
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn requests() -> (TailRequest, OlderRequest) {
    let scope = Scope::new(id("r"), id("o"), id("s"), id("i"), id("k"), id("e"));
    (
        TailRequest {
            scope: scope.clone(),
            generation: 2,
            max_records: 1,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        },
        OlderRequest {
            scope,
            generation: 2,
            before: 2,
            max_records: 1,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        },
    )
}
fn require_future_reader(conn: &rusqlite::Connection) {
    conn.execute(
        "UPDATE source_meta SET reader_required=?1",
        [1u64.to_be_bytes()],
    )
    .unwrap();
}

#[test]
fn explicit_and_legacy_reads_keep_exact_results_and_ignore_writer_only_requirements() {
    let file = Fixture::new();
    let mut source = file.open();
    let (tail, older) = requests();
    let support = CapabilitySupport::current();
    let (metadata, empty) = source.tail_with_capabilities(&tail, support).unwrap();
    assert_eq!(metadata.position(), 0);
    assert_eq!(empty, source.tail(&tail).unwrap());

    source.append(&id("a"), b"one").unwrap();
    source.append(&id("b"), b"two").unwrap();
    source
        .conn
        .execute(
            "UPDATE source_meta SET writer_required=?1",
            [1u64.to_be_bytes()],
        )
        .unwrap();
    drop(source);
    let mut source = file.open();

    let (metadata, suffix) = source.tail_with_capabilities(&tail, support).unwrap();
    assert_eq!(metadata.scope(), &tail.scope);
    assert_eq!(metadata.position(), suffix.watermark);
    assert_eq!(metadata.writer_required(), 1);
    assert_eq!(suffix.request, tail);
    assert_eq!(suffix.records[0].payload.as_slice(), b"two");
    assert_eq!(suffix, source.tail(&tail).unwrap());

    let (metadata, prefix) = source.older_with_capabilities(&older, support).unwrap();
    assert_eq!(metadata.scope(), &older.scope);
    assert_eq!(metadata.position(), suffix.watermark);
    assert_eq!(prefix.request, older);
    assert_eq!(prefix.records[0].payload.as_slice(), b"one");
    assert_eq!(prefix, source.older(&older).unwrap());
}

#[test]
fn unrecognized_reader_requirement_refuses_before_floor_or_payload() {
    let file = Fixture::new();
    let mut source = file.open();
    let (tail, older) = requests();
    source.append(&id("a"), b"one").unwrap();
    // Deliberately unavailable floor shows the refusal precedes even this read.
    source
        .conn
        .execute("DELETE FROM source_history", [])
        .unwrap();
    require_future_reader(&source.conn);
    let expected = HistorySourceError::Incompatible(CapabilityError::MissingReader(1));
    assert_eq!(
        source.tail_with_capabilities(&tail, CapabilitySupport::current()),
        Err(expected.clone())
    );
    assert_eq!(
        source.older_with_capabilities(&older, CapabilitySupport::current()),
        Err(expected.clone())
    );
    assert_eq!(source.tail(&tail), Err(expected.clone()));
    assert_eq!(source.older(&older), Err(expected));
    assert_eq!(source.history_payload_bytes(), 0);
}

#[test]
fn admitted_wal_snapshot_stays_coherent_across_competing_publication() {
    let file = Fixture::new();
    let mut source = file.open();
    let mut writer = file.open();
    let (tail, older) = requests();
    writer.append(&id("a"), b"old").unwrap();

    let support = CapabilitySupport::current();
    let tx = source.conn.transaction().unwrap();
    let (before, floor) = history_meta(&tx, &tail.scope, support).unwrap();
    assert_eq!(before.position(), 1);
    assert_eq!(before.reader_required(), 0);
    // Deterministic interleaving, no sleeps. An unimplemented future-format
    // publication is fixture data only; the public writer cannot publish it.
    let commit = writer.conn.transaction().unwrap();
    commit
        .execute("INSERT INTO source_records VALUES(2, 'b', X'6E6577')", [])
        .unwrap();
    commit.execute("UPDATE source_meta SET head=2", []).unwrap();
    require_future_reader(&commit);
    commit.commit().unwrap();

    let records = read_backward(
        &tx,
        &tail.scope,
        before.position() + 1,
        floor + 1,
        1,
        100,
        100,
    )
    .unwrap();
    assert_eq!(records[0].payload.as_slice(), b"old");
    let (still, _) = history_meta(&tx, &tail.scope, support).unwrap();
    assert_eq!(still, before);
    drop(tx);

    let expected = HistorySourceError::Incompatible(CapabilityError::MissingReader(1));
    assert_eq!(
        source.tail_with_capabilities(&tail, support),
        Err(expected.clone())
    );
    assert_eq!(
        source.older_with_capabilities(&older, support),
        Err(expected)
    );
}
