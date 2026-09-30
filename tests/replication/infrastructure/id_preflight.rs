//! Real SQLite encoding, storage envelope and borrowed record acquisition rows.
use super::replica::REPLICA_RECORD_METADATA_SQL;
use super::source::{FORWARD_RECORD_METADATA_SQL, HISTORY_RECORD_METADATA_SQL};
use super::{SqliteReferenceSource, SqliteReplicaStore, MAX_STORED_ID_BYTES};
use crate::replication::application::{RecordSource, ReplicaStore, SourceError, StoreError};
use crate::replication::domain::{
    validate_page, Checkpoint, Id, Limits, PageRequest, Scope, MAX_ID_BYTES,
};
use crate::replication::history::{HistorySource, HistorySourceError, OlderRequest, TailRequest};
use rusqlite::{params, Connection, Params, ToSql};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const ENCODINGS: [&str; 3] = ["UTF-8", "UTF-16le", "UTF-16be"];
// Count the shared decoder's actual materialization/copy attempts, not allocator bytes.
thread_local! { static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) }; }
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub(super) fn materialized() {
    COUNTS.with(|counts| {
        let (text, payload) = counts.get();
        counts.set((text + 1, payload));
    });
}

pub(super) fn copied_payload() {
    COUNTS.with(|counts| {
        let (text, payload) = counts.get();
        counts.set((text, payload + 1));
    });
}

fn reset_counts() {
    COUNTS.with(|counts| counts.set((0, 0)));
}

fn counts() -> (usize, usize) {
    COUNTS.with(Cell::get)
}

fn id(text: &str) -> Id {
    Id::new(text).unwrap()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        loop {
            let next = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "nessa-sync-id-preflight-{}-{next}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("temporary directory: {error}"),
            }
        }
    }

    fn path(&self, file: &str) -> PathBuf {
        self.0.join(file)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("only this fixture's directory is removed");
    }
}

fn initialize_encoding(path: &Path, encoding: &str) {
    let db = Connection::open(path).unwrap();
    db.pragma_update(None, "encoding", encoding).unwrap();
    // Persist the selected encoding before the adapter initializes its schema.
    db.execute_batch("CREATE TABLE encoding_marker (v TEXT); DROP TABLE encoding_marker;")
        .unwrap();
    let actual: String = db
        .query_row("PRAGMA encoding", [], |row| row.get(0))
        .unwrap();
    assert_eq!(actual, encoding);
}

struct Fixture {
    source: SqliteReferenceSource,
    replica: SqliteReplicaStore,
    request: PageRequest,
    directory: TestDirectory,
}

impl Fixture {
    fn open_source(directory: &TestDirectory, scope: &Scope) -> SqliteReferenceSource {
        SqliteReferenceSource::open(
            directory.path("source.sqlite"),
            scope.origin().clone(),
            scope.stream().clone(),
            scope.incarnation().clone(),
            scope.schema().clone(),
        )
        .unwrap()
    }

    fn new(record_id: &str, encoding: &str) -> Self {
        let directory = TestDirectory::new();
        for file in ["source.sqlite", "replica.sqlite"] {
            initialize_encoding(&directory.path(file), encoding);
        }
        let scope = Scope::new(id("r"), id("o"), id("s"), id("i"), id("v"), id("e"));
        let mut source = Self::open_source(&directory, &scope);
        source.append(&id(record_id), b"payload").unwrap();
        let request = PageRequest {
            scope: scope.clone(),
            after: 0,
            target: 1,
            max_records: 1,
            max_payload_bytes: 32,
            max_record_bytes: 32,
        };
        let page = source.page(&request).unwrap();
        let plan = validate_page(
            &Checkpoint::new(scope, 0),
            &request,
            page,
            Limits::new(1, 32, 32).unwrap(),
        )
        .unwrap();
        let mut replica = SqliteReplicaStore::open(directory.path("replica.sqlite")).unwrap();
        replica.apply(plan).unwrap();
        Self {
            source,
            replica,
            request,
            directory,
        }
    }

    fn reopened(self) -> Self {
        let Self {
            source,
            replica,
            request,
            directory,
        } = self;
        drop(source);
        drop(replica);
        let source = Self::open_source(&directory, &request.scope);
        let replica = SqliteReplicaStore::open(directory.path("replica.sqlite")).unwrap();
        Self {
            source,
            replica,
            request,
            directory,
        }
    }

    fn tail_request(&self) -> TailRequest {
        TailRequest {
            scope: self.request.scope.clone(),
            generation: 1,
            max_records: 1,
            max_payload_bytes: 32,
            max_record_bytes: 32,
        }
    }

    fn older_request(&self) -> OlderRequest {
        OlderRequest {
            scope: self.request.scope.clone(),
            generation: 1,
            before: 2,
            max_records: 1,
            max_payload_bytes: 32,
            max_record_bytes: 32,
        }
    }

    fn replace_stored_id(&self, value: &dyn ToSql, cast_text: bool) {
        let expression = if cast_text { "CAST(?1 AS TEXT)" } else { "?1" };
        for (file, table) in [
            ("source.sqlite", "source_records"),
            ("replica.sqlite", "replica_records"),
        ] {
            let db = Connection::open(self.directory.path(file)).unwrap();
            db.execute(
                &format!("UPDATE {table} SET record_id = {expression} WHERE position = 1"),
                [value],
            )
            .unwrap();
        }
    }

    fn oversized_payload(&self) {
        for (file, table) in [
            ("source.sqlite", "source_records"),
            ("replica.sqlite", "replica_records"),
        ] {
            let db = Connection::open(self.directory.path(file)).unwrap();
            db.execute(
                &format!("UPDATE {table} SET payload = zeroblob(?1) WHERE position = 1"),
                [i64::try_from(self.request.max_payload_bytes + 1).unwrap()],
            )
            .unwrap();
        }
    }

    fn assert_progress_unchanged(&mut self) {
        assert_eq!(self.source.head(&self.request.scope).unwrap(), 1);
        assert_eq!(
            self.replica
                .load(&self.request.scope)
                .unwrap()
                .unwrap()
                .position(),
            1
        );
        assert_eq!(self.replica.count(&self.request.scope).unwrap(), 1);
    }

    fn assert_id_refused(&mut self, text_attempts: usize) {
        reset_counts();
        assert_eq!(
            self.source.page(&self.request),
            Err(SourceError::Unavailable)
        );
        assert_eq!(counts(), (text_attempts, 0));
        reset_counts();
        assert_eq!(
            self.source.tail(&self.tail_request()),
            Err(HistorySourceError::Unavailable)
        );
        assert_eq!(counts(), (text_attempts, 0));
        reset_counts();
        assert_eq!(
            self.source.older(&self.older_request()),
            Err(HistorySourceError::Unavailable)
        );
        assert_eq!(counts(), (text_attempts, 0));
        reset_counts();
        assert_eq!(
            self.replica.read_after(&self.request.scope, 0, 1, 32),
            Err(StoreError::Failed)
        );
        assert_eq!(counts(), (text_attempts, 0));
        self.assert_progress_unchanged();
    }

    fn assert_payload_refused(&mut self) {
        reset_counts();
        assert_eq!(
            self.source.page(&self.request),
            Err(SourceError::OversizedRecord)
        );
        assert_eq!(counts(), (0, 0));
        assert_eq!(
            self.source.tail(&self.tail_request()),
            Err(HistorySourceError::OversizedRecord)
        );
        assert_eq!(counts(), (0, 0));
        assert_eq!(
            self.source.older(&self.older_request()),
            Err(HistorySourceError::OversizedRecord)
        );
        assert_eq!(counts(), (0, 0));
        assert_eq!(
            self.replica.read_after(&self.request.scope, 0, 1, 32),
            Err(StoreError::Failed)
        );
        assert_eq!(counts(), (0, 0));
        self.assert_progress_unchanged();
    }
}

fn exact_byte_identities() -> Vec<String> {
    vec![
        "x".repeat(MAX_ID_BYTES),
        "é".repeat(MAX_ID_BYTES / 2) + &"x".repeat(MAX_ID_BYTES % 2),
        "🙂".repeat(MAX_ID_BYTES / 4) + &"x".repeat(MAX_ID_BYTES % 4),
        "\0".to_owned() + &"é".repeat((MAX_ID_BYTES - 1) / 2) + &"x".repeat((MAX_ID_BYTES - 1) % 2),
    ]
}

#[test]
fn forward_source_reads_identities_at_published_utf8_limit() {
    for encoding in ENCODINGS {
        for text in exact_byte_identities() {
            assert_eq!(text.len(), MAX_ID_BYTES);
            let mut fixture = Fixture::new(&text, encoding).reopened();
            reset_counts();
            let page = fixture.source.page(&fixture.request).unwrap();
            assert_eq!(page.records[0].id.as_str(), text);
            assert_eq!(counts(), (1, 1));
        }
    }
}

#[test]
fn history_source_reads_identities_at_published_utf8_limit() {
    for encoding in ENCODINGS {
        for text in exact_byte_identities() {
            let mut fixture = Fixture::new(&text, encoding).reopened();
            reset_counts();
            let tail = fixture.source.tail(&fixture.tail_request()).unwrap();
            assert_eq!(tail.records[0].id.as_str(), text);
            assert_eq!(counts(), (1, 1));
            reset_counts();
            let older = fixture.source.older(&fixture.older_request()).unwrap();
            assert_eq!(older.records[0].id.as_str(), text);
            assert_eq!(counts(), (1, 1));
        }
    }
}

#[test]
fn replica_reads_identities_at_published_utf8_limit() {
    for encoding in ENCODINGS {
        for text in exact_byte_identities() {
            let mut fixture = Fixture::new(&text, encoding).reopened();
            reset_counts();
            let records = fixture
                .replica
                .read_after(&fixture.request.scope, 0, 1, 32)
                .unwrap();
            assert_eq!(records[0].id.as_str(), text);
            assert_eq!(counts(), (1, 1));
        }
    }
}

#[test]
fn oversized_storage_is_refused_before_utf8_materialization_even_with_large_payload() {
    for encoding in ENCODINGS {
        for text in [
            "x".repeat(MAX_STORED_ID_BYTES + 1),
            "🙂".repeat(MAX_STORED_ID_BYTES / 4) + &"x".repeat(MAX_STORED_ID_BYTES % 4 + 1),
        ] {
            for payload_oversize in [false, true] {
                let fixture = Fixture::new("original", encoding);
                fixture.replace_stored_id(&text, false);
                if payload_oversize {
                    fixture.oversized_payload();
                }
                let mut fixture = fixture.reopened();
                fixture.assert_id_refused(0);
            }
        }
    }
}

#[test]
fn bounded_semantic_oversize_is_refused_before_payload_copy() {
    for encoding in ENCODINGS {
        let text = "é".repeat(MAX_ID_BYTES / 2) + &"x".repeat(MAX_ID_BYTES % 2 + 1);
        let fixture = Fixture::new("original", encoding);
        fixture.replace_stored_id(&text, false);
        let mut fixture = fixture.reopened();
        fixture.assert_id_refused(1);
    }
}

#[test]
fn bounded_blank_stored_identity_is_refused_by_domain_constructor() {
    for encoding in ENCODINGS {
        for text in ["", "\u{2003}"] {
            let fixture = Fixture::new("original", encoding);
            fixture.replace_stored_id(&text, false);
            let mut fixture = fixture.reopened();
            fixture.assert_id_refused(1);
        }
    }
}

#[test]
fn wrong_type_and_invalid_returned_utf8_are_refused_before_payload_copy() {
    let bytes = vec![0x80];
    for encoding in ENCODINGS {
        let fixture = Fixture::new("original", encoding);
        fixture.replace_stored_id(&bytes, false);
        let mut fixture = fixture.reopened();
        fixture.assert_id_refused(1);
    }
    // Invalid UTF-8 in a UTF-8 TEXT value is not changed by SQLite's text API.
    // UTF-16 physical decoding remains SQLite-owned, as the design table states.
    let fixture = Fixture::new("original", "UTF-8");
    fixture.replace_stored_id(&bytes, true);
    let mut fixture = fixture.reopened();
    fixture.assert_id_refused(1);
}

#[test]
fn oversized_payload_precedes_bounded_semantic_id_inspection() {
    for encoding in ENCODINGS {
        for id_invalid in [false, true] {
            let fixture = Fixture::new("original", encoding);
            if id_invalid {
                let text = "é".repeat(MAX_ID_BYTES / 2) + &"x".repeat(MAX_ID_BYTES % 2 + 1);
                fixture.replace_stored_id(&text, false);
            }
            fixture.oversized_payload();
            let mut fixture = fixture.reopened();
            fixture.assert_payload_refused();
        }
    }
}

fn assert_metadata_avoids_text_conversion(path: &Path, sql: &str, parameters: impl Params) {
    let db = Connection::open(path).unwrap();
    let mut statement = db.prepare(&format!("EXPLAIN {sql}")).unwrap();
    let instructions = statement
        .query_map(parameters, |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(6)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // Bundled SQLite 3.53.2's OPFLAG_BYTELENARG is 0xc0. A direct octet_length
    // column uses that flag so overflow text bytes need not be materialized.
    assert!(instructions
        .iter()
        .any(|(opcode, flags)| opcode == "Column" && flags & 0xc0 == 0xc0));
}

#[test]
fn actual_metadata_queries_use_sqlite_stored_byte_length_optimization() {
    for encoding in ENCODINGS {
        let fixture = Fixture::new("original", encoding).reopened();
        assert_metadata_avoids_text_conversion(
            &fixture.directory.path("source.sqlite"),
            FORWARD_RECORD_METADATA_SQL,
            params![0, 1, 1, i64::try_from(MAX_STORED_ID_BYTES).unwrap()],
        );
        assert_metadata_avoids_text_conversion(
            &fixture.directory.path("source.sqlite"),
            HISTORY_RECORD_METADATA_SQL,
            params![1, 2, 1, i64::try_from(MAX_STORED_ID_BYTES).unwrap()],
        );
        assert_metadata_avoids_text_conversion(
            &fixture.directory.path("replica.sqlite"),
            REPLICA_RECORD_METADATA_SQL,
            params![
                "r",
                "o",
                "s",
                0,
                1,
                1,
                i64::try_from(MAX_STORED_ID_BYTES).unwrap()
            ],
        );
    }
}
