//! Source-file format and atomic publication, owned by the record append path.
use super::super::source_metadata::{decode, Publication};
use super::*;
use crate::replication::domain::{
    validate_head_transition, validate_writer_capabilities, CapabilityError, CapabilitySupport,
    HeadTransitionError, SOURCE_HEAD_VERSION,
};

/// Source-owned compare-and-append refusal. No variant changes receiver progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourcePublicationError {
    /// Expected publication belongs to another immutable source identity.
    IdentityChanged,
    /// Another committed publication changed head or requirements; reload metadata.
    Stale,
    /// This writer does not implement the persisted or requested requirements.
    Incompatible(CapabilityError),
    /// Requested requirements violate incarnation monotonicity.
    Transition(HeadTransitionError),
    /// Local transaction failed, conflicted, or has an uncertain commit outcome.
    Store(StoreError),
}

pub(super) fn initialize(
    conn: &mut rusqlite::Connection,
    identity: [&Id; 4],
) -> Result<(), SqliteOpenError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let application: i64 = tx.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    let mut version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if application == 0 && version == 0 {
        let tables: i64 = tx.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        if tables != 0 {
            return Err(SqliteOpenError::UnexpectedDatabase);
        }
        tx.execute_batch(SCHEMA)?;
        tx.execute(
            "INSERT INTO source_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
            params![
                identity[0].as_str(),
                identity[1].as_str(),
                identity[2].as_str(),
                identity[3].as_str()
            ],
        )?;
        tx.pragma_update(None, "application_id", SOURCE_APPLICATION_ID)?;
        version = 1;
    } else if application != SOURCE_APPLICATION_ID || !matches!(version, 1..=3) {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    // Validate immutable ownership before migrating any existing file.
    let saved: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT origin,stream,incarnation,schema_id FROM source_meta WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    match saved {
        Some((o, s, i, k))
            if [&o, &s, &i, &k]
                .iter()
                .zip(identity)
                .all(|(a, b)| a.as_str() == b.as_str()) => {}
        Some(_) => return Err(SqliteOpenError::SourceIdentityMismatch),
        None => return Err(SqliteOpenError::UnexpectedDatabase),
    }
    let columns: Vec<String> = {
        let mut statement = tx.prepare("PRAGMA table_info(source_meta)")?;
        let rows = statement
            .query_map([], |r| r.get(1))?
            .collect::<Result<_, _>>()?;
        rows
    };
    let mut expected = vec![
        "singleton",
        "origin",
        "stream",
        "incarnation",
        "schema_id",
        "head",
    ];
    if version >= 2 {
        expected.extend(["metadata_version", "reader_required", "writer_required"]);
    }
    if columns != expected {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    if version == 1 {
        tx.execute_batch("ALTER TABLE source_meta ADD COLUMN metadata_version INTEGER NOT NULL DEFAULT 1;
            ALTER TABLE source_meta ADD COLUMN reader_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(reader_required)='blob' AND length(reader_required)=8);
            ALTER TABLE source_meta ADD COLUMN writer_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(writer_required)='blob' AND length(writer_required)=8);")?;
        // Older slice2 files may lack the later history-floor table.
        tx.execute_batch("CREATE TABLE IF NOT EXISTS source_history (singleton INTEGER PRIMARY KEY CHECK(singleton=1), pruned_through INTEGER NOT NULL CHECK(pruned_through>=0));
            INSERT OR IGNORE INTO source_history VALUES(1,0);")?;
    }
    if version < 3 {
        super::retention::migrate(&tx)?;
        tx.pragma_update(None, "user_version", 3)?;
    }
    super::retention::verify(&tx)?;
    read(&tx)?; // A malformed/missing/future partial row must never become zero.
    tx.commit()?;
    Ok(())
}

pub(super) fn read(conn: &rusqlite::Connection) -> rusqlite::Result<Publication> {
    conn.query_row("SELECT metadata_version,head,
        CASE WHEN typeof(reader_required)='blob' AND length(reader_required)=8 THEN reader_required END,
        CASE WHEN typeof(writer_required)='blob' AND length(writer_required)=8 THEN writer_required END
        FROM source_meta WHERE singleton=1",[],decode)
}

impl SqliteReferenceSource {
    /// Atomically compare a fresh exact source publication and append one fact
    /// with monotonic requirements. Receiver/access epoch do not own this state.
    /// Only implemented encoder bits may be published (currently both masks0).
    /// A duplicate ID never publishes requirements; exact duplicates preserve
    /// normal append idempotence only when the requested masks are unchanged.
    /// On Stale/Uncertain reload source_head; retry is not proof of prior commit.
    pub fn compare_append(
        &mut self,
        expected: &SourceHead,
        id: &Id,
        payload: &[u8],
        reader: u64,
        writer: u64,
    ) -> Result<u64, SourcePublicationError> {
        if !self.matches(expected.scope()) {
            return Err(SourcePublicationError::IdentityChanged);
        }
        self.append_publication(id, payload, Some((expected, reader, writer)))
    }
    pub(super) fn append_publication(
        &mut self,
        id: &Id,
        payload: &[u8],
        update: Option<(&SourceHead, u64, u64)>,
    ) -> Result<u64, SourcePublicationError> {
        use SourcePublicationError as Error;
        if payload.len() > MAX_RECORD_BYTES {
            return Err(Error::Store(StoreError::Failed));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| Error::Store(StoreError::Failed))?;
        let current = read(&tx).map_err(|_| Error::Store(StoreError::Failed))?;
        current.writer().map_err(Error::Incompatible)?;
        let (reader, writer) = if let Some((expected, reader, writer)) = update {
            if expected.version() != current.version
                || expected.position() != current.position
                || expected.reader_required() != current.reader
                || expected.writer_required() != current.writer
            {
                return Err(Error::Stale);
            }
            let proposed =
                SourceHead::new(expected.scope().clone(), current.position, reader, writer);
            validate_head_transition(&current.head(expected.scope()), &proposed)
                .map_err(Error::Transition)?;
            validate_writer_capabilities(&proposed, CapabilitySupport::current())
                .map_err(Error::Incompatible)?;
            (reader, writer)
        } else {
            (current.reader, current.writer)
        };
        let existing: Option<(i64, i64)> = tx
            .query_row(
                "SELECT position,length(payload) FROM source_records WHERE record_id=?1",
                [id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(|_| Error::Store(StoreError::Failed))?;
        if let Some((position, length)) = existing {
            if usize::try_from(length).ok() != Some(payload.len())
                || (reader, writer) != (current.reader, current.writer)
            {
                return Err(Error::Store(StoreError::ConflictingRecord));
            }
            let saved: Vec<u8> = tx
                .query_row(
                    "SELECT payload FROM source_records WHERE record_id=?1",
                    [id.as_str()],
                    |r| r.get(0),
                )
                .map_err(|_| Error::Store(StoreError::Failed))?;
            return if saved == payload {
                u64::try_from(position).map_err(|_| Error::Store(StoreError::Failed))
            } else {
                Err(Error::Store(StoreError::ConflictingRecord))
            };
        }
        let next = current
            .position
            .checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or(Error::Store(StoreError::Failed))?;
        tx.execute("UPDATE source_meta SET head=?1,metadata_version=?2,reader_required=?3,writer_required=?4 WHERE singleton=1",params![next,SOURCE_HEAD_VERSION,reader.to_be_bytes().as_slice(),writer.to_be_bytes().as_slice()]).map_err(|_|Error::Store(StoreError::Failed))?;
        tx.execute(
            "INSERT INTO source_records(position,record_id,payload) VALUES(?1,?2,?3)",
            params![next, id.as_str(), payload],
        )
        .map_err(|_| Error::Store(StoreError::Failed))?;
        tx.commit()
            .map_err(|_| Error::Store(StoreError::Uncertain))?;
        u64::try_from(next).map_err(|_| Error::Store(StoreError::Failed))
    }
}
