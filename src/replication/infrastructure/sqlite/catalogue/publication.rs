//! Catalogue-owned migration and atomic current-value publication.
use super::super::source_metadata::{decode, Publication};
use super::*;
use crate::replication::domain::{
    validate_head_transition, validate_writer_capabilities, CapabilityError, CapabilitySupport,
    HeadTransitionError, SOURCE_HEAD_VERSION,
};
/// Source-owned compare-and-upsert refusal; receiver progress is unaffected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CataloguePublicationError {
    /// Expected publication belongs to another immutable source identity.
    IdentityChanged,
    /// Head/version/requirements changed; reload the current publication.
    Stale,
    /// Current or requested requirements exceed this encoder's support.
    Incompatible(CapabilityError),
    /// Requirements would regress within this source incarnation.
    Transition(HeadTransitionError),
    /// Local mutation failed, conflicted, or has an uncertain commit outcome.
    Store(CatalogueStoreError),
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
        tx.execute_batch(SOURCE_SCHEMA)?;
        tx.execute(
            "INSERT INTO catalogue_source_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
            params![
                identity[0].as_str(),
                identity[1].as_str(),
                identity[2].as_str(),
                identity[3].as_str()
            ],
        )?;
        tx.pragma_update(None, "application_id", CATALOGUE_SOURCE_APPLICATION_ID)?;
        version = 1;
    } else if application != CATALOGUE_SOURCE_APPLICATION_ID || !matches!(version, 1..=4) {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    // Validate immutable ownership before migrating any existing file.
    let saved: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT origin,stream,incarnation,schema_id FROM catalogue_source_meta WHERE singleton=1",
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
        let mut statement = tx.prepare("PRAGMA table_info(catalogue_source_meta)")?;
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
    if version == 4 {
        expected.push("horizon");
    }
    if columns != expected {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    if version == 1 {
        tx.execute_batch("ALTER TABLE catalogue_source_meta ADD COLUMN metadata_version INTEGER NOT NULL DEFAULT 1;
            ALTER TABLE catalogue_source_meta ADD COLUMN reader_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(reader_required)='blob' AND length(reader_required)=8);
            ALTER TABLE catalogue_source_meta ADD COLUMN writer_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(writer_required)='blob' AND length(writer_required)=8);")?;
        version = 2;
    }
    let columns: Vec<String> = {
        let mut statement = tx.prepare("PRAGMA table_info(catalogue_source_entries)")?;
        let rows = statement
            .query_map([], |r| r.get(1))?
            .collect::<Result<_, _>>()?;
        rows
    };
    let mut expected = vec!["entry_id", "creation", "revision", "deleted", "payload"];
    if version >= 3 {
        expected.push("manifest_visible");
    }
    if columns != expected {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    if version == 2 {
        // Identity survives independently of the bounded serving index. All
        // existing rows stay served: collection requires later horizon admission.
        tx.execute_batch("ALTER TABLE catalogue_source_entries ADD COLUMN manifest_visible INTEGER NOT NULL DEFAULT 1
            CHECK(manifest_visible IN (0,1) AND (manifest_visible = 1 OR deleted = 1));
            DROP INDEX catalogue_source_page;
            CREATE INDEX catalogue_source_page ON catalogue_source_entries(creation,entry_id) WHERE manifest_visible = 1;")?;
        version = 3;
    }
    if version == 3 {
        tx.execute_batch("ALTER TABLE catalogue_source_meta ADD COLUMN horizon INTEGER NOT NULL DEFAULT 0 CHECK(horizon>=0 AND horizon<=head);")?;
        tx.pragma_update(None, "user_version", 4)?;
    }
    read(&tx)?; // A malformed/missing/future partial row must never become zero.
    tx.commit()?;
    Ok(())
}

pub(super) fn read(conn: &Connection) -> rusqlite::Result<Publication> {
    read_snapshot(conn).map(|p| p.0)
}
pub(super) fn read_snapshot(conn: &Connection) -> rusqlite::Result<(Publication, u64)> {
    conn.query_row(
        "SELECT metadata_version,head,
 CASE WHEN typeof(reader_required)='blob' AND length(reader_required)=8 THEN reader_required END,
 CASE WHEN typeof(writer_required)='blob' AND length(writer_required)=8 THEN writer_required END,
 CASE WHEN typeof(horizon)='integer' THEN horizon END
 FROM catalogue_source_meta WHERE singleton=1",
        [],
        |r| {
            let p = decode(r)?;
            let raw: i64 = r.get(4)?;
            let h = u64::try_from(raw).map_err(|_| rusqlite::Error::InvalidQuery)?;
            if h > p.position {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok((p, h))
        },
    )
}

impl SqliteCatalogueSource {
    /// Atomically compare head/version/masks and publish one current value with
    /// monotonic requirements. Only implemented encoder bits may be requested
    /// (currently zero). Equal payload cannot publish a metadata-only change.
    /// Receiver/access epoch do not own source requirements. On Stale/Uncertain,
    /// reload publication and inspect the current entry before retrying.
    pub fn compare_upsert(
        &mut self,
        expected: &SourceHead,
        entry_id: &Id,
        payload: &[u8],
        reader: u64,
        writer: u64,
    ) -> Result<u64, CataloguePublicationError> {
        if !self.matches(expected.scope()) {
            return Err(CataloguePublicationError::IdentityChanged);
        }
        self.upsert_publication(entry_id, payload, Some((expected, reader, writer)))
    }
    pub(super) fn upsert_publication(
        &mut self,
        entry_id: &Id,
        payload: &[u8],
        update: Option<(&SourceHead, u64, u64)>,
    ) -> Result<u64, CataloguePublicationError> {
        use CataloguePublicationError as Error;
        if payload.len() > MAX_CATALOGUE_PAYLOAD_BYTES {
            return Err(Error::Store(CatalogueStoreError::Failed));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| Error::Store(CatalogueStoreError::Failed))?;
        let current = read(&tx).map_err(|_| Error::Store(CatalogueStoreError::Failed))?;
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
        let existing:Option<(i64,i64,i64,Vec<u8>)>=tx.query_row("SELECT creation,revision,deleted,payload FROM catalogue_source_entries WHERE entry_id=?1",[entry_id.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(|_|Error::Store(CatalogueStoreError::Failed))?;
        if existing
            .as_ref()
            .is_some_and(|(_, _, deleted, _)| *deleted != 0)
        {
            return Err(Error::Store(CatalogueStoreError::Fenced));
        }
        if let Some((_, revision, _, prior)) = &existing {
            if prior == payload {
                if (reader, writer) != (current.reader, current.writer) {
                    return Err(Error::Store(CatalogueStoreError::Conflict));
                }
                return to_u64(*revision).map_err(Error::Store);
            }
        }
        let revision = current
            .position
            .checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or(Error::Store(CatalogueStoreError::Failed))?;
        let creation = existing.map_or(revision, |(creation, _, _, _)| creation);
        tx.execute("UPDATE catalogue_source_meta SET head=?1,metadata_version=?2,reader_required=?3,writer_required=?4 WHERE singleton=1",params![revision,SOURCE_HEAD_VERSION,reader.to_be_bytes().as_slice(),writer.to_be_bytes().as_slice()]).map_err(|_|Error::Store(CatalogueStoreError::Failed))?;
        tx.execute("INSERT INTO catalogue_source_entries(entry_id,creation,revision,deleted,payload) VALUES(?1,?2,?3,0,?4) ON CONFLICT(entry_id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload",params![entry_id.as_str(),creation,revision,payload]).map_err(|_|Error::Store(CatalogueStoreError::Failed))?;
        tx.commit()
            .map_err(|_| Error::Store(CatalogueStoreError::Uncertain))?;
        to_u64(revision).map_err(Error::Store)
    }
}
