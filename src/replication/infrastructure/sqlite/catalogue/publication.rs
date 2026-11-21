//! Catalogue-owned migration and atomic current-value publication.
use super::super::source_metadata::{decode, Publication};
use super::*;
use crate::replication::domain::{
    validate_head_transition, validate_writer_capabilities, CapabilityError, CapabilitySupport,
    HeadTransitionError, SOURCE_HEAD_VERSION,
};
/// Source-owned publication/collection refusal; receiver progress is unaffected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CataloguePublicationError {
    /// Expected publication belongs to another immutable source identity.
    IdentityChanged,
    /// Head/version/requirements/horizon changed; reload the current publication.
    Stale,
    /// Current or requested requirements exceed this encoder's support.
    Incompatible(CapabilityError),
    /// Requirements would regress within this source incarnation.
    Transition(HeadTransitionError),
    /// Requested coverage is outside the source's monotone inclusive bounds.
    InvalidHorizon {
        /// Previous committed horizon.
        previous: u64,
        /// Requested horizon.
        requested: u64,
        /// Current committed source revision.
        head: u64,
    },
    /// This source requires the explicit retention-policy collector.
    RetentionPolicyRequired,
    /// Local mutation failed, conflicted, or has an uncertain commit outcome.
    Store(CatalogueStoreError),
}
/// Confirmed native collection receipt; no receiver progress or grant is implied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogueCollection {
    /// Committed v2 source publication; entry revision/masks are unchanged.
    pub head: CatalogueHorizonHead,
    /// Newly hidden retired serving markers, never permanent identity rows.
    pub hidden_markers: usize,
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
    } else if application != CATALOGUE_SOURCE_APPLICATION_ID || !matches!(version, 1..=7) {
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
    if version >= 4 {
        expected.push("horizon");
    }
    if version >= 5 {
        expected.push("retention_now_ms");
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
        version = 4;
    }
    if version == 4 {
        retention::migrate(&tx)?;
        version = 5;
    }
    if version == 5 {
        tx.execute_batch(
            "CREATE TABLE catalogue_retired_proof (
            entry_id TEXT PRIMARY KEY, creation INTEGER NOT NULL CHECK(creation>0),
            revision INTEGER NOT NULL CHECK(revision>=creation)
        ) WITHOUT ROWID;
        CREATE INDEX catalogue_compaction_candidates ON catalogue_source_entries(revision, entry_id)
            WHERE manifest_visible=0 AND deleted=1 AND length(payload)=0;
        CREATE TRIGGER catalogue_retired_no_insert BEFORE INSERT ON catalogue_source_entries
        WHEN EXISTS(SELECT 1 FROM catalogue_retired_proof WHERE entry_id=NEW.entry_id)
        BEGIN SELECT RAISE(ABORT, 'retired identity'); END;",
        )?;
        tx.pragma_update(None, "user_version", 7)?;
    } else if version == 6 {
        // Draft v6 was pushed and may have produced source files. Rebuild only
        // that older index; v5 already creates the final index above.
        tx.execute_batch("DROP INDEX catalogue_compaction_candidates;
            CREATE INDEX catalogue_compaction_candidates ON catalogue_source_entries(revision, entry_id)
            WHERE manifest_visible=0 AND deleted=1 AND length(payload)=0;")?;
        tx.pragma_update(None, "user_version", 7)?;
    }
    compaction::verify(&tx)?;
    retention::verify(&tx)?;
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
        let retired: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM catalogue_retired_proof WHERE entry_id=?1)",
                [entry_id.as_str()],
                |r| r.get(0),
            )
            .map_err(|_| Error::Store(CatalogueStoreError::Failed))?;
        if retired {
            return Err(Error::Store(CatalogueStoreError::Fenced));
        }
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

/// Source-internal seam: caller owns this source identity and an Immediate write
/// transaction. Catalogue pin validation/selection must run in this same
/// transaction before this helper; it must not precompute eligibility outside it.
/// This helper does not commit or open another transaction. Publish its candidate
/// receipt only after the caller's commit succeeds.
pub(super) fn collect_in_transaction(
    tx: &rusqlite::Transaction<'_>,
    expected: &CatalogueHorizonHead,
    requested: u64,
) -> Result<CatalogueCollection, CataloguePublicationError> {
    use CataloguePublicationError as E;
    let (publication, previous) =
        read_snapshot(tx).map_err(|_| E::Store(CatalogueStoreError::Failed))?;
    publication.writer().map_err(E::Incompatible)?;
    let current = CatalogueHorizonHead::new(publication.head(expected.source.scope()), previous);
    if current != *expected {
        return Err(E::Stale);
    }
    if requested < previous || requested > publication.position {
        return Err(E::InvalidHorizon {
            previous,
            requested,
            head: publication.position,
        });
    }
    let horizon = to_i64(requested).map_err(E::Store)?;
    let changed = tx
        .execute(
            "UPDATE catalogue_source_meta SET horizon=?1 WHERE singleton=1",
            [horizon],
        )
        .map_err(|_| E::Store(CatalogueStoreError::Failed))?;
    if changed != 1 {
        return Err(E::Store(CatalogueStoreError::Failed));
    }
    let hidden_markers=tx.execute("UPDATE catalogue_source_entries SET manifest_visible=0 WHERE manifest_visible=1 AND deleted=1 AND revision<?1",[horizon]).map_err(|_|E::Store(CatalogueStoreError::Failed))?;
    Ok(CatalogueCollection {
        head: CatalogueHorizonHead::new(publication.head(expected.source.scope()), requested),
        hidden_markers,
    })
}
impl SqliteCatalogueSource {
    /// Explicitly activate/advance native deletion coverage and collect eligible
    /// serving markers atomically under the existing source writer. The caller
    /// owns source policy; after any pin registration, use
    /// [`Self::collect_with_retention`] even if all pins have expired.
    /// Immutable source identity, complete v2 publication and writer support are
    /// checked before any effect, including an equal-horizon no-op. Bounds are
    /// previous<=requested<=head; only retired revision<requested is hidden.
    /// Live rows, permanent IDs, creation/deletion revisions and payloads remain.
    /// On Stale/Uncertain reload v2 metadata and inspect before retrying. Stop old
    /// binaries before activation; current legacy read APIs refuse positive h.
    pub fn compare_collect(
        &mut self,
        expected: &CatalogueHorizonHead,
        requested: u64,
    ) -> Result<CatalogueCollection, CataloguePublicationError> {
        if !self.matches(expected.source.scope()) {
            return Err(CataloguePublicationError::IdentityChanged);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CataloguePublicationError::Store(CatalogueStoreError::Failed))?;
        if retention::has_registration(&tx)
            .map_err(|_| CataloguePublicationError::Store(CatalogueStoreError::Failed))?
        {
            return Err(CataloguePublicationError::RetentionPolicyRequired);
        }
        let receipt = collect_in_transaction(&tx, expected, requested)?;
        tx.commit()
            .map_err(|_| CataloguePublicationError::Store(CatalogueStoreError::Uncertain))?;
        Ok(receipt)
    }
}

#[cfg(test)]
mod collection_commit_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn commit_refusal_is_uncertain_and_reload_has_no_partial_publication() {
        let path = std::env::temp_dir().join(format!(
            "owned-collection-commit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let id = |s: &str| Id::new(s).unwrap();
        let scope = Scope::new(
            id("receiver"),
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
            id("epoch"),
        );
        let mut source = SqliteCatalogueSource::open(
            &path,
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
        )
        .unwrap();
        source.upsert(&id("retired"), b"old").unwrap();
        source.delete(&id("retired")).unwrap();
        source.upsert(&id("live"), b"live").unwrap();
        let expected = source
            .horizon_head(&scope, CapabilitySupport::current())
            .unwrap();
        // Ordinary deferred constraint failure occurs at COMMIT, after both
        // statements ran. It must never expose a half-activated source.
        source.conn.execute_batch("PRAGMA foreign_keys=ON;
        CREATE TABLE fixture_parent(id INTEGER PRIMARY KEY);
        CREATE TABLE fixture_child(id INTEGER REFERENCES fixture_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER fixture_commit AFTER UPDATE OF horizon ON catalogue_source_meta BEGIN INSERT INTO fixture_child VALUES(1); END;").unwrap();
        assert_eq!(
            source.compare_collect(&expected, 3),
            Err(CataloguePublicationError::Store(
                CatalogueStoreError::Uncertain
            ))
        );
        assert_eq!(
            source
                .horizon_head(&scope, CapabilitySupport::current())
                .unwrap(),
            expected
        );
        assert_eq!(source.conn.query_row("SELECT manifest_visible FROM catalogue_source_entries WHERE entry_id='retired'",[],|r|r.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(
            source
                .conn
                .query_row("SELECT count(*) FROM fixture_child", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        source
            .conn
            .execute_batch(
                "DROP TRIGGER fixture_commit; DROP TABLE fixture_child; DROP TABLE fixture_parent;",
            )
            .unwrap();
        drop(source);
        let mut source = SqliteCatalogueSource::open(
            &path,
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
        )
        .unwrap();
        assert_eq!(
            source
                .horizon_head(&scope, CapabilitySupport::current())
                .unwrap(),
            expected
        );
        assert_eq!(
            source.compare_collect(&expected, 3).unwrap().hidden_markers,
            1
        );
        drop(source);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod collection_seam_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn helper_keeps_publication_and_membership_in_callers_uncommitted_transaction() {
        let path = std::env::temp_dir().join(format!(
            "owned-collection-seam-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let id = |s: &str| Id::new(s).unwrap();
        let scope = Scope::new(
            id("receiver"),
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
            id("epoch"),
        );
        let mut source = SqliteCatalogueSource::open(
            &path,
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
        )
        .unwrap();
        source.upsert(&id("retired"), b"old").unwrap();
        source.delete(&id("retired")).unwrap();
        source.upsert(&id("live"), b"live").unwrap();
        let expected = source
            .horizon_head(&scope, CapabilitySupport::current())
            .unwrap();
        let observer = Connection::open(&path).unwrap();
        let snapshot = || {
            observer.query_row("SELECT horizon,(SELECT manifest_visible FROM catalogue_source_entries WHERE entry_id='retired') FROM catalogue_source_meta",[],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?))).unwrap()
        };
        let tx = source
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let pending = collect_in_transaction(&tx, &expected, 3).unwrap();
        assert_eq!(pending.hidden_markers, 1);
        assert_eq!(snapshot(), (0, 1));
        tx.rollback().unwrap();
        assert_eq!(snapshot(), (0, 1));
        let tx = source
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        collect_in_transaction(&tx, &expected, 3).unwrap();
        assert_eq!(snapshot(), (0, 1));
        tx.commit().unwrap();
        assert_eq!(snapshot(), (3, 0));
        drop(observer);
        drop(source);
        std::fs::remove_file(path).unwrap();
    }
}
