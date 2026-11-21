//! Separate SQLite reference source and receiver for current-value catalogues.
//! Source rows hold only the latest value; receiver rows hold only the latest
//! applied value and one resumable finite pass.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::replication::catalogue::{
    catalogue_progress_after_begin, catalogue_progress_after_page, catalogue_progress_after_reset,
    validate_catalogue_pass, validate_catalogue_progress, validate_catalogue_revision_transition,
    validate_manifest_request, CatalogueHeadSource, CatalogueManifestRead, CataloguePagePlan,
    CataloguePass, CatalogueProgress, CatalogueReadSource, CatalogueSource, CatalogueSourceError,
    CatalogueStore, CatalogueStoreError, CatalogueValidationError, CatalogueValueRead, EntryKey,
    ManifestEntry, ManifestPage, ManifestRequest, ResolvedEntry, MAX_CATALOGUE_ENTRIES,
    MAX_CATALOGUE_PAYLOAD_BYTES,
};
use crate::replication::catalogue::{
    CatalogueHorizonHead, CatalogueHorizonSource, CatalogueHorizonStore, CatalogueReadKind,
    HorizonManifestRead, HorizonValueRead,
};
use crate::replication::domain::{validate_capabilities, CapabilitySupport, Id, Scope, SourceHead};

use super::{
    configure_connection, open_connection, SqliteOpenError, CATALOGUE_REPLICA_APPLICATION_ID,
    CATALOGUE_SOURCE_APPLICATION_ID,
};

mod compaction;
mod horizon;
mod publication;
mod recovery;
mod retention;
mod source_connection;
pub use compaction::SqliteCatalogueMaintenance;
pub use publication::{CatalogueCollection, CataloguePublicationError};
pub use retention::{CataloguePinReceipt, CatalogueRetentionError};

const SOURCE_SCHEMA: &str = "
CREATE TABLE catalogue_source_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    origin TEXT NOT NULL, stream TEXT NOT NULL, incarnation TEXT NOT NULL,
    schema_id TEXT NOT NULL, head INTEGER NOT NULL CHECK (head >= 0)
);
CREATE TABLE catalogue_source_entries (
    entry_id TEXT PRIMARY KEY, creation INTEGER NOT NULL CHECK (creation > 0),
    revision INTEGER NOT NULL CHECK (revision >= creation),
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)), payload BLOB NOT NULL
);
CREATE INDEX catalogue_source_page ON catalogue_source_entries (creation, entry_id);
";

const STORE_SCHEMA: &str = "
CREATE TABLE catalogue_progress (
    receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL,
    incarnation TEXT NOT NULL, schema_id TEXT NOT NULL, access_epoch TEXT NOT NULL,
    completed INTEGER NOT NULL CHECK (completed >= 0),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    active_boundary INTEGER, cursor_creation INTEGER, cursor_id TEXT,
    PRIMARY KEY (receiver, origin, stream)
) WITHOUT ROWID;
CREATE TABLE catalogue_entries (
    receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL,
    entry_id TEXT NOT NULL, creation INTEGER NOT NULL CHECK (creation > 0),
    revision INTEGER NOT NULL CHECK (revision >= creation),
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)), payload BLOB NOT NULL,
    PRIMARY KEY (receiver, origin, stream, entry_id),
    FOREIGN KEY (receiver, origin, stream)
        REFERENCES catalogue_progress (receiver, origin, stream)
) WITHOUT ROWID;
";

fn id(value: String) -> Result<Id, CatalogueStoreError> {
    Id::new(value).map_err(|_| CatalogueStoreError::Failed)
}
fn to_i64(value: u64) -> Result<i64, CatalogueStoreError> {
    i64::try_from(value).map_err(|_| CatalogueStoreError::Failed)
}
fn to_u64(value: i64) -> Result<u64, CatalogueStoreError> {
    u64::try_from(value).map_err(|_| CatalogueStoreError::Failed)
}

/// Latest-value source with atomic revision assignment. A deleted ID cannot be
/// resurrected; replacing the entire catalogue requires a new incarnation.
/// Physical compaction is available only on the explicit offline owner:
///
/// ```compile_fail
/// use nessa_sync::replication::infrastructure::SqliteCatalogueSource;
/// fn compact(source: &mut SqliteCatalogueSource) { source.compact_retired(1); }
/// ```
pub struct SqliteCatalogueSource {
    conn: Connection,
    origin: Id,
    stream: Id,
    incarnation: Id,
    schema: Id,
    head_reads: u64,
    manifest_reads: u64,
    resolve_reads: u64,
    manifest_bytes: u64,
    payload_bytes: u64,
}

impl SqliteCatalogueSource {
    /// Opens an existing matching source, or initializes a new isolated file.
    /// A matching identity with a v1–v6 marker returns
    /// [`SqliteOpenError::OfflineUpgradeRequired`] before validating its legacy
    /// layout; offline maintenance can still reject an incomplete file.
    /// Existing v7 files are checked without schema DDL. SQLite may still
    /// perform connection, journal, or lock handling.
    pub fn open(
        path: impl AsRef<Path>,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
    ) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        publication::initialize(&mut conn, [&origin, &stream, &incarnation, &schema], false)?;
        source_connection::configure(&conn)?;
        Ok(Self {
            conn,
            origin,
            stream,
            incarnation,
            schema,
            head_reads: 0,
            manifest_reads: 0,
            resolve_reads: 0,
            manifest_bytes: 0,
            payload_bytes: 0,
        })
    }

    /// Replaces one current value and advances the global revision atomically.
    /// Fresh unsupported writer requirements return Failed before no-op success
    /// or mutation; compare_upsert preserves typed publication errors.
    pub fn upsert(&mut self, entry_id: &Id, payload: &[u8]) -> Result<u64, CatalogueStoreError> {
        self.upsert_publication(entry_id, payload, None)
            .map_err(|error| match error {
                CataloguePublicationError::Store(error) => error,
                _ => CatalogueStoreError::Failed,
            })
    }

    /// Replaces a visible entry with a retained deletion marker, preserving its key.
    pub fn delete(&mut self, entry_id: &Id) -> Result<u64, CatalogueStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        publication::read(&tx)
            .map_err(|_| CatalogueStoreError::Failed)?
            .writer()
            .map_err(|_| CatalogueStoreError::Failed)?;
        let existing: Option<i64> = tx
            .query_row(
                "SELECT deleted FROM catalogue_source_entries WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| CatalogueStoreError::Failed)?;
        match existing {
            None => {
                let retired: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM catalogue_retired_proof WHERE entry_id=?1)",
                        [entry_id.as_str()],
                        |r| r.get(0),
                    )
                    .map_err(|_| CatalogueStoreError::Failed)?;
                return Err(if retired {
                    CatalogueStoreError::Fenced
                } else {
                    CatalogueStoreError::Failed
                });
            }
            Some(1) => return Err(CatalogueStoreError::Fenced),
            Some(_) => {}
        }
        let head: i64 = tx
            .query_row(
                "SELECT head FROM catalogue_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueStoreError::Failed)?;
        let revision = head.checked_add(1).ok_or(CatalogueStoreError::Failed)?;
        tx.execute("UPDATE catalogue_source_entries SET revision = ?1, deleted = 1, payload = x'' WHERE entry_id = ?2", params![revision, entry_id.as_str()]).map_err(|_| CatalogueStoreError::Failed)?;
        tx.execute(
            "UPDATE catalogue_source_meta SET head = ?1 WHERE singleton = 1",
            [revision],
        )
        .map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        to_u64(revision)
    }

    /// Metadata page read count on this handle.
    pub fn manifest_reads(&self) -> u64 {
        self.manifest_reads
    }
    /// Current-value resolution read count on this handle.
    pub fn resolve_reads(&self) -> u64 {
        self.resolve_reads
    }
    /// Sum of compact ID and revision bytes returned by manifest reads.
    pub fn manifest_bytes(&self) -> u64 {
        self.manifest_bytes
    }
    /// Sum of opaque current payload bytes returned by resolution reads.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
    /// Head read count on this handle.
    pub fn head_reads(&self) -> u64 {
        self.head_reads
    }

    fn matches(&self, scope: &Scope) -> bool {
        scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
    }
}

impl CatalogueSource for SqliteCatalogueSource {
    fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError> {
        if !self.matches(scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let (value, horizon) = publication::read_snapshot(&self.conn)
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        horizon::admit(
            None,
            0,
            0,
            &CatalogueHorizonHead::new(value.head(scope), horizon),
        )?;
        value.reader().map_err(CatalogueSourceError::Incompatible)?;
        self.head_reads += 1;
        Ok(value.position)
    }

    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestPage, CatalogueSourceError> {
        self.manifest_with_head(request, CapabilitySupport::current())
            .map(|read| read.page)
    }
    fn resolve(
        &mut self,
        pass: &CataloguePass,
        entry_id: &Id,
        max_payload_bytes: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        self.resolve_with_head(
            pass,
            entry_id,
            max_payload_bytes,
            CapabilitySupport::current(),
        )
        .map(|read| read.value)
    }
}

impl SqliteCatalogueSource {
    fn manifest_snapshot(
        &mut self,
        request: &ManifestRequest,
        supported: CapabilitySupport,
        kind: Option<CatalogueReadKind>,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        if !self.matches(&request.pass.scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        validate_manifest_request(request, MAX_CATALOGUE_ENTRIES)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let boundary = i64::try_from(request.pass.boundary)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let completed = i64::try_from(request.pass.completed)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let cursor_creation = request.pass.cursor.as_ref().map_or(0, |key| key.creation);
        let cursor_creation =
            i64::try_from(cursor_creation).map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let cursor_id = request
            .pass
            .cursor
            .as_ref()
            .map_or("", |key| key.id.as_str());
        let limit = i64::try_from(request.max_entries + 1)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let tx = self
            .conn
            .transaction()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let (publication, horizon) =
            publication::read_snapshot(&tx).map_err(|_| CatalogueSourceError::Unavailable)?;
        let snapshot_head = publication.head(&request.pass.scope);
        validate_capabilities(&snapshot_head, supported)
            .map_err(CatalogueSourceError::Incompatible)?;
        horizon::admit(
            kind,
            request.pass.completed,
            request.pass.boundary,
            &CatalogueHorizonHead::new(snapshot_head.clone(), horizon),
        )?;
        let head: i64 = tx
            .query_row(
                "SELECT head FROM catalogue_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if head < boundary {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let mut rows: Vec<(String, i64, i64, i64)> = {
            let mut statement = tx.prepare("SELECT entry_id, creation, revision, deleted FROM catalogue_source_entries
                WHERE manifest_visible = 1 AND creation <= ?1 AND revision > ?2 AND (creation > ?3 OR (creation = ?3 AND entry_id > ?4))
                ORDER BY creation, entry_id LIMIT ?5").map_err(|_| CatalogueSourceError::Unavailable)?;
            let rows = statement
                .query_map(
                    params![boundary, completed, cursor_creation, cursor_id, limit],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|_| CatalogueSourceError::Unavailable)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| CatalogueSourceError::Unavailable)?;
            rows
        };
        tx.commit().map_err(|_| CatalogueSourceError::Unavailable)?;
        let has_more = rows.len() > request.max_entries;
        rows.truncate(request.max_entries);
        let mut entries = Vec::with_capacity(rows.len());
        let mut bytes = 0u64;
        for (entry_id, creation, revision, deleted) in rows {
            let id = Id::new(entry_id).map_err(|_| CatalogueSourceError::Unavailable)?;
            bytes = bytes.saturating_add(id.as_str().len() as u64 + 17);
            entries.push(ManifestEntry {
                key: EntryKey {
                    creation: u64::try_from(creation)
                        .map_err(|_| CatalogueSourceError::Unavailable)?,
                    id,
                },
                revision: u64::try_from(revision).map_err(|_| CatalogueSourceError::Unavailable)?,
                deleted: deleted != 0,
            });
        }
        self.manifest_reads += 1;
        self.manifest_bytes = self.manifest_bytes.saturating_add(bytes);
        if entries
            .iter()
            .any(|entry| entry.revision > snapshot_head.position())
        {
            return Err(CatalogueSourceError::Unavailable);
        }
        Ok(HorizonManifestRead {
            head: CatalogueHorizonHead::new(snapshot_head, horizon),
            page: ManifestPage {
                request: request.clone(),
                entries,
                has_more,
            },
        })
    }

    fn value_snapshot(
        &mut self,
        pass: &CataloguePass,
        entry_id: &Id,
        max_payload_bytes: usize,
        supported: CapabilitySupport,
        kind: Option<CatalogueReadKind>,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        if !self.matches(&pass.scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        validate_catalogue_pass(pass).map_err(|_| CatalogueSourceError::InvalidRequest)?;
        if max_payload_bytes == 0 || max_payload_bytes > MAX_CATALOGUE_PAYLOAD_BYTES {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let (publication, horizon) =
            publication::read_snapshot(&tx).map_err(|_| CatalogueSourceError::Unavailable)?;
        let snapshot_head = publication.head(&pass.scope);
        validate_capabilities(&snapshot_head, supported)
            .map_err(CatalogueSourceError::Incompatible)?;
        horizon::admit(
            kind,
            pass.completed,
            pass.boundary,
            &CatalogueHorizonHead::new(snapshot_head.clone(), horizon),
        )?;
        if snapshot_head.position() < pass.boundary {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let metadata: Option<(i64, i64, i64, i64)> = tx.query_row(
            "SELECT creation, revision, deleted, length(payload) FROM catalogue_source_entries WHERE entry_id = ?1 UNION ALL SELECT creation, revision, 1, 0 FROM catalogue_retired_proof WHERE entry_id = ?1", [entry_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        ).optional().map_err(|_| CatalogueSourceError::Unavailable)?;
        let (creation, revision, deleted, len) =
            metadata.ok_or(CatalogueSourceError::Unavailable)?;
        if usize::try_from(len).map_err(|_| CatalogueSourceError::Unavailable)? > max_payload_bytes
        {
            return Err(CatalogueSourceError::OversizedEntry);
        }
        if u64::try_from(revision).map_err(|_| CatalogueSourceError::Unavailable)?
            > snapshot_head.position()
        {
            return Err(CatalogueSourceError::Unavailable);
        }
        let payload: Vec<u8> = tx
            .query_row(
                "SELECT payload FROM catalogue_source_entries WHERE entry_id = ?1 UNION ALL SELECT x'' FROM catalogue_retired_proof WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        tx.commit().map_err(|_| CatalogueSourceError::Unavailable)?;
        self.resolve_reads += 1;
        self.payload_bytes = self.payload_bytes.saturating_add(payload.len() as u64);
        Ok(HorizonValueRead {
            head: CatalogueHorizonHead::new(snapshot_head, horizon),
            value: ResolvedEntry {
                manifest: ManifestEntry {
                    key: EntryKey {
                        creation: u64::try_from(creation)
                            .map_err(|_| CatalogueSourceError::Unavailable)?,
                        id: entry_id.clone(),
                    },
                    revision: u64::try_from(revision)
                        .map_err(|_| CatalogueSourceError::Unavailable)?,
                    deleted: deleted != 0,
                },
                payload,
            },
        })
    }
}

/// Durable receiver with one active finite pass and current cached entries.
pub struct SqliteCatalogueStore {
    conn: Connection,
}

impl SqliteCatalogueStore {
    /// Opens an isolated receiver file without replacing unrelated user data.
    /// Version1/2 receivers migrate atomically to version3 recovery/coverage
    /// metadata; populated cache/progress survive. Stop old binaries before upgrading.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        recovery::initialize(&mut conn)?;
        configure_connection(&conn)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { conn })
    }

    /// Returns one locally cached current entry without a source read.
    pub fn cached_entry(
        &self,
        scope: &Scope,
        entry_id: &Id,
    ) -> Result<Option<ResolvedEntry>, CatalogueStoreError> {
        let saved = read_progress(&self.conn, scope)?;
        if saved
            .as_ref()
            .is_some_and(|progress| progress.scope != *scope)
        {
            return Err(CatalogueStoreError::ResetRequired);
        }
        let row: Option<(i64, i64, i64, Vec<u8>)> = self.conn.query_row(
            "SELECT creation, revision, deleted, payload FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND entry_id = ?4",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), entry_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        ).optional().map_err(|_| CatalogueStoreError::Failed)?;
        row.map(|(creation, revision, deleted, payload)| {
            Ok(ResolvedEntry {
                manifest: ManifestEntry {
                    key: EntryKey {
                        creation: to_u64(creation)?,
                        id: entry_id.clone(),
                    },
                    revision: to_u64(revision)?,
                    deleted: deleted != 0,
                },
                payload,
            })
        })
        .transpose()
    }

    /// Number of locally cached current identities, including deletion markers.
    pub fn count(&self, scope: &Scope) -> Result<u64, CatalogueStoreError> {
        let saved = read_progress(&self.conn, scope)?;
        if saved
            .as_ref()
            .is_some_and(|progress| progress.scope != *scope)
        {
            return Err(CatalogueStoreError::ResetRequired);
        }
        let count: i64 = self.conn.query_row("SELECT COUNT(*) FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()], |row| row.get(0)).map_err(|_| CatalogueStoreError::Failed)?;
        to_u64(count)
    }
}

fn read_progress(
    conn: &Connection,
    scope: &Scope,
) -> Result<Option<CatalogueProgress>, CatalogueStoreError> {
    type Row = (
        String,
        String,
        String,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        Option<String>,
    );
    let row: Option<Row> = conn.query_row(
        "SELECT incarnation, schema_id, access_epoch, completed, generation, active_boundary, cursor_creation, cursor_id
         FROM catalogue_progress WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
        params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?))
    ).optional().map_err(|_| CatalogueStoreError::Failed)?;
    row.map(
        |(
            incarnation,
            schema,
            epoch,
            completed,
            generation,
            boundary,
            cursor_creation,
            cursor_id,
        )| {
            let saved_scope = Scope::new(
                scope.receiver().clone(),
                scope.origin().clone(),
                scope.stream().clone(),
                id(incarnation)?,
                id(schema)?,
                id(epoch)?,
            );
            let completed = to_u64(completed)?;
            let generation = to_u64(generation)?;
            let cursor = match (cursor_creation, cursor_id) {
                (Some(creation), Some(id_value)) => Some(EntryKey {
                    creation: to_u64(creation)?,
                    id: id(id_value)?,
                }),
                (None, None) => None,
                _ => return Err(CatalogueStoreError::Failed),
            };
            if boundary.is_none() && cursor.is_some() {
                return Err(CatalogueStoreError::Failed);
            }
            let active = boundary
                .map(|boundary| -> Result<CataloguePass, CatalogueStoreError> {
                    Ok(CataloguePass {
                        scope: saved_scope.clone(),
                        completed,
                        boundary: to_u64(boundary)?,
                        cursor,
                        generation,
                    })
                })
                .transpose()?;
            let progress = CatalogueProgress {
                scope: saved_scope,
                completed,
                generation,
                active,
            };
            validate_catalogue_progress(&progress).map_err(CatalogueStoreError::from)?;
            Ok(progress)
        },
    )
    .transpose()
}

impl CatalogueStore for SqliteCatalogueStore {
    fn progress(
        &mut self,
        scope: &Scope,
    ) -> Result<Option<CatalogueProgress>, CatalogueStoreError> {
        recovery::progress_snapshot(&mut self.conn, scope, || {})
    }

    fn begin(
        &mut self,
        scope: &Scope,
        expected: Option<CatalogueProgress>,
        boundary: u64,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        recovery::ordinary(&tx, scope)?;
        let current = read_progress(&tx, scope)?;
        if current != expected {
            return Err(CatalogueStoreError::Stale);
        }
        horizon::check_completion(&tx, scope, current.as_ref().map_or(0, |p| p.completed))?;
        let progress = catalogue_progress_after_begin(scope, current.as_ref(), boundary)
            .map_err(CatalogueStoreError::from)?;
        let active_boundary = progress
            .active
            .as_ref()
            .map(|pass| to_i64(pass.boundary))
            .transpose()?;
        tx.execute("INSERT INTO catalogue_progress (receiver, origin, stream, incarnation, schema_id, access_epoch, completed, generation, active_boundary, cursor_creation, cursor_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL)
            ON CONFLICT(receiver, origin, stream) DO UPDATE SET completed=excluded.completed, generation=excluded.generation, active_boundary=excluded.active_boundary, cursor_creation=NULL, cursor_id=NULL",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str(), to_i64(progress.completed)?, to_i64(progress.generation)?, active_boundary]
        ).map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(progress)
    }

    fn cached_revision(
        &mut self,
        scope: &Scope,
        entry_id: &Id,
    ) -> Result<Option<u64>, CatalogueStoreError> {
        let saved = read_progress(&self.conn, scope)?;
        if saved
            .as_ref()
            .is_some_and(|progress| progress.scope != *scope)
        {
            return Err(CatalogueStoreError::ResetRequired);
        }
        let revision: Option<i64> = self.conn.query_row("SELECT revision FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND entry_id = ?4",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), entry_id.as_str()], |row| row.get(0)).optional().map_err(|_| CatalogueStoreError::Failed)?;
        revision.map(to_u64).transpose()
    }

    fn apply_page(
        &mut self,
        plan: CataloguePagePlan,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        let progress = catalogue_progress_after_page(&plan).map_err(CatalogueStoreError::from)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        recovery::ordinary(&tx, &plan.pass.scope)?;
        let current = read_progress(&tx, &plan.pass.scope)?.ok_or(CatalogueStoreError::Stale)?;
        if current.scope != plan.pass.scope {
            return Err(CatalogueStoreError::ResetRequired);
        }
        if current.active.as_ref() != Some(&plan.pass) {
            return Err(CatalogueStoreError::Stale);
        }
        let scope = &plan.pass.scope;
        horizon::check_completion(&tx, scope, plan.pass.completed)?;
        write_values(&tx, scope, &plan.entries, &plan.unchanged)?;
        let cursor = progress
            .active
            .as_ref()
            .and_then(|pass| pass.cursor.as_ref());
        let cursor_creation = cursor.map(|key| to_i64(key.creation)).transpose()?;
        let active_boundary = progress
            .active
            .as_ref()
            .map(|pass| to_i64(pass.boundary))
            .transpose()?;
        tx.execute("UPDATE catalogue_progress SET completed = ?4, active_boundary = ?5, cursor_creation = ?6, cursor_id = ?7, generation = ?8
            WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), to_i64(progress.completed)?, active_boundary, cursor_creation, cursor.map(|key| key.id.as_str()), to_i64(progress.generation)?]
        ).map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(progress)
    }

    fn reset(
        &mut self,
        scope: &Scope,
        expected: CatalogueProgress,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        let progress =
            catalogue_progress_after_reset(scope, &expected).map_err(CatalogueStoreError::from)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        // An explicitly authorized scope replacement may cancel old work;
        // same-scope normal reset must not abandon unclassified candidates.
        if *scope == expected.scope {
            recovery::ordinary(&tx, scope)?;
        }
        let current = read_progress(&tx, scope)?.ok_or(CatalogueStoreError::Stale)?;
        if current != expected {
            return Err(CatalogueStoreError::Stale);
        }
        tx.execute(
            "DELETE FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3",
            params![
                scope.receiver().as_str(),
                scope.origin().as_str(),
                scope.stream().as_str()
            ],
        )
        .map_err(|_| CatalogueStoreError::Failed)?;
        tx.execute("DELETE FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND deleted = 0",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()])
            .map_err(|_| CatalogueStoreError::Failed)?;
        tx.execute("UPDATE catalogue_progress SET incarnation = ?4, schema_id = ?5, access_epoch = ?6,
            completed = 0, generation = ?7, active_boundary = NULL, cursor_creation = NULL, cursor_id = NULL, reset_phase = 0, reset_boundary = NULL
            WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str(), to_i64(progress.generation)?])
            .map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(progress)
    }
}

fn write_values(
    tx: &Connection,
    scope: &Scope,
    entries: &[ResolvedEntry],
    unchanged: &[ManifestEntry],
) -> Result<(), CatalogueStoreError> {
    for entry in unchanged {
        let saved: Option<(i64, i64, i64)> = tx.query_row("SELECT creation, revision, deleted FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND entry_id = ?4",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), entry.key.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional().map_err(|_| CatalogueStoreError::Failed)?;
        let (creation, revision, deleted) = saved.ok_or(CatalogueStoreError::Stale)?;
        let cached = ManifestEntry {
            key: EntryKey {
                creation: to_u64(creation)?,
                id: entry.key.id.clone(),
            },
            revision: to_u64(revision)?,
            deleted: deleted != 0,
        };
        validate_catalogue_revision_transition(entry, &cached).map_err(revision_error)?;
    }
    for value in entries {
        let key = &value.manifest.key;
        let saved: Option<(i64, i64, i64, Vec<u8>)> = tx.query_row("SELECT creation, revision, deleted, payload FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND entry_id = ?4",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), key.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).optional().map_err(|_| CatalogueStoreError::Failed)?;
        if let Some((creation, revision, deleted, payload)) = saved {
            let cached = ManifestEntry {
                key: EntryKey {
                    creation: to_u64(creation)?,
                    id: key.id.clone(),
                },
                revision: to_u64(revision)?,
                deleted: deleted != 0,
            };
            let (earlier, later) = if cached.revision > value.manifest.revision {
                (&value.manifest, &cached)
            } else {
                (&cached, &value.manifest)
            };
            validate_catalogue_revision_transition(earlier, later).map_err(revision_error)?;
            if cached.revision > value.manifest.revision {
                continue;
            }
            if cached.revision == value.manifest.revision {
                if payload != value.payload {
                    return Err(CatalogueStoreError::Conflict);
                }
                continue;
            }
        }
        tx.execute("INSERT INTO catalogue_entries (receiver, origin, stream, entry_id, creation, revision, deleted, payload)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(receiver, origin, stream, entry_id) DO UPDATE SET revision=excluded.revision, deleted=excluded.deleted, payload=excluded.payload",
                params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), key.id.as_str(), to_i64(key.creation)?, to_i64(value.manifest.revision)?, i64::from(value.manifest.deleted), &value.payload]
            ).map_err(|_| CatalogueStoreError::Failed)?;
    }
    Ok(())
}

fn revision_error(error: CatalogueValidationError) -> CatalogueStoreError {
    match error {
        CatalogueValidationError::DeletionFence => CatalogueStoreError::Fenced,
        _ => CatalogueStoreError::Conflict,
    }
}

impl CatalogueHeadSource for SqliteCatalogueSource {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, CatalogueSourceError> {
        if !self.matches(scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        self.head_reads += 1;
        let (value, horizon) = publication::read_snapshot(&self.conn)
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        horizon::admit(
            None,
            0,
            0,
            &CatalogueHorizonHead::new(value.head(scope), horizon),
        )?;
        Ok(value.head(scope))
    }
}
