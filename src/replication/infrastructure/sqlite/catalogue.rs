//! Separate SQLite reference source and receiver for current-value catalogues.
//! Source rows hold only the latest value; receiver rows hold only the latest
//! applied value and one resumable finite pass.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::replication::catalogue::{
    validate_catalogue_page_plan, validate_catalogue_revision_transition, CataloguePagePlan,
    CataloguePass, CatalogueProgress, CatalogueSource, CatalogueSourceError, CatalogueStore,
    CatalogueStoreError, CatalogueValidationError, EntryKey, ManifestEntry, ManifestPage,
    ManifestRequest, ResolvedEntry, MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES,
};
use crate::replication::domain::{Id, Scope};

use super::{
    configure_connection, ensure_schema, open_connection, SqliteOpenError,
    CATALOGUE_REPLICA_APPLICATION_ID, CATALOGUE_SOURCE_APPLICATION_ID,
};

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
    pub fn open(
        path: impl AsRef<Path>,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
    ) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        ensure_schema(
            &mut conn,
            CATALOGUE_SOURCE_APPLICATION_ID,
            SOURCE_SCHEMA,
            |tx| {
                tx.execute(
                    "INSERT INTO catalogue_source_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
                    params![
                        origin.as_str(),
                        stream.as_str(),
                        incarnation.as_str(),
                        schema.as_str()
                    ],
                )?;
                Ok(())
            },
        )?;
        let saved: Option<(String, String, String, String)> = conn.query_row(
            "SELECT origin, stream, incarnation, schema_id FROM catalogue_source_meta WHERE singleton = 1",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        ).optional()?;
        match saved {
            Some((o, s, i, k))
                if o == origin.as_str()
                    && s == stream.as_str()
                    && i == incarnation.as_str()
                    && k == schema.as_str() => {}
            Some(_) => return Err(SqliteOpenError::SourceIdentityMismatch),
            None => return Err(SqliteOpenError::UnexpectedDatabase),
        }
        configure_connection(&conn)?;
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
    pub fn upsert(&mut self, entry_id: &Id, payload: &[u8]) -> Result<u64, CatalogueStoreError> {
        if payload.len() > MAX_CATALOGUE_PAYLOAD_BYTES {
            return Err(CatalogueStoreError::Failed);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        let existing: Option<(i64, i64, i64, Vec<u8>)> = tx
            .query_row(
                "SELECT creation, revision, deleted, payload FROM catalogue_source_entries WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|_| CatalogueStoreError::Failed)?;
        if existing
            .as_ref()
            .is_some_and(|(_, _, deleted, _)| *deleted != 0)
        {
            return Err(CatalogueStoreError::Fenced);
        }
        if let Some((_, revision, _, prior)) = &existing {
            if prior == payload {
                return to_u64(*revision);
            }
        }
        let head: i64 = tx
            .query_row(
                "SELECT head FROM catalogue_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueStoreError::Failed)?;
        let revision = head.checked_add(1).ok_or(CatalogueStoreError::Failed)?;
        let creation = existing.map_or(revision, |(creation, _, _, _)| creation);
        tx.execute("INSERT INTO catalogue_source_entries (entry_id, creation, revision, deleted, payload) VALUES (?1, ?2, ?3, 0, ?4)
            ON CONFLICT(entry_id) DO UPDATE SET revision=excluded.revision, payload=excluded.payload",
            params![entry_id.as_str(), creation, revision, payload]).map_err(|_| CatalogueStoreError::Failed)?;
        tx.execute(
            "UPDATE catalogue_source_meta SET head = ?1 WHERE singleton = 1",
            [revision],
        )
        .map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        to_u64(revision)
    }

    /// Replaces a visible entry with a retained deletion marker, preserving its key.
    pub fn delete(&mut self, entry_id: &Id) -> Result<u64, CatalogueStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
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
            None => return Err(CatalogueStoreError::Failed),
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
        let head: i64 = self
            .conn
            .query_row(
                "SELECT head FROM catalogue_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        self.head_reads += 1;
        u64::try_from(head).map_err(|_| CatalogueSourceError::Unavailable)
    }

    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestPage, CatalogueSourceError> {
        if !self.matches(&request.pass.scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        if request.max_entries == 0
            || request.max_entries > MAX_CATALOGUE_ENTRIES
            || request.pass.boundary <= request.pass.completed
        {
            return Err(CatalogueSourceError::InvalidRequest);
        }
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
                WHERE creation <= ?1 AND revision > ?2 AND (creation > ?3 OR (creation = ?3 AND entry_id > ?4))
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
        Ok(ManifestPage {
            request: request.clone(),
            entries,
            has_more,
        })
    }

    fn resolve(
        &mut self,
        pass: &CataloguePass,
        entry_id: &Id,
        max_payload_bytes: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        if !self.matches(&pass.scope) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        if max_payload_bytes == 0 || max_payload_bytes > MAX_CATALOGUE_PAYLOAD_BYTES {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let metadata: Option<(i64, i64, i64, i64)> = tx.query_row(
            "SELECT creation, revision, deleted, length(payload) FROM catalogue_source_entries WHERE entry_id = ?1", [entry_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        ).optional().map_err(|_| CatalogueSourceError::Unavailable)?;
        let (creation, revision, deleted, len) =
            metadata.ok_or(CatalogueSourceError::Unavailable)?;
        if usize::try_from(len).map_err(|_| CatalogueSourceError::Unavailable)? > max_payload_bytes
        {
            return Err(CatalogueSourceError::OversizedEntry);
        }
        let payload: Vec<u8> = tx
            .query_row(
                "SELECT payload FROM catalogue_source_entries WHERE entry_id = ?1",
                [entry_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        tx.commit().map_err(|_| CatalogueSourceError::Unavailable)?;
        self.resolve_reads += 1;
        self.payload_bytes = self.payload_bytes.saturating_add(payload.len() as u64);
        Ok(ResolvedEntry {
            manifest: ManifestEntry {
                key: EntryKey {
                    creation: u64::try_from(creation)
                        .map_err(|_| CatalogueSourceError::Unavailable)?,
                    id: entry_id.clone(),
                },
                revision: u64::try_from(revision).map_err(|_| CatalogueSourceError::Unavailable)?,
                deleted: deleted != 0,
            },
            payload,
        })
    }
}

/// Durable receiver with one active finite pass and current cached entries.
pub struct SqliteCatalogueStore {
    conn: Connection,
}

impl SqliteCatalogueStore {
    /// Opens an isolated receiver file without replacing unrelated user data.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        ensure_schema(
            &mut conn,
            CATALOGUE_REPLICA_APPLICATION_ID,
            STORE_SCHEMA,
            |_| Ok(()),
        )?;
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
                .map(|boundary| {
                    Ok(CataloguePass {
                        scope: saved_scope.clone(),
                        completed,
                        boundary: to_u64(boundary)?,
                        cursor,
                        generation,
                    })
                })
                .transpose()?;
            if generation == 0 && (completed != 0 || active.is_some()) {
                return Err(CatalogueStoreError::Failed);
            }
            if active
                .as_ref()
                .is_some_and(|pass| pass.boundary <= completed)
            {
                return Err(CatalogueStoreError::Failed);
            }
            Ok(CatalogueProgress {
                scope: saved_scope,
                completed,
                generation,
                active,
            })
        },
    )
    .transpose()
}

impl CatalogueStore for SqliteCatalogueStore {
    fn progress(
        &mut self,
        scope: &Scope,
    ) -> Result<Option<CatalogueProgress>, CatalogueStoreError> {
        read_progress(&self.conn, scope)
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
        let current = read_progress(&tx, scope)?;
        if current != expected {
            return Err(CatalogueStoreError::Stale);
        }
        if current.as_ref().is_some_and(|saved| saved.scope != *scope) {
            return Err(CatalogueStoreError::ResetRequired);
        }
        let completed = current.as_ref().map_or(0, |saved| saved.completed);
        let confirmed_empty = current.is_none() && boundary == 0;
        if (!confirmed_empty && boundary <= completed)
            || current.as_ref().is_some_and(|saved| saved.active.is_some())
        {
            return Err(CatalogueStoreError::Stale);
        }
        let generation = current
            .as_ref()
            .map_or(1, |saved| saved.generation.checked_add(1).unwrap_or(0));
        if generation == 0 {
            return Err(CatalogueStoreError::Failed);
        }
        tx.execute("INSERT INTO catalogue_progress (receiver, origin, stream, incarnation, schema_id, access_epoch, completed, generation, active_boundary, cursor_creation, cursor_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL)
            ON CONFLICT(receiver, origin, stream) DO UPDATE SET completed=excluded.completed, generation=excluded.generation, active_boundary=excluded.active_boundary, cursor_creation=NULL, cursor_id=NULL",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str(), to_i64(completed)?, to_i64(generation)?, if confirmed_empty { None } else { Some(to_i64(boundary)?) }]
        ).map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        read_progress(&self.conn, scope)?.ok_or(CatalogueStoreError::Failed)
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
        validate_catalogue_page_plan(&plan).map_err(|_| CatalogueStoreError::Conflict)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        let current = read_progress(&tx, &plan.pass.scope)?.ok_or(CatalogueStoreError::Stale)?;
        if current.scope != plan.pass.scope {
            return Err(CatalogueStoreError::ResetRequired);
        }
        if current.active.as_ref() != Some(&plan.pass) {
            return Err(CatalogueStoreError::Stale);
        }
        let scope = &plan.pass.scope;
        for entry in &plan.unchanged {
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
        for value in &plan.entries {
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
        let cursor_creation = if plan.final_page {
            None
        } else {
            plan.next_cursor
                .as_ref()
                .map(|key| to_i64(key.creation))
                .transpose()?
        };
        let cursor_id = if plan.final_page {
            None
        } else {
            plan.next_cursor.as_ref().map(|key| key.id.as_str())
        };
        tx.execute("UPDATE catalogue_progress SET completed = ?4, active_boundary = ?5, cursor_creation = ?6, cursor_id = ?7
            WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), to_i64(if plan.final_page { plan.pass.boundary } else { plan.pass.completed })?, if plan.final_page { None } else { Some(to_i64(plan.pass.boundary)?) }, cursor_creation, cursor_id]
        ).map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        read_progress(&self.conn, scope)?.ok_or(CatalogueStoreError::Failed)
    }

    fn reset(
        &mut self,
        scope: &Scope,
        expected: CatalogueProgress,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        if expected.scope.receiver() != scope.receiver()
            || expected.scope.origin() != scope.origin()
            || expected.scope.stream() != scope.stream()
        {
            return Err(CatalogueStoreError::ResetRequired);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        let current = read_progress(&tx, scope)?.ok_or(CatalogueStoreError::Stale)?;
        if current != expected {
            return Err(CatalogueStoreError::Stale);
        }
        let generation = current
            .generation
            .checked_add(1)
            .ok_or(CatalogueStoreError::Failed)?;
        tx.execute("DELETE FROM catalogue_entries WHERE receiver = ?1 AND origin = ?2 AND stream = ?3 AND deleted = 0",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str()])
            .map_err(|_| CatalogueStoreError::Failed)?;
        tx.execute("UPDATE catalogue_progress SET incarnation = ?4, schema_id = ?5, access_epoch = ?6,
            completed = 0, generation = ?7, active_boundary = NULL, cursor_creation = NULL, cursor_id = NULL
            WHERE receiver = ?1 AND origin = ?2 AND stream = ?3",
            params![scope.receiver().as_str(), scope.origin().as_str(), scope.stream().as_str(), scope.incarnation().as_str(), scope.schema().as_str(), scope.access_epoch().as_str(), to_i64(generation)?])
            .map_err(|_| CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        read_progress(&self.conn, scope)?.ok_or(CatalogueStoreError::Failed)
    }
}

fn revision_error(error: CatalogueValidationError) -> CatalogueStoreError {
    match error {
        CatalogueValidationError::DeletionFence => CatalogueStoreError::Fenced,
        _ => CatalogueStoreError::Conflict,
    }
}
