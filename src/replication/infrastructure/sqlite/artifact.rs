//! Isolated SQLite reference source and receiver cache for opaque artifacts.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::replication::artifacts::{
    validate_chunk, validate_manifest, ArtifactCacheError, ArtifactCacheIndex, ArtifactKey,
    ArtifactManifest, ArtifactState, CachedArtifact, ChunkReply, ChunkRequest, ChunkSource,
    ChunkSourceError, ContentIdentity, ManifestReply, ManifestRequest, ManifestSource,
    ManifestSourceError, Sha256Digest, TransferProgress, TransferStore, TransferStoreError,
    MAX_CHUNK_BYTES,
};
use crate::replication::domain::{Id, Scope};

use super::{
    configure_connection, ensure_schema, open_connection, SqliteOpenError,
    ARTIFACT_CACHE_APPLICATION_ID, ARTIFACT_SOURCE_APPLICATION_ID,
};

const SOURCE_SCHEMA: &str = "
CREATE TABLE artifact_source_meta (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 origin TEXT NOT NULL, stream TEXT NOT NULL, incarnation TEXT NOT NULL, schema_id TEXT NOT NULL,
 head INTEGER NOT NULL CHECK (head >= 0)
);
CREATE TABLE artifact_source_entries (
 artifact_id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK (revision > 0),
 deleted INTEGER NOT NULL CHECK (deleted IN (0,1)), length INTEGER NOT NULL CHECK (length >= 0),
 digest BLOB NOT NULL, payload BLOB NOT NULL
);
";

const CACHE_SCHEMA: &str = "
CREATE TABLE artifact_cache (
 receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL, incarnation TEXT NOT NULL,
 schema_id TEXT NOT NULL, artifact_id TEXT NOT NULL, access_epoch TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK (revision > 0), deleted INTEGER NOT NULL CHECK (deleted IN (0,1)),
 length INTEGER NOT NULL CHECK (length >= 0), digest BLOB NOT NULL,
 next_offset INTEGER NOT NULL CHECK (next_offset >= 0), verified INTEGER NOT NULL CHECK (verified IN (0,1)),
 PRIMARY KEY (receiver, origin, stream, incarnation, schema_id, artifact_id)
) WITHOUT ROWID;
CREATE TABLE artifact_chunks (
 receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL, incarnation TEXT NOT NULL,
 schema_id TEXT NOT NULL, artifact_id TEXT NOT NULL, offset INTEGER NOT NULL CHECK (offset >= 0),
 bytes BLOB NOT NULL,
 PRIMARY KEY (receiver, origin, stream, incarnation, schema_id, artifact_id, offset),
 FOREIGN KEY (receiver, origin, stream, incarnation, schema_id, artifact_id)
  REFERENCES artifact_cache (receiver, origin, stream, incarnation, schema_id, artifact_id)
) WITHOUT ROWID;
CREATE TABLE artifact_revoked_scopes (
 receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL, incarnation TEXT NOT NULL,
 schema_id TEXT NOT NULL, artifact_id TEXT NOT NULL, access_epoch TEXT NOT NULL,
 PRIMARY KEY (receiver, origin, stream, incarnation, schema_id, artifact_id, access_epoch)
) WITHOUT ROWID;
";

fn i64_value(value: u64) -> Result<i64, TransferStoreError> {
    i64::try_from(value).map_err(|_| TransferStoreError::Failed)
}
fn u64_value(value: i64) -> Result<u64, TransferStoreError> {
    u64::try_from(value).map_err(|_| TransferStoreError::Failed)
}
fn digest(bytes: Vec<u8>) -> Result<Sha256Digest, TransferStoreError> {
    Ok(Sha256Digest(
        bytes.try_into().map_err(|_| TransferStoreError::Failed)?,
    ))
}

/// One latest-value source file. This storage adapter is not an authorizer;
/// callers must compose it behind current policy, as the loopback server does.
pub struct SqliteArtifactSource {
    conn: Connection,
    origin: Id,
    stream: Id,
    incarnation: Id,
    schema: Id,
}

impl SqliteArtifactSource {
    /// Opens an isolated source file or checks its immutable identity.
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
            ARTIFACT_SOURCE_APPLICATION_ID,
            SOURCE_SCHEMA,
            |tx| {
                tx.execute(
                    "INSERT INTO artifact_source_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
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
        let saved: Option<(String,String,String,String)> = conn.query_row(
            "SELECT origin, stream, incarnation, schema_id FROM artifact_source_meta WHERE singleton = 1",
            [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))
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
        })
    }

    fn matches(&self, scope: &Scope) -> bool {
        scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
    }

    /// Commits a new current byte version. Exact repeats leave revision alone.
    pub fn upsert(&mut self, artifact_id: &Id, bytes: &[u8]) -> Result<u64, TransferStoreError> {
        let identity = ContentIdentity::of(bytes);
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        let old: Option<(i64,i64,Vec<u8>)> = tx.query_row(
            "SELECT revision, deleted, digest FROM artifact_source_entries WHERE artifact_id = ?1",
            [artifact_id.as_str()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))
        ).optional().map_err(|_| TransferStoreError::Failed)?;
        if let Some((revision, deleted, prior_digest)) = &old {
            if *deleted != 0 {
                return Err(TransferStoreError::Fenced);
            }
            let prior_length: i64 = tx
                .query_row(
                    "SELECT length FROM artifact_source_entries WHERE artifact_id = ?1",
                    [artifact_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| TransferStoreError::Failed)?;
            if u64_value(prior_length)? == identity.length && prior_digest == &identity.digest.0 {
                return u64_value(*revision);
            }
        }
        let head: i64 = tx
            .query_row(
                "SELECT head FROM artifact_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| TransferStoreError::Failed)?;
        let next = head.checked_add(1).ok_or(TransferStoreError::Failed)?;
        tx.execute("INSERT INTO artifact_source_entries (artifact_id, revision, deleted, length, digest, payload)
            VALUES (?1, ?2, 0, ?3, ?4, ?5)
            ON CONFLICT(artifact_id) DO UPDATE SET revision=excluded.revision, deleted=0,
                length=excluded.length, digest=excluded.digest, payload=excluded.payload",
            params![artifact_id.as_str(),next,i64_value(identity.length)?,&identity.digest.0[..],bytes]
        ).map_err(|_| TransferStoreError::Failed)?;
        tx.execute(
            "UPDATE artifact_source_meta SET head = ?1 WHERE singleton = 1",
            [next],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        u64_value(next)
    }

    /// Replaces a live value with a retained deletion marker.
    pub fn delete(&mut self, artifact_id: &Id) -> Result<u64, TransferStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        let deleted: Option<i64> = tx
            .query_row(
                "SELECT deleted FROM artifact_source_entries WHERE artifact_id = ?1",
                [artifact_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| TransferStoreError::Failed)?;
        if deleted != Some(0) {
            return Err(TransferStoreError::Fenced);
        }
        let head: i64 = tx
            .query_row(
                "SELECT head FROM artifact_source_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|_| TransferStoreError::Failed)?;
        let next = head.checked_add(1).ok_or(TransferStoreError::Failed)?;
        tx.execute(
            "UPDATE artifact_source_entries SET revision=?2, deleted=1, length=0,
            digest=x'', payload=x'' WHERE artifact_id=?1",
            params![artifact_id.as_str(), next],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.execute(
            "UPDATE artifact_source_meta SET head=?1 WHERE singleton=1",
            [next],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        u64_value(next)
    }
}

impl ManifestSource for SqliteArtifactSource {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        if !self.matches(&request.key.scope) {
            return Err(ManifestSourceError::ScopeChanged);
        }
        let row: Option<(i64,i64,i64,Vec<u8>)> = self.conn.query_row(
            "SELECT revision, deleted, length, digest FROM artifact_source_entries WHERE artifact_id=?1",
            [request.key.id.as_str()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))
        ).optional().map_err(|_| ManifestSourceError::Unavailable)?;
        let Some((revision, deleted, length, hash)) = row else {
            return Err(ManifestSourceError::Missing);
        };
        let state = if deleted != 0 {
            ArtifactState::Deleted
        } else {
            ArtifactState::Live(ContentIdentity {
                length: u64::try_from(length).map_err(|_| ManifestSourceError::Unavailable)?,
                digest: Sha256Digest(
                    hash.try_into()
                        .map_err(|_| ManifestSourceError::Unavailable)?,
                ),
            })
        };
        let manifest = ArtifactManifest {
            key: request.key.clone(),
            revision: u64::try_from(revision).map_err(|_| ManifestSourceError::Unavailable)?,
            state,
        };
        let reply = ManifestReply {
            request: request.clone(),
            manifest,
        };
        validate_manifest(request, &reply, None).map_err(|_| ManifestSourceError::Unavailable)?;
        Ok(reply)
    }
}

impl ChunkSource for SqliteArtifactSource {
    fn chunk(&mut self, request: &ChunkRequest) -> Result<ChunkReply, ChunkSourceError> {
        if !self.matches(&request.manifest.key.scope) {
            return Err(ChunkSourceError::ScopeChanged);
        }
        let ArtifactState::Live(content) = request.manifest.state else {
            return Err(ChunkSourceError::InvalidRequest);
        };
        if request.max_bytes == 0
            || request.max_bytes > MAX_CHUNK_BYTES
            || request.offset >= content.length
            || request.manifest.revision == 0
        {
            return Err(ChunkSourceError::InvalidRequest);
        }
        let tx = self
            .conn
            .transaction()
            .map_err(|_| ChunkSourceError::Unavailable)?;
        let current: Option<(i64,i64,i64,Vec<u8>)> = tx.query_row(
            "SELECT revision, deleted, length, digest FROM artifact_source_entries WHERE artifact_id=?1",
            [request.manifest.key.id.as_str()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))
        ).optional().map_err(|_| ChunkSourceError::Unavailable)?;
        let Some((revision, deleted, length, hash)) = current else {
            return Err(ChunkSourceError::Unavailable);
        };
        if deleted != 0 {
            return Err(ChunkSourceError::Deleted);
        }
        if u64::try_from(revision).ok() != Some(request.manifest.revision)
            || u64::try_from(length).ok() != Some(content.length)
            || hash != content.digest.0
        {
            return Err(ChunkSourceError::VersionChanged);
        }
        let requested = (content.length - request.offset).min(request.max_bytes as u64);
        let start = i64::try_from(
            request
                .offset
                .checked_add(1)
                .ok_or(ChunkSourceError::InvalidRequest)?,
        )
        .map_err(|_| ChunkSourceError::InvalidRequest)?;
        let amount = i64::try_from(requested).map_err(|_| ChunkSourceError::InvalidRequest)?;
        let bytes: Vec<u8> = tx
            .query_row(
                "SELECT substr(payload, ?2, ?3) FROM artifact_source_entries WHERE artifact_id=?1",
                params![request.manifest.key.id.as_str(), start, amount],
                |row| row.get(0),
            )
            .map_err(|_| ChunkSourceError::Unavailable)?;
        tx.commit().map_err(|_| ChunkSourceError::Unavailable)?;
        let reply = ChunkReply {
            request: request.clone(),
            bytes,
        };
        validate_chunk(request, &reply).map_err(|_| ChunkSourceError::Unavailable)?;
        Ok(reply)
    }
}

/// Durable receiver metadata and staged chunks; no source authority.
pub struct SqliteArtifactCache {
    conn: Connection,
}

impl SqliteArtifactCache {
    /// Opens a separate cache file without replacing unrelated data.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteOpenError> {
        let mut conn = open_connection(path)?;
        ensure_schema(
            &mut conn,
            ARTIFACT_CACHE_APPLICATION_ID,
            CACHE_SCHEMA,
            |_| Ok(()),
        )?;
        configure_connection(&conn)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { conn })
    }
}

fn cache_row(
    conn: &Connection,
    key: &ArtifactKey,
) -> Result<Option<TransferProgress>, TransferStoreError> {
    type Row = (String, i64, i64, i64, Vec<u8>, i64, i64);
    let row: Option<Row> = conn
        .query_row(
            "SELECT access_epoch, revision, deleted, length, digest, next_offset, verified
         FROM artifact_cache WHERE receiver=?1 AND origin=?2 AND stream=?3 AND incarnation=?4
          AND schema_id=?5 AND artifact_id=?6",
            params![
                key.scope.receiver().as_str(),
                key.scope.origin().as_str(),
                key.scope.stream().as_str(),
                key.scope.incarnation().as_str(),
                key.scope.schema().as_str(),
                key.id.as_str()
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(|_| TransferStoreError::Failed)?;
    row.map(|(epoch,revision,deleted,length,hash,next,verified)| {
        let scope = Scope::new(key.scope.receiver().clone(),key.scope.origin().clone(),
            key.scope.stream().clone(),key.scope.incarnation().clone(),key.scope.schema().clone(),
            Id::new(epoch).map_err(|_| TransferStoreError::Failed)?);
        let state = if deleted != 0 { ArtifactState::Deleted } else {
            ArtifactState::Live(ContentIdentity { length: u64_value(length)?, digest: digest(hash)? })
        };
        let next_offset = u64_value(next)?;
        let manifest = ArtifactManifest { key: ArtifactKey { scope, id: key.id.clone() },
            revision: u64_value(revision)?, state };
        if manifest.revision == 0 || (verified != 0 && (deleted != 0 ||
            !matches!(manifest.state, ArtifactState::Live(content) if next_offset == content.length))) {
            return Err(TransferStoreError::Failed);
        }
        Ok(TransferProgress { manifest, next_offset, verified: verified != 0 })
    }).transpose()
}

fn clear_chunks(conn: &Connection, key: &ArtifactKey) -> Result<(), TransferStoreError> {
    conn.execute(
        "DELETE FROM artifact_chunks WHERE receiver=?1 AND origin=?2 AND stream=?3
        AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6",
        params![
            key.scope.receiver().as_str(),
            key.scope.origin().as_str(),
            key.scope.stream().as_str(),
            key.scope.incarnation().as_str(),
            key.scope.schema().as_str(),
            key.id.as_str()
        ],
    )
    .map_err(|_| TransferStoreError::Failed)?;
    Ok(())
}

fn is_revoked(conn: &Connection, key: &ArtifactKey) -> Result<bool, TransferStoreError> {
    let found: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM artifact_revoked_scopes WHERE receiver=?1
        AND origin=?2 AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6
        AND access_epoch=?7",
            params![
                key.scope.receiver().as_str(),
                key.scope.origin().as_str(),
                key.scope.stream().as_str(),
                key.scope.incarnation().as_str(),
                key.scope.schema().as_str(),
                key.id.as_str(),
                key.scope.access_epoch().as_str()
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| TransferStoreError::Failed)?;
    Ok(found.is_some())
}

impl TransferStore for SqliteArtifactCache {
    fn progress(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<Option<TransferProgress>, TransferStoreError> {
        if is_revoked(&self.conn, &manifest.key)? {
            return Err(TransferStoreError::ScopeChanged);
        }
        let saved = cache_row(&self.conn, &manifest.key)?;
        if let Some(state) = &saved {
            if matches!(state.manifest.state, ArtifactState::Deleted)
                && matches!(manifest.state, ArtifactState::Live(_))
            {
                return Err(TransferStoreError::Fenced);
            }
            if state.manifest.key.scope != manifest.key.scope {
                return Err(TransferStoreError::ScopeChanged);
            }
            if state.manifest != *manifest {
                return Err(TransferStoreError::Stale);
            }
        }
        Ok(saved)
    }

    fn accept_manifest(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<TransferProgress, TransferStoreError> {
        if manifest.revision == 0 {
            return Err(TransferStoreError::Conflict);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        if is_revoked(&tx, &manifest.key)? {
            return Err(TransferStoreError::ScopeChanged);
        }
        let saved = cache_row(&tx, &manifest.key)?;
        if let Some(state) = &saved {
            if matches!(state.manifest.state, ArtifactState::Deleted)
                && matches!(manifest.state, ArtifactState::Live(_))
            {
                return Err(TransferStoreError::Fenced);
            }
            if manifest.revision < state.manifest.revision {
                return Err(TransferStoreError::Stale);
            }
            if manifest.revision == state.manifest.revision
                && manifest.state != state.manifest.state
            {
                return Err(TransferStoreError::Conflict);
            }
            if state.manifest == *manifest {
                return Ok(state.clone());
            }
            clear_chunks(&tx, &manifest.key)?;
        }
        let (deleted, length, hash) = match manifest.state {
            ArtifactState::Live(content) => {
                (0, i64_value(content.length)?, content.digest.0.to_vec())
            }
            ArtifactState::Deleted => (1, 0, Vec::new()),
        };
        tx.execute("INSERT INTO artifact_cache (receiver,origin,stream,incarnation,schema_id,
            artifact_id,access_epoch,revision,deleted,length,digest,next_offset,verified)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,0,0)
            ON CONFLICT(receiver,origin,stream,incarnation,schema_id,artifact_id) DO UPDATE SET
             access_epoch=excluded.access_epoch, revision=excluded.revision, deleted=excluded.deleted,
             length=excluded.length, digest=excluded.digest, next_offset=0, verified=0",
            params![manifest.key.scope.receiver().as_str(),manifest.key.scope.origin().as_str(),
                manifest.key.scope.stream().as_str(),manifest.key.scope.incarnation().as_str(),
                manifest.key.scope.schema().as_str(),manifest.key.id.as_str(),
                manifest.key.scope.access_epoch().as_str(),i64_value(manifest.revision)?,
                deleted,length,hash]
        ).map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        cache_row(&self.conn, &manifest.key)?.ok_or(TransferStoreError::Failed)
    }

    fn append(&mut self, reply: &ChunkReply) -> Result<TransferProgress, TransferStoreError> {
        validate_chunk(&reply.request, reply).map_err(|_| TransferStoreError::Conflict)?;
        let manifest = &reply.request.manifest;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        if is_revoked(&tx, &manifest.key)? {
            return Err(TransferStoreError::ScopeChanged);
        }
        let saved = cache_row(&tx, &manifest.key)?.ok_or(TransferStoreError::Stale)?;
        if saved.manifest != *manifest {
            return Err(if matches!(saved.manifest.state, ArtifactState::Deleted) {
                TransferStoreError::Fenced
            } else {
                TransferStoreError::Stale
            });
        }
        if reply.request.offset < saved.next_offset {
            let old: Option<Vec<u8>> = tx.query_row(
                "SELECT bytes FROM artifact_chunks WHERE receiver=?1 AND origin=?2 AND stream=?3
                  AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6 AND offset=?7",
                params![manifest.key.scope.receiver().as_str(),manifest.key.scope.origin().as_str(),
                    manifest.key.scope.stream().as_str(),manifest.key.scope.incarnation().as_str(),
                    manifest.key.scope.schema().as_str(),manifest.key.id.as_str(),i64_value(reply.request.offset)?],
                |row| row.get(0)
            ).optional().map_err(|_| TransferStoreError::Failed)?;
            return if old.as_deref() == Some(&reply.bytes) {
                Ok(saved)
            } else {
                Err(TransferStoreError::Conflict)
            };
        }
        if reply.request.offset != saved.next_offset || saved.verified {
            return Err(TransferStoreError::Stale);
        }
        tx.execute("INSERT INTO artifact_chunks (receiver,origin,stream,incarnation,schema_id,artifact_id,offset,bytes)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![manifest.key.scope.receiver().as_str(),manifest.key.scope.origin().as_str(),
                manifest.key.scope.stream().as_str(),manifest.key.scope.incarnation().as_str(),
                manifest.key.scope.schema().as_str(),manifest.key.id.as_str(),
                i64_value(reply.request.offset)?,&reply.bytes]
        ).map_err(|_| TransferStoreError::Failed)?;
        let next = reply
            .request
            .offset
            .checked_add(reply.bytes.len() as u64)
            .ok_or(TransferStoreError::Failed)?;
        tx.execute(
            "UPDATE artifact_cache SET next_offset=?7 WHERE receiver=?1 AND origin=?2
            AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6",
            params![
                manifest.key.scope.receiver().as_str(),
                manifest.key.scope.origin().as_str(),
                manifest.key.scope.stream().as_str(),
                manifest.key.scope.incarnation().as_str(),
                manifest.key.scope.schema().as_str(),
                manifest.key.id.as_str(),
                i64_value(next)?
            ],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        cache_row(&self.conn, &manifest.key)?.ok_or(TransferStoreError::Failed)
    }

    fn publish(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<TransferProgress, TransferStoreError> {
        let ArtifactState::Live(content) = manifest.state else {
            return Err(TransferStoreError::Fenced);
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        if is_revoked(&tx, &manifest.key)? {
            return Err(TransferStoreError::ScopeChanged);
        }
        let saved = cache_row(&tx, &manifest.key)?.ok_or(TransferStoreError::Stale)?;
        if saved.manifest != *manifest {
            return Err(TransferStoreError::Stale);
        }
        if saved.verified {
            return Ok(saved);
        }
        if saved.next_offset != content.length {
            return Err(TransferStoreError::Stale);
        }
        let mut hasher = Sha256::new();
        let mut position = 0u64;
        {
            let mut statement = tx
                .prepare(
                    "SELECT offset, bytes FROM artifact_chunks WHERE receiver=?1
                AND origin=?2 AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6
                ORDER BY offset",
                )
                .map_err(|_| TransferStoreError::Failed)?;
            let mut rows = statement
                .query(params![
                    manifest.key.scope.receiver().as_str(),
                    manifest.key.scope.origin().as_str(),
                    manifest.key.scope.stream().as_str(),
                    manifest.key.scope.incarnation().as_str(),
                    manifest.key.scope.schema().as_str(),
                    manifest.key.id.as_str()
                ])
                .map_err(|_| TransferStoreError::Failed)?;
            while let Some(row) = rows.next().map_err(|_| TransferStoreError::Failed)? {
                let offset: i64 = row.get(0).map_err(|_| TransferStoreError::Failed)?;
                let bytes: Vec<u8> = row.get(1).map_err(|_| TransferStoreError::Failed)?;
                if u64_value(offset)? != position {
                    return Err(TransferStoreError::Conflict);
                }
                position = position
                    .checked_add(bytes.len() as u64)
                    .ok_or(TransferStoreError::Failed)?;
                if position > content.length {
                    return Err(TransferStoreError::Conflict);
                }
                hasher.update(&bytes);
            }
        }
        if position != content.length || Sha256Digest(hasher.finalize().into()) != content.digest {
            clear_chunks(&tx, &manifest.key)?;
            tx.execute(
                "UPDATE artifact_cache SET next_offset=0 WHERE receiver=?1 AND origin=?2
                AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6",
                params![
                    manifest.key.scope.receiver().as_str(),
                    manifest.key.scope.origin().as_str(),
                    manifest.key.scope.stream().as_str(),
                    manifest.key.scope.incarnation().as_str(),
                    manifest.key.scope.schema().as_str(),
                    manifest.key.id.as_str()
                ],
            )
            .map_err(|_| TransferStoreError::Failed)?;
            tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
            return Err(TransferStoreError::HashMismatch);
        }
        tx.execute(
            "UPDATE artifact_cache SET verified=1 WHERE receiver=?1 AND origin=?2
            AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6",
            params![
                manifest.key.scope.receiver().as_str(),
                manifest.key.scope.origin().as_str(),
                manifest.key.scope.stream().as_str(),
                manifest.key.scope.incarnation().as_str(),
                manifest.key.scope.schema().as_str(),
                manifest.key.id.as_str()
            ],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        cache_row(&self.conn, &manifest.key)?.ok_or(TransferStoreError::Failed)
    }
}

impl ArtifactCacheIndex for SqliteArtifactCache {
    fn lookup(&mut self, key: &ArtifactKey) -> Result<Option<CachedArtifact>, ArtifactCacheError> {
        if is_revoked(&self.conn, key).map_err(|_| ArtifactCacheError::Unavailable)? {
            return Err(ArtifactCacheError::ScopeChanged);
        }
        let saved = cache_row(&self.conn, key).map_err(|_| ArtifactCacheError::Unavailable)?;
        if saved
            .as_ref()
            .is_some_and(|state| state.manifest.key.scope != key.scope)
        {
            return Err(ArtifactCacheError::ScopeChanged);
        }
        Ok(saved.map(|state| CachedArtifact {
            manifest: state.manifest,
            has_candidate_bytes: state.verified,
        }))
    }
}

impl SqliteArtifactCache {
    /// Fences an explicitly denied scope and erases its live bytes. The same
    /// epoch cannot silently resume; a later grant needs a new access epoch.
    pub fn revoke(&mut self, key: &ArtifactKey) -> Result<(), TransferStoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransferStoreError::Failed)?;
        tx.execute(
            "INSERT OR IGNORE INTO artifact_revoked_scopes
            (receiver,origin,stream,incarnation,schema_id,artifact_id,access_epoch)
            VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                key.scope.receiver().as_str(),
                key.scope.origin().as_str(),
                key.scope.stream().as_str(),
                key.scope.incarnation().as_str(),
                key.scope.schema().as_str(),
                key.id.as_str(),
                key.scope.access_epoch().as_str()
            ],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        clear_chunks(&tx, key)?;
        tx.execute(
            "DELETE FROM artifact_cache WHERE receiver=?1 AND origin=?2 AND stream=?3
            AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6 AND deleted=0",
            params![
                key.scope.receiver().as_str(),
                key.scope.origin().as_str(),
                key.scope.stream().as_str(),
                key.scope.incarnation().as_str(),
                key.scope.schema().as_str(),
                key.id.as_str()
            ],
        )
        .map_err(|_| TransferStoreError::Failed)?;
        tx.commit().map_err(|_| TransferStoreError::Uncertain)?;
        Ok(())
    }

    /// Reads a complete verified reference value, rechecking its full hash.
    /// Product adapters should stream large files instead of assembling a Vec.
    pub fn read_verified(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<Option<Vec<u8>>, TransferStoreError> {
        let saved = self.progress(manifest)?;
        let Some(state) = saved else {
            return Ok(None);
        };
        if !state.verified {
            return Ok(None);
        }
        let ArtifactState::Live(content) = manifest.state else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        let mut statement = self
            .conn
            .prepare(
                "SELECT offset, bytes FROM artifact_chunks WHERE receiver=?1
            AND origin=?2 AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND artifact_id=?6
            ORDER BY offset",
            )
            .map_err(|_| TransferStoreError::Failed)?;
        let mut rows = statement
            .query(params![
                manifest.key.scope.receiver().as_str(),
                manifest.key.scope.origin().as_str(),
                manifest.key.scope.stream().as_str(),
                manifest.key.scope.incarnation().as_str(),
                manifest.key.scope.schema().as_str(),
                manifest.key.id.as_str()
            ])
            .map_err(|_| TransferStoreError::Failed)?;
        while let Some(row) = rows.next().map_err(|_| TransferStoreError::Failed)? {
            let offset: i64 = row.get(0).map_err(|_| TransferStoreError::Failed)?;
            let part: Vec<u8> = row.get(1).map_err(|_| TransferStoreError::Failed)?;
            if u64_value(offset)? != bytes.len() as u64 {
                return Err(TransferStoreError::Conflict);
            }
            bytes.extend_from_slice(&part);
        }
        if ContentIdentity::of(&bytes) != content {
            return Err(TransferStoreError::HashMismatch);
        }
        Ok(Some(bytes))
    }
}
