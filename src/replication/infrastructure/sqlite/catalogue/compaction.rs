//! Bounded physical movement of already hidden markers into exact proof.
use super::*;

const MAX_BATCH: usize = 256;

pub(super) fn verify(conn: &Connection) -> Result<(), SqliteOpenError> {
    let mut stmt = conn.prepare("PRAGMA table_info(catalogue_retired_proof)")?;
    let cols = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if cols != ["entry_id", "creation", "revision"] {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    for (kind, name) in [
        ("index", "catalogue_compaction_candidates"),
        ("trigger", "catalogue_retired_no_insert"),
    ] {
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type=?1 AND name=?2",
            params![kind, name],
            |r| r.get(0),
        )?;
        if count != 1 {
            return Err(SqliteOpenError::UnexpectedDatabase);
        }
    }
    Ok(())
}

/// Explicit offline owner of catalogue schema upgrade and compaction.
///
/// Before opening, the host must stop all other readers and writers, prevent
/// their restart, and hold exclusive source ownership until this handle is dropped.
/// SQLite does not enforce that process-level condition.
pub struct SqliteCatalogueMaintenance {
    conn: Connection,
}

impl SqliteCatalogueMaintenance {
    /// Open an existing source and atomically upgrade a valid older schema.
    /// A recognized legacy marker does not guarantee its layout is valid;
    /// damaged or incomplete files can be rejected without committing migration.
    /// The caller must establish and maintain exclusive offline ownership. No
    /// source file is created, renamed, or replaced. After an uncertain result,
    /// inspect the existing database and WAL before retrying.
    pub fn open(
        path: impl AsRef<Path>,
        origin: Id,
        stream: Id,
        incarnation: Id,
        schema: Id,
    ) -> Result<Self, SqliteOpenError> {
        let mut conn =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        publication::initialize(&mut conn, [&origin, &stream, &incarnation, &schema], true)?;
        source_connection::configure(&conn)?;
        Ok(Self { conn })
    }

    /// Move at most 256 hidden retired entries below the committed horizon to
    /// exact durable proof in one transaction. This does not change the horizon
    /// or retention pins. Repeating after an uncertain commit requires inspection.
    /// Exclusive offline ownership must remain in force throughout this call.
    pub fn compact_retired(&mut self, limit: usize) -> Result<usize, CatalogueStoreError> {
        if limit == 0 || limit > MAX_BATCH {
            return Err(CatalogueStoreError::Failed);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        let (publication, horizon) =
            publication::read_snapshot(&tx).map_err(|_| CatalogueStoreError::Failed)?;
        publication
            .writer()
            .map_err(|_| CatalogueStoreError::Failed)?;
        let ids: Vec<String> = {
            let mut stmt = tx.prepare("SELECT entry_id FROM catalogue_source_entries INDEXED BY catalogue_compaction_candidates WHERE manifest_visible=0 AND deleted=1 AND length(payload)=0 AND revision<?1 ORDER BY revision,entry_id LIMIT ?2").map_err(|_| CatalogueStoreError::Failed)?;
            let rows = stmt
                .query_map(params![to_i64(horizon)?, limit as i64], |r| r.get(0))
                .map_err(|_| CatalogueStoreError::Failed)?
                .collect::<Result<_, _>>()
                .map_err(|_| CatalogueStoreError::Failed)?;
            rows
        };
        for id in &ids {
            let inserted = tx.execute("INSERT INTO catalogue_retired_proof(entry_id,creation,revision) SELECT entry_id,creation,revision FROM catalogue_source_entries WHERE entry_id=?1 AND manifest_visible=0 AND deleted=1 AND length(payload)=0 AND revision<?2", params![id, to_i64(horizon)?]).map_err(|_| CatalogueStoreError::Failed)?;
            let deleted = tx.execute("DELETE FROM catalogue_source_entries WHERE entry_id=?1 AND manifest_visible=0 AND deleted=1 AND length(payload)=0 AND revision<?2", params![id, to_i64(horizon)?]).map_err(|_| CatalogueStoreError::Failed)?;
            if inserted != 1 || deleted != 1 {
                return Err(CatalogueStoreError::Failed);
            }
        }
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(ids.len())
    }
}
