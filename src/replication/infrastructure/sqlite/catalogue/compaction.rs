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

impl SqliteCatalogueSource {
    /// Move at most 256 hidden retired entries below the committed horizon to
    /// exact durable proof in one transaction. This does not change the horizon
    /// or retention pins. Repeating after an uncertain commit requires inspection.
    /// Hosts must stop all older source readers and writers before upgrading the
    /// source file and keep them stopped.
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
