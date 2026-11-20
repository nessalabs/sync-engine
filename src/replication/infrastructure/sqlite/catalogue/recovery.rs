//! Receiver-local recovery journal; the parent progress row owns generation/phase.
use super::*;
use crate::replication::catalogue::{CatalogueRecovery, CatalogueRecoveryStore, RecoveryPhase};
const CANDIDATES:&str="CREATE TABLE catalogue_candidates (
receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL, entry_id TEXT NOT NULL,
creation INTEGER NOT NULL CHECK(creation>0), revision INTEGER NOT NULL CHECK(revision>=creation),
PRIMARY KEY(receiver,origin,stream,entry_id),
FOREIGN KEY(receiver,origin,stream) REFERENCES catalogue_progress(receiver,origin,stream)) WITHOUT ROWID;";
fn failed(_: rusqlite::Error) -> CatalogueStoreError {
    CatalogueStoreError::Failed
}
pub(super) fn initialize(conn: &mut Connection) -> Result<(), SqliteOpenError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let app: i64 = tx.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    let mut version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if app == 0 && version == 0 {
        let tables: i64 = tx.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        if tables != 0 {
            return Err(SqliteOpenError::UnexpectedDatabase);
        }
        tx.execute_batch(STORE_SCHEMA)?;
        tx.pragma_update(None, "application_id", CATALOGUE_REPLICA_APPLICATION_ID)?;
        version = 1;
    } else if app != CATALOGUE_REPLICA_APPLICATION_ID || !matches!(version, 1..=3) {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    if version == 1 {
        tx.execute_batch("ALTER TABLE catalogue_progress ADD COLUMN reset_phase INTEGER NOT NULL DEFAULT 0 CHECK(reset_phase BETWEEN 0 AND 3);
ALTER TABLE catalogue_progress ADD COLUMN reset_boundary INTEGER CHECK(reset_boundary>=0);")?;
        tx.execute_batch(CANDIDATES)?;
        version = 2;
    }
    if version == 2 {
        tx.execute_batch(super::horizon::OBSERVATIONS)?;
        tx.pragma_update(None, "user_version", 3)?;
    }
    super::horizon::validate_layout(&tx)?;
    let cols: Vec<String> = {
        let mut q = tx.prepare("PRAGMA table_info(catalogue_progress)")?;
        let v = q.query_map([], |r| r.get(1))?.collect::<Result<_, _>>()?;
        v
    };
    if cols
        != [
            "receiver",
            "origin",
            "stream",
            "incarnation",
            "schema_id",
            "access_epoch",
            "completed",
            "generation",
            "active_boundary",
            "cursor_creation",
            "cursor_id",
            "reset_phase",
            "reset_boundary",
        ]
    {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    let cols: Vec<String> = {
        let mut q = tx.prepare("PRAGMA table_info(catalogue_candidates)")?;
        let v = q.query_map([], |r| r.get(1))?.collect::<Result<_, _>>()?;
        v
    };
    if cols
        != [
            "receiver", "origin", "stream", "entry_id", "creation", "revision",
        ]
    {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    let cols: Vec<String> = {
        let mut q = tx.prepare("PRAGMA table_info(catalogue_entries)")?;
        let v = q.query_map([], |r| r.get(1))?.collect::<Result<_, _>>()?;
        v
    };
    if cols
        != [
            "receiver", "origin", "stream", "entry_id", "creation", "revision", "deleted",
            "payload",
        ]
    {
        return Err(SqliteOpenError::UnexpectedDatabase);
    }
    tx.commit()?;
    Ok(())
}
pub(super) fn ordinary(conn: &Connection, s: &Scope) -> Result<(), CatalogueStoreError> {
    if read(conn, s)?.is_some() {
        return Err(CatalogueStoreError::ResetRequired);
    }
    Ok(())
}
fn read(conn: &Connection, s: &Scope) -> Result<Option<CatalogueRecovery>, CatalogueStoreError> {
    read_with_hook(conn, s, || {})
}
fn read_with_hook(
    conn: &Connection,
    s: &Scope,
    after_phase: impl FnOnce(),
) -> Result<Option<CatalogueRecovery>, CatalogueStoreError> {
    let row:Option<(i64,Option<i64>)>=conn.query_row("SELECT reset_phase,reset_boundary FROM catalogue_progress WHERE receiver=?1 AND origin=?2 AND stream=?3",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(failed)?;
    let Some((phase, boundary)) = row else {
        return Ok(None);
    };
    if phase == 0 {
        return if boundary.is_none() {
            Ok(None)
        } else {
            Err(CatalogueStoreError::Failed)
        };
    }
    let phase = match phase {
        1 => RecoveryPhase::Enumerate,
        2 => RecoveryPhase::Reconcile,
        3 => RecoveryPhase::Finish,
        _ => return Err(CatalogueStoreError::Failed),
    };
    after_phase();
    let progress = read_progress(conn, s)?.ok_or(CatalogueStoreError::Failed)?;
    if progress.scope != *s {
        return Err(CatalogueStoreError::ResetRequired);
    }
    let r = CatalogueRecovery {
        progress,
        boundary: to_u64(boundary.ok_or(CatalogueStoreError::Failed)?)?,
        phase,
    };
    crate::replication::catalogue::recovery_valid(&r).map_err(|_| CatalogueStoreError::Failed)?;
    Ok(Some(r))
}
// Hooks are private deterministic inter-query fixture boundaries, empty in production.
pub(super) fn progress_snapshot(
    conn: &mut Connection,
    s: &Scope,
    after_fence: impl FnOnce(),
) -> Result<Option<CatalogueProgress>, CatalogueStoreError> {
    let tx = conn.transaction().map_err(failed)?;
    ordinary(&tx, s)?;
    after_fence();
    let p = read_progress(&tx, s)?;
    tx.commit().map_err(failed)?;
    Ok(p)
}
fn reload(
    conn: &mut Connection,
    s: &Scope,
    after_phase: impl FnOnce(),
) -> Result<Option<CatalogueRecovery>, CatalogueStoreError> {
    let tx = conn.transaction().map_err(failed)?;
    let r = read_with_hook(&tx, s, after_phase)?;
    tx.commit().map_err(failed)?;
    Ok(r)
}
fn check(conn: &Connection, r: &CatalogueRecovery) -> Result<(), CatalogueStoreError> {
    if read(conn, &r.progress.scope)?.as_ref() != Some(r) {
        return Err(CatalogueStoreError::Stale);
    }
    if r.phase == RecoveryPhase::Finish && r.progress.completed == r.boundary {
        horizon::check_completion(conn, &r.progress.scope, r.progress.completed)
    } else {
        horizon::check_reset_boundary(conn, &r.progress.scope, r.boundary)
    }
}
fn save(conn: &Connection, r: &CatalogueRecovery) -> Result<(), CatalogueStoreError> {
    let p = &r.progress;
    let s = &p.scope;
    let a = p.active.as_ref();
    let cursor = a.and_then(|p| p.cursor.as_ref());
    let phase = match r.phase {
        RecoveryPhase::Enumerate => 1,
        RecoveryPhase::Reconcile => 2,
        RecoveryPhase::Finish => 3,
    };
    conn.execute("UPDATE catalogue_progress SET completed=?4,generation=?5,active_boundary=?6,cursor_creation=?7,cursor_id=?8,reset_phase=?9,reset_boundary=?10 WHERE receiver=?1 AND origin=?2 AND stream=?3",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),to_i64(p.completed)?,to_i64(p.generation)?,a.map(|p|to_i64(p.boundary)).transpose()?,cursor.map(|k|to_i64(k.creation)).transpose()?,cursor.map(|k|k.id.as_str()),phase,to_i64(r.boundary)?]).map_err(failed)?;
    Ok(())
}
fn floor(
    conn: &Connection,
    s: &Scope,
    id: &Id,
) -> Result<Option<ManifestEntry>, CatalogueStoreError> {
    let row:Option<(i64,i64)>=conn.query_row("SELECT creation,revision FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3 AND entry_id=?4",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),id.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(failed)?;
    row.map(|(c, r)| {
        Ok(ManifestEntry {
            key: EntryKey {
                creation: to_u64(c)?,
                id: id.clone(),
            },
            revision: to_u64(r)?,
            deleted: false,
        })
    })
    .transpose()
}
fn remove(conn: &Connection, s: &Scope, id: &Id) -> Result<(), CatalogueStoreError> {
    conn.execute("DELETE FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3 AND entry_id=?4",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),id.as_str()]).map_err(failed)?;
    Ok(())
}
fn remaining(conn: &Connection, s: &Scope) -> Result<i64, CatalogueStoreError> {
    conn.query_row(
        "SELECT count(*) FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3",
        params![
            s.receiver().as_str(),
            s.origin().as_str(),
            s.stream().as_str()
        ],
        |r| r.get(0),
    )
    .map_err(failed)
}
fn start(
    conn: &mut Connection,
    s: &Scope,
    expected: Option<CatalogueProgress>,
    boundary: u64,
) -> Result<CatalogueRecovery, CatalogueStoreError> {
    if expected.as_ref().is_some_and(|p| p.scope != *s) {
        return Err(CatalogueStoreError::ResetRequired);
    }
    let next = match &expected {
        Some(p) => crate::replication::catalogue::recovery_start(p, boundary)?,
        None => crate::replication::catalogue::recovery_fresh_start(s, boundary)?,
    };
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(failed)?;
    if read_progress(&tx, s)? != expected {
        return Err(CatalogueStoreError::Stale);
    }
    horizon::check_reset_boundary(&tx, s, boundary)?;
    if expected.is_none() {
        let count:i64=tx.query_row("SELECT count(*) FROM catalogue_entries WHERE receiver=?1 AND origin=?2 AND stream=?3",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()],|r|r.get(0)).map_err(failed)?;
        if count != 0 || remaining(&tx, s)? != 0 {
            return Err(CatalogueStoreError::Conflict);
        }
        tx.execute("INSERT INTO catalogue_progress(receiver,origin,stream,incarnation,schema_id,access_epoch,completed,generation) VALUES(?1,?2,?3,?4,?5,?6,0,0)",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),s.incarnation().as_str(),s.schema().as_str(),s.access_epoch().as_str()]).map_err(failed)?;
    }
    // Pending candidates from an interrupted same-scope reset remain owned
    // by the parent generation. Fresh live rows raise their revision floor.
    let conflict:i64=tx.query_row("SELECT count(*) FROM catalogue_candidates c JOIN catalogue_entries e USING(receiver,origin,stream,entry_id) WHERE c.receiver=?1 AND c.origin=?2 AND c.stream=?3 AND c.creation!=e.creation",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()],|r|r.get(0)).map_err(failed)?;
    if conflict != 0 {
        return Err(CatalogueStoreError::Conflict);
    }
    let max:i64=tx.query_row("SELECT max(revision) FROM (SELECT revision FROM catalogue_entries WHERE receiver=?1 AND origin=?2 AND stream=?3 UNION ALL SELECT revision FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3)",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()],|r|Ok(r.get::<_,Option<i64>>(0)?.unwrap_or(0))).map_err(failed)?;
    if to_i64(boundary)? < max {
        return Err(CatalogueStoreError::ResetRequired);
    }
    tx.execute("INSERT INTO catalogue_candidates(receiver,origin,stream,entry_id,creation,revision) SELECT receiver,origin,stream,entry_id,creation,revision FROM catalogue_entries WHERE receiver=?1 AND origin=?2 AND stream=?3 AND deleted=0 ON CONFLICT(receiver,origin,stream,entry_id) DO UPDATE SET revision=max(revision,excluded.revision)",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()]).map_err(failed)?;
    tx.execute(
        "DELETE FROM catalogue_entries WHERE receiver=?1 AND origin=?2 AND stream=?3 AND deleted=0",
        params![
            s.receiver().as_str(),
            s.origin().as_str(),
            s.stream().as_str()
        ],
    )
    .map_err(failed)?;
    save(&tx, &next)?;
    tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
    Ok(next)
}
impl CatalogueRecoveryStore for SqliteCatalogueStore {
    fn recovery(&mut self, s: &Scope) -> Result<Option<CatalogueRecovery>, CatalogueStoreError> {
        reload(&mut self.conn, s, || {})
    }
    fn start_recovery(
        &mut self,
        s: &Scope,
        expected: CatalogueProgress,
        boundary: u64,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        start(&mut self.conn, s, Some(expected), boundary)
    }
    fn start_fresh_recovery(
        &mut self,
        s: &Scope,
        boundary: u64,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        start(&mut self.conn, s, None, boundary)
    }
    fn recovery_page(
        &mut self,
        r: &CatalogueRecovery,
        plan: CataloguePagePlan,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        let next = crate::replication::catalogue::recovery_page(r, &plan)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        check(&tx, r)?;
        for value in &plan.entries {
            if let Some(f) = floor(&tx, &r.progress.scope, &value.manifest.key.id)? {
                validate_catalogue_revision_transition(&f, &value.manifest)
                    .map_err(revision_error)?;
            }
        }
        write_values(&tx, &r.progress.scope, &plan.entries, &plan.unchanged)?;
        // A confirmed page is already current owned evidence. Remove only
        // those candidates in the same transaction as values and cursor.
        for value in &plan.entries {
            remove(&tx, &r.progress.scope, &value.manifest.key.id)?;
        }
        save(&tx, &next)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(next)
    }
    fn recovery_candidates(
        &mut self,
        r: &CatalogueRecovery,
        limit: usize,
    ) -> Result<Vec<ManifestEntry>, CatalogueStoreError> {
        if r.phase != RecoveryPhase::Reconcile || limit == 0 || limit > MAX_CATALOGUE_ENTRIES {
            return Err(CatalogueStoreError::Conflict);
        }
        let tx = self.conn.transaction().map_err(failed)?;
        check(&tx, r)?;
        let s = &r.progress.scope;
        let mut q=tx.prepare("SELECT entry_id,creation,revision FROM catalogue_candidates WHERE receiver=?1 AND origin=?2 AND stream=?3 ORDER BY entry_id LIMIT ?4").map_err(failed)?;
        let rows = q
            .query_map(
                params![
                    s.receiver().as_str(),
                    s.origin().as_str(),
                    s.stream().as_str(),
                    limit as i64
                ],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(failed)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(failed)?;
        rows.into_iter()
            .map(|(i, c, v)| {
                Ok(ManifestEntry {
                    key: EntryKey {
                        id: id(i)?,
                        creation: to_u64(c)?,
                    },
                    revision: to_u64(v)?,
                    deleted: false,
                })
            })
            .collect()
    }
    fn recovery_values(
        &mut self,
        r: &CatalogueRecovery,
        values: Vec<ResolvedEntry>,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        if r.phase != RecoveryPhase::Reconcile
            || values.is_empty()
            || values.len() > MAX_CATALOGUE_ENTRIES
        {
            return Err(CatalogueStoreError::Conflict);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        check(&tx, r)?;
        let mut total = 0usize;
        let mut ids = std::collections::HashSet::new();
        for value in &values {
            if !ids.insert(&value.manifest.key.id) {
                return Err(CatalogueStoreError::Conflict);
            }
            let f = floor(&tx, &r.progress.scope, &value.manifest.key.id)?
                .ok_or(CatalogueStoreError::Stale)?;
            crate::replication::catalogue::validate_resolved(
                &f,
                value,
                MAX_CATALOGUE_PAYLOAD_BYTES,
            )
            .map_err(revision_error)?;
            total = total
                .checked_add(value.payload.len())
                .ok_or(CatalogueStoreError::Conflict)?;
            if total > MAX_CATALOGUE_PAYLOAD_BYTES {
                return Err(CatalogueStoreError::Conflict);
            }
        }
        write_values(&tx, &r.progress.scope, &values, &[])?;
        for value in &values {
            remove(&tx, &r.progress.scope, &value.manifest.key.id)?;
        }
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(r.clone())
    }
    fn advance_recovery(
        &mut self,
        r: &CatalogueRecovery,
    ) -> Result<CatalogueRecovery, CatalogueStoreError> {
        let next = crate::replication::catalogue::recovery_advance(r)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        check(&tx, r)?;
        if remaining(&tx, &r.progress.scope)? != 0 {
            return Err(CatalogueStoreError::Stale);
        }
        save(&tx, &next)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(next)
    }
    fn finish_recovery(&mut self, r: &CatalogueRecovery) -> Result<(), CatalogueStoreError> {
        if r.phase != RecoveryPhase::Finish || r.progress.completed != r.boundary {
            return Err(CatalogueStoreError::Stale);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failed)?;
        check(&tx, r)?;
        if remaining(&tx, &r.progress.scope)? != 0 {
            return Err(CatalogueStoreError::Stale);
        }
        let s = &r.progress.scope;
        tx.execute("UPDATE catalogue_progress SET reset_phase=0,reset_boundary=NULL WHERE receiver=?1 AND origin=?2 AND stream=?3",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str()]).map_err(failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)?;
        Ok(())
    }
}
#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn fixture() -> (
        Fixture,
        SqliteCatalogueStore,
        SqliteCatalogueStore,
        Scope,
        CatalogueProgress,
    ) {
        let p = std::env::temp_dir().join(format!(
            "owned-reset-snapshot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        let path = p.join("cache");
        let a = SqliteCatalogueStore::open(&path).unwrap();
        let mut b = SqliteCatalogueStore::open(&path).unwrap();
        let i = |s: &str| Id::new(s).unwrap();
        let s = Scope::new(
            i("receiver"),
            i("origin"),
            i("stream"),
            i("first"),
            i("schema"),
            i("epoch"),
        );
        a.conn.execute("INSERT INTO catalogue_progress(receiver,origin,stream,incarnation,schema_id,access_epoch,completed,generation) VALUES('receiver','origin','stream','first','schema','epoch',1,1)",[]).unwrap();
        let progress = b.progress(&s).unwrap().unwrap();
        (Fixture(p), a, b, s, progress)
    }
    #[test]
    fn normal_progress_is_one_snapshot_across_a_competing_reset_start() {
        let (_f, mut a, mut b, s, p) = fixture();
        let before = progress_snapshot(&mut a.conn, &s, || {
            b.start_recovery(&s, p.clone(), 1).unwrap();
        })
        .unwrap();
        assert_eq!(before, Some(p));
        assert_eq!(a.progress(&s), Err(CatalogueStoreError::ResetRequired));
    }
    #[test]
    fn recovery_reload_does_not_mix_phase_and_a_superseding_generation() {
        let (_f, mut a, mut b, s, p) = fixture();
        let old = b.start_recovery(&s, p, 1).unwrap();
        let read = reload(&mut a.conn, &s, || {
            b.start_recovery(&s, old.progress.clone(), 2).unwrap();
        })
        .unwrap();
        assert_eq!(read, Some(old));
        let next = a.recovery(&s).unwrap().unwrap();
        assert_eq!(next.boundary, 2);
    }
}
