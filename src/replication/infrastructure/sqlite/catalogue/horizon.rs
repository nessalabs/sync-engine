//! Native v2 coverage snapshots; no collector or wire fallback.
use super::*;
use crate::replication::catalogue::validate_catalogue_coverage;

pub(super) const OBSERVATIONS:&str="CREATE TABLE catalogue_horizons (
receiver TEXT NOT NULL, origin TEXT NOT NULL, stream TEXT NOT NULL,
incarnation TEXT NOT NULL, schema_id TEXT NOT NULL, access_epoch TEXT NOT NULL,
contract_version INTEGER NOT NULL CHECK(contract_version=2), horizon INTEGER NOT NULL CHECK(horizon>=0),
PRIMARY KEY(receiver,origin,stream,incarnation,schema_id,access_epoch)) WITHOUT ROWID;";
pub(super) fn validate_layout(c: &Connection) -> rusqlite::Result<()> {
    let mut q = c.prepare("PRAGMA table_info(catalogue_horizons)")?;
    let cols: Vec<String> = q.query_map([], |r| r.get(1))?.collect::<Result<_, _>>()?;
    if cols
        != [
            "receiver",
            "origin",
            "stream",
            "incarnation",
            "schema_id",
            "access_epoch",
            "contract_version",
            "horizon",
        ]
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(())
}
pub(super) fn admit(
    kind: Option<CatalogueReadKind>,
    completed: u64,
    head: &CatalogueHorizonHead,
) -> Result<(), CatalogueSourceError> {
    match kind {
        None if head.horizon > 0 => Err(CatalogueSourceError::HorizonRequired),
        None => Ok(()),
        Some(CatalogueReadKind::Incremental) => validate_catalogue_coverage(completed, head),
        Some(CatalogueReadKind::FullReset) if completed == 0 => Ok(()),
        Some(CatalogueReadKind::FullReset) => Err(CatalogueSourceError::InvalidRequest),
    }
}
fn observed(c: &Connection, s: &Scope) -> Result<Option<u64>, CatalogueStoreError> {
    let row:Option<(i64,i64)>=c.query_row("SELECT contract_version,horizon FROM catalogue_horizons WHERE receiver=?1 AND origin=?2 AND stream=?3 AND incarnation=?4 AND schema_id=?5 AND access_epoch=?6",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),s.incarnation().as_str(),s.schema().as_str(),s.access_epoch().as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(|_|CatalogueStoreError::Failed)?;
    row.map(|(v, h)| {
        if v == 2 {
            to_u64(h)
        } else {
            Err(CatalogueStoreError::Failed)
        }
    })
    .transpose()
}
pub(super) fn check_completion(
    c: &Connection,
    s: &Scope,
    completed: u64,
) -> Result<(), CatalogueStoreError> {
    let horizon = observed(c, s)?.unwrap_or(0);
    if completed < horizon {
        Err(CatalogueStoreError::HorizonExpired { completed, horizon })
    } else {
        Ok(())
    }
}
impl CatalogueHorizonStore for SqliteCatalogueStore {
    fn observed_horizon(&mut self, s: &Scope) -> Result<Option<u64>, CatalogueStoreError> {
        observed(&self.conn, s)
    }
    fn observe_horizon(&mut self, s: &Scope, horizon: u64) -> Result<(), CatalogueStoreError> {
        let h = to_i64(horizon)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CatalogueStoreError::Failed)?;
        if let Some(previous) = observed(&tx, s)? {
            if horizon < previous {
                return Err(CatalogueStoreError::HorizonRegressed {
                    previous,
                    received: horizon,
                });
            }
        }
        tx.execute("INSERT INTO catalogue_horizons VALUES(?1,?2,?3,?4,?5,?6,2,?7) ON CONFLICT(receiver,origin,stream,incarnation,schema_id,access_epoch) DO UPDATE SET horizon=excluded.horizon",params![s.receiver().as_str(),s.origin().as_str(),s.stream().as_str(),s.incarnation().as_str(),s.schema().as_str(),s.access_epoch().as_str(),h]).map_err(|_|CatalogueStoreError::Failed)?;
        tx.commit().map_err(|_| CatalogueStoreError::Uncertain)
    }
}
impl CatalogueReadSource for SqliteCatalogueSource {
    fn manifest_with_head(
        &mut self,
        r: &ManifestRequest,
        s: CapabilitySupport,
    ) -> Result<CatalogueManifestRead, CatalogueSourceError> {
        let read = self.manifest_snapshot(r, s, None)?;
        Ok(CatalogueManifestRead {
            head: read.head.source,
            page: read.page,
        })
    }
    fn resolve_with_head(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        s: CapabilitySupport,
    ) -> Result<CatalogueValueRead, CatalogueSourceError> {
        let read = self.value_snapshot(p, id, b, s, None)?;
        Ok(CatalogueValueRead {
            head: read.head.source,
            value: read.value,
        })
    }
}
impl CatalogueHorizonSource for SqliteCatalogueSource {
    fn horizon_head(
        &mut self,
        s: &Scope,
        supported: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        if !self.matches(s) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let (p, h) = publication::read_snapshot(&self.conn)
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let source = p.head(s);
        validate_capabilities(&source, supported).map_err(CatalogueSourceError::Incompatible)?;
        self.head_reads += 1;
        Ok(CatalogueHorizonHead::new(source, h))
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        s: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        self.manifest_snapshot(r, s, Some(k))
    }
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        s: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        self.value_snapshot(p, id, b, s, Some(k))
    }
}
