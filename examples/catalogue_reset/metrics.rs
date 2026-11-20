//! Host-only counters over the existing store; no policy or persistence owner.
use super::*;
pub(super) struct MeasuredStore {
    inner: SqliteCatalogueStore,
    pub(super) observations: u64,
    pub(super) journal_effects: u64,
    pub(super) candidate_reads: u64,
}
impl MeasuredStore {
    pub(super) fn new(inner: SqliteCatalogueStore) -> Self {
        Self {
            inner,
            observations: 0,
            journal_effects: 0,
            candidate_reads: 0,
        }
    }
}
macro_rules! forward {($($name:ident($($arg:ident:$ty:ty),*)->$ret:ty;)*)=>{$(fn $name(&mut self,$($arg:$ty),*)->$ret{self.inner.$name($($arg),*)})*};}
macro_rules! effect {($($name:ident($($arg:ident:$ty:ty),*)->$ret:ty;)*)=>{$(fn $name(&mut self,$($arg:$ty),*)->$ret{let result=self.inner.$name($($arg),*);if result.is_ok(){self.journal_effects+=1;}result})*};}
impl CatalogueStore for MeasuredStore {
    forward! {progress(s:&Scope)->Result<Option<CatalogueProgress>,CatalogueStoreError>;begin(s:&Scope,p:Option<CatalogueProgress>,h:u64)->Result<CatalogueProgress,CatalogueStoreError>;cached_revision(s:&Scope,i:&Id)->Result<Option<u64>,CatalogueStoreError>;apply_page(p:CataloguePagePlan)->Result<CatalogueProgress,CatalogueStoreError>;reset(s:&Scope,p:CatalogueProgress)->Result<CatalogueProgress,CatalogueStoreError>;}
}
impl CatalogueHorizonStore for MeasuredStore {
    forward! {observed_horizon(s:&Scope)->Result<Option<u64>,CatalogueStoreError>;}
    fn observe_horizon(&mut self, s: &Scope, h: u64) -> Result<(), CatalogueStoreError> {
        self.inner.observe_horizon(s, h)?;
        self.observations += 1;
        Ok(())
    }
}
impl CatalogueRecoveryStore for MeasuredStore {
    forward! {recovery(s:&Scope)->Result<Option<CatalogueRecovery>,CatalogueStoreError>;}
    effect! {start_recovery(s:&Scope,p:CatalogueProgress,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;start_fresh_recovery(s:&Scope,h:u64)->Result<CatalogueRecovery,CatalogueStoreError>;recovery_page(r:&CatalogueRecovery,p:CataloguePagePlan)->Result<CatalogueRecovery,CatalogueStoreError>;recovery_values(r:&CatalogueRecovery,v:Vec<ResolvedEntry>)->Result<CatalogueRecovery,CatalogueStoreError>;advance_recovery(r:&CatalogueRecovery)->Result<CatalogueRecovery,CatalogueStoreError>;finish_recovery(r:&CatalogueRecovery)->Result<(),CatalogueStoreError>;}
    fn recovery_candidates(
        &mut self,
        r: &CatalogueRecovery,
        n: usize,
    ) -> Result<Vec<ManifestEntry>, CatalogueStoreError> {
        let result = self.inner.recovery_candidates(r, n)?;
        self.candidate_reads += 1;
        Ok(result)
    }
}
