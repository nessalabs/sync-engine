//! Explicit epoch-enabled record-only file mode; no legacy mutation facade.

use std::{fs::OpenOptions, io, time::Duration};

use rusqlite::OpenFlags;

use super::*;
use crate::replication::domain::{validate_capabilities, CapabilityError, CapabilitySupport};
use crate::replication::origin_epoch::{
    NativeEpochAdmission, NativeEpochHistoryAdmission, NativeEpochTailAdmission,
    OriginAuthorityKey, OriginEpoch,
};

const APPLICATION_ID: i64 = 0x4e53_5348;
const EPOCH_SCHEMA: &str = "
CREATE TABLE replica_epoch_binding (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    receiver TEXT NOT NULL,
    origin TEXT NOT NULL,
    authority TEXT NOT NULL,
    epoch BLOB CHECK (
        epoch IS NULL OR (
            typeof(epoch) = 'blob' AND length(epoch) = 8 AND epoch != X'0000000000000000'
        )
    )
);";
const BINDING: &str =
    "SELECT receiver, origin, authority, epoch FROM replica_epoch_binding WHERE singleton=1";

/// One immutable host-selected receiver/origin binding, across access scopes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochBinding {
    receiver: Id,
    origin: Id,
    authority: OriginAuthorityKey,
}

impl EpochBinding {
    /// Select the authorities of a dedicated receiver file; the host authenticates
    /// this association independently of any frame or local progress.
    pub fn new(receiver: Id, origin: Id, authority: OriginAuthorityKey) -> Self {
        Self {
            receiver,
            origin,
            authority,
        }
    }

    fn matches(&self, scope: &Scope) -> bool {
        scope.receiver() == &self.receiver && scope.origin() == &self.origin
    }
}

/// Protected receiver provisioning or opening failed.
#[derive(Debug)]
pub enum EpochReplicaOpenError {
    /// Exclusive file creation failed, including an occupied path.
    Create(io::Error),
    /// Local SQLite identity, schema, or immutable binding was refused.
    Database(SqliteOpenError),
}

impl std::fmt::Display for EpochReplicaOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(error) => write!(f, "cannot provision epoch receiver: {error}"),
            Self::Database(error) => write!(f, "cannot open epoch receiver: {error}"),
        }
    }
}

impl std::error::Error for EpochReplicaOpenError {}

/// Protected native result or underlying record transaction refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpochReplicaError {
    /// The configured host cannot verify the supplied evidence.
    Unadmitted,
    /// Verified authority, receiver, origin or exact result scope differs.
    WrongBinding,
    /// The stamp is older than the durable authority floor.
    Fenced,
    /// No exact matching trusted epoch was observed before apply.
    Unobserved,
    /// Head format or reader requirements are incompatible.
    Incompatible(CapabilityError),
    /// The existing record store refused or could not confirm its operation.
    Store(StoreError),
}

/// Qualified native history operation or canonical transaction refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpochHistoryError {
    /// Exact trusted binding or durable writer-floor admission was refused.
    Epoch(EpochReplicaError),
    /// The existing history transaction refused or could not confirm the operation.
    History(HistoryStoreError),
}

/// Compatibility name for qualified tail refusals.
pub type EpochTailError = EpochHistoryError;

use EpochReplicaError as Error;

fn no_create(path: &Path) -> Result<Connection, SqliteOpenError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

fn bind(
    conn: &Connection,
    expected: &EpochBinding,
) -> Result<Option<OriginEpoch>, SqliteOpenError> {
    let (receiver, origin, authority, raw): (String, String, String, Option<[u8; 8]>) = conn
        .query_row(BINDING, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
    if receiver != expected.receiver.as_str()
        || origin != expected.origin.as_str()
        || authority != expected.authority.id().as_str()
    {
        return Err(SqliteOpenError::SourceIdentityMismatch);
    }
    raw.map(|v| OriginEpoch::new(u64::from_be_bytes(v)).ok_or(SqliteOpenError::UnexpectedDatabase))
        .transpose()
}

fn storage(_: impl std::fmt::Debug) -> Error {
    Error::Store(StoreError::Failed)
}

/// Native record SQLite receiver with mandatory host epoch admission.
///
/// A distinct application ID prevents the ordinary replica/history opener
/// from writing this file. No ordinary apply or history port, reset, deletion or raw
/// connection handle is exposed. The record schema and transaction body are
/// shared with the legacy receiver, not a separate checkpoint owner.
///
/// The protected store cannot be passed to an unqualified legacy writer:
/// ```compile_fail
/// use nessa_sync::replication::application::ReplicaStore;
/// use nessa_sync::replication::infrastructure::SqliteEpochReplicaStore;
/// use nessa_sync::replication::origin_epoch::NativeEpochAdmission;
/// fn write_legacy(_: &mut impl ReplicaStore) {}
/// fn must_not_bypass<A: NativeEpochAdmission>(store: &mut SqliteEpochReplicaStore<A>) {
///     write_legacy(store);
/// }
/// ```
///
/// Qualified tails likewise do not expose the ordinary history mutation port:
/// ```compile_fail
/// use nessa_sync::replication::history::HistoryStore;
/// use nessa_sync::replication::infrastructure::SqliteEpochReplicaStore;
/// use nessa_sync::replication::origin_epoch::NativeEpochTailAdmission;
/// fn write_legacy(_: &mut impl HistoryStore) {}
/// fn must_not_bypass<A: NativeEpochTailAdmission>(store: &mut SqliteEpochReplicaStore<A>) {
///     write_legacy(store);
/// }
/// ```
pub struct SqliteEpochReplicaStore<A> {
    store: SqliteReplicaStore,
    binding: EpochBinding,
    admission: A,
}

impl<A: NativeEpochAdmission> SqliteEpochReplicaStore<A> {
    /// Exclusively initialize a new protected file. Legacy migration is not
    /// supported. The caller owns a trusted filesystem location.
    pub fn provision(
        path: impl AsRef<Path>,
        binding: EpochBinding,
        admission: A,
    ) -> Result<Self, EpochReplicaOpenError> {
        let path = path.as_ref();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(EpochReplicaOpenError::Create)?;
        let mut conn = no_create(path).map_err(EpochReplicaOpenError::Database)?;
        let schema = format!("{SCHEMA}{EPOCH_SCHEMA}");
        ensure_schema(&mut conn, APPLICATION_ID, &schema, |tx| {
            tx.execute(
                "INSERT INTO replica_epoch_binding VALUES (1, ?1, ?2, ?3, NULL)",
                params![
                    binding.receiver.as_str(),
                    binding.origin.as_str(),
                    binding.authority.id().as_str()
                ],
            )?;
            Ok(())
        })
        .map_err(EpochReplicaOpenError::Database)?;
        Self::finish_open(conn, binding, admission).map_err(EpochReplicaOpenError::Database)
    }

    /// Open an existing protected file with the exact original binding.
    /// Opening does not create a missing file or convert a legacy store.
    pub fn open(
        path: impl AsRef<Path>,
        binding: EpochBinding,
        admission: A,
    ) -> Result<Self, SqliteOpenError> {
        Self::finish_open(no_create(path.as_ref())?, binding, admission)
    }

    fn finish_open(
        conn: Connection,
        binding: EpochBinding,
        admission: A,
    ) -> Result<Self, SqliteOpenError> {
        let id: i64 = conn.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if id != APPLICATION_ID || version != 1 {
            return Err(SqliteOpenError::UnexpectedDatabase);
        }
        bind(&conn, &binding)?;
        configure_connection(&conn)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self {
            store: SqliteReplicaStore { conn },
            binding,
            admission,
        })
    }

    fn check(&self, key: &OriginAuthorityKey, scope: &Scope, exact: &Scope) -> Result<(), Error> {
        if &self.binding.authority != key || !self.binding.matches(scope) || scope != exact {
            Err(Error::WrongBinding)
        } else {
            Ok(())
        }
    }

    /// Observe a verified head, monotonically updating only the authority floor.
    /// A head position never writes records or advances record progress.
    pub fn observe_head(&mut self, evidence: A::HeadEvidence) -> Result<OriginEpoch, Error> {
        let (key, scope, epoch, head) = self
            .admission
            .admit_head(evidence)
            .ok_or(Error::Unadmitted)?
            .into_parts();
        self.check(&key, &scope, head.scope())?;
        validate_capabilities(&head, CapabilitySupport::current()).map_err(Error::Incompatible)?;
        let tx = self
            .store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        match bind(&tx, &self.binding).map_err(storage)? {
            Some(current) if current > epoch => return Err(Error::Fenced),
            Some(current) if current == epoch => return Ok(current),
            _ => {}
        }
        tx.execute(
            "UPDATE replica_epoch_binding SET epoch=?1 WHERE singleton=1",
            [epoch.get().to_be_bytes()],
        )
        .map_err(storage)?;
        tx.commit()
            .map_err(|_| Error::Store(StoreError::Uncertain))?;
        Ok(epoch)
    }

    fn require_epoch(
        tx: &rusqlite::Transaction<'_>,
        binding: &EpochBinding,
        epoch: OriginEpoch,
    ) -> Result<(), Error> {
        match bind(tx, binding).map_err(storage)? {
            Some(current) if current > epoch => Err(Error::Fenced),
            Some(current) if current == epoch => Ok(()),
            _ => Err(Error::Unobserved),
        }
    }

    /// Apply an admitted validated record plan only when its exact stamp matches
    /// the durable floor. Checking and ordinary record apply share one writer
    /// transaction, including the idempotent replay path.
    pub fn apply(&mut self, evidence: A::PlanEvidence) -> Result<(), Error> {
        let (key, scope, epoch, plan) = self
            .admission
            .admit_plan(evidence)
            .ok_or(Error::Unadmitted)?
            .into_parts();
        self.check(&key, &scope, plan.expected().scope())?;
        let tx = self
            .store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        Self::require_epoch(&tx, &self.binding, epoch)?;
        if stage_plan(&tx, &plan).map_err(Error::Store)? {
            tx.commit()
                .map_err(|_| Error::Store(StoreError::Uncertain))?;
        }
        Ok(())
    }

    /// Reload the durable authority floor, including after an uncertain write.
    /// This reports local observation, not the current source claim.
    pub fn observed_epoch(&self) -> Result<Option<OriginEpoch>, Error> {
        bind(&self.store.conn, &self.binding).map_err(storage)
    }

    /// Read exact local durable progress; this does not grant network access.
    pub fn checkpoint(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, Error> {
        if !self.binding.matches(scope) {
            return Err(Error::WrongBinding);
        }
        self.store.load(scope).map_err(Error::Store)
    }

    /// Read a bounded exact-scope local record page without network effects.
    pub fn read_after(
        &mut self,
        scope: &Scope,
        after: u64,
        max_records: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<Record>, Error> {
        if !self.binding.matches(scope) {
            return Err(Error::WrongBinding);
        }
        self.store
            .read_after(scope, after, max_records, max_payload_bytes)
            .map_err(Error::Store)
    }
}

impl<A: NativeEpochTailAdmission> SqliteEpochReplicaStore<A> {
    /// Install a trusted claim-qualified tail using the existing snapshot
    /// transaction. Compare the durable floor before any history effect or
    /// duplicate/generation decision. A tail cannot raise that floor.
    pub fn install_tail(
        &mut self,
        evidence: A::TailEvidence,
    ) -> Result<HistoryProgress, EpochTailError> {
        let (key, scope, epoch, plan) = self
            .admission
            .admit_tail(evidence)
            .ok_or(EpochTailError::Epoch(Error::Unadmitted))?
            .into_parts();
        self.check(&key, &scope, &plan.snapshot().request.scope)
            .map_err(EpochTailError::Epoch)?;
        let tx = self
            .store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| EpochTailError::History(HistoryStoreError::Failed))?;
        Self::require_epoch(&tx, &self.binding, epoch).map_err(EpochTailError::Epoch)?;
        let progress = stage_tail(&tx, plan.snapshot()).map_err(EpochTailError::History)?;
        tx.commit()
            .map_err(|_| EpochTailError::History(HistoryStoreError::Uncertain))?;
        Ok(progress)
    }

    /// Reload exact local snapshot progress and coverage without network effects.
    pub fn history_progress(
        &self,
        scope: &Scope,
    ) -> Result<Option<HistoryProgress>, EpochTailError> {
        if !self.binding.matches(scope) {
            return Err(EpochTailError::Epoch(Error::WrongBinding));
        }
        let progress =
            history_progress(&self.store.conn, scope).map_err(EpochTailError::History)?;
        if progress.as_ref().is_some_and(|saved| &saved.scope != scope) {
            return Err(EpochTailError::History(HistoryStoreError::ResetRequired));
        }
        Ok(progress)
    }
}

impl<A: NativeEpochHistoryAdmission> SqliteEpochReplicaStore<A> {
    /// Install an admitted older page into existing tail coverage.
    ///
    /// Check the writer floor in the existing mutation transaction before
    /// overlap, generation or payload decisions. An older page does not
    /// observe an epoch, replace a snapshot or advance the live checkpoint.
    pub fn install_older(
        &mut self,
        evidence: A::OlderEvidence,
    ) -> Result<HistoryProgress, EpochHistoryError> {
        let (key, scope, epoch, plan) = self
            .admission
            .admit_older(evidence)
            .ok_or(EpochHistoryError::Epoch(Error::Unadmitted))?
            .into_parts();
        self.check(&key, &scope, &plan.page().request.scope)
            .map_err(EpochHistoryError::Epoch)?;
        let tx = self
            .store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| EpochHistoryError::History(HistoryStoreError::Failed))?;
        Self::require_epoch(&tx, &self.binding, epoch).map_err(EpochHistoryError::Epoch)?;
        let progress = stage_older(&tx, plan.page()).map_err(EpochHistoryError::History)?;
        tx.commit()
            .map_err(|_| EpochHistoryError::History(HistoryStoreError::Uncertain))?;
        Ok(progress)
    }
}
