//! One bounded artifact chunk and durable staging contracts.

use super::{
    validate_manifest, ArtifactManifest, ArtifactState, ManifestError, ManifestRequest,
    ManifestSource, ManifestSourceError,
};

/// Maximum data bytes in one reference transfer chunk.
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;

/// Exact immutable content version and offset requested from a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkRequest {
    /// Validated current live manifest including receiver scope and digest.
    pub manifest: ArtifactManifest,
    /// First byte not yet durably staged.
    pub offset: u64,
    /// Positive bound no greater than [`MAX_CHUNK_BYTES`].
    pub max_bytes: usize,
}

/// One source response, including exact request identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkReply {
    /// Request whose bytes were read under current authorization.
    pub request: ChunkRequest,
    /// Contiguous bytes starting at the requested offset.
    pub bytes: Vec<u8>,
}

/// Pure refusal before any receiver-store effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkValidationError {
    /// No live committed content or invalid offset/limit.
    InvalidRequest,
    /// Reply belongs to another scope, version, offset or limit.
    WrongRequest,
    /// Reply is short, long or otherwise not the exact requested range.
    WrongLength,
}

/// Validates an exact chunk; the source must separately check current grants
/// and ensure the requested version still equals its current version.
pub fn validate_chunk(
    request: &ChunkRequest,
    reply: &ChunkReply,
) -> Result<(), ChunkValidationError> {
    let ArtifactState::Live(content) = request.manifest.state else {
        return Err(ChunkValidationError::InvalidRequest);
    };
    if request.manifest.revision == 0
        || request.max_bytes == 0
        || request.max_bytes > MAX_CHUNK_BYTES
        || request.offset >= content.length
    {
        return Err(ChunkValidationError::InvalidRequest);
    }
    if reply.request != *request {
        return Err(ChunkValidationError::WrongRequest);
    }
    let expected = (content.length - request.offset).min(request.max_bytes as u64) as usize;
    if reply.bytes.len() != expected {
        return Err(ChunkValidationError::WrongLength);
    }
    Ok(())
}

/// Source refusal with no claim that missing bytes imply deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkSourceError {
    /// Source or current authorization cannot be established.
    Unavailable,
    /// Current policy explicitly denied this receiver.
    Denied,
    /// Current access scope or incarnation differs.
    ScopeChanged,
    /// Current artifact revision or content identity differs.
    VersionChanged,
    /// A retained deletion marker replaced live content.
    Deleted,
    /// Request exceeds source bounds.
    InvalidRequest,
}

/// Reads bounded ranges only while current policy and manifest still agree.
pub trait ChunkSource: ManifestSource {
    /// Returns exact bytes. One call does not retain a long source transaction
    /// across network I/O or promise that the version remains current later.
    fn chunk(&mut self, request: &ChunkRequest) -> Result<ChunkReply, ChunkSourceError>;
}

/// Durable state for one exact manifest in one receiver cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferProgress {
    /// Manifest accepted before this staging sequence.
    pub manifest: ArtifactManifest,
    /// Next byte to request; bytes below it committed with this progress.
    pub next_offset: u64,
    /// True only after full length and SHA-256 verification committed.
    pub verified: bool,
}

/// Atomic cache refusal. A hash mismatch discards suspect staging bytes and
/// resets its offset; other rejected transactions leave progress unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferStoreError {
    /// Store failed without a confirmed commit.
    Failed,
    /// Commit reply was lost; reload progress before retrying.
    Uncertain,
    /// Another writer advanced the manifest or offset.
    Stale,
    /// Current scope differs or access epoch changed.
    ScopeChanged,
    /// Retained deletion fence rejects these bytes.
    Fenced,
    /// Repeated offset carried different bytes or identity.
    Conflict,
    /// Complete staged bytes failed the length or SHA-256 check.
    HashMismatch,
}

/// Receiver-owned cache with atomic manifest, offset and verified publication.
pub trait TransferStore {
    /// Returns current manifest and staging progress for an exact key.
    fn progress(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<Option<TransferProgress>, TransferStoreError>;
    /// Accepts a currently authorized manifest. A newer revision drops stale
    /// staging; a deletion keeps its fence and makes content unavailable.
    fn accept_manifest(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<TransferProgress, TransferStoreError>;
    /// Commits an exact next chunk and offset in one transaction. A repeat of
    /// the same committed bytes is an idempotent no-op.
    fn append(&mut self, reply: &ChunkReply) -> Result<TransferProgress, TransferStoreError>;
    /// Checks complete staged bytes and atomically exposes them as verified.
    /// The store compares the exact current manifest again at commit time.
    fn publish(
        &mut self,
        manifest: &ArtifactManifest,
    ) -> Result<TransferProgress, TransferStoreError>;
}

/// Application-level transfer refusal with source and store causes preserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferError {
    /// Manifest lookup failed or current policy refused it.
    ManifestSource(ManifestSourceError),
    /// A source range failed or current version changed.
    ChunkSource(ChunkSourceError),
    /// Source response failed pure validation.
    Validation(ChunkValidationError),
    /// Current manifest failed identity or revision validation.
    Manifest(ManifestError),
    /// Receiver store rejected the transition.
    Store(TransferStoreError),
    /// Source now has a newer version; the cache accepted its manifest.
    VersionChanged,
    /// Source now has a retained deletion marker; the cache accepted its fence.
    Deleted,
}

/// Transfers at most one chunk from durable progress. The host chooses when
/// to call again, leaving a scheduling point for urgent work after every chunk.
/// A returned complete offset still requires [`publish_if_current`].
pub fn transfer_one<S: ChunkSource, D: TransferStore>(
    manifest: &ArtifactManifest,
    max_bytes: usize,
    source: &mut S,
    store: &mut D,
) -> Result<TransferProgress, TransferError> {
    let ArtifactState::Live(content) = manifest.state else {
        return Err(TransferError::Deleted);
    };
    if max_bytes == 0 || max_bytes > MAX_CHUNK_BYTES {
        return Err(TransferError::Validation(
            ChunkValidationError::InvalidRequest,
        ));
    }
    let progress = match store.progress(manifest).map_err(TransferError::Store)? {
        Some(progress) => progress,
        None => store
            .accept_manifest(manifest)
            .map_err(TransferError::Store)?,
    };
    if progress.verified || progress.next_offset == content.length {
        return Ok(progress);
    }
    let request = ChunkRequest {
        manifest: manifest.clone(),
        offset: progress.next_offset,
        max_bytes,
    };
    let reply = source.chunk(&request).map_err(TransferError::ChunkSource)?;
    validate_chunk(&request, &reply).map_err(TransferError::Validation)?;
    store.append(&reply).map_err(TransferError::Store)
}

/// Rechecks the current authorized manifest before publishing completed
/// staged bytes. A newer manifest or deletion is installed first; a failed
/// source check cannot turn an old stage into visible verified content.
pub fn publish_if_current<S: ManifestSource, D: TransferStore>(
    expected: &ArtifactManifest,
    source: &mut S,
    store: &mut D,
) -> Result<TransferProgress, TransferError> {
    let request = ManifestRequest {
        key: expected.key.clone(),
    };
    let reply = source
        .manifest(&request)
        .map_err(TransferError::ManifestSource)?;
    validate_manifest(&request, &reply, Some(expected)).map_err(TransferError::Manifest)?;
    if reply.manifest != *expected {
        let deleted = matches!(reply.manifest.state, ArtifactState::Deleted);
        store
            .accept_manifest(&reply.manifest)
            .map_err(TransferError::Store)?;
        return Err(if deleted {
            TransferError::Deleted
        } else {
            TransferError::VersionChanged
        });
    }
    store.publish(expected).map_err(TransferError::Store)
}
