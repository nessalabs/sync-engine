//! Artifact manifests and local availability without file semantics or transfer.
//!
//! A host owns artifact identity, access decisions, storage and scheduling.
//! This module validates one current manifest and classifies verified local
//! content. No record delivery or transcript apply downloads file bytes.

use sha2::{Digest, Sha256};

use crate::replication::domain::{Id, Scope};

/// Stable artifact identity in one exact receiving scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactKey {
    /// Source, receiver, stream, incarnation, schema and access epoch.
    pub scope: Scope,
    /// Opaque host-selected artifact identity.
    pub id: Id,
}

/// SHA-256 of complete artifact bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sha256Digest(pub [u8; 32]);

impl Sha256Digest {
    /// Computes the content digest of a small in-memory value. Large adapters
    /// may stream through `sha2::Sha256` and construct this value at completion.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

/// Immutable description of one complete byte version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentIdentity {
    /// Exact byte count, including zero for an empty artifact.
    pub length: u64,
    /// SHA-256 over precisely those bytes.
    pub digest: Sha256Digest,
}

impl ContentIdentity {
    /// Computes content identity for a small in-memory example value.
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            length: bytes.len() as u64,
            digest: Sha256Digest::of(bytes),
        }
    }
}

/// Current source state; deletion carries no transferable bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArtifactState {
    /// This revision names one immutable content version.
    Live(ContentIdentity),
    /// Retained deletion fence for this identity.
    Deleted,
}

/// One versioned current-state answer from the owning host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactManifest {
    /// Identity and exact authorized scope of this answer.
    pub key: ArtifactKey,
    /// Source-assigned monotonic revision within its incarnation.
    pub revision: u64,
    /// Current bytes or retained deletion marker.
    pub state: ArtifactState,
}

/// Exact requested artifact and scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestRequest {
    /// Receiver-bound artifact identity.
    pub key: ArtifactKey,
}

/// A source response echoed against its request before applying to a cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestReply {
    /// Exact request the source answered.
    pub request: ManifestRequest,
    /// Current authorized manifest.
    pub manifest: ArtifactManifest,
}

/// Why a new manifest cannot replace the saved one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestError {
    /// Response belongs to a different request or scope.
    Foreign,
    /// Revision zero is not committed source state.
    InvalidRevision,
    /// Source returned an older revision.
    Stale,
    /// Same revision changed its content or deletion state.
    Conflict,
    /// A retained deletion fence cannot be resurrected in this identity.
    Fenced,
}

/// Validates identity, revision and deletion order before a host persists a
/// manifest. The host must retain deletion markers across access-epoch resets.
pub fn validate_manifest(
    request: &ManifestRequest,
    reply: &ManifestReply,
    previous: Option<&ArtifactManifest>,
) -> Result<(), ManifestError> {
    if reply.request != *request || reply.manifest.key != request.key {
        return Err(ManifestError::Foreign);
    }
    if reply.manifest.revision == 0 {
        return Err(ManifestError::InvalidRevision);
    }
    if let Some(saved) = previous {
        if saved.key != request.key {
            return Err(ManifestError::Foreign);
        }
        if matches!(saved.state, ArtifactState::Deleted)
            && matches!(reply.manifest.state, ArtifactState::Live(_))
        {
            return Err(ManifestError::Fenced);
        }
        if reply.manifest.revision < saved.revision {
            return Err(ManifestError::Stale);
        }
        if reply.manifest.revision == saved.revision && reply.manifest.state != saved.state {
            return Err(ManifestError::Conflict);
        }
    }
    Ok(())
}

/// Result of checking complete local bytes against their current manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    /// Retained deletion marker; cached bytes must not be shown.
    Deleted,
    /// Bytes were verified against the current content identity.
    CachedVerified,
    /// Metadata is known and the source is reachable for a later fetch.
    MetadataOnly,
    /// Metadata is known, but missing bytes cannot currently be fetched.
    SourceUnavailable,
    /// Local bytes fail the current length or SHA-256 check.
    HashMismatch,
}

/// Classifies a local complete-byte candidate. `None` means bytes are absent.
/// A mismatch never becomes a verified cache hit even while offline.
pub fn availability(
    manifest: &ArtifactManifest,
    local_bytes: Option<&[u8]>,
    source_reachable: bool,
) -> Availability {
    let ArtifactState::Live(expected) = manifest.state else {
        return Availability::Deleted;
    };
    match local_bytes {
        Some(bytes) if ContentIdentity::of(bytes) == expected => Availability::CachedVerified,
        Some(_) => Availability::HashMismatch,
        None if source_reachable => Availability::MetadataOnly,
        None => Availability::SourceUnavailable,
    }
}

/// Typed manifest-source outcome. `Missing` is not a deletion marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestSourceError {
    /// Source cannot be reached or cannot currently establish the answer.
    Unavailable,
    /// Current policy denies this request.
    Denied,
    /// Source identity or access epoch changed.
    ScopeChanged,
    /// No authenticated current answer exists; never infer deletion from this.
    Missing,
}

/// Host adapter for one current, authorized artifact manifest.
pub trait ManifestSource {
    /// Checks current policy and reads one matching manifest. Calls must not
    /// return file bytes, and a refusal must not be interpreted as deletion.
    fn manifest(&mut self, request: &ManifestRequest)
        -> Result<ManifestReply, ManifestSourceError>;
}

/// Host cache result for an exact artifact key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedArtifact {
    /// Last locally accepted metadata.
    pub manifest: ArtifactManifest,
    /// Whether complete bytes exist locally. The transfer adapter must verify
    /// their length and hash again before exposing them as `CachedVerified`.
    pub has_candidate_bytes: bool,
}

/// Narrow local metadata/cache lookup. Byte staging and verification belong
/// to the later transfer port, not to a transcript record or a backup API.
pub trait ArtifactCacheIndex {
    /// Reads a locally known manifest and byte-presence hint without network I/O.
    fn lookup(&mut self, key: &ArtifactKey) -> Result<Option<CachedArtifact>, ArtifactCacheError>;
}

/// Local cache lookup failed without changing the source manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactCacheError {
    /// Cache cannot currently be read or verified.
    Unavailable,
    /// Saved identity differs and requires an explicit reset decision.
    ScopeChanged,
}
