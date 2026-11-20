//! Exact authorized artifact reads on the real serialized TLS channel.
//! No discovery, stage/application ownership, or automatic retry.

use super::{
    access_code, active_client, put_scope, respond_checked, serve_bound_with_hooks, Arc,
    AtomicBool, BoundHooks, CapabilityRoutes, Client, Decoder, InterruptibleSessionIo, Mutex,
    Packet, RecordSession, RecordSessionError, RecordSource, Scope, ScopeAuthorizer, SessionLimits,
    SourceError, HEADER, MIN_CONTROL_BYTES,
};
use crate::replication::artifacts::{
    chunk_length, validate_manifest, ArtifactKey, ArtifactManifest, ArtifactState, ChunkReply,
    ChunkRequest, ChunkSource, ChunkSourceError, ContentIdentity, ManifestReply, ManifestRequest,
    ManifestSource, ManifestSourceError, Sha256Digest,
};
use crate::replication::domain::MAX_ID_BYTES;
use crate::replication::infrastructure::session::TlsSessionIo;
use std::io::{self, Write};
use std::sync::atomic::Ordering;

mod chunk;

const MANIFEST: u8 = 16;
pub(super) fn operation(kind: u8) -> bool {
    matches!(kind, MANIFEST | chunk::CHUNK)
}
const VERSION: u16 = 1;
const MAX_KEY_BYTES: usize = 7 * (1 + MAX_ID_BYTES);
const MAX_RESPONSE: usize = 1 + MAX_KEY_BYTES + 8 + 1 + 8 + 32;
const _: () = assert!(MAX_RESPONSE <= MIN_CONTROL_BYTES - HEADER);

/// Host's current grant for one requested artifact operation and TLS peer.
/// A record-scope grant alone authorizes neither metadata nor content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactSessionAccess {
    /// The peer may perform this exact requested operation.
    Allowed,
    /// The peer cannot perform this operation, whether or not the key exists.
    Denied,
    /// No current grant can be established.
    Unverifiable,
}

impl ArtifactSessionAccess {
    fn code(self) -> u8 {
        match self {
            Self::Allowed => 0,
            Self::Denied => 1,
            Self::Unverifiable => 2,
        }
    }
}

/// Current host grant for the explicit metadata-only endpoint.
pub use ArtifactSessionAccess as ArtifactManifestAccess;

/// Exact read operation offered to the host's current artifact policy.
/// Allowing a metadata lookup does not imply that a byte range is allowed.
#[derive(Clone, Copy, Debug)]
pub enum ArtifactSessionOperation<'a> {
    /// Check independently before returning a current manifest or absence.
    Manifest(&'a ArtifactKey),
    /// Check independently before reading or emitting this exact live range.
    Chunk(&'a ChunkRequest),
}
fn put_key(bytes: &mut Vec<u8>, key: &ArtifactKey) {
    put_scope(bytes, &key.scope);
    let id = key.id.as_str().as_bytes();
    bytes.push(id.len() as u8);
    bytes.extend_from_slice(id);
}

fn read_key(d: &mut Decoder<'_>) -> Result<ArtifactKey, RecordSessionError> {
    Ok(ArtifactKey {
        scope: d.scope()?,
        id: d.id()?,
    })
}

fn encode_request(request: &ManifestRequest) -> Vec<u8> {
    let mut bytes = VERSION.to_be_bytes().to_vec();
    put_key(&mut bytes, &request.key);
    bytes
}

fn decode_request(bytes: &[u8], scope: &Scope) -> Result<ManifestRequest, RecordSessionError> {
    let mut d = Decoder::new(bytes);
    if d.take(2)? != VERSION.to_be_bytes() {
        return Err(RecordSessionError::Protocol);
    }
    let key = read_key(&mut d)?;
    d.finish()?;
    if key.scope != *scope {
        return Err(RecordSessionError::Source(SourceError::IdentityChanged));
    }
    Ok(ManifestRequest { key })
}

fn encode_response(
    request: &ManifestRequest,
    result: Result<ManifestReply, ManifestSourceError>,
) -> (u8, Vec<u8>) {
    let reply = match result {
        Ok(reply) if validate_manifest(request, &reply, None).is_ok() => reply,
        Err(ManifestSourceError::Missing) => return (0, vec![0]),
        Err(ManifestSourceError::Denied) => return (1, Vec::new()),
        Err(ManifestSourceError::ScopeChanged) => return (5, Vec::new()),
        Ok(_) | Err(ManifestSourceError::Unavailable) => return (2, Vec::new()),
    };
    let mut bytes = vec![1];
    put_key(&mut bytes, &reply.request.key);
    bytes.extend_from_slice(&reply.manifest.revision.to_be_bytes());
    match reply.manifest.state {
        ArtifactState::Deleted => bytes.push(0),
        ArtifactState::Live(content) => {
            bytes.push(1);
            bytes.extend_from_slice(&content.length.to_be_bytes());
            bytes.extend_from_slice(&content.digest.0);
        }
    }
    (0, bytes)
}

fn decode_response(
    bytes: &[u8],
    request: &ManifestRequest,
) -> Result<Option<ManifestReply>, RecordSessionError> {
    let mut d = Decoder::new(bytes);
    match d.byte()? {
        0 => {
            d.finish()?;
            Ok(None)
        }
        1 => {
            let key = read_key(&mut d)?;
            let revision = d.u64()?;
            let state = match d.byte()? {
                0 => ArtifactState::Deleted,
                1 => ArtifactState::Live(ContentIdentity {
                    length: d.u64()?,
                    digest: Sha256Digest(
                        d.take(32)?
                            .try_into()
                            .map_err(|_| RecordSessionError::Protocol)?,
                    ),
                }),
                _ => return Err(RecordSessionError::Protocol),
            };
            d.finish()?;
            let reply = ManifestReply {
                request: ManifestRequest { key: key.clone() },
                manifest: ArtifactManifest {
                    key,
                    revision,
                    state,
                },
            };
            validate_manifest(request, &reply, None).map_err(|_| RecordSessionError::Protocol)?;
            Ok(Some(reply))
        }
        _ => Err(RecordSessionError::Protocol),
    }
}

pub(super) trait ServerRead {
    fn chunks(&self) -> bool;
    fn authorize(&mut self, operation: ArtifactSessionOperation<'_>) -> ArtifactSessionAccess;
    fn prepare(&mut self, request: &Request, policy: &mut dyn ScopeAuthorizer) -> (u8, Vec<u8>);
}

pub(super) enum Request {
    Manifest(ManifestRequest),
    Chunk(ChunkRequest),
}
impl Request {
    fn access(&self) -> ArtifactSessionOperation<'_> {
        match self {
            Self::Manifest(request) => ArtifactSessionOperation::Manifest(&request.key),
            Self::Chunk(request) => ArtifactSessionOperation::Chunk(request),
        }
    }
    fn scope(&self) -> &Scope {
        match self {
            Self::Manifest(request) => &request.key.scope,
            Self::Chunk(request) => &request.manifest.key.scope,
        }
    }
}

fn current_access(
    route: &mut (impl ServerRead + ?Sized),
    policy: &mut dyn ScopeAuthorizer,
    request: &Request,
) -> u8 {
    let scope = request.scope();
    let status = access_code(policy.authorize(scope), scope);
    if status != 0 {
        status
    } else {
        route.authorize(request.access()).code()
    }
}

type ChunkRead<M> = fn(&mut M, &ChunkRequest) -> Result<ChunkReply, ChunkSourceError>;
struct Routes<P, G, M> {
    peer: Sha256Digest,
    authorize: P,
    factory: G,
    chunk: Option<ChunkRead<M>>,
}

impl<P, G, M> ServerRead for Routes<P, G, M>
where
    P: FnMut(Sha256Digest, ArtifactSessionOperation<'_>) -> ArtifactSessionAccess,
    G: FnMut(&Scope) -> Result<M, ManifestSourceError>,
    M: ManifestSource,
{
    fn chunks(&self) -> bool {
        self.chunk.is_some()
    }

    fn authorize(&mut self, operation: ArtifactSessionOperation<'_>) -> ArtifactSessionAccess {
        (self.authorize)(self.peer, operation)
    }

    fn prepare(&mut self, request: &Request, policy: &mut dyn ScopeAuthorizer) -> (u8, Vec<u8>) {
        let status = current_access(self, policy, request);
        if status != 0 {
            return (status, Vec::new());
        }
        let mut source = match (self.factory)(request.scope()) {
            Ok(source) => source,
            Err(error) => {
                return match request {
                    Request::Manifest(request) => encode_response(request, Err(error)),
                    Request::Chunk(_) => (chunk::factory_error(error), Vec::new()),
                };
            }
        };
        let status = current_access(self, policy, request);
        if status != 0 {
            return (status, Vec::new());
        }
        let result = match request {
            Request::Manifest(request) => encode_response(request, source.manifest(request)),
            Request::Chunk(request) => {
                let Some(read) = self.chunk else {
                    return (4, Vec::new());
                };
                chunk::encode_response(request, read(&mut source, request))
            }
        };
        // The source (and any snapshot) is gone before any response writes.
        drop(source);
        result
    }
}

pub(super) fn respond(
    route: &mut dyn ServerRead,
    stream: &mut impl Write,
    packet: &Packet,
    scope: &Scope,
    policy: &mut impl ScopeAuthorizer,
    limit: usize,
) -> Result<(), RecordSessionError> {
    if packet.kind == chunk::CHUNK && !route.chunks() {
        return Err(RecordSessionError::Protocol);
    }
    // A valid binding is not current authority, including for a scope mismatch.
    let status = access_code(policy.authorize(scope), scope);
    if status != 0 {
        return respond_checked(stream, packet, status, &[], limit, || status);
    }
    let decoded = match packet.kind {
        MANIFEST => decode_request(&packet.payload, scope).map(Request::Manifest),
        chunk::CHUNK => chunk::decode_request(&packet.payload, scope).map(Request::Chunk),
        _ => return Err(RecordSessionError::Protocol),
    };
    let request = match decoded {
        Ok(request) => request,
        Err(RecordSessionError::Source(error)) => {
            let status = super::source_error_code(error);
            return respond_checked(stream, packet, status, &[], limit, || {
                access_code(policy.authorize(scope), scope)
            });
        }
        Err(error) => return Err(error),
    };
    let (status, body) = route.prepare(&request, policy);
    // Includes authorized absence and every content or refusal frame. Earlier
    // sent fragments cannot be recalled; the client never exposes a partial DTO.
    respond_checked(stream, packet, status, &body, limit, || {
        current_access(route, policy, &request)
    })
}

/// Enable manifest v1 on an admitted TLS *server* stream, alongside the
/// existing single-scope record operations. The host supplies current record
/// policy and a separate artifact policy keyed by the actual verified client
/// certificate's SHA-256 (leaf DER) and the full artifact key. This API does
/// not turn possession of a certificate or record grant into an artifact
/// grant. Only the host may decide that the selected peer permits this API.
/// Both checks precede source acquisition and are repeated before the only
/// reply frame. They do not form a transaction with external policy/storage.
/// No scheduler, chunk bytes, discovery, automatic fallback or cache apply.
/// A terminal error returns channel disposal to the host.
pub fn serve_tls_artifact_manifest_session<A, F, R, P, G, M>(
    stream: &mut TlsSessionIo,
    session: u64,
    supported: SessionLimits,
    policy: &mut A,
    open_record_source: F,
    mut authorize_artifact: P,
    open_artifact_source: G,
) -> Result<(), RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
    P: FnMut(Sha256Digest, &ArtifactKey) -> ArtifactManifestAccess,
    G: FnMut(&Scope) -> Result<M, ManifestSourceError>,
    M: ManifestSource,
{
    let peer = stream
        .verified_client_leaf()
        .ok_or_else(|| io::Error::from(io::ErrorKind::PermissionDenied))?;
    let mut route = Routes::<_, _, M> {
        peer,
        authorize: move |peer, operation: ArtifactSessionOperation<'_>| match operation {
            ArtifactSessionOperation::Manifest(key) => authorize_artifact(peer, key),
            ArtifactSessionOperation::Chunk(_) => ArtifactSessionAccess::Denied,
        },
        factory: open_artifact_source,
        chunk: None,
    };
    serve_bound_with_hooks(
        stream,
        session,
        supported,
        policy,
        open_record_source,
        BoundHooks {
            follow: None,
            artifact: Some(&mut route),
        },
        CapabilityRoutes::legacy(),
    )
}

/// Explicitly enable manifest and bounded chunk reads alongside ordinary
/// record operations on the existing serialized TLS server session.
/// The host must independently authorize each operation for the exact
/// verified peer fingerprint and exact key/range. A metadata grant must not
/// authorize bytes. Current scope and artifact grants are checked before
/// opening the source, again before reading, and before each reply fragment.
/// No storage transaction spans I/O. Refusal cannot recall already emitted
/// data, and a closed channel is never silently replaced by this adapter.
pub fn serve_tls_artifact_transfer_session<A, F, R, P, G, M>(
    stream: &mut TlsSessionIo,
    session: u64,
    supported: SessionLimits,
    policy: &mut A,
    open_record_source: F,
    authorize_artifact: P,
    open_artifact_source: G,
) -> Result<(), RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
    R: RecordSource,
    P: FnMut(Sha256Digest, ArtifactSessionOperation<'_>) -> ArtifactSessionAccess,
    G: FnMut(&Scope) -> Result<M, ManifestSourceError>,
    M: ChunkSource,
{
    let peer = stream
        .verified_client_leaf()
        .ok_or_else(|| io::Error::from(io::ErrorKind::PermissionDenied))?;
    let mut route = Routes {
        peer,
        authorize: authorize_artifact,
        factory: open_artifact_source,
        chunk: Some(M::chunk),
    };
    serve_bound_with_hooks(
        stream,
        session,
        supported,
        policy,
        open_record_source,
        BoundHooks {
            follow: None,
            artifact: Some(&mut route),
        },
        CapabilityRoutes::legacy(),
    )
}

/// Opt-in manifest metadata port using the same serialized TLS session and
/// exact current binding as the ordinary record ports. It does not apply or
/// replace cached state. A terminal error fences the shared connection.
pub struct ArtifactManifestSessionSource {
    shared: Arc<Mutex<Client<TlsSessionIo>>>,
    closing: Arc<AtomicBool>,
}

impl RecordSession<TlsSessionIo> {
    /// Obtain the explicit artifact v1 port. Use only with a peer known by
    /// the host to support this operation. No legacy or plaintext fallback.
    /// Use an interruptible connection constructor when the host needs to
    /// interrupt an in-progress request via [`RecordSession::close`].
    pub fn artifact_manifest_source(&self) -> ArtifactManifestSessionSource {
        ArtifactManifestSessionSource {
            shared: Arc::clone(&self.shared),
            closing: Arc::clone(&self.closing),
        }
    }
}

impl ArtifactManifestSessionSource {
    fn rpc<T>(
        &self,
        scope: &Scope,
        kind: u8,
        body: Vec<u8>,
        reply_bound: usize,
        decode: impl FnOnce(&[u8]) -> Result<(T, usize), RecordSessionError>,
    ) -> Result<T, RecordSessionError> {
        let mut c = active_client(&self.shared, &self.closing)?;
        c.scope(scope)?;
        let interrupt = c.wire.io.as_ref().map(TlsSessionIo::interrupt_handle);
        let Client { wire, identity, .. } = &mut *c;
        let result = wire
            .exchange(kind, identity, body, reply_bound)
            .and_then(|body| decode(&body));
        let outcome = match result {
            Ok((reply, payload)) => {
                wire.counters.artifact_payload_bytes += payload as u64;
                wire.counters.protocol_bytes =
                    wire.counters.protocol_bytes.saturating_sub(payload as u64);
                Ok(reply)
            }
            Err(
                error @ RecordSessionError::Source(
                    SourceError::Denied | SourceError::Unavailable | SourceError::IdentityChanged,
                ),
            ) => Err(error),
            Err(error @ RecordSessionError::Source(SourceError::InvalidRequest))
                if kind == chunk::CHUNK =>
            {
                Err(error)
            }
            Err(error) => {
                wire.close();
                Err(error)
            }
        };
        c.observe_close();
        let terminal = c.wire.io.is_none();
        if terminal {
            self.closing.store(true, Ordering::Release);
        }
        drop(c);
        if terminal {
            if let Some(interrupt) = interrupt {
                interrupt.interrupt()?;
            }
        }
        outcome
    }
}

impl ManifestSource for ArtifactManifestSessionSource {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        let result = self.rpc(
            &request.key.scope,
            MANIFEST,
            encode_request(request),
            MAX_RESPONSE,
            |body| decode_response(body, request).map(|reply| (reply, 0)),
        );
        match result {
            Ok(Some(reply)) => Ok(reply),
            Ok(None) => Err(ManifestSourceError::Missing),
            Err(RecordSessionError::Source(SourceError::Denied)) => {
                Err(ManifestSourceError::Denied)
            }
            Err(RecordSessionError::Source(SourceError::IdentityChanged)) => {
                Err(ManifestSourceError::ScopeChanged)
            }
            Err(_) => Err(ManifestSourceError::Unavailable),
        }
    }
}

/// Explicit bounded transfer port sharing the existing serialized TLS session.
/// It has no local cache, retry loop or scheduler. A complete validated
/// response can be passed to the existing transfer application.
pub struct ArtifactTransferSessionSource(ArtifactManifestSessionSource);

impl RecordSession<TlsSessionIo> {
    /// Obtain manifest plus exact-range support. Only use when host
    /// configuration establishes that the peer enabled the transfer endpoint.
    /// An unsupported or interrupted channel does not fall back to another path.
    pub fn artifact_transfer_source(&self) -> ArtifactTransferSessionSource {
        ArtifactTransferSessionSource(self.artifact_manifest_source())
    }
}

impl ManifestSource for ArtifactTransferSessionSource {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        self.0.manifest(request)
    }
}

impl ChunkSource for ArtifactTransferSessionSource {
    fn chunk(&mut self, request: &ChunkRequest) -> Result<ChunkReply, ChunkSourceError> {
        let length = chunk_length(request).map_err(|_| ChunkSourceError::InvalidRequest)?;
        let result = self.0.rpc(
            &request.manifest.key.scope,
            chunk::CHUNK,
            chunk::encode_request(request)?,
            chunk::reply_bound(request, length),
            |body| chunk::decode_response(body, request, length),
        );
        match result {
            Ok(reply) => reply,
            Err(RecordSessionError::Source(SourceError::Denied)) => Err(ChunkSourceError::Denied),
            Err(RecordSessionError::Source(SourceError::IdentityChanged)) => {
                Err(ChunkSourceError::ScopeChanged)
            }
            Err(RecordSessionError::Source(SourceError::InvalidRequest)) => {
                Err(ChunkSourceError::InvalidRequest)
            }
            Err(_) => Err(ChunkSourceError::Unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;

    fn maximum_id(c: char) -> Id {
        Id::new(c.to_string().repeat(MAX_ID_BYTES)).unwrap()
    }

    #[test]
    fn maximum_exact_manifest_fits_one_minimum_frame_and_uses_full_identity() {
        let request = ManifestRequest {
            key: ArtifactKey {
                scope: Scope::new(
                    maximum_id('a'),
                    maximum_id('b'),
                    maximum_id('c'),
                    maximum_id('d'),
                    maximum_id('e'),
                    maximum_id('f'),
                ),
                id: maximum_id('g'),
            },
        };
        let bytes = encode_request(&request);
        assert_eq!(bytes.len(), 2 + MAX_KEY_BYTES);
        assert_eq!(decode_request(&bytes, &request.key.scope).unwrap(), request);
        let reply = ManifestReply {
            request: request.clone(),
            manifest: ArtifactManifest {
                key: request.key.clone(),
                revision: 1,
                state: ArtifactState::Live(ContentIdentity::of(b"")),
            },
        };
        let (status, mut bytes) = encode_response(&request, Ok(reply.clone()));
        assert_eq!(status, 0);
        assert_eq!(bytes.len(), MAX_RESPONSE);
        assert_eq!(decode_response(&bytes, &request).unwrap(), Some(reply));

        let mut different = request.clone();
        different.key.id = maximum_id('h');
        assert!(decode_response(&bytes, &different).is_err());

        bytes.push(0);
        assert!(decode_response(&bytes, &request).is_err());
    }

    #[test]
    fn only_authorized_absence_has_a_result_tag_and_unusable_source_dtos_are_not_sent() {
        let key = ArtifactKey {
            scope: Scope::new(
                maximum_id('a'),
                maximum_id('b'),
                maximum_id('c'),
                maximum_id('d'),
                maximum_id('e'),
                maximum_id('f'),
            ),
            id: maximum_id('g'),
        };
        let request = ManifestRequest { key: key.clone() };
        let bad_reply = ManifestReply {
            request: request.clone(),
            manifest: ArtifactManifest {
                key,
                revision: 0,
                state: ArtifactState::Deleted,
            },
        };
        assert_eq!(encode_response(&request, Ok(bad_reply)), (2, Vec::new()));
        assert_eq!(
            encode_response(&request, Err(ManifestSourceError::Denied)),
            (1, Vec::new())
        );
        let (status, bytes) = encode_response(&request, Err(ManifestSourceError::Missing));
        assert_eq!(status, 0);
        assert_eq!(decode_response(&bytes, &request).unwrap(), None);
    }
}
