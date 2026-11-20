//! Exact authorized artifact manifests on the real serialized TLS channel.
//! No discovery, file bytes, stage/application ownership, or automatic retry.

use super::{
    access_code, active_client, put_scope, serve_bound_with_hooks, write_packet, Arc, AtomicBool,
    BoundHooks, CapabilityRoutes, Client, Decoder, InterruptibleSessionIo, Mutex, Packet,
    RecordSession, RecordSessionError, RecordSource, Scope, ScopeAuthorizer, SessionLimits,
    SourceError, HEADER, MIN_CONTROL_BYTES, REPLY,
};
use crate::replication::artifacts::{
    validate_manifest, ArtifactKey, ArtifactManifest, ArtifactState, ContentIdentity,
    ManifestReply, ManifestRequest, ManifestSource, ManifestSourceError, Sha256Digest,
};
use crate::replication::domain::MAX_ID_BYTES;
use crate::replication::infrastructure::session::TlsSessionIo;
use std::io::{self, Write};
use std::sync::atomic::Ordering;

pub(super) const MANIFEST: u8 = 16;
const VERSION: u16 = 1;
const MAX_KEY_BYTES: usize = 7 * (1 + MAX_ID_BYTES);
const MAX_RESPONSE: usize = 1 + MAX_KEY_BYTES + 8 + 1 + 8 + 32;
const _: () = assert!(MAX_RESPONSE <= MIN_CONTROL_BYTES - HEADER);

/// Host's current grant for an exact artifact key and the verified TLS peer.
/// A record-scope grant alone never authorizes artifact metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactManifestAccess {
    /// The peer may read this exact artifact's current metadata.
    Allowed,
    /// The peer is not permitted to read this key, whether or not it exists.
    Denied,
    /// No current grant can be established.
    Unverifiable,
}

impl ArtifactManifestAccess {
    fn code(self) -> u8 {
        match self {
            Self::Allowed => 0,
            Self::Denied => 1,
            Self::Unverifiable => 2,
        }
    }
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

pub(super) type ServerRead<'a> =
    &'a mut dyn FnMut(&ManifestRequest, &mut dyn ScopeAuthorizer) -> (u8, Vec<u8>);

pub(super) fn respond(
    read: &mut ServerRead<'_>,
    stream: &mut impl Write,
    packet: &Packet,
    scope: &Scope,
    policy: &mut impl ScopeAuthorizer,
    limit: usize,
) -> Result<(), RecordSessionError> {
    // A valid binding is not current authority, including for a scope mismatch.
    let status = access_code(policy.authorize(scope), scope);
    let (status, payload) = if status != 0 {
        (status, Vec::new())
    } else {
        match decode_request(&packet.payload, scope) {
            Ok(request) => read(&request, policy),
            Err(RecordSessionError::Source(SourceError::IdentityChanged)) => (5, Vec::new()),
            Err(error) => return Err(error),
        }
    };
    write_packet(
        stream,
        &Packet {
            kind: MANIFEST | REPLY,
            status,
            meta: packet.meta,
            total: payload.len(),
            offset: 0,
            payload,
        },
        limit,
    )?;
    stream.flush()?;
    Ok(())
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
    mut open_artifact_source: G,
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
    let mut read = |request: &ManifestRequest, policy: &mut dyn ScopeAuthorizer| {
        let scope = &request.key.scope;
        let mut artifact_access = || authorize_artifact(peer, &request.key).code();
        let status = artifact_access();
        if status != 0 {
            return (status, Vec::new());
        }
        let mut current = || {
            let status = access_code(policy.authorize(scope), scope);
            if status != 0 {
                status
            } else {
                artifact_access()
            }
        };
        let result = match open_artifact_source(scope) {
            Ok(mut source) => {
                // Acquiring the source can itself block. Recheck before using
                // it; the handle drops before the final policy check and I/O.
                let status = current();
                if status != 0 {
                    return (status, Vec::new());
                }
                source.manifest(request)
            }
            Err(error) => Err(error),
        };
        let (status, payload) = encode_response(request, result);
        // An authorized absence also reveals a lookup result. Recheck before
        // emitting presence, absence or an artifact-specific refusal.
        let admission = current();
        if admission != 0 {
            (admission, Vec::new())
        } else {
            (status, payload)
        }
    };
    serve_bound_with_hooks(
        stream,
        session,
        supported,
        policy,
        open_record_source,
        BoundHooks {
            follow: None,
            artifact: Some(&mut read),
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

impl ManifestSource for ArtifactManifestSessionSource {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        let mut c = active_client(&self.shared, &self.closing)
            .map_err(|_| ManifestSourceError::Unavailable)?;
        c.scope(&request.key.scope).map_err(|error| match error {
            SourceError::IdentityChanged => ManifestSourceError::ScopeChanged,
            _ => ManifestSourceError::Unavailable,
        })?;
        let interrupt = c.wire.io.as_ref().map(TlsSessionIo::interrupt_handle);
        let Client { wire, identity, .. } = &mut *c;
        let result = wire
            .exchange(MANIFEST, identity, encode_request(request), MAX_RESPONSE)
            .and_then(|body| decode_response(&body, request));
        let outcome = match result {
            Ok(Some(reply)) => Ok(reply),
            Ok(None) => Err(ManifestSourceError::Missing),
            Err(RecordSessionError::Source(SourceError::Denied)) => {
                Err(ManifestSourceError::Denied)
            }
            Err(RecordSessionError::Source(SourceError::Unavailable)) => {
                Err(ManifestSourceError::Unavailable)
            }
            Err(RecordSessionError::Source(SourceError::IdentityChanged)) => {
                Err(ManifestSourceError::ScopeChanged)
            }
            Err(_) => {
                wire.close();
                Err(ManifestSourceError::Unavailable)
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
                // ManifestSource has no transport-disposal detail. If the OS
                // interrupt fails, surface unavailable and retain the host's
                // explicit close/deadline disposal obligation.
                if interrupt.interrupt().is_err() {
                    return Err(ManifestSourceError::Unavailable);
                }
            }
        }
        outcome
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
