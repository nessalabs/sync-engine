//! One exact bounded range. Fragments belong to the existing serialized wire
//! owner; this codec accepts only a complete response echo.

use super::{put_key, read_key, Decoder, RecordSessionError, Scope, SourceError};
use crate::replication::artifacts::{
    chunk_length, validate_chunk, ArtifactManifest, ArtifactState, ChunkReply, ChunkRequest,
    ChunkSourceError, ContentIdentity, ManifestSourceError, Sha256Digest,
};

pub(super) const CHUNK: u8 = 17;
const VERSION: u16 = 1;
const FIXED_REQUEST: usize = 2 + 8 + 8 + 32 + 8 + 4;

fn write_request(out: &mut Vec<u8>, request: &ChunkRequest) -> Result<(), ChunkSourceError> {
    let ArtifactState::Live(content) = request.manifest.state else {
        return Err(ChunkSourceError::InvalidRequest);
    };
    let count = u32::try_from(request.max_bytes).map_err(|_| ChunkSourceError::InvalidRequest)?;
    out.extend_from_slice(&VERSION.to_be_bytes());
    put_key(out, &request.manifest.key);
    out.extend_from_slice(&request.manifest.revision.to_be_bytes());
    out.extend_from_slice(&content.length.to_be_bytes());
    out.extend_from_slice(&content.digest.0);
    out.extend_from_slice(&request.offset.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn read_request(d: &mut Decoder<'_>) -> Result<ChunkRequest, RecordSessionError> {
    if d.take(2)? != VERSION.to_be_bytes() {
        return Err(RecordSessionError::Protocol);
    }
    let key = read_key(d)?;
    let revision = d.u64()?;
    let length = d.u64()?;
    let digest = Sha256Digest(
        d.take(32)?
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    );
    let offset = d.u64()?;
    let max_bytes = u32::from_be_bytes(
        d.take(4)?
            .try_into()
            .map_err(|_| RecordSessionError::Protocol)?,
    ) as usize;
    Ok(ChunkRequest {
        manifest: ArtifactManifest {
            key,
            revision,
            state: ArtifactState::Live(ContentIdentity { length, digest }),
        },
        offset,
        max_bytes,
    })
}

pub(super) fn encode_request(request: &ChunkRequest) -> Result<Vec<u8>, ChunkSourceError> {
    let mut out = Vec::new();
    write_request(&mut out, request)?;
    Ok(out)
}

pub(super) fn decode_request(
    bytes: &[u8],
    scope: &Scope,
) -> Result<ChunkRequest, RecordSessionError> {
    let mut d = Decoder::new(bytes);
    let request = read_request(&mut d)?;
    d.finish()?;
    if request.manifest.key.scope != *scope {
        return Err(SourceError::IdentityChanged.into());
    }
    chunk_length(&request).map_err(|_| SourceError::InvalidRequest)?;
    Ok(request)
}

pub(super) fn reply_bound(request: &ChunkRequest, length: usize) -> usize {
    1 + FIXED_REQUEST
        + super::super::scope_bytes(&request.manifest.key.scope)
        + 1
        + request.manifest.key.id.as_str().len()
        + length
}

pub(super) fn factory_error(error: ManifestSourceError) -> u8 {
    match error {
        ManifestSourceError::Denied => 1,
        ManifestSourceError::ScopeChanged => 5,
        ManifestSourceError::Unavailable | ManifestSourceError::Missing => 2,
    }
}

pub(super) fn encode_response(
    request: &ChunkRequest,
    result: Result<ChunkReply, ChunkSourceError>,
) -> (u8, Vec<u8>) {
    let reply = match result {
        Ok(reply) if validate_chunk(request, &reply).is_ok() => reply,
        Err(ChunkSourceError::VersionChanged) => return (0, vec![1]),
        Err(ChunkSourceError::Deleted) => return (0, vec![2]),
        Err(ChunkSourceError::Denied) => return (1, Vec::new()),
        Err(ChunkSourceError::InvalidRequest) => return (4, Vec::new()),
        Err(ChunkSourceError::ScopeChanged) => return (5, Vec::new()),
        Ok(_) | Err(ChunkSourceError::Unavailable) => return (2, Vec::new()),
    };
    let mut bytes = Vec::with_capacity(reply_bound(request, reply.bytes.len()));
    bytes.push(0);
    if write_request(&mut bytes, &reply.request).is_err() {
        return (2, Vec::new());
    }
    bytes.extend_from_slice(&reply.bytes);
    (0, bytes)
}

type Received = (Result<ChunkReply, ChunkSourceError>, usize);

pub(super) fn decode_response(
    bytes: &[u8],
    request: &ChunkRequest,
    length: usize,
) -> Result<Received, RecordSessionError> {
    let mut d = Decoder::new(bytes);
    let result = match d.byte()? {
        0 => {
            let echoed = read_request(&mut d)?;
            if echoed != *request {
                return Err(RecordSessionError::Protocol);
            }
            let bytes = d.take(length)?;
            d.finish()?;
            let reply = ChunkReply {
                request: echoed,
                bytes: bytes.to_vec(),
            };
            validate_chunk(request, &reply).map_err(|_| RecordSessionError::Protocol)?;
            return Ok((Ok(reply), length));
        }
        1 => ChunkSourceError::VersionChanged,
        2 => ChunkSourceError::Deleted,
        _ => return Err(RecordSessionError::Protocol),
    };
    d.finish()?;
    Ok((Err(result), 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::artifacts::{ArtifactKey, MAX_CHUNK_BYTES};
    use crate::replication::domain::{Id, MAX_ID_BYTES};
    use crate::replication::infrastructure::session::{MAX_CONTROL_BYTES, MIN_CONTROL_BYTES};

    fn maximum_request() -> ChunkRequest {
        let id = |c: &str| Id::new(c.repeat(MAX_ID_BYTES)).unwrap();
        ChunkRequest {
            manifest: ArtifactManifest {
                key: ArtifactKey {
                    scope: Scope::new(id("a"), id("b"), id("c"), id("d"), id("e"), id("f")),
                    id: id("g"),
                },
                revision: 1,
                state: ArtifactState::Live(ContentIdentity::of(&[42; 4][..])),
            },
            offset: 0,
            max_bytes: MAX_CHUNK_BYTES,
        }
    }

    #[test]
    fn maximum_key_and_budget_request_fits_one_frame_and_replies_echo_exact_range() {
        let request = maximum_request();
        let bytes = encode_request(&request).unwrap();
        assert!(bytes.len() <= MIN_CONTROL_BYTES - super::super::super::HEADER);
        assert_eq!(
            decode_request(&bytes, &request.manifest.key.scope).unwrap(),
            request
        );

        let reply = ChunkReply {
            request: request.clone(),
            bytes: vec![42; 4],
        };
        let (status, mut bytes) = encode_response(&request, Ok(reply.clone()));
        assert_eq!(status, 0);
        assert_eq!(bytes.len(), reply_bound(&request, 4));
        assert!(reply_bound(&request, MAX_CHUNK_BYTES) > MAX_CONTROL_BYTES);
        assert_eq!(
            decode_response(&bytes, &request, 4).unwrap(),
            (Ok(reply), 4)
        );

        let mut other = request.clone();
        other.max_bytes -= 1; // Same final length, wrong request identity.
        assert!(decode_response(&bytes, &other, 4).is_err());
        bytes.pop();
        assert!(decode_response(&bytes, &request, 4).is_err());
    }

    #[test]
    fn unavailable_absence_never_becomes_a_deletion_and_refusal_has_no_content() {
        let request = maximum_request();
        assert_eq!(factory_error(ManifestSourceError::Missing), 2);
        assert_eq!(
            encode_response(&request, Err(ChunkSourceError::Denied)),
            (1, Vec::new())
        );
        for refusal in [ChunkSourceError::VersionChanged, ChunkSourceError::Deleted] {
            let (status, body) = encode_response(&request, Err(refusal));
            assert_eq!(status, 0);
            assert_eq!(body.len(), 1);
            assert_eq!(
                decode_response(&body, &request, 4).unwrap(),
                (Err(refusal), 0)
            );
        }
        let mut out_of_bound = request;
        out_of_bound.max_bytes += 1;
        assert!(matches!(
            decode_request(
                &encode_request(&out_of_bound).unwrap(),
                &out_of_bound.manifest.key.scope
            ),
            Err(RecordSessionError::Source(SourceError::InvalidRequest))
        ));
    }
}
