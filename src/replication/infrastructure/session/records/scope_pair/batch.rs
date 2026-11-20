//! Exactly two metadata observations on the original pair connection.
use super::*;
use crate::replication::domain::{
    validate_capabilities, Checkpoint, KNOWN_READER_CAPABILITIES, SOURCE_HEAD_VERSION,
};

pub(in super::super) const HEAD_BATCH: u8 = 10;

/// One current-policy outcome, correlated with the caller's checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PairHeadObservation {
    checkpoint: Checkpoint,
    outcome: Result<SourceHead, SourceError>,
}
impl PairHeadObservation {
    /// Retained exact checkpoint; this operation does not advance it.
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    /// Owned admitted metadata or the typed refusal for this scope alone.
    pub fn outcome(&self) -> &Result<SourceHead, SourceError> {
        &self.outcome
    }
    /// Whether the admitted head is at the supplied checkpoint position.
    /// Metadata remains present; equality does not assert unchanged formats.
    pub fn at_checkpoint(&self) -> bool {
        self.outcome
            .as_ref()
            .is_ok_and(|head| head.position() == self.checkpoint.position())
    }
}

#[derive(PartialEq, Eq)]
struct Slot {
    handle: u32,
    generation: u64,
    checkpoint: Checkpoint,
}
impl Slot {
    fn for_id(id: &ScopeIdentity, checkpoint: &Checkpoint) -> Self {
        Self {
            handle: id.handle(),
            generation: id.generation(),
            checkpoint: checkpoint.clone(),
        }
    }
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.handle.to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(&self.checkpoint.position().to_be_bytes());
        put_scope(out, self.checkpoint.scope());
    }
    fn decode(d: &mut Decoder<'_>) -> Result<Self, RecordSessionError> {
        let handle = u32::from_be_bytes(
            d.take(4)?
                .try_into()
                .map_err(|_| RecordSessionError::Protocol)?,
        );
        let generation = d.u64()?;
        let position = d.u64()?;
        Ok(Self {
            handle,
            generation,
            checkpoint: Checkpoint::new(d.scope()?, position),
        })
    }
    fn matches_bound(&self, scope: &Scope, slot: u32) -> bool {
        self.handle == slot
            && self.generation == u64::from(slot)
            && self.checkpoint.scope() == scope
    }
    fn max_reply(&self) -> usize {
        // Slot echo: handle/generation/checkpoint + scope; status and at most
        // 26 metadata bytes (capability refusal data is strictly shorter).
        20 + scope_bytes(self.checkpoint.scope()) + 1 + capabilities::META_BYTES
    }
}

fn access<A: ScopeAuthorizer>(policy: &mut A, scope: &Scope) -> Result<(), SourceError> {
    match policy.authorize(scope) {
        Access::Allowed(allowed) if allowed == *scope => Ok(()),
        Access::Allowed(_) | Access::IdentityChanged => Err(SourceError::IdentityChanged),
        Access::Denied => Err(SourceError::Denied),
        Access::Unverifiable => Err(SourceError::Unavailable),
        Access::InvalidResponse => Err(SourceError::InvalidRequest),
    }
}

fn observe<A, F, R>(
    scope: &Scope,
    support: CapabilitySupport,
    policy: &mut A,
    factory: &mut F,
    read_head: capabilities::ViewHead<R>,
) -> Result<SourceHead, SourceError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
{
    access(policy, scope)?;
    let mut view = factory(scope)?;
    access(policy, scope)?;
    let head = read_head(&mut view, scope)?;
    if head.scope() != scope {
        return Err(SourceError::IdentityChanged);
    }
    validate_capabilities(&head, support).map_err(SourceError::Incompatible)?;
    Ok(head)
}

fn put_outcome(out: &mut Vec<u8>, outcome: Result<SourceHead, SourceError>) {
    match outcome {
        Ok(head) => {
            out.push(0);
            capabilities::put_metadata(out, &head);
        }
        Err(SourceError::Incompatible(error)) => match capabilities::encode_incompatible(error) {
            Ok(fields) => {
                out.extend_from_slice(&[8, fields.len() as u8]);
                out.extend_from_slice(&fields);
            }
            // Invalid provider metadata is a refusal for this slot only.
            Err(_) => out.push(source_error_code(SourceError::InvalidRequest)),
        },
        Err(other) => out.push(source_error_code(other)),
    }
}

pub(in super::super) fn serve_batch<S, A, F, R>(
    stream: &mut S,
    request: &Packet,
    scopes: [&Scope; 2],
    policy: &mut A,
    factory: &mut F,
    read_head: capabilities::ViewHead<R>,
    limit: usize,
) -> Result<(), RecordSessionError>
where
    S: Write,
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
{
    let mut d = Decoder::new(&request.payload);
    if capabilities::version(&mut d)? != SOURCE_HEAD_VERSION {
        return Err(RecordSessionError::Protocol);
    }
    let supported = CapabilitySupport::new(d.u64()? & KNOWN_READER_CAPABILITIES, 0)
        .map_err(|_| RecordSessionError::Protocol)?;
    let slots = [Slot::decode(&mut d)?, Slot::decode(&mut d)?];
    d.finish()?;
    if !slots[0].matches_bound(scopes[0], 1) || !slots[1].matches_bound(scopes[1], 2) {
        return Err(RecordSessionError::Protocol);
    }
    let first = observe(scopes[0], supported, policy, factory, read_head);
    let second = observe(scopes[1], supported, policy, factory, read_head);
    // No source handle survives. Recheck both scopes before encoding the
    // shared frame; neither may suppress its neighbor's independent outcome.
    let outcomes = [
        access(policy, scopes[0]).and(first),
        access(policy, scopes[1]).and(second),
    ];
    let mut payload = Vec::new();
    for (slot, outcome) in slots.iter().zip(outcomes) {
        slot.encode(&mut payload);
        put_outcome(&mut payload, outcome);
    }
    write_packet(
        stream,
        &Packet {
            kind: HEAD_BATCH | REPLY,
            status: 0,
            meta: request.meta,
            total: payload.len(),
            offset: 0,
            payload,
        },
        limit,
    )?;
    stream.flush()?;
    Ok(())
}

fn decode_one(
    d: &mut Decoder<'_>,
    expected: &Slot,
    support: CapabilitySupport,
) -> Result<PairHeadObservation, RecordSessionError> {
    if Slot::decode(d)? != *expected {
        return Err(RecordSessionError::Protocol);
    }
    let outcome = match d.byte()? {
        0 => {
            let head = capabilities::metadata(d, expected.checkpoint.scope().clone())?;
            validate_capabilities(&head, support).map_err(|_| RecordSessionError::Protocol)?;
            Ok(head)
        }
        code @ 1..=6 => Err(decode_source_error(code)),
        8 => {
            let len = usize::from(d.byte()?);
            Err(SourceError::Incompatible(
                capabilities::decode_incompatible(d.take(len)?)?,
            ))
        }
        _ => return Err(RecordSessionError::Protocol),
    };
    Ok(PairHeadObservation {
        checkpoint: expected.checkpoint.clone(),
        outcome,
    })
}

impl<S: Read + Write> ScopePairHeadsSession<S> {
    /// Read two independently authorized capability heads in one bounded RPC.
    /// Checkpoints must belong to the exact primary and secondary bindings in
    /// that order. Both bindings must still be locally active. No retry or
    /// legacy fallback occurs, and no checkpoint is loaded or advanced here.
    pub fn heads(
        &self,
        primary: &Checkpoint,
        secondary: &Checkpoint,
    ) -> Result<[PairHeadObservation; 2], RecordSessionError> {
        if !self.batched {
            return Err(SourceError::InvalidRequest.into());
        }
        let mut c = active_client(&self.inner.shared, &self.inner.closing)?;
        let ids = [
            c.identity.clone(),
            self.secondary.as_ref().map_err(Clone::clone)?.clone(),
        ];
        for (identity, checkpoint) in ids.iter().zip([primary, secondary]) {
            if identity.scope() != checkpoint.scope() {
                return Err(SourceError::IdentityChanged.into());
            }
            c.owner.check_scope(identity).map_err(|error| match error {
                super::super::super::LifecycleError::StaleIdentity => SourceError::IdentityChanged,
                _ => SourceError::Unavailable,
            })?;
        }
        let slots = [
            Slot::for_id(&ids[0], primary),
            Slot::for_id(&ids[1], secondary),
        ];
        let mut request = capabilities::prefix(self.support);
        for slot in &slots {
            slot.encode(&mut request);
        }
        let max_reply = slots.iter().map(Slot::max_reply).sum();
        c.wire.counters.head_checks += 2;
        let result = c
            .wire
            .exchange(HEAD_BATCH, &ids[0], request, max_reply)
            .and_then(|body| {
                let mut d = Decoder::new(&body);
                let rows = [
                    decode_one(&mut d, &slots[0], self.support)?,
                    decode_one(&mut d, &slots[1], self.support)?,
                ];
                d.finish()?;
                Ok(rows)
            });
        if let Ok(rows) = &result {
            for (id, row) in ids.iter().zip(rows) {
                if row.outcome == Err(SourceError::IdentityChanged) {
                    c.owner
                        .invalidate_scope(id)
                        .map_err(|_| RecordSessionError::Protocol)?;
                }
            }
        } else {
            // Only an entirely correlated batch yields either result.
            c.wire.close();
        }
        c.observe_close();
        let terminal = c.wire.io.is_none();
        drop(c);
        if terminal {
            self.inner.close()?;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::{Id, MAX_ID_BYTES};

    #[test]
    fn largest_exact_pair_request_and_response_fit_one_minimum_frame() {
        let id = |c: &str| Id::new(c.repeat(MAX_ID_BYTES)).unwrap();
        let exact = |stream| Scope::new(id("r"), id("o"), id(stream), id("i"), id("s"), id("e"));
        let slots = [
            Slot {
                handle: 1,
                generation: 1,
                checkpoint: Checkpoint::new(exact("a"), 1),
            },
            Slot {
                handle: 2,
                generation: 2,
                checkpoint: Checkpoint::new(exact("b"), 2),
            },
        ];
        let request_len = capabilities::prefix(CapabilitySupport::current()).len()
            + slots
                .iter()
                .map(|s| s.max_reply() - 1 - capabilities::META_BYTES)
                .sum::<usize>();
        let response_len = slots.iter().map(Slot::max_reply).sum::<usize>();
        assert!(request_len + HEADER <= MIN_CONTROL_BYTES);
        assert!(response_len + HEADER <= MIN_CONTROL_BYTES);
    }
    #[test]
    fn a_different_checkpoint_echo_is_not_a_head_observation() {
        let id = |value| Id::new(value).unwrap();
        let scope = Scope::new(id("r"), id("o"), id("t"), id("i"), id("s"), id("e"));
        let expected = Slot {
            handle: 1,
            generation: 1,
            checkpoint: Checkpoint::new(scope.clone(), 42),
        };
        let echoed = Slot {
            checkpoint: Checkpoint::new(scope.clone(), 41),
            ..expected
        };
        let mut body = Vec::new();
        echoed.encode(&mut body);
        put_outcome(&mut body, Ok(SourceHead::new(scope, 42, 0, 0)));
        assert!(matches!(
            decode_one(
                &mut Decoder::new(&body),
                &expected,
                CapabilitySupport::current()
            ),
            Err(RecordSessionError::Protocol)
        ));
    }
}
