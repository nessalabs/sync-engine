//! One explicit protected version over the verified serialized TLS channel.
//! Neither decrypted numbers nor ordinary record operations are authority.

use std::time::Instant;

use rustls::pki_types::ServerName;

use super::*;
use crate::replication::domain::{
    validate_capabilities, validate_page, CapabilitySupport, Checkpoint, CommitPlan, SourceHead,
    ValidationError,
};
use crate::replication::infrastructure::session::{
    TcpSessionIo, TlsIdentity, TlsPeer, TlsSessionIo,
};
use crate::replication::infrastructure::{
    EpochSourceError, OriginClaimError, SqliteEpochRecordSource, SqliteOriginAuthority,
};
use crate::replication::origin_epoch::{
    AdmittedEpochRead, NativeEpochAdmission, OriginAuthorityKey, OriginEpoch,
};

const OFFER: &[u8; 8] = b"NSEPOCH1";
const ACCEPT: &[u8; 8] = b"NSEACK01";

/// Refusal at the standalone protected session boundary.
#[derive(Debug)]
pub enum EpochSessionError {
    /// No trusted matching ledger claim was available.
    Authority(OriginClaimError),
    /// The configured current claim, authenticated channel or reply stamp did not agree.
    WrongClaim,
    /// The exact page failed canonical record/checkpoint validation.
    Validation(ValidationError),
    /// The existing bounded TLS/record channel or scope operation refused.
    Record(RecordSessionError),
}

impl From<RecordSessionError> for EpochSessionError {
    fn from(error: RecordSessionError) -> Self {
        Self::Record(error)
    }
}
impl From<io::Error> for EpochSessionError {
    fn from(error: io::Error) -> Self {
        RecordSessionError::Io(error).into()
    }
}

struct ExpectedClaim {
    scope: Scope,
    key: OriginAuthorityKey,
    epoch: OriginEpoch,
    ledger: SqliteOriginAuthority,
}

impl ExpectedClaim {
    fn current(&self) -> Result<(), EpochSessionError> {
        if self
            .ledger
            .current(&self.key)
            .map_err(EpochSessionError::Authority)?
            != Some(self.epoch)
        {
            return Err(EpochSessionError::WrongClaim);
        }
        Ok(())
    }

    fn check_stamp(&self, decoder: &mut Decoder<'_>) -> Result<(), EpochSessionError> {
        if decoder.id().map_err(RecordSessionError::from)? != *self.key.id()
            || decoder.u64().map_err(RecordSessionError::from)? != self.epoch.get()
        {
            return Err(EpochSessionError::WrongClaim);
        }
        self.current()
    }

    fn stamp_bytes(&self) -> usize {
        1 + self.key.id().as_str().len() + 8
    }
}

/// Immutable standalone host configuration for one exact source claim.
///
/// Trusted deployment must independently associate this origin/key, confirmed
/// ledger epoch and exact writer leaf, outside the source backup. Never build
/// this input from a locator, peer reply, restored source data or unauthenticated
/// certificate. A local ledger mismatch fails closed; this type does not
/// issue credentials or establish a cross-host assignment.
pub struct EpochSessionBinding {
    claim: ExpectedClaim,
    peer: TlsPeer,
}

impl EpochSessionBinding {
    /// Bind an independently host-approved scope, current claim and TLS leaf.
    pub fn new(
        scope: Scope,
        key: OriginAuthorityKey,
        epoch: OriginEpoch,
        ledger: SqliteOriginAuthority,
        peer: TlsPeer,
    ) -> Self {
        Self {
            claim: ExpectedClaim {
                scope,
                key,
                epoch,
                ledger,
            },
            peer,
        }
    }
}

/// Head evidence made only by a verified exact protected session result.
pub struct VerifiedEpochHead(AdmittedEpochRead<SourceHead>);

/// Page evidence made only by a verified exact protected session result.
pub struct VerifiedEpochPage(AdmittedEpochRead<CommitPlan>);

/// Device-side current scope policy for applying admitted protected results.
///
/// Only actual session evidence is accepted. This policy rechecks exact current
/// scope at apply/observation; the protected store separately rechecks its
/// durable floor before writes, including delayed evidence obtained earlier.
pub struct EpochSessionAdmission<A>(A);

impl<A> EpochSessionAdmission<A> {
    /// Compose the existing exact scope-authorizer port, not a new authority.
    pub fn new(authorizer: A) -> Self {
        Self(authorizer)
    }
}

impl<A: ScopeAuthorizer> EpochSessionAdmission<A> {
    fn admit<T>(&mut self, admitted: AdmittedEpochRead<T>) -> Option<AdmittedEpochRead<T>> {
        let (key, scope, epoch, value) = admitted.into_parts();
        matches!(self.0.authorize(&scope), Access::Allowed(current) if current == scope)
            .then(|| AdmittedEpochRead::new(key, scope, epoch, value))
    }
}

impl<A: ScopeAuthorizer> NativeEpochAdmission for EpochSessionAdmission<A> {
    type HeadEvidence = VerifiedEpochHead;
    type PlanEvidence = VerifiedEpochPage;

    fn admit_head(&mut self, evidence: VerifiedEpochHead) -> Option<AdmittedEpochRead<SourceHead>> {
        self.admit(evidence.0)
    }

    fn admit_plan(&mut self, evidence: VerifiedEpochPage) -> Option<AdmittedEpochRead<CommitPlan>> {
        self.admit(evidence.0)
    }
}

/// One authenticated serialized protected record session. It exposes only
/// claim-qualified operations, not a legacy `RecordSource` or bare parts.
pub struct EpochRecordSession {
    inner: RecordSession<TlsSessionIo>,
    claim: ExpectedClaim,
}

impl EpochRecordSession {
    /// Establish the existing exact-leaf TLS policy from the host binding,
    /// then require the single protected app version. Never try legacy fallback.
    pub fn connect(
        tcp: TcpSessionIo,
        expected_name: ServerName<'static>,
        local: TlsIdentity,
        binding: EpochSessionBinding,
        offered: SessionLimits,
        deadline: Instant,
    ) -> Result<Self, EpochSessionError> {
        let EpochSessionBinding { claim, peer } = binding;
        claim.current()?;
        let mut stream = TlsSessionIo::connect(tcp, expected_name, local, peer, deadline)?;
        stream.write_all(OFFER)?;
        stream.flush()?;
        let mut accepted = [0; 8];
        stream.read_exact(&mut accepted)?;
        if &accepted != ACCEPT {
            return Err(RecordSessionError::Protocol.into());
        }
        let inner = RecordSession::connect_interruptible(stream, claim.scope.clone(), offered)?;
        {
            let mut client = active_client(&inner.shared, &inner.closing)?;
            client.wire.counters.protocol_bytes += (OFFER.len() + ACCEPT.len()) as u64;
            client.wire.counters.exchanges += 1;
        }
        claim.current()?;
        Ok(Self { inner, claim })
    }

    fn use_client<T>(
        &self,
        f: impl FnOnce(&mut Client<TlsSessionIo>, &ExpectedClaim) -> Result<T, EpochSessionError>,
    ) -> Result<T, EpochSessionError> {
        let mut client = active_client(&self.inner.shared, &self.inner.closing)?;
        let result = self
            .claim
            .current()
            .and_then(|()| f(&mut client, &self.claim));
        if result.is_err() {
            client.wire.close();
        }
        client.observe_close();
        drop(client);
        if result.is_err() {
            // Wire shutdown alone does not drop the retained interrupt
            // descriptor. Preserve the refusal and request physical close.
            let _ = self.inner.close();
        }
        result
    }

    /// Read a verified compatible stamped head. The caller must submit it to
    /// the protected receiver before subsequent same-epoch payload can apply.
    pub fn head(&self) -> Result<VerifiedEpochHead, EpochSessionError> {
        self.use_client(|client, claim| {
            let Client { identity, wire, .. } = client;
            wire.counters.head_checks += 1;
            let support = CapabilitySupport::current();
            let response = wire.exchange(
                capabilities::HEAD_CAP,
                identity,
                capabilities::prefix(support),
                claim.stamp_bytes() + scope_bytes(&claim.scope) + capabilities::META_BYTES,
            )?;
            let mut decoder = Decoder::new(&response);
            claim.check_stamp(&mut decoder)?;
            let scope = decoder.scope().map_err(RecordSessionError::from)?;
            if scope != claim.scope {
                return Err(RecordSessionError::Protocol.into());
            }
            let head = capabilities::metadata(&mut decoder, scope)?;
            decoder.finish().map_err(RecordSessionError::from)?;
            validate_capabilities(&head, support)
                .map_err(|e| RecordSessionError::Source(SourceError::Incompatible(e)))?;
            Ok(VerifiedEpochHead(AdmittedEpochRead::new(
                claim.key.clone(),
                claim.scope.clone(),
                claim.epoch,
                head,
            )))
        })
    }

    /// Read and canonically validate a stamped page against caller expectations.
    /// Exact scope, range, maximum records and bytes must match the actual echo.
    pub fn page(
        &self,
        checkpoint: &Checkpoint,
        request: &PageRequest,
        limits: Limits,
    ) -> Result<VerifiedEpochPage, EpochSessionError> {
        self.use_client(|client, claim| {
            client
                .scope(&request.scope)
                .map_err(RecordSessionError::from)?;
            validate_page_request(request, limits).map_err(EpochSessionError::Validation)?;
            let Client { identity, wire, .. } = client;
            validate_page_request(request, physical(wire.record_bytes))
                .map_err(EpochSessionError::Validation)?;
            if checkpoint.scope() != &request.scope || checkpoint.position() != request.after {
                return Err(EpochSessionError::Validation(ValidationError::InvalidRange));
            }
            let mut offer = capabilities::prefix(CapabilitySupport::current());
            page_request(&mut offer, request);
            let response = wire.exchange(
                capabilities::PAGE_CAP,
                identity,
                offer,
                claim
                    .stamp_bytes()
                    .saturating_add(page_bound(
                        &request.scope,
                        request.max_records,
                        request.max_payload_bytes,
                    ))
                    .min(MAX_FRAME_BYTES),
            )?;
            let mut decoder = Decoder::new(&response);
            claim.check_stamp(&mut decoder)?;
            let body_len = response
                .len()
                .checked_sub(claim.stamp_bytes())
                .ok_or(RecordSessionError::Protocol)?;
            let bytes = decoder.take(body_len).map_err(RecordSessionError::from)?;
            decoder.finish().map_err(RecordSessionError::from)?;
            let page = decode_page(bytes, request)?;
            let plan = validate_page(checkpoint, request, page, limits)
                .map_err(EpochSessionError::Validation)?;
            client.received(plan.records());
            Ok(VerifiedEpochPage(AdmittedEpochRead::new(
                claim.key.clone(),
                claim.scope.clone(),
                claim.epoch,
                plan,
            )))
        })
    }

    /// Application protocol counters, including protected preface, excluding TLS bytes.
    pub fn counters(&self) -> Result<WireCounters, EpochSessionError> {
        Ok(self.inner.counters()?)
    }

    /// Close the existing independently interruptible secure connection.
    pub fn close(&self) -> Result<(), EpochSessionError> {
        Ok(self.inner.close()?)
    }
}

fn page_request(out: &mut Vec<u8>, request: &PageRequest) {
    for n in [
        request.after,
        request.target,
        request.max_records as u64,
        request.max_payload_bytes as u64,
        request.max_record_bytes as u64,
    ] {
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn encode_stamp(out: &mut Vec<u8>, key: &OriginAuthorityKey, epoch: OriginEpoch) {
    let bytes = key.id().as_str().as_bytes();
    out.push(bytes.len() as u8);
    out.extend_from_slice(bytes);
    out.extend_from_slice(&epoch.get().to_be_bytes());
}

fn source_failure(error: EpochSourceError) -> RecordSessionError {
    match error {
        EpochSourceError::Authority(OriginClaimError::Stale { .. }) => SourceError::IdentityChanged,
        EpochSourceError::Authority(_) => SourceError::Unavailable,
        EpochSourceError::Source(error) => error,
        _ => SourceError::InvalidRequest,
    }
    .into()
}

fn read_protected(
    packet: &Packet,
    scope: &Scope,
    source: &mut SqliteEpochRecordSource,
) -> Result<Vec<u8>, RecordSessionError> {
    let mut decoder = Decoder::new(&packet.payload);
    let support = CapabilitySupport::current();
    let prefix = capabilities::prefix(support);
    if decoder.take(prefix.len())? != prefix {
        return Err(RecordSessionError::Protocol);
    }
    let mut output = Vec::new();
    if packet.kind == capabilities::HEAD_CAP {
        decoder.finish()?;
        let (key, epoch, head) = source
            .capture_head(scope)
            .map_err(source_failure)?
            .into_parts();
        if head.scope() != scope {
            return Err(RecordSessionError::Protocol);
        }
        validate_capabilities(&head, support).map_err(SourceError::Incompatible)?;
        encode_stamp(&mut output, &key, epoch);
        put_scope(&mut output, head.scope());
        capabilities::put_metadata(&mut output, &head);
    } else if packet.kind == capabilities::PAGE_CAP {
        let request = PageRequest {
            scope: scope.clone(),
            after: decoder.u64()?,
            target: decoder.u64()?,
            max_records: capabilities::number(&mut decoder)?,
            max_payload_bytes: capabilities::number(&mut decoder)?,
            max_record_bytes: capabilities::number(&mut decoder)?,
        };
        decoder.finish()?;
        validate_page_request(&request, physical(MAX_RECORD_BYTES))
            .map_err(|_| SourceError::InvalidRequest)?;
        let (key, epoch, page) = source
            .capture_page(&request)
            .map_err(source_failure)?
            .into_wire();
        encode_stamp(&mut output, &key, epoch);
        output.extend_from_slice(&encode_record_page(&request, page)?);
    } else {
        return Err(RecordSessionError::Protocol);
    }
    Ok(output)
}

fn write_protected_binding(
    stream: &mut TlsSessionIo,
    session: u64,
    limit: usize,
    scope: &Scope,
    policy: &mut impl ScopeAuthorizer,
    source: &mut SqliteEpochRecordSource,
) -> Result<bool, RecordSessionError> {
    let ControlFrame::Bind {
        correlation,
        scope: requested,
    } = read_control(stream, limit)?
    else {
        return Err(RecordSessionError::Protocol);
    };
    if correlation
        != (Correlation {
            session,
            request: FIRST_REQUEST,
        })
        || requested != *scope
    {
        return Err(RecordSessionError::Protocol);
    }
    let result = (|| {
        capabilities::policy(policy, scope)?;
        let (_, _, head) = source
            .capture_head(scope)
            .map_err(source_failure)?
            .into_parts();
        capabilities::policy(policy, scope)?;
        Ok(head.position())
    })();
    let (bound, frame) = match result {
        Ok(head) => (
            true,
            ControlFrame::Bound {
                correlation,
                // This advisory v1 head is not a protected epoch observation.
                binding: BindingMetadata {
                    handle: 1,
                    generation: 1,
                    scope: scope.clone(),
                    head,
                },
            },
        ),
        Err(error) => (
            false,
            ControlFrame::Refused {
                correlation,
                reason: source_error(error),
            },
        ),
    };
    write_control(stream, &frame, limit)?;
    stream.flush()?;
    Ok(bound)
}

/// Serve only the selected protected version over an already verified TLS
/// channel. The host selects the exact scope and current client policy from
/// that authenticated peer, and supplies the guarded source's actual claim.
/// No legacy head, page, fallback, listener, key issuance or retry is exposed.
pub fn serve_epoch_record_session(
    stream: &mut TlsSessionIo,
    session: u64,
    supported: SessionLimits,
    scope: &Scope,
    policy: &mut impl ScopeAuthorizer,
    source: &mut SqliteEpochRecordSource,
) -> Result<(), RecordSessionError> {
    supported
        .validate()
        .map_err(|_| RecordSessionError::Protocol)?;
    if session == 0 {
        return Err(RecordSessionError::Protocol);
    }
    let mut offer = [0; 8];
    stream.read_exact(&mut offer)?;
    if &offer != OFFER {
        return Err(RecordSessionError::Protocol);
    }
    stream.write_all(ACCEPT)?;
    stream.flush()?;
    let ControlFrame::Hello(offered) = read_control(stream, supported.control_bytes as usize)?
    else {
        return Err(RecordSessionError::Protocol);
    };
    let selected = SessionLimits {
        control_bytes: offered.control_bytes.min(supported.control_bytes),
        bindings: 1,
    };
    let limit = selected.control_bytes as usize;
    write_control(
        stream,
        &ControlFrame::Welcome {
            session,
            limits: selected,
            record_bytes: MAX_RECORD_BYTES as u32,
        },
        limit,
    )?;
    stream.flush()?;
    if !write_protected_binding(stream, session, limit, scope, policy, source)? {
        return Ok(());
    }
    let mut last = FIRST_REQUEST;
    while let Some(packet) = read_packet(stream, limit)? {
        if !matches!(packet.kind, capabilities::HEAD_CAP | capabilities::PAGE_CAP)
            || packet.status != 0
            || packet.offset != 0
            || packet.total != packet.payload.len()
            || (
                packet.meta.session,
                packet.meta.handle,
                packet.meta.generation,
            ) != (session, 1, 1)
            || last.checked_add(1) != Some(packet.meta.request)
        {
            return Err(RecordSessionError::Protocol);
        }
        last = packet.meta.request;
        let result = capabilities::policy(policy, scope)
            .and_then(|()| read_protected(&packet, scope, source));
        let (status, body) = match result {
            Ok(body) => (0, body),
            Err(RecordSessionError::Source(SourceError::Incompatible(error))) => {
                (8, capabilities::encode_incompatible(error)?)
            }
            Err(RecordSessionError::Source(error)) => (source_error_code(error), Vec::new()),
            Err(error) => return Err(error),
        };
        respond(stream, &packet, status, &body, scope, policy, limit)?;
    }
    Ok(())
}
