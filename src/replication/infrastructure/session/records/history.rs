//! Capability-aware history frames. The session owns correlation and fragment
//! admission; the existing history application/store owns installation.
use super::capabilities::{metadata, number, policy, prefix, put_metadata, version};
use super::*;
use crate::replication::domain::{
    validate_capabilities, CapabilityError, CapabilitySupport, SourceHead,
    KNOWN_READER_CAPABILITIES, SOURCE_HEAD_VERSION,
};
use crate::replication::history::{
    validate_older, validate_tail, CapabilityHistorySource, HistorySourceError, OlderPage,
    OlderRequest, TailRequest, TailSnapshot,
};
use crate::replication::infrastructure::loopback::put_wire_record;

const TAIL: u8 = 8;
const OLDER: u8 = 9;
pub(super) fn operation(kind: u8) -> bool {
    matches!(kind, TAIL | OLDER)
}

type ReadTail<R> = fn(
    &mut R,
    &TailRequest,
    CapabilitySupport,
) -> Result<(SourceHead, TailSnapshot), HistorySourceError>;
type ReadOlder<R> = fn(
    &mut R,
    &OlderRequest,
    CapabilitySupport,
) -> Result<(SourceHead, OlderPage), HistorySourceError>;

pub(super) struct ServerReads<R> {
    tail: ReadTail<R>,
    older: ReadOlder<R>,
}

impl<R: CapabilityHistorySource> ServerReads<R> {
    pub(super) fn new() -> Self {
        Self {
            tail: R::tail_with_capabilities,
            older: R::older_with_capabilities,
        }
    }
}

pub(super) fn history_source(error: SourceError) -> HistorySourceError {
    match error {
        SourceError::Incompatible(e) => HistorySourceError::Incompatible(e),
        SourceError::Denied => HistorySourceError::Denied,
        SourceError::Unavailable => HistorySourceError::Unavailable,
        SourceError::Pruned => HistorySourceError::ResetRequired,
        SourceError::InvalidRequest => HistorySourceError::InvalidRequest,
        SourceError::IdentityChanged => HistorySourceError::IdentityChanged,
        SourceError::OversizedRecord => HistorySourceError::OversizedRecord,
    }
}

fn record_source(error: HistorySourceError) -> RecordSessionError {
    RecordSessionError::Source(match error {
        HistorySourceError::Incompatible(e) => SourceError::Incompatible(e),
        HistorySourceError::Denied => SourceError::Denied,
        HistorySourceError::Unavailable => SourceError::Unavailable,
        HistorySourceError::ResetRequired => SourceError::Pruned,
        HistorySourceError::InvalidRequest => SourceError::InvalidRequest,
        HistorySourceError::IdentityChanged => SourceError::IdentityChanged,
        HistorySourceError::OversizedRecord => SourceError::OversizedRecord,
    })
}

#[derive(PartialEq, Eq)]
struct Request {
    common: TailRequest,
    before: Option<u64>,
}
impl Request {
    fn older(&self, before: u64) -> OlderRequest {
        OlderRequest {
            scope: self.common.scope.clone(),
            generation: self.common.generation,
            before,
            max_records: self.common.max_records,
            max_payload_bytes: self.common.max_payload_bytes,
            max_record_bytes: self.common.max_record_bytes,
        }
    }
    fn from_older(r: &OlderRequest) -> Self {
        Self {
            common: TailRequest {
                scope: r.scope.clone(),
                generation: r.generation,
                max_records: r.max_records,
                max_payload_bytes: r.max_payload_bytes,
                max_record_bytes: r.max_record_bytes,
            },
            before: Some(r.before),
        }
    }
    fn kind(&self) -> u8 {
        if self.before.is_some() {
            OLDER
        } else {
            TAIL
        }
    }
    fn numbers(&self, out: &mut Vec<u8>) {
        for value in [
            self.common.generation,
            self.common.max_records as u64,
            self.common.max_payload_bytes as u64,
            self.common.max_record_bytes as u64,
        ] {
            out.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(before) = self.before {
            out.extend_from_slice(&before.to_be_bytes());
        }
    }
    fn decode(kind: u8, scope: Scope, d: &mut Decoder<'_>) -> Result<Self, RecordSessionError> {
        let common = TailRequest {
            scope,
            generation: d.u64()?,
            max_records: number(d)?,
            max_payload_bytes: number(d)?,
            max_record_bytes: number(d)?,
        };
        let before = (kind == OLDER).then(|| d.u64()).transpose()?;
        Ok(Self { common, before })
    }
    fn response_bound(&self) -> usize {
        // Both forms have the same overhead: older's extra request field
        // takes the space of the tail's first-position field.
        const ECHO_AND_METADATA: usize = 6 * 8 + 26 + 1;
        let scope = scope_bytes(&self.common.scope);
        let per_record = 13 + crate::replication::domain::MAX_ID_BYTES + scope;
        (scope
            + ECHO_AND_METADATA
            + self.common.max_records * per_record
            + self.common.max_payload_bytes)
            .min(MAX_FRAME_BYTES)
    }

    fn validate(&self, record_bytes: usize) -> Result<(), RecordSessionError> {
        if self.common.generation == 0 || self.before.is_some_and(|n| n <= 1) {
            return Err(SourceError::InvalidRequest.into());
        }
        validate_open_page_request(
            &OpenPageRequest {
                scope: self.common.scope.clone(),
                after: 0,
                max_records: self.common.max_records,
                max_payload_bytes: self.common.max_payload_bytes,
                max_record_bytes: self.common.max_record_bytes,
            },
            physical(record_bytes),
        )
        .map_err(|_| SourceError::InvalidRequest.into())
    }
}

enum Response {
    Tail(TailSnapshot),
    Older(OlderPage),
}

impl Response {
    fn records(&self) -> &[Record] {
        match self {
            Self::Tail(r) => &r.records,
            Self::Older(r) => &r.records,
        }
    }
    fn validate(
        self,
        request: &Request,
        head: &SourceHead,
        bounds: Limits,
    ) -> Result<Self, RecordSessionError> {
        if head.scope() != &request.common.scope {
            return Err(RecordSessionError::Protocol);
        }
        match (self, request.before) {
            (Self::Tail(r), None) if r.watermark == head.position() => {
                validate_tail(&request.common, r, bounds)
                    .map(|plan| Self::Tail(plan.into_snapshot()))
                    .map_err(|_| RecordSessionError::Protocol)
            }
            (Self::Older(r), Some(before)) if before.saturating_sub(1) <= head.position() => {
                validate_older(&request.older(before), r, bounds)
                    .map(|plan| Self::Older(plan.into_page()))
                    .map_err(|_| RecordSessionError::Protocol)
            }
            _ => Err(RecordSessionError::Protocol),
        }
    }
}

pub(super) fn server_response<A, F, R>(
    packet: &Packet,
    scope: &Scope,
    authorizer: &mut A,
    factory: &mut F,
    reads: Option<&ServerReads<R>>,
) -> Result<Vec<u8>, RecordSessionError>
where
    A: ScopeAuthorizer,
    F: FnMut(&Scope) -> Result<R, SourceError>,
{
    let reads = reads.ok_or(SourceError::InvalidRequest)?;
    let mut d = Decoder::new(&packet.payload);
    let offered = version(&mut d).map_err(source_error)?;
    if offered != SOURCE_HEAD_VERSION {
        return Err(SourceError::Incompatible(CapabilityError::UnsupportedVersion(offered)).into());
    }
    let offered = d.u64().map_err(|_| SourceError::InvalidRequest)?;
    let support = CapabilitySupport::new(offered & KNOWN_READER_CAPABILITIES, 0)
        .map_err(SourceError::Incompatible)?;
    let r = Request::decode(packet.kind, scope.clone(), &mut d).map_err(source_error)?;
    d.finish().map_err(|_| SourceError::InvalidRequest)?;
    r.validate(MAX_RECORD_BYTES)?;
    policy(authorizer, scope)?;
    let (head, response) = {
        let mut source = factory(scope)?;
        policy(authorizer, scope)?;
        if let Some(before) = r.before {
            let (head, page) =
                (reads.older)(&mut source, &r.older(before), support).map_err(record_source)?;
            (head, Response::Older(page))
        } else {
            let (head, tail) =
                (reads.tail)(&mut source, &r.common, support).map_err(record_source)?;
            (head, Response::Tail(tail))
        }
    };
    validate_capabilities(&head, support).map_err(SourceError::Incompatible)?;
    let response = response.validate(&r, &head, physical(MAX_RECORD_BYTES))?;
    let mut out = Vec::new();
    put_scope(&mut out, scope);
    r.numbers(&mut out);
    put_metadata(&mut out, &head);
    match &response {
        Response::Tail(tail) => {
            out.extend_from_slice(&tail.first.to_be_bytes());
            out.extend_from_slice(&tail.oldest_available.to_be_bytes());
        }
        Response::Older(page) => out.extend_from_slice(&page.oldest_available.to_be_bytes()),
    }
    out.push(response.records().len() as u8);
    for record in response.records() {
        put_wire_record(&mut out, record);
    }
    Ok(out)
}

fn decode_response(
    body: &[u8],
    r: &Request,
    support: CapabilitySupport,
    bounds: Limits,
) -> Result<(SourceHead, Response), RecordSessionError> {
    let mut d = Decoder::new(body);
    let scope = d.scope()?;
    let echo = Request::decode(r.kind(), scope.clone(), &mut d)?;
    if echo != *r {
        return Err(RecordSessionError::Protocol);
    }
    let head = metadata(&mut d, scope)?;
    validate_capabilities(&head, support).map_err(SourceError::Incompatible)?;
    let first = r.before.is_none().then(|| d.u64()).transpose()?;
    let floor = d.u64()?;
    let count = usize::from(d.byte()?);
    if count > r.common.max_records {
        return Err(RecordSessionError::TooLarge);
    }
    let mut records = Vec::with_capacity(count);
    let mut remainder = r.common.max_payload_bytes;
    for _ in 0..count {
        let record = d.wire_record(r.common.max_record_bytes.min(remainder))?;
        remainder -= record.payload.len();
        records.push(record);
    }
    d.finish()?;
    let response = if let Some(first) = first {
        Response::Tail(TailSnapshot {
            request: echo.common,
            watermark: head.position(),
            first,
            oldest_available: floor,
            records,
        })
    } else {
        Response::Older(OlderPage {
            request: echo.older(r.before.ok_or(RecordSessionError::Protocol)?),
            oldest_available: floor,
            records,
        })
    };
    Ok((head.clone(), response.validate(r, &head, bounds)?))
}

pub(super) struct HistorySessionSource<S> {
    pub(super) inner: RecordSessionSource<S>,
}

impl<S: Read + Write> HistorySessionSource<S> {
    fn read(
        &mut self,
        r: Request,
        support: CapabilitySupport,
    ) -> Result<(SourceHead, Response), HistorySourceError> {
        let mut c = active_client(&self.inner.shared, &self.inner.closing)
            .map_err(|_| HistorySourceError::Unavailable)?;
        c.scope(&r.common.scope).map_err(history_source)?;
        let record_bytes = c.wire.record_bytes;
        r.validate(record_bytes)
            .map_err(|e| history_source(source_error(e)))?;
        let mut payload = prefix(support);
        r.numbers(&mut payload);
        let expected_body = r.response_bound();
        let result = {
            let Client { identity, wire, .. } = &mut *c;
            wire.exchange(r.kind(), identity, payload, expected_body)
                .and_then(|body| decode_response(&body, &r, support, physical(record_bytes)))
        };
        let result = match result {
            Ok((head, response)) => {
                // Backward pages are not repeated forward frames and must not
                // move the forward duplicate counter's observed high water.
                c.count_payload(response.records().iter().map(|r| r.payload.len()).sum());
                Ok((head, response))
            }
            Err(e) => {
                if matches!(
                    &e,
                    RecordSessionError::Protocol | RecordSessionError::TooLarge
                ) {
                    c.wire.close();
                }
                Err(history_source(source_error(e)))
            }
        };
        c.observe_close();
        result
    }

    pub(super) fn tail(
        &mut self,
        r: &TailRequest,
        support: CapabilitySupport,
    ) -> Result<(SourceHead, TailSnapshot), HistorySourceError> {
        let (head, response) = self.read(
            Request {
                common: r.clone(),
                before: None,
            },
            support,
        )?;
        match response {
            Response::Tail(tail) => Ok((head, tail)),
            _ => Err(HistorySourceError::InvalidRequest),
        }
    }

    pub(super) fn older(
        &mut self,
        r: &OlderRequest,
        support: CapabilitySupport,
    ) -> Result<(SourceHead, OlderPage), HistorySourceError> {
        let (head, response) = self.read(Request::from_older(r), support)?;
        match response {
            Response::Older(page) => Ok((head, page)),
            _ => Err(HistorySourceError::InvalidRequest),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;

    fn request(before: Option<u64>) -> Request {
        let id = |v| Id::new(v).unwrap();
        Request {
            common: TailRequest {
                scope: Scope::new(id("r"), id("o"), id("s"), id("i"), id("k"), id("e")),
                generation: 5,
                max_records: 1,
                max_payload_bytes: 8,
                max_record_bytes: 8,
            },
            before,
        }
    }
    fn body(r: &Request, required: u64, record: bool) -> Vec<u8> {
        let mut out = Vec::new();
        put_scope(&mut out, &r.common.scope);
        r.numbers(&mut out);
        put_metadata(
            &mut out,
            &SourceHead::new(r.common.scope.clone(), 2, required, 0),
        );
        if r.before.is_none() {
            out.extend_from_slice(&2u64.to_be_bytes());
        }
        out.extend_from_slice(&1u64.to_be_bytes());
        out.push(1);
        if record {
            put_wire_record(
                &mut out,
                &Record {
                    scope: r.common.scope.clone(),
                    position: r.before.map_or(2, |n| n - 1),
                    id: Id::new("fact").unwrap(),
                    payload: b"fact".to_vec(),
                },
            );
        }
        out
    }

    #[test]
    fn exact_history_echo_and_bounds_are_checked_before_payload() {
        for before in [None, Some(2)] {
            let r = request(before);
            let support = CapabilitySupport::current();
            let decode = |bytes: &[u8]| decode_response(bytes, &r, support, physical(8));
            let (_, response) = decode(&body(&r, 0, true)).unwrap();
            assert_eq!(response.records()[0].payload, b"fact");
            let mut stale = request(before);
            stale.common.generation -= 1;
            assert!(matches!(
                decode(&body(&stale, 1, false)),
                Err(RecordSessionError::Protocol)
            ));
            assert!(matches!(
                decode(&body(&r, 1, false)),
                Err(RecordSessionError::Source(SourceError::Incompatible(
                    CapabilityError::MissingReader(1)
                )))
            ));
            let mut extra = body(&r, 0, true);
            extra.push(0);
            assert!(decode(&extra).is_err());
        }
    }
}
