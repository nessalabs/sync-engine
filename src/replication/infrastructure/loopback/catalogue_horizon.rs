//! Explicit catalogue v2 snapshots on the existing one-request development route.
#[cfg(any(feature = "sqlite", test))]
use super::catalogue_capabilities::metadata;
use super::catalogue_capabilities::{invalid, publication, read_metadata, transport, u16_value};
use super::*;
use crate::replication::catalogue::{
    coverage_base, validate_catalogue_horizon, validate_manifest_entry, CatalogueError,
    CatalogueHorizonHead, CatalogueHorizonSource, CatalogueReadKind, HorizonManifestRead,
    HorizonValueRead, CATALOGUE_HORIZON_VERSION,
};
#[cfg(feature = "sqlite")]
use crate::replication::domain::KNOWN_READER_CAPABILITIES;
use crate::replication::domain::{CapabilityError, CapabilitySupport, SOURCE_HEAD_VERSION};
/// Explicit v2 coverage head; never a v1 reinterpretation or fallback.
pub const CATALOGUE_HEAD_HORIZON: u8 = 20;
/// Explicit v2 manifest, including incremental/reset admission kind.
pub const CATALOGUE_MANIFEST_HORIZON: u8 = 21;
/// Explicit v2 owned current-value/deletion read.
pub const CATALOGUE_VALUE_HORIZON: u8 = 22;
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Head(Scope),
    Manifest(ManifestRequest, CatalogueReadKind),
    Value(CataloguePass, Id, usize, CatalogueReadKind),
}
fn kind_byte(k: CatalogueReadKind) -> u8 {
    match k {
        CatalogueReadKind::Incremental => 0,
        CatalogueReadKind::FullReset => 1,
    }
}
fn read_kind(d: &mut Decoder<'_>) -> Result<CatalogueReadKind, FrameError> {
    match d.byte()? {
        0 => Ok(CatalogueReadKind::Incremental),
        1 => Ok(CatalogueReadKind::FullReset),
        _ => Err(FrameError::Malformed),
    }
}
impl Call {
    fn operation(&self) -> u8 {
        match self {
            Self::Head(_) => CATALOGUE_HEAD_HORIZON,
            Self::Manifest(..) => CATALOGUE_MANIFEST_HORIZON,
            Self::Value(..) => CATALOGUE_VALUE_HORIZON,
        }
    }
    fn scope(&self) -> &Scope {
        match self {
            Self::Head(s) => s,
            Self::Manifest(r, _) => &r.pass.scope,
            Self::Value(p, ..) => &p.scope,
        }
    }
    fn pass(&self) -> Option<(&CataloguePass, CatalogueReadKind)> {
        match self {
            Self::Head(_) => None,
            Self::Manifest(r, k) => Some((&r.pass, *k)),
            Self::Value(p, _, _, k) => Some((p, *k)),
        }
    }
    fn validate(&self) -> Result<(), CatalogueSourceError> {
        match self {
            Self::Head(_) => {}
            Self::Manifest(r, _) => validate_manifest_request(r, MAX_CATALOGUE_ENTRIES)
                .map_err(|_| CatalogueSourceError::InvalidRequest)?,
            Self::Value(p, _, budget, _) => {
                validate_catalogue_pass(p).map_err(|_| CatalogueSourceError::InvalidRequest)?;
                if *budget == 0 || *budget > MAX_PAGE_PAYLOAD {
                    return Err(CatalogueSourceError::InvalidRequest);
                }
            }
        }
        if let Some((p, k)) = self.pass() {
            coverage_base(k, p.completed, p.boundary)?;
        }
        Ok(())
    }
    fn put(&self, out: &mut Vec<u8>) {
        match self {
            Self::Head(s) => put_scope(out, s),
            Self::Manifest(r, k) => {
                out.push(kind_byte(*k));
                put_catalogue_pass(out, &r.pass);
                out.extend_from_slice(&(r.max_entries as u64).to_be_bytes());
            }
            Self::Value(p, id, b, k) => {
                out.push(kind_byte(*k));
                put_catalogue_pass(out, p);
                put_id(out, id);
                out.extend_from_slice(&(*b as u64).to_be_bytes());
            }
        }
    }
    fn read(d: &mut Decoder<'_>, op: u8) -> Result<Self, FrameError> {
        Ok(match op {
            CATALOGUE_HEAD_HORIZON => Self::Head(d.scope()?),
            CATALOGUE_MANIFEST_HORIZON => {
                let k = read_kind(d)?;
                let pass = d.catalogue_pass()?;
                let max_entries = usize::try_from(d.u64()?).map_err(|_| FrameError::Malformed)?;
                Self::Manifest(ManifestRequest { pass, max_entries }, k)
            }
            CATALOGUE_VALUE_HORIZON => {
                let k = read_kind(d)?;
                let p = d.catalogue_pass()?;
                let id = d.id()?;
                let b = usize::try_from(d.u64()?).map_err(|_| FrameError::Malformed)?;
                Self::Value(p, id, b, k)
            }
            _ => return Err(FrameError::Malformed),
        })
    }
}
#[cfg(any(feature = "sqlite", test))]
fn header(code: u8, offered: u16, reader: u64, call: &Call) -> Vec<u8> {
    let mut out = vec![code, call.operation()];
    out.extend_from_slice(&offered.to_be_bytes());
    out.extend_from_slice(&reader.to_be_bytes());
    call.put(&mut out);
    out
}
#[cfg(any(feature = "sqlite", test))]
fn put_head(out: &mut Vec<u8>, h: &CatalogueHorizonHead) {
    metadata(out, &h.source);
    out.extend_from_slice(&h.horizon.to_be_bytes());
}
fn read_head(
    d: &mut Decoder<'_>,
    call: &Call,
    supported: CapabilitySupport,
) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
    let n = read_metadata(d).map_err(invalid)?;
    let floor = d.u64().map_err(invalid)?;
    let head = publication(
        n,
        call.scope(),
        supported,
        call.pass().map_or(0, |(p, _)| p.boundary),
    )?;
    let h = CatalogueHorizonHead::new(head, floor);
    check_head(&h, call.scope(), supported)?;
    Ok(h)
}
fn check_head(
    h: &CatalogueHorizonHead,
    s: &Scope,
    support: CapabilitySupport,
) -> Result<(), CatalogueSourceError> {
    validate_catalogue_horizon(h, s, support).map_err(|e| match e {
        CatalogueError::Incompatible(e) => CatalogueSourceError::Incompatible(e),
        CatalogueError::Source(e) => e,
        _ => CatalogueSourceError::InvalidRequest,
    })
}
enum Reply {
    Head(CatalogueHorizonHead),
    Manifest(HorizonManifestRead),
    Value(HorizonValueRead),
}
fn decode(
    body: &[u8],
    call: &Call,
    supported: CapabilitySupport,
) -> Result<Reply, CatalogueSourceError> {
    let mut d = Decoder::new(body);
    let code = d.byte().map_err(invalid)?;
    if body.len() == 1 {
        return Err(match code {
            1 => CatalogueSourceError::Denied,
            9 => CatalogueSourceError::IdentityChanged,
            _ => CatalogueSourceError::InvalidRequest,
        });
    }
    if ![0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11].contains(&code) {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    if d.byte().map_err(invalid)? != call.operation()
        || u16_value(&mut d).map_err(invalid)? != CATALOGUE_HORIZON_VERSION
        || d.u64().map_err(invalid)? != supported.reader_bits()
        || Call::read(&mut d, call.operation()).map_err(invalid)? != *call
    {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    if code == 8 {
        let error = match d.byte().map_err(invalid)? {
            1 => {
                let v = u16_value(&mut d).map_err(invalid)?;
                if v == SOURCE_HEAD_VERSION {
                    return Err(CatalogueSourceError::InvalidRequest);
                }
                CapabilityError::UnsupportedVersion(v)
            }
            2 => {
                let bits = d.u64().map_err(invalid)?;
                if bits == 0 || bits & supported.reader_bits() != 0 {
                    return Err(CatalogueSourceError::InvalidRequest);
                }
                CapabilityError::MissingReader(bits)
            }
            _ => return Err(CatalogueSourceError::InvalidRequest),
        };
        d.finish().map_err(invalid)?;
        return Err(CatalogueSourceError::Incompatible(error));
    }
    if code == 10 {
        let v = u16_value(&mut d).map_err(invalid)?;
        d.finish().map_err(invalid)?;
        return Err(if v == CATALOGUE_HORIZON_VERSION {
            CatalogueSourceError::InvalidRequest
        } else {
            CatalogueSourceError::HorizonVersion(v)
        });
    }
    if code != 0 && code != 11 {
        d.finish().map_err(invalid)?;
        return Err(decode_catalogue_error(code));
    }
    let head = read_head(&mut d, call, supported)?;
    if let Some((p, k)) = call.pass() {
        let point = coverage_base(k, p.completed, p.boundary)?;
        if code == 11 {
            d.finish().map_err(invalid)?;
            if point >= head.horizon {
                return Err(CatalogueSourceError::InvalidRequest);
            }
            return Err(CatalogueSourceError::HorizonExpired {
                completed: p.completed,
                head: Box::new(head),
            });
        }
        if point < head.horizon {
            return Err(CatalogueSourceError::InvalidRequest);
        }
    } else if code == 11 {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    let reply = match call {
        Call::Head(_) => Reply::Head(head),
        Call::Manifest(r, _) => {
            let has_more = match d.byte().map_err(invalid)? {
                0 => false,
                1 => true,
                _ => return Err(CatalogueSourceError::InvalidRequest),
            };
            let count = usize::from(u16_value(&mut d).map_err(invalid)?);
            if count > r.max_entries {
                return Err(CatalogueSourceError::InvalidRequest);
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let e = d.manifest_entry().map_err(invalid)?;
                if e.revision > head.source.position() {
                    return Err(CatalogueSourceError::InvalidRequest);
                }
                entries.push(e)
            }
            let page = ManifestPage {
                request: r.clone(),
                entries,
                has_more,
            };
            validate_manifest(r, &page, MAX_CATALOGUE_ENTRIES)
                .map_err(|_| CatalogueSourceError::InvalidRequest)?;
            Reply::Manifest(HorizonManifestRead { head, page })
        }
        Call::Value(p, id, budget, _) => {
            let descriptor = d.manifest_entry().map_err(invalid)?;
            validate_manifest_entry(&descriptor)
                .map_err(|_| CatalogueSourceError::InvalidRequest)?;
            if descriptor.key.id != *id
                || descriptor.key.creation > p.boundary
                || descriptor.revision > head.source.position()
            {
                return Err(CatalogueSourceError::InvalidRequest);
            }
            let len = u32::from_be_bytes(
                d.take(4)
                    .map_err(invalid)?
                    .try_into()
                    .map_err(|_| CatalogueSourceError::InvalidRequest)?,
            ) as usize;
            if len > *budget || (descriptor.deleted && len != 0) {
                return Err(CatalogueSourceError::InvalidRequest);
            }
            let payload = d.take(len).map_err(invalid)?.to_vec();
            Reply::Value(HorizonValueRead {
                head,
                value: ResolvedEntry {
                    manifest: descriptor,
                    payload,
                },
            })
        }
    };
    d.finish().map_err(invalid)?;
    Ok(reply)
}
/// Opt-in v2 catalogue client on the existing development route. It implements
/// only explicit coverage ports; never retries, resets or falls back to v1.
/// The host owns current authorization, durable progress and reset generation.
/// Plaintext fixture tokens are not production channel authentication.
pub struct HorizonCatalogueClient {
    inner: LoopbackClient,
}
impl HorizonCatalogueClient {
    /// Open no connection yet; select the existing loopback address and credential.
    pub fn new(address: SocketAddrV4, token: Id) -> Result<Self, FrameError> {
        Ok(Self {
            inner: LoopbackClient::new(address, token)?,
        })
    }
    /// Actual complete-exchange accounting, with descriptor/payload separation.
    pub fn counters(&self) -> WireCounters {
        self.inner.counters()
    }
    fn read(
        &mut self,
        call: &Call,
        support: CapabilitySupport,
    ) -> Result<Reply, CatalogueSourceError> {
        call.validate()?;
        let mut out = vec![call.operation()];
        put_id(&mut out, &self.inner.token);
        out.extend_from_slice(&CATALOGUE_HORIZON_VERSION.to_be_bytes());
        out.extend_from_slice(&support.reader_bits().to_be_bytes());
        call.put(&mut out);
        let body = self.inner.exchange(&out).map_err(transport)?;
        let reply = decode(&body, call, support)?;
        match &reply {
            Reply::Head(_) => {}
            Reply::Manifest(r) => {
                let bytes = r
                    .page
                    .entries
                    .iter()
                    .map(|e| e.key.id.as_str().len() as u64 + 17)
                    .sum::<u64>();
                self.inner.counters.catalogue_manifest_bytes += bytes;
                self.inner.counters.protocol_bytes =
                    self.inner.counters.protocol_bytes.saturating_sub(bytes)
            }
            Reply::Value(r) => {
                let bytes = r.value.payload.len() as u64;
                self.inner.counters.catalogue_payload_bytes += bytes;
                self.inner.counters.protocol_bytes =
                    self.inner.counters.protocol_bytes.saturating_sub(bytes);
                let key = (call.scope().clone(), r.value.manifest.key.id.clone());
                if self
                    .inner
                    .seen_catalogue_revisions
                    .get(&key)
                    .is_some_and(|v| *v >= r.value.manifest.revision)
                {
                    self.inner.counters.catalogue_duplicate_bytes += bytes
                }
                self.inner
                    .seen_catalogue_revisions
                    .insert(key, r.value.manifest.revision);
            }
        }
        Ok(reply)
    }
}
impl CatalogueHorizonSource for HorizonCatalogueClient {
    fn horizon_head(
        &mut self,
        s: &Scope,
        support: CapabilitySupport,
    ) -> Result<CatalogueHorizonHead, CatalogueSourceError> {
        match self.read(&Call::Head(s.clone()), support)? {
            Reply::Head(h) => Ok(h),
            _ => Err(CatalogueSourceError::InvalidRequest),
        }
    }
    fn horizon_manifest(
        &mut self,
        r: &ManifestRequest,
        support: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonManifestRead, CatalogueSourceError> {
        match self.read(&Call::Manifest(r.clone(), k), support)? {
            Reply::Manifest(r) => Ok(r),
            _ => Err(CatalogueSourceError::InvalidRequest),
        }
    }
    fn horizon_value(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
        support: CapabilitySupport,
        k: CatalogueReadKind,
    ) -> Result<HorizonValueRead, CatalogueSourceError> {
        match self.read(&Call::Value(p.clone(), id.clone(), b, k), support)? {
            Reply::Value(r) => Ok(r),
            _ => Err(CatalogueSourceError::InvalidRequest),
        }
    }
}
#[cfg(feature = "sqlite")]
fn answer(
    config: &LoopbackConfig,
    counters: &ServerCounters,
    call: &Call,
    reader: u64,
    support: CapabilitySupport,
) -> Result<Vec<u8>, CatalogueSourceError> {
    call.validate()?;
    let mut source = config
        .open_catalogue_source()
        .map_err(|_| CatalogueSourceError::Unavailable)?;
    let mut body = header(0, CATALOGUE_HORIZON_VERSION, reader, call);
    match call {
        Call::Head(s) => {
            counters
                .catalogue_head_reads
                .fetch_add(1, Ordering::Relaxed);
            let h = source.horizon_head(s, support)?;
            check_head(&h, s, support)?;
            put_head(&mut body, &h)
        }
        Call::Manifest(r, k) => {
            counters
                .catalogue_manifest_reads
                .fetch_add(1, Ordering::Relaxed);
            let read = source.horizon_manifest(r, support, *k)?;
            check_head(&read.head, &r.pass.scope, support)?;
            put_head(&mut body, &read.head);
            body.push(u8::from(read.page.has_more));
            body.extend_from_slice(&(read.page.entries.len() as u16).to_be_bytes());
            for e in read.page.entries {
                put_manifest_entry(&mut body, &e)
            }
        }
        Call::Value(p, id, b, k) => {
            counters
                .catalogue_resolve_reads
                .fetch_add(1, Ordering::Relaxed);
            let read = source.horizon_value(p, id, *b, support, *k)?;
            check_head(&read.head, &p.scope, support)?;
            put_head(&mut body, &read.head);
            put_manifest_entry(&mut body, &read.value.manifest);
            body.extend_from_slice(&(read.value.payload.len() as u32).to_be_bytes());
            body.extend_from_slice(&read.value.payload)
        }
    }
    Ok(body)
}
#[cfg(feature = "sqlite")]
pub(super) fn serve(
    stream: &mut TcpStream,
    config: &LoopbackConfig,
    counters: &ServerCounters,
    operation: u8,
    token: &Id,
    mut input: Decoder<'_>,
) -> Result<(), FrameError> {
    let offered = u16_value(&mut input)?;
    let reader = input.u64()?;
    let call = Call::read(&mut input, operation)?;
    input.finish()?;
    if !catalogue_access(config, token, call.scope(), counters, stream)? {
        return Ok(());
    }
    let result = if offered != CATALOGUE_HORIZON_VERSION {
        Err(CatalogueSourceError::HorizonVersion(offered))
    } else {
        CapabilitySupport::new(reader & KNOWN_READER_CAPABILITIES, 0)
            .map_err(CatalogueSourceError::Incompatible)
            .and_then(|support| answer(config, counters, &call, reader, support))
    };
    let response = match result {
        Ok(out) => out,
        Err(CatalogueSourceError::HorizonExpired { head, .. }) => {
            let mut out = header(11, offered, reader, &call);
            put_head(&mut out, &head);
            out
        }
        Err(CatalogueSourceError::HorizonVersion(v)) => {
            let mut out = header(10, offered, reader, &call);
            out.extend_from_slice(&v.to_be_bytes());
            out
        }
        Err(CatalogueSourceError::Incompatible(e)) => {
            let mut out = header(8, offered, reader, &call);
            match e {
                CapabilityError::UnsupportedVersion(v) if v != SOURCE_HEAD_VERSION => {
                    out.push(1);
                    out.extend_from_slice(&v.to_be_bytes())
                }
                CapabilityError::MissingReader(bits) if bits != 0 => {
                    out.push(2);
                    out.extend_from_slice(&bits.to_be_bytes())
                }
                _ => return write_catalogue_frame(stream, &[7], config, counters),
            }
            out
        }
        Err(e) => header(catalogue_error_code(e), offered, reader, &call),
    };
    if !catalogue_access(config, token, call.scope(), counters, stream)? {
        return Ok(());
    }
    write_catalogue_frame(stream, &response, config, counters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::SourceHead;
    fn id(s: &str) -> Id {
        Id::new(s).unwrap()
    }
    fn scope() -> Scope {
        Scope::new(
            id("receiver"),
            id("origin"),
            id("stream"),
            id("incarnation"),
            id("schema"),
            id("epoch"),
        )
    }
    fn pass() -> CataloguePass {
        CataloguePass {
            scope: scope(),
            completed: 1,
            boundary: 4,
            cursor: None,
            generation: 1,
        }
    }
    fn value_call() -> Call {
        Call::Value(pass(), id("entry"), 6, CatalogueReadKind::Incremental)
    }
    fn h(floor: u64) -> CatalogueHorizonHead {
        CatalogueHorizonHead::new(SourceHead::new(scope(), 5, 0, u64::MAX), floor)
    }
    fn envelope(code: u8, call: &Call, floor: u64) -> Vec<u8> {
        let mut out = header(code, CATALOGUE_HORIZON_VERSION, 0, call);
        put_head(&mut out, &h(floor));
        out
    }
    fn value(call: &Call) -> Vec<u8> {
        let mut out = envelope(0, call, 1);
        put_manifest_entry(
            &mut out,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("entry"),
                },
                revision: 5,
                deleted: false,
            },
        );
        out.extend_from_slice(&3u32.to_be_bytes());
        out.extend_from_slice(b"abc");
        out
    }
    fn invalid_result(result: Result<Reply, CatalogueSourceError>) {
        assert!(matches!(result, Err(CatalogueSourceError::InvalidRequest)))
    }
    #[test]
    fn current_value_equality_and_newer_than_boundary_are_validated_before_payload() {
        let c = value_call();
        match decode(&value(&c), &c, CapabilitySupport::current()).unwrap() {
            Reply::Value(r) => {
                assert_eq!(r.value.manifest.revision, 5);
                assert_eq!(r.head.horizon, 1);
                assert_eq!(r.value.payload, b"abc")
            }
            _ => panic!("wrong reply"),
        }
        let mut valid_but_expired = value(&c);
        let floor = header(0, 2, 0, &c).len() + 26;
        valid_but_expired[floor..floor + 8].copy_from_slice(&2u64.to_be_bytes());
        invalid_result(decode(&valid_but_expired, &c, CapabilitySupport::current()));
        let mut r = envelope(0, &c, 2);
        r.extend_from_slice(b"not an opaque value");
        invalid_result(decode(&r, &c, CapabilitySupport::current()));
        let mut future = value(&c);
        let start = header(0, 2, 0, &c).len();
        future[start..start + 2].copy_from_slice(&9u16.to_be_bytes());
        assert!(matches!(
            decode(&future, &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::Incompatible(
                CapabilityError::UnsupportedVersion(9)
            ))
        ));
    }
    #[test]
    fn exact_resource_kind_generation_cursor_and_budget_echoes_are_required() {
        let c = value_call();
        let mut alternatives = Vec::new();
        let mut p = pass();
        p.generation += 1;
        alternatives.push(Call::Value(
            p,
            id("entry"),
            6,
            CatalogueReadKind::Incremental,
        ));
        let mut p = pass();
        p.cursor = Some(EntryKey {
            creation: 1,
            id: id("earlier"),
        });
        alternatives.push(Call::Value(
            p,
            id("entry"),
            6,
            CatalogueReadKind::Incremental,
        ));
        alternatives.push(Call::Value(
            pass(),
            id("other"),
            6,
            CatalogueReadKind::Incremental,
        ));
        alternatives.push(Call::Value(
            pass(),
            id("entry"),
            5,
            CatalogueReadKind::Incremental,
        ));
        alternatives.push(Call::Value(
            pass(),
            id("entry"),
            6,
            CatalogueReadKind::FullReset,
        ));
        let mut p = pass();
        p.completed = 0;
        alternatives.push(Call::Value(
            p,
            id("entry"),
            6,
            CatalogueReadKind::Incremental,
        ));
        for other in alternatives {
            invalid_result(decode(&value(&other), &c, CapabilitySupport::current()));
            invalid_result(decode(
                &envelope(11, &other, 3),
                &c,
                CapabilitySupport::current(),
            ))
        }
    }
    #[test]
    fn typed_expiry_requires_actual_read_kind_point_and_head_is_not_expirable() {
        let c = value_call();
        assert!(matches!(
            decode(&envelope(11, &c, 2), &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::HorizonExpired { completed: 1, .. })
        ));
        invalid_result(decode(
            &envelope(11, &c, 1),
            &c,
            CapabilitySupport::current(),
        ));
        let mut p = pass();
        p.completed = 0;
        let reset = Call::Value(p, id("entry"), 6, CatalogueReadKind::FullReset);
        invalid_result(decode(
            &envelope(11, &reset, 2),
            &reset,
            CapabilitySupport::current(),
        ));
        assert!(matches!(
            decode(
                &envelope(11, &reset, 5),
                &reset,
                CapabilitySupport::current()
            ),
            Err(CatalogueSourceError::HorizonExpired { completed: 0, .. })
        ));
        let head = Call::Head(scope());
        invalid_result(decode(
            &envelope(11, &head, 2),
            &head,
            CapabilitySupport::current(),
        ));
        let mut trailing = envelope(11, &c, 2);
        trailing.push(0);
        invalid_result(decode(&trailing, &c, CapabilitySupport::current()));
    }
    #[test]
    fn normal_refusals_are_correlated_unknown_and_malformed_are_fatal() {
        let c = value_call();
        for code in [0, 2, 3, 4, 5, 7, 8, 10, 11, 255] {
            invalid_result(decode(&[code], &c, CapabilitySupport::current()))
        }
        assert!(matches!(
            decode(&[1], &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::Denied)
        ));
        assert!(matches!(
            decode(&[9], &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::IdentityChanged)
        ));
        assert!(matches!(
            decode(&header(2, 2, 0, &c), &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::Unavailable)
        ));
        let mut version = header(10, 2, 0, &c);
        version.extend_from_slice(&3u16.to_be_bytes());
        assert!(matches!(
            decode(&version, &c, CapabilitySupport::current()),
            Err(CatalogueSourceError::HorizonVersion(3))
        ));
        let other = Call::Head(scope());
        invalid_result(decode(
            &header(2, 2, 0, &other),
            &c,
            CapabilitySupport::current(),
        ));
        let mut unknown = header(200, 2, 0, &c);
        unknown.extend_from_slice(b"opaque");
        invalid_result(decode(&unknown, &c, CapabilitySupport::current()));
    }
    #[test]
    fn manifest_equality_and_metadata_bounds_precede_descriptors() {
        let c = Call::Manifest(
            ManifestRequest {
                pass: pass(),
                max_entries: 1,
            },
            CatalogueReadKind::Incremental,
        );
        let mut out = envelope(0, &c, 1);
        out.push(0);
        out.extend_from_slice(&1u16.to_be_bytes());
        put_manifest_entry(
            &mut out,
            &ManifestEntry {
                key: EntryKey {
                    creation: 2,
                    id: id("entry"),
                },
                revision: 5,
                deleted: false,
            },
        );
        assert!(matches!(
            decode(&out, &c, CapabilitySupport::current()),
            Ok(Reply::Manifest(_))
        ));
        let head_call = Call::Head(scope());
        invalid_result(decode(
            &envelope(0, &head_call, 6),
            &head_call,
            CapabilitySupport::current(),
        ));
        let start = header(0, 2, 0, &c).len();
        out[start + 26..start + 34].copy_from_slice(&6u64.to_be_bytes());
        invalid_result(decode(&out, &c, CapabilitySupport::current()));
    }
    #[test]
    fn capability_and_version_refusals_require_exact_nonempty_details() {
        let call = value_call();
        let supported = CapabilitySupport::current();
        for reason in [
            vec![],
            vec![1, 0, 1],
            vec![2, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![99],
        ] {
            let mut out = header(8, 2, 0, &call);
            out.extend_from_slice(&reason);
            invalid_result(decode(&out, &call, supported));
        }
        let mut out = header(8, 2, 0, &call);
        out.push(2);
        out.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            decode(&out, &call, supported),
            Err(CatalogueSourceError::Incompatible(
                CapabilityError::MissingReader(1)
            ))
        ));
        out.push(0);
        invalid_result(decode(&out, &call, supported));
        let mut out = header(8, 2, 0, &Call::Head(scope()));
        out.extend_from_slice(&[1, 0, 3]);
        invalid_result(decode(&out, &call, supported));
        let mut out = header(10, 2, 0, &call);
        out.extend_from_slice(&2u16.to_be_bytes());
        invalid_result(decode(&out, &call, supported));
        let mut out = header(10, 2, 0, &call);
        out.extend_from_slice(&3u16.to_be_bytes());
        out.push(0);
        invalid_result(decode(&out, &call, supported));
        let mut out = value(&call);
        out[2..4].copy_from_slice(&3u16.to_be_bytes());
        invalid_result(decode(&out, &call, supported));
    }
    #[test]
    fn existing_full_value_budget_remains_bounded_and_roundtrips() {
        let call = Call::Value(
            pass(),
            id("entry"),
            MAX_PAGE_PAYLOAD,
            CatalogueReadKind::Incremental,
        );
        let mut out = envelope(0, &call, 1);
        put_manifest_entry(
            &mut out,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("entry"),
                },
                revision: 5,
                deleted: false,
            },
        );
        out.extend_from_slice(&(MAX_PAGE_PAYLOAD as u32).to_be_bytes());
        out.extend_from_slice(&vec![42; MAX_PAGE_PAYLOAD]);
        assert!(out.len() < MAX_FRAME_BYTES);
        match decode(&out, &call, CapabilitySupport::current()).unwrap() {
            Reply::Value(r) => assert_eq!(r.value.payload.len(), MAX_PAGE_PAYLOAD),
            _ => panic!("wrong reply"),
        }
        invalid_result(decode(
            &out[..out.len() - 1],
            &call,
            CapabilitySupport::current(),
        ));
        let offset = header(0, 2, 0, &call).len() + 34 + 1 + "entry".len() + 16 + 1;
        out[offset..offset + 4].copy_from_slice(&((MAX_PAGE_PAYLOAD + 1) as u32).to_be_bytes());
        out.push(42);
        invalid_result(decode(&out, &call, CapabilitySupport::current()));
    }
}
