//! Explicit version1 catalogue operations on the existing one-request connection.
use super::*;
use crate::replication::catalogue::{
    validate_manifest_entry, CatalogueHeadSource, CatalogueManifestRead, CatalogueReadSource,
    CatalogueValueRead,
};
#[cfg(feature = "sqlite")]
use crate::replication::domain::KNOWN_READER_CAPABILITIES;
use crate::replication::domain::{
    validate_capabilities, CapabilityError, CapabilitySupport, SourceHead, SOURCE_HEAD_VERSION,
};
/// Version1 source-head read; no legacy fallback.
pub const CATALOGUE_HEAD_CAP: u8 = 17;
/// Version1 manifest read with coherent publication metadata.
pub const CATALOGUE_MANIFEST_CAP: u8 = 18;
/// Version1 current-value read with coherent publication metadata.
pub const CATALOGUE_VALUE_CAP: u8 = 19;
fn u16_value(d: &mut Decoder<'_>) -> Result<u16, FrameError> {
    Ok(u16::from_be_bytes(
        d.take(2)?.try_into().map_err(|_| FrameError::Malformed)?,
    ))
}
fn prefix(out: &mut Vec<u8>, reader: u64) {
    out.extend_from_slice(&SOURCE_HEAD_VERSION.to_be_bytes());
    out.extend_from_slice(&reader.to_be_bytes());
}
#[cfg(any(feature = "sqlite", test))]
fn metadata(out: &mut Vec<u8>, head: &SourceHead) {
    out.extend_from_slice(&head.version().to_be_bytes());
    for n in [
        head.position(),
        head.reader_required(),
        head.writer_required(),
    ] {
        out.extend_from_slice(&n.to_be_bytes());
    }
}
type MetadataNumbers = (u16, u64, u64, u64);
fn read_metadata(d: &mut Decoder<'_>) -> Result<MetadataNumbers, FrameError> {
    Ok((u16_value(d)?, d.u64()?, d.u64()?, d.u64()?))
}
fn publication(
    numbers: MetadataNumbers,
    scope: &Scope,
    support: CapabilitySupport,
    minimum: u64,
) -> Result<SourceHead, CatalogueSourceError> {
    let head = SourceHead::advertised(scope.clone(), numbers.0, numbers.1, numbers.2, numbers.3);
    validate_capabilities(&head, support).map_err(CatalogueSourceError::Incompatible)?;
    if head.position() < minimum {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    Ok(head)
}
fn invalid(_: FrameError) -> CatalogueSourceError {
    CatalogueSourceError::InvalidRequest
}
fn transport(error: FrameError) -> CatalogueSourceError {
    match error {
        FrameError::Io(_) => CatalogueSourceError::Unavailable,
        _ => CatalogueSourceError::InvalidRequest,
    }
}
fn reply<'a>(
    body: &'a [u8],
    operation: u8,
    supported: CapabilitySupport,
) -> Result<(Decoder<'a>, MetadataNumbers), CatalogueSourceError> {
    let mut d = Decoder::new(body);
    let code = d.byte().map_err(invalid)?;
    if code != 0 {
        if code == 8 {
            echo(&mut d, operation, supported).map_err(invalid)?;
            let error = match d.byte().map_err(invalid)? {
                1 => {
                    let value = u16_value(&mut d).map_err(invalid)?;
                    if value == SOURCE_HEAD_VERSION {
                        return Err(CatalogueSourceError::InvalidRequest);
                    }
                    CapabilityError::UnsupportedVersion(value)
                }
                2 => {
                    let bits = d.u64().map_err(invalid)?;
                    if bits == 0 {
                        return Err(CatalogueSourceError::InvalidRequest);
                    }
                    CapabilityError::MissingReader(bits)
                }
                _ => return Err(CatalogueSourceError::InvalidRequest),
            };
            d.finish().map_err(invalid)?;
            return Err(CatalogueSourceError::Incompatible(error));
        }
        d.finish().map_err(invalid)?;
        return Err(match code {
            1 => CatalogueSourceError::Denied,
            2 => CatalogueSourceError::Unavailable,
            3 | 9 => CatalogueSourceError::IdentityChanged,
            4 => CatalogueSourceError::InvalidRequest,
            5 => CatalogueSourceError::OversizedEntry,
            _ => CatalogueSourceError::InvalidRequest,
        });
    }
    echo(&mut d, operation, supported).map_err(invalid)?;
    let numbers = read_metadata(&mut d).map_err(invalid)?;
    Ok((d, numbers))
}
fn echo(
    d: &mut Decoder<'_>,
    operation: u8,
    supported: CapabilitySupport,
) -> Result<(), FrameError> {
    if d.byte()? != operation
        || u16_value(d)? != SOURCE_HEAD_VERSION
        || d.u64()? != supported.reader_bits()
    {
        return Err(FrameError::Malformed);
    }
    Ok(())
}
fn decode_head(
    body: &[u8],
    scope: &Scope,
    support: CapabilitySupport,
) -> Result<SourceHead, CatalogueSourceError> {
    let (mut d, n) = reply(body, CATALOGUE_HEAD_CAP, support)?;
    if d.scope().map_err(invalid)? != *scope {
        return Err(CatalogueSourceError::IdentityChanged);
    }
    d.finish().map_err(invalid)?;
    publication(n, scope, support, 0)
}
fn decode_manifest(
    body: &[u8],
    r: &ManifestRequest,
    support: CapabilitySupport,
) -> Result<CatalogueManifestRead, CatalogueSourceError> {
    let (mut d, n) = reply(body, CATALOGUE_MANIFEST_CAP, support)?;
    if d.catalogue_pass().map_err(invalid)? != r.pass
        || d.u64().map_err(invalid)? != r.max_entries as u64
    {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    let head = publication(n, &r.pass.scope, support, r.pass.boundary)?;
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
        if e.revision > head.position() {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        entries.push(e);
    }
    d.finish().map_err(invalid)?;
    let page = ManifestPage {
        request: r.clone(),
        entries,
        has_more,
    };
    validate_manifest(r, &page, MAX_CATALOGUE_ENTRIES)
        .map_err(|_| CatalogueSourceError::InvalidRequest)?;
    Ok(CatalogueManifestRead { head, page })
}
fn decode_value(
    body: &[u8],
    pass: &CataloguePass,
    id: &Id,
    budget: usize,
    support: CapabilitySupport,
) -> Result<CatalogueValueRead, CatalogueSourceError> {
    let (mut d, n) = reply(body, CATALOGUE_VALUE_CAP, support)?;
    if d.catalogue_pass().map_err(invalid)? != *pass
        || d.id().map_err(invalid)? != *id
        || d.u64().map_err(invalid)? != budget as u64
    {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    let head = publication(n, &pass.scope, support, pass.boundary)?;
    let descriptor = d.manifest_entry().map_err(invalid)?;
    validate_manifest_entry(&descriptor).map_err(|_| CatalogueSourceError::InvalidRequest)?;
    if descriptor.key.id != *id
        || descriptor.key.creation > pass.boundary
        || descriptor.revision > head.position()
    {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    let len = u32::from_be_bytes(
        d.take(4)
            .map_err(invalid)?
            .try_into()
            .map_err(|_| CatalogueSourceError::InvalidRequest)?,
    ) as usize;
    if len > budget || (descriptor.deleted && len != 0) {
        return Err(CatalogueSourceError::InvalidRequest);
    }
    let payload = d.take(len).map_err(invalid)?.to_vec();
    d.finish().map_err(invalid)?;
    Ok(CatalogueValueRead {
        head,
        value: ResolvedEntry {
            manifest: descriptor,
            payload,
        },
    })
}
/// Capability-aware catalogue adapter on the existing development loopback route.
/// Each operation selects the versioned dialect; no connection option, implicit
/// retry or legacy fallback exists. Use begin_or_resume_with_capabilities so saved
/// active passes receive fresh admission before a manifest request. Tokens remain
/// development fixtures; this type supplies no production channel authentication.
pub struct CapabilityCatalogueClient {
    inner: LoopbackClient,
    supported: CapabilitySupport,
}
impl CapabilityCatalogueClient {
    /// Select implemented reader support on the existing address/token transport.
    pub fn new(
        address: SocketAddrV4,
        token: Id,
        supported: CapabilitySupport,
    ) -> Result<Self, FrameError> {
        Ok(Self {
            inner: LoopbackClient::new(address, token)?,
            supported,
        })
    }
    /// Actual wire accounting, including metadata; descriptors/payload are separated.
    pub fn counters(&self) -> WireCounters {
        self.inner.counters()
    }
    fn request(&self, operation: u8, support: CapabilitySupport) -> Vec<u8> {
        let mut out = vec![operation];
        put_id(&mut out, &self.inner.token);
        prefix(&mut out, support.reader_bits());
        out
    }
}
impl CatalogueHeadSource for CapabilityCatalogueClient {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, CatalogueSourceError> {
        let mut out = self.request(CATALOGUE_HEAD_CAP, self.supported);
        put_scope(&mut out, scope);
        let body = self.inner.exchange(&out).map_err(transport)?;
        decode_head(&body, scope, self.supported)
    }
}
impl CatalogueSource for CapabilityCatalogueClient {
    fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError> {
        self.source_head(scope).map(|h| h.position())
    }
    fn manifest(&mut self, r: &ManifestRequest) -> Result<ManifestPage, CatalogueSourceError> {
        self.manifest_with_head(r, self.supported).map(|r| r.page)
    }
    fn resolve(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        b: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        self.resolve_with_head(p, id, b, self.supported)
            .map(|r| r.value)
    }
}
impl CatalogueReadSource for CapabilityCatalogueClient {
    fn manifest_with_head(
        &mut self,
        r: &ManifestRequest,
        support: CapabilitySupport,
    ) -> Result<CatalogueManifestRead, CatalogueSourceError> {
        validate_manifest_request(r, MAX_CATALOGUE_ENTRIES)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        let mut out = self.request(CATALOGUE_MANIFEST_CAP, support);
        put_catalogue_pass(&mut out, &r.pass);
        out.extend_from_slice(&(r.max_entries as u64).to_be_bytes());
        let body = self.inner.exchange(&out).map_err(transport)?;
        let read = decode_manifest(&body, r, support)?;
        let bytes = read
            .page
            .entries
            .iter()
            .map(|e| e.key.id.as_str().len() as u64 + 17)
            .sum::<u64>();
        self.inner.counters.catalogue_manifest_bytes += bytes;
        self.inner.counters.protocol_bytes =
            self.inner.counters.protocol_bytes.saturating_sub(bytes);
        Ok(read)
    }
    fn resolve_with_head(
        &mut self,
        p: &CataloguePass,
        id: &Id,
        budget: usize,
        support: CapabilitySupport,
    ) -> Result<CatalogueValueRead, CatalogueSourceError> {
        validate_catalogue_pass(p).map_err(|_| CatalogueSourceError::InvalidRequest)?;
        if budget == 0 || budget > MAX_PAGE_PAYLOAD {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let mut out = self.request(CATALOGUE_VALUE_CAP, support);
        put_catalogue_pass(&mut out, p);
        put_id(&mut out, id);
        out.extend_from_slice(&(budget as u64).to_be_bytes());
        let body = self.inner.exchange(&out).map_err(transport)?;
        let read = decode_value(&body, p, id, budget, support)?;
        let bytes = read.value.payload.len() as u64;
        self.inner.counters.catalogue_payload_bytes += bytes;
        self.inner.counters.protocol_bytes =
            self.inner.counters.protocol_bytes.saturating_sub(bytes);
        let key = (p.scope.clone(), id.clone());
        if self
            .inner
            .seen_catalogue_revisions
            .get(&key)
            .is_some_and(|r| *r >= read.value.manifest.revision)
        {
            self.inner.counters.catalogue_duplicate_bytes += bytes;
        }
        self.inner
            .seen_catalogue_revisions
            .insert(key, read.value.manifest.revision);
        Ok(read)
    }
}
#[cfg(feature = "sqlite")]
enum Call {
    Head(Scope),
    Manifest(ManifestRequest),
    Value(CataloguePass, Id, usize),
}
#[cfg(feature = "sqlite")]
impl Call {
    fn scope(&self) -> &Scope {
        match self {
            Self::Head(s) => s,
            Self::Manifest(r) => &r.pass.scope,
            Self::Value(p, _, _) => &p.scope,
        }
    }
}
#[cfg(feature = "sqlite")]
fn answer(
    config: &LoopbackConfig,
    counters: &ServerCounters,
    operation: u8,
    offered: u16,
    reader: u64,
    call: &Call,
) -> Result<Vec<u8>, CatalogueSourceError> {
    if offered != SOURCE_HEAD_VERSION {
        return Err(CatalogueSourceError::Incompatible(
            CapabilityError::UnsupportedVersion(offered),
        ));
    }
    let supported = CapabilitySupport::new(reader & KNOWN_READER_CAPABILITIES, 0)
        .map_err(CatalogueSourceError::Incompatible)?;
    match call {
        Call::Head(_) => {}
        Call::Manifest(r) => validate_manifest_request(r, MAX_CATALOGUE_ENTRIES)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?,
        Call::Value(pass, _, budget) => {
            validate_catalogue_pass(pass).map_err(|_| CatalogueSourceError::InvalidRequest)?;
            if *budget == 0 || *budget > MAX_PAGE_PAYLOAD {
                return Err(CatalogueSourceError::InvalidRequest);
            }
        }
    }
    let mut source = config
        .open_catalogue_source()
        .map_err(|_| CatalogueSourceError::Unavailable)?;
    let mut body = vec![0, operation];
    body.extend_from_slice(&offered.to_be_bytes());
    body.extend_from_slice(&reader.to_be_bytes());
    match call {
        Call::Head(scope) => {
            counters
                .catalogue_head_reads
                .fetch_add(1, Ordering::Relaxed);
            let head = source.source_head(scope)?;
            validate_capabilities(&head, supported).map_err(CatalogueSourceError::Incompatible)?;
            metadata(&mut body, &head);
            put_scope(&mut body, scope);
        }
        Call::Manifest(r) => {
            validate_manifest_request(r, MAX_CATALOGUE_ENTRIES)
                .map_err(|_| CatalogueSourceError::InvalidRequest)?;
            counters
                .catalogue_manifest_reads
                .fetch_add(1, Ordering::Relaxed);
            let read = source.manifest_with_head(r, supported)?;
            metadata(&mut body, &read.head);
            put_catalogue_pass(&mut body, &read.page.request.pass);
            body.extend_from_slice(&(read.page.request.max_entries as u64).to_be_bytes());
            body.push(u8::from(read.page.has_more));
            body.extend_from_slice(&(read.page.entries.len() as u16).to_be_bytes());
            for entry in &read.page.entries {
                put_manifest_entry(&mut body, entry);
            }
        }
        Call::Value(pass, id, budget) => {
            validate_catalogue_pass(pass).map_err(|_| CatalogueSourceError::InvalidRequest)?;
            if *budget == 0 || *budget > MAX_PAGE_PAYLOAD {
                return Err(CatalogueSourceError::InvalidRequest);
            }
            counters
                .catalogue_resolve_reads
                .fetch_add(1, Ordering::Relaxed);
            let read = source.resolve_with_head(pass, id, *budget, supported)?;
            metadata(&mut body, &read.head);
            put_catalogue_pass(&mut body, pass);
            put_id(&mut body, id);
            body.extend_from_slice(&(*budget as u64).to_be_bytes());
            put_manifest_entry(&mut body, &read.value.manifest);
            body.extend_from_slice(&(read.value.payload.len() as u32).to_be_bytes());
            body.extend_from_slice(&read.value.payload);
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
    let call = match operation {
        CATALOGUE_HEAD_CAP => Call::Head(input.scope()?),
        CATALOGUE_MANIFEST_CAP => {
            let pass = input.catalogue_pass()?;
            let max_entries = usize::try_from(input.u64()?).map_err(|_| FrameError::Malformed)?;
            Call::Manifest(ManifestRequest { pass, max_entries })
        }
        CATALOGUE_VALUE_CAP => {
            let pass = input.catalogue_pass()?;
            let id = input.id()?;
            let budget = usize::try_from(input.u64()?).map_err(|_| FrameError::Malformed)?;
            Call::Value(pass, id, budget)
        }
        _ => return Err(FrameError::Malformed),
    };
    input.finish()?;
    if !catalogue_access(config, token, call.scope(), counters, stream)? {
        return Ok(());
    }
    // answer returns only owned DTO bytes; source handles/transactions are gone
    // before this function writes or injects the ordinary disconnect fixture.
    let response = match answer(config, counters, operation, offered, reader, &call) {
        Ok(body) => body,
        Err(CatalogueSourceError::Incompatible(error)) => {
            let mut body = vec![8, operation];
            body.extend_from_slice(&offered.to_be_bytes());
            body.extend_from_slice(&reader.to_be_bytes());
            match error {
                CapabilityError::UnsupportedVersion(v) if v != SOURCE_HEAD_VERSION => {
                    body.push(1);
                    body.extend_from_slice(&v.to_be_bytes());
                }
                CapabilityError::MissingReader(bits) if bits != 0 => {
                    body.push(2);
                    body.extend_from_slice(&bits.to_be_bytes());
                }
                _ => return write_catalogue_frame(stream, &[7], config, counters),
            }
            body
        }
        Err(error) => vec![catalogue_error_code(error)],
    };
    if !catalogue_access(config, token, call.scope(), counters, stream)? {
        return Ok(());
    }
    write_catalogue_frame(stream, &response, config, counters)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(s: &str) -> Id {
        Id::new(s).unwrap()
    }
    fn scope() -> Scope {
        Scope::new(
            id("phone"),
            id("gateway"),
            id("transcript-index"),
            id("first"),
            id("opaque-v1"),
            id("epoch-1"),
        )
    }
    fn pass() -> CataloguePass {
        CataloguePass {
            scope: scope(),
            completed: 0,
            boundary: 1,
            cursor: None,
            generation: 1,
        }
    }
    fn header(op: u8, head: u64, reader: u64, writer: u64) -> Vec<u8> {
        let mut out = vec![0, op];
        prefix(&mut out, 0);
        metadata(&mut out, &SourceHead::new(scope(), head, reader, writer));
        out
    }
    fn value(head: u64, reader: u64, writer: u64, revision: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = header(CATALOGUE_VALUE_CAP, head, reader, writer);
        put_catalogue_pass(&mut out, &pass());
        put_id(&mut out, &id("a"));
        out.extend_from_slice(&100u64.to_be_bytes());
        put_manifest_entry(
            &mut out,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("a"),
                },
                revision,
                deleted: false,
            },
        );
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }
    #[test]
    fn capability_metadata_precedes_opaque_decoding_and_writer_only_continues() {
        let mut body = header(CATALOGUE_VALUE_CAP, 1, 1, 0);
        put_catalogue_pass(&mut body, &pass());
        put_id(&mut body, &id("a"));
        body.extend_from_slice(&100u64.to_be_bytes());
        assert_eq!(
            decode_value(&body, &pass(), &id("a"), 100, CapabilitySupport::current()),
            Err(CatalogueSourceError::Incompatible(
                CapabilityError::MissingReader(1)
            ))
        );
        assert_eq!(
            decode_value(
                &value(1, 0, u64::MAX, 1, b"v"),
                &pass(),
                &id("a"),
                100,
                CapabilitySupport::current()
            )
            .unwrap()
            .value
            .payload,
            b"v"
        );
    }
    #[test]
    fn exact_operation_support_scope_generation_id_budget_and_revision_cover_are_required() {
        let valid = value(1, 0, 0, 1, b"v");
        for at in [1usize, 2, 11, 13] {
            let mut changed = valid.clone();
            changed[at] ^= 1;
            assert!(decode_value(
                &changed,
                &pass(),
                &id("a"),
                100,
                CapabilitySupport::current()
            )
            .is_err());
        }
        let mut p = pass();
        p.generation += 1;
        assert_eq!(
            decode_value(&valid, &p, &id("a"), 100, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        let mut p = pass();
        p.scope = Scope::new(
            id("laptop"),
            id("gateway"),
            id("transcript-index"),
            id("first"),
            id("opaque-v1"),
            id("epoch-1"),
        );
        assert_eq!(
            decode_value(&valid, &p, &id("a"), 100, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!(
            decode_value(&valid, &pass(), &id("b"), 100, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!(
            decode_value(&valid, &pass(), &id("a"), 99, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!(
            decode_value(
                &value(1, 0, 0, 2, b"v"),
                &pass(),
                &id("a"),
                100,
                CapabilitySupport::current()
            ),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!(
            decode_value(
                &value(0, 0, 0, 1, b"v"),
                &pass(),
                &id("a"),
                100,
                CapabilitySupport::current()
            ),
            Err(CatalogueSourceError::InvalidRequest)
        );
    }
    #[test]
    fn current_publication_must_cover_the_captured_boundary_even_for_an_older_value() {
        let mut p = pass();
        p.boundary = 2;
        let mut body = header(CATALOGUE_VALUE_CAP, 1, 0, 0);
        put_catalogue_pass(&mut body, &p);
        put_id(&mut body, &id("a"));
        body.extend_from_slice(&100u64.to_be_bytes());
        put_manifest_entry(
            &mut body,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("a"),
                },
                revision: 1,
                deleted: false,
            },
        );
        body.extend_from_slice(&1u32.to_be_bytes());
        body.push(b'v');
        assert_eq!(
            decode_value(&body, &p, &id("a"), 100, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
    }
    #[test]
    fn manifest_revision_cover_and_canonical_resources_precede_return() {
        let r = ManifestRequest {
            pass: pass(),
            max_entries: 1,
        };
        let mut body = header(CATALOGUE_MANIFEST_CAP, 1, 0, 0);
        put_catalogue_pass(&mut body, &r.pass);
        body.extend_from_slice(&1u64.to_be_bytes());
        body.push(0);
        body.extend_from_slice(&1u16.to_be_bytes());
        put_manifest_entry(
            &mut body,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("a"),
                },
                revision: 2,
                deleted: false,
            },
        );
        assert_eq!(
            decode_manifest(&body, &r, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        let r = ManifestRequest {
            pass: pass(),
            max_entries: 2,
        };
        assert_eq!(
            decode_manifest(&body, &r, CapabilitySupport::current()),
            Err(CatalogueSourceError::InvalidRequest)
        );
    }
    #[test]
    fn bounded_known_refusals_and_unknown_codes_never_become_retryable() {
        for detail in [
            vec![255],
            vec![2, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![1, 0, 1],
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 0],
        ] {
            let mut b = vec![8, CATALOGUE_HEAD_CAP];
            prefix(&mut b, 0);
            b.extend(detail);
            assert_eq!(
                decode_head(&b, &scope(), CapabilitySupport::current()),
                Err(CatalogueSourceError::InvalidRequest)
            );
        }
        for b in [vec![255], vec![7], vec![2, 0]] {
            assert_eq!(
                decode_head(&b, &scope(), CapabilitySupport::current()),
                Err(CatalogueSourceError::InvalidRequest)
            );
        }
        assert_eq!(
            decode_head(&[2], &scope(), CapabilitySupport::current()),
            Err(CatalogueSourceError::Unavailable)
        );
        let mut b = vec![8, CATALOGUE_HEAD_CAP];
        prefix(&mut b, 0);
        b.extend([1, 0, 2]);
        assert_eq!(
            decode_head(&b, &scope(), CapabilitySupport::current()),
            Err(CatalogueSourceError::Incompatible(
                CapabilityError::UnsupportedVersion(2)
            ))
        );
    }
    #[test]
    fn ordinary_unsupported_peer_is_not_retried_as_a_legacy_operation() {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let peer = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let request = read_frame(&mut socket).unwrap();
            assert_eq!(request[0], CATALOGUE_HEAD_CAP);
            write_frame(&mut socket, &[4]).unwrap();
        });
        let mut client =
            CapabilityCatalogueClient::new(address, id("read"), CapabilitySupport::current())
                .unwrap();
        assert_eq!(
            client.source_head(&scope()),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!(client.counters().exchanges, 1);
        peer.join().unwrap();
    }
    #[cfg(feature = "sqlite")]
    mod tcp {
        use super::*;
        use crate::replication::{
            catalogue::{
                apply_next_page, begin_or_resume_with_capabilities, CatalogueError, CatalogueStore,
            },
            infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
        };
        use std::sync::atomic::AtomicUsize;
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        struct Folder(PathBuf);
        impl Folder {
            fn new() -> Self {
                let p = std::env::temp_dir().join(format!(
                    "owned-catalogue-cap-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                std::fs::create_dir(&p).unwrap();
                Self(p)
            }
            fn path(&self, s: &str) -> PathBuf {
                self.0.join(s)
            }
        }
        impl Drop for Folder {
            fn drop(&mut self) {
                std::fs::remove_dir_all(&self.0).unwrap();
            }
        }
        fn source(path: &std::path::Path) -> SqliteCatalogueSource {
            SqliteCatalogueSource::open(
                path,
                id("gateway"),
                id("transcript-index"),
                id("first"),
                id("opaque-v1"),
            )
            .unwrap()
        }
        fn seed(path: &std::path::Path) {
            let mut s = source(path);
            for name in ["a", "b", "c"] {
                s.upsert(&id(name), b"old").unwrap();
            }
        }
        fn future(path: &std::path::Path, reader: u64, writer: u64) {
            rusqlite::Connection::open(path)
                .unwrap()
                .execute(
                    "UPDATE catalogue_source_meta SET reader_required=?1,writer_required=?2",
                    rusqlite::params![
                        reader.to_be_bytes().as_slice(),
                        writer.to_be_bytes().as_slice()
                    ],
                )
                .unwrap();
        }
        struct Server {
            address: SocketAddrV4,
            stop: Arc<AtomicBool>,
            join: Option<std::thread::JoinHandle<()>>,
            counters: Arc<ServerCounters>,
        }
        impl Server {
            fn new(path: PathBuf) -> Self {
                let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
                listener.set_nonblocking(true).unwrap();
                let address = match listener.local_addr().unwrap() {
                    std::net::SocketAddr::V4(a) => a,
                    _ => unreachable!(),
                };
                let config = LoopbackConfig {
                    source_path: path.with_extension("records.db"),
                    catalogue_source_path: Some(path),
                    artifact_source_path: None,
                    catalogue_fault: None,
                    origin: id("gateway"),
                    stream: id("transcript-index"),
                    incarnation: id("first"),
                    schema: id("opaque-v1"),
                    access_epoch: id("epoch-1"),
                    read_token: id("read"),
                    write_token: id("write"),
                    allowed_receivers: vec![id("phone")],
                };
                let stop = Arc::new(AtomicBool::new(false));
                let done = stop.clone();
                let counters = Arc::new(ServerCounters::default());
                let observed = counters.clone();
                let join = thread::spawn(move || {
                    let wake = (Mutex::new(0), Condvar::new());
                    while !done.load(Ordering::SeqCst) {
                        match listener.accept() {
                            Ok((socket, _)) => {
                                let _ = serve_one(socket, &config, &wake, &observed);
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(1))
                            }
                            Err(e) => panic!("owned listener: {e}"),
                        }
                    }
                });
                Self {
                    address,
                    stop,
                    join: Some(join),
                    counters,
                }
            }
            fn client(&self) -> CapabilityCatalogueClient {
                CapabilityCatalogueClient::new(
                    self.address,
                    id("read"),
                    CapabilitySupport::current(),
                )
                .unwrap()
            }
        }
        impl Drop for Server {
            fn drop(&mut self) {
                self.stop.store(true, Ordering::SeqCst);
                self.join.take().unwrap().join().unwrap();
            }
        }
        fn begin(
            c: &mut CapabilityCatalogueClient,
            d: &mut SqliteCatalogueStore,
        ) -> Result<Option<CataloguePass>, CatalogueError> {
            begin_or_resume_with_capabilities(
                &scope(),
                CapabilitySupport::current(),
                &mut MemoryAuthorizer::allowed(scope()),
                c,
                d,
            )
        }
        #[test]
        fn cold_and_resumed_typed_refusal_keep_reopened_cache_and_cursor() {
            for populated in [false, true] {
                let f = Folder::new();
                let p = f.path("source.db");
                seed(&p);
                let server = Server::new(p.clone());
                let mut c = server.client();
                let path = f.path("cache.db");
                let mut store = SqliteCatalogueStore::open(&path).unwrap();
                if populated {
                    let p = begin(&mut c, &mut store).unwrap().unwrap();
                    apply_next_page(
                        &p,
                        1,
                        100,
                        &mut MemoryAuthorizer::allowed(scope()),
                        &mut c,
                        &mut store,
                    )
                    .unwrap();
                }
                let saved = store.progress(&scope()).unwrap();
                let count = store.count(&scope()).unwrap();
                future(&p, 1, u64::MAX);
                let manifests = server
                    .counters
                    .catalogue_manifest_reads
                    .load(Ordering::Relaxed);
                let resolves = server
                    .counters
                    .catalogue_resolve_reads
                    .load(Ordering::Relaxed);
                let wire = c.counters();
                assert_eq!(
                    begin(&mut c, &mut store),
                    Err(CatalogueError::Incompatible(
                        CapabilityError::MissingReader(1)
                    ))
                );
                assert_eq!(c.counters().exchanges, wire.exchanges + 1);
                assert_eq!(
                    c.counters().catalogue_payload_bytes,
                    wire.catalogue_payload_bytes
                );
                assert_eq!(
                    server
                        .counters
                        .catalogue_manifest_reads
                        .load(Ordering::Relaxed),
                    manifests
                );
                assert_eq!(
                    server
                        .counters
                        .catalogue_resolve_reads
                        .load(Ordering::Relaxed),
                    resolves
                );
                drop(store);
                let mut reopened = SqliteCatalogueStore::open(path).unwrap();
                assert_eq!(reopened.progress(&scope()).unwrap(), saved);
                assert_eq!(reopened.count(&scope()).unwrap(), count);
            }
        }
        #[test]
        fn writer_only_reads_and_each_newer_value_echo_its_own_publication() {
            let f = Folder::new();
            let p = f.path("source.db");
            seed(&p);
            let server = Server::new(p.clone());
            let mut c = server.client();
            let mut cache = SqliteCatalogueStore::open(f.path("cache.db")).unwrap();
            let pass = begin(&mut c, &mut cache).unwrap().unwrap();
            assert_eq!(pass.boundary, 3);
            let r = ManifestRequest {
                pass: pass.clone(),
                max_entries: 3,
            };
            assert_eq!(
                c.manifest_with_head(&r, CapabilitySupport::current())
                    .unwrap()
                    .head
                    .position(),
                3
            );
            let mut s = source(&p);
            s.upsert(&id("b"), b"new").unwrap();
            s.delete(&id("c")).unwrap();
            future(&p, 0, u64::MAX);
            let read = c
                .resolve_with_head(&pass, &id("b"), 100, CapabilitySupport::current())
                .unwrap();
            assert_eq!(
                (
                    read.head.position(),
                    read.head.writer_required(),
                    read.value.manifest.revision,
                    read.value.payload
                ),
                (5, u64::MAX, 4, b"new".to_vec())
            );
            let deleted = c
                .resolve_with_head(&pass, &id("c"), 100, CapabilitySupport::current())
                .unwrap();
            assert_eq!(
                (
                    deleted.head.position(),
                    deleted.value.manifest.revision,
                    deleted.value.manifest.deleted
                ),
                (5, 5, true)
            );
            assert!(deleted.value.payload.is_empty());
            let done = apply_next_page(
                &pass,
                3,
                100,
                &mut MemoryAuthorizer::allowed(scope()),
                &mut c,
                &mut cache,
            )
            .unwrap();
            assert_eq!(done.completed, 3);
            assert_eq!(begin(&mut c, &mut cache).unwrap().unwrap().boundary, 5);
        }
        struct ChangeAfterFirst {
            client: CapabilityCatalogueClient,
            path: PathBuf,
            calls: usize,
        }
        impl CatalogueSource for ChangeAfterFirst {
            fn head(&mut self, s: &Scope) -> Result<u64, CatalogueSourceError> {
                self.client.head(s)
            }
            fn manifest(
                &mut self,
                r: &ManifestRequest,
            ) -> Result<ManifestPage, CatalogueSourceError> {
                self.client.manifest(r)
            }
            fn resolve(
                &mut self,
                p: &CataloguePass,
                id: &Id,
                b: usize,
            ) -> Result<ResolvedEntry, CatalogueSourceError> {
                let value = self.client.resolve(p, id, b)?;
                self.calls += 1;
                if self.calls == 1 {
                    let mut s = source(&self.path);
                    s.upsert(&super::id("b"), b"new").unwrap();
                    future(&self.path, 1, 0);
                }
                Ok(value)
            }
        }
        #[test]
        fn refusal_after_first_value_leaves_the_entire_real_tcp_page_unapplied() {
            let f = Folder::new();
            let p = f.path("source.db");
            seed(&p);
            let server = Server::new(p.clone());
            let mut c = server.client();
            let path = f.path("cache.db");
            let mut cache = SqliteCatalogueStore::open(&path).unwrap();
            let pass = begin(&mut c, &mut cache).unwrap().unwrap();
            let saved = cache.progress(&scope()).unwrap();
            let mut c = ChangeAfterFirst {
                client: c,
                path: p,
                calls: 0,
            };
            assert_eq!(
                apply_next_page(
                    &pass,
                    3,
                    100,
                    &mut MemoryAuthorizer::allowed(scope()),
                    &mut c,
                    &mut cache
                ),
                Err(CatalogueError::Incompatible(
                    CapabilityError::MissingReader(1)
                ))
            );
            assert_eq!(c.client.counters().catalogue_payload_bytes, 3);
            assert_eq!(cache.count(&scope()).unwrap(), 0);
            drop(cache);
            let mut cache = SqliteCatalogueStore::open(path).unwrap();
            assert_eq!(cache.progress(&scope()).unwrap(), saved);
            assert_eq!(cache.count(&scope()).unwrap(), 0);
        }
        #[test]
        fn native_snapshot_support_gate_precedes_payload_and_legacy_route_never_downgrades() {
            let f = Folder::new();
            let p = f.path("source.db");
            seed(&p);
            let mut s = source(&p);
            let selected = CataloguePass {
                scope: scope(),
                completed: 0,
                boundary: 3,
                cursor: None,
                generation: 1,
            };
            let mut beyond = selected.clone();
            beyond.boundary = 4;
            assert_eq!(
                s.resolve_with_head(&beyond, &id("a"), 100, CapabilitySupport::current()),
                Err(CatalogueSourceError::IdentityChanged)
            );
            assert_eq!(s.payload_bytes(), 0);
            future(&p, 1, 0);
            assert_eq!(
                s.manifest_with_head(
                    &ManifestRequest {
                        pass: selected.clone(),
                        max_entries: 3
                    },
                    CapabilitySupport::current()
                ),
                Err(CatalogueSourceError::Incompatible(
                    CapabilityError::MissingReader(1)
                ))
            );
            assert_eq!(s.manifest_reads(), 0);
            assert_eq!(s.manifest_bytes(), 0);
            assert_eq!(
                s.resolve_with_head(&selected, &id("a"), 100, CapabilitySupport::current()),
                Err(CatalogueSourceError::Incompatible(
                    CapabilityError::MissingReader(1)
                ))
            );
            assert_eq!(s.payload_bytes(), 0);
            assert_eq!(s.resolve_reads(), 0);
            let server = Server::new(p);
            let mut old = LoopbackClient::new(server.address, id("read")).unwrap();
            assert_eq!(
                CatalogueSource::head(&mut old, &scope()),
                Err(CatalogueSourceError::InvalidRequest)
            );
            let mut c = CapabilityCatalogueClient::new(
                server.address,
                id("wrong"),
                CapabilitySupport::current(),
            )
            .unwrap();
            assert_eq!(c.source_head(&scope()), Err(CatalogueSourceError::Denied));
            assert_eq!(
                server
                    .counters
                    .catalogue_resolve_reads
                    .load(Ordering::Relaxed),
                0
            );
        }
        #[test]
        fn maximum_existing_wire_budget_roundtrips_with_added_metadata() {
            let f = Folder::new();
            let p = f.path("source.db");
            source(&p)
                .upsert(&id("a"), &vec![42; MAX_PAGE_PAYLOAD])
                .unwrap();
            let server = Server::new(p);
            let mut c = server.client();
            let p = pass();
            let read = c
                .resolve_with_head(&p, &id("a"), MAX_PAGE_PAYLOAD, CapabilitySupport::current())
                .unwrap();
            assert_eq!(read.head.position(), 1);
            assert_eq!(read.value.payload, vec![42; MAX_PAGE_PAYLOAD]);
        }
    }
}
