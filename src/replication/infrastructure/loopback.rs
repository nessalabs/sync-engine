//! Development-only, loopback-bound framed transport. The server authorizes
//! each request before opening the reference source. No public pairing, TLS,
//! relay, or remote exposure is provided by this adapter.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
#[cfg(feature = "sqlite")]
use std::net::TcpListener;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
#[cfg(feature = "sqlite")]
use std::path::PathBuf;
#[cfg(feature = "sqlite")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(feature = "sqlite")]
use std::sync::{Arc, Condvar, Mutex};
#[cfg(feature = "sqlite")]
use std::thread;
use std::time::Duration;

use crate::replication::application::{Access, RecordSource, ScopeAuthorizer, SourceError};
use crate::replication::artifacts::{
    validate_chunk, ArtifactKey, ArtifactManifest, ArtifactState, ChunkReply, ChunkRequest,
    ChunkSource, ChunkSourceError, ContentIdentity, ManifestReply as ArtifactManifestReply,
    ManifestRequest as ArtifactManifestRequest, ManifestSource, ManifestSourceError, Sha256Digest,
    MAX_CHUNK_BYTES,
};
use crate::replication::catalogue::{
    CataloguePass, CatalogueSource, CatalogueSourceError, EntryKey, ManifestEntry, ManifestPage,
    ManifestRequest, ResolvedEntry,
};
use crate::replication::domain::{Id, Page, PageRequest, Record, Scope};
use crate::replication::history::{
    HistorySource, HistorySourceError, OlderPage, OlderRequest, TailRequest, TailSnapshot,
};

#[cfg(feature = "sqlite")]
use super::{SqliteArtifactSource, SqliteCatalogueSource, SqliteReferenceSource};

mod record_server;
pub use record_server::{LoopbackReadConfig, LoopbackRecordServer};

/// Maximum complete wire body, checked before allocating a read buffer.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum payload bytes in one record page.
pub const MAX_PAGE_PAYLOAD: usize = 512 * 1024;
/// Maximum records in one record page.
pub const MAX_PAGE_RECORDS: usize = 64;
const MAX_CATALOGUE_ENTRIES: usize = 128;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// A frame refused before body allocation or decode.
#[derive(Debug)]
pub enum FrameError {
    /// Read or write failed, including a truncated frame.
    Io(io::Error),
    /// Announced body exceeded the wire limit.
    TooLarge,
    /// Body was malformed or carried an invalid identity.
    Malformed,
}

/// One-shot response fault for the local catalogue transport lab.
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogueFault {
    /// Close the connection before sending the first catalogue response.
    DropFirstReply,
    /// Announce the first response length and send only its status byte.
    TruncateFirstReply,
}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Reads one length-prefixed frame without allocating an over-limit body.
pub fn read_frame(reader: &mut impl Read) -> Result<Vec<u8>, FrameError> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge);
    }
    let mut body = vec![0; len];
    reader.read_exact(&mut body)?;
    Ok(body)
}

/// Writes one bounded frame; caller owns flushing and connection lifetime.
pub fn write_frame(writer: &mut impl Write, body: &[u8]) -> Result<(), FrameError> {
    if body.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge);
    }
    writer.write_all(&(body.len() as u32).to_be_bytes())?;
    writer.write_all(body)?;
    Ok(())
}

fn put_id(out: &mut Vec<u8>, id: &Id) {
    let bytes = id.as_str().as_bytes();
    out.push(bytes.len() as u8);
    out.extend_from_slice(bytes);
}

fn put_scope(out: &mut Vec<u8>, scope: &Scope) {
    for id in [
        scope.receiver(),
        scope.origin(),
        scope.stream(),
        scope.incarnation(),
        scope.schema(),
        scope.access_epoch(),
    ] {
        put_id(out, id);
    }
}

fn put_history_limits(
    out: &mut Vec<u8>,
    generation: u64,
    count: usize,
    bytes: usize,
    record: usize,
) {
    for value in [generation, count as u64, bytes as u64, record as u64] {
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn put_wire_record(out: &mut Vec<u8>, record: &Record) {
    out.extend_from_slice(&record.position.to_be_bytes());
    put_id(out, &record.id);
    put_scope(out, &record.scope);
    out.extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&record.payload);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], FrameError> {
        let end = self.offset.checked_add(len).ok_or(FrameError::Malformed)?;
        let part = self
            .bytes
            .get(self.offset..end)
            .ok_or(FrameError::Malformed)?;
        self.offset = end;
        Ok(part)
    }

    fn byte(&mut self) -> Result<u8, FrameError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, FrameError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| FrameError::Malformed)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn id(&mut self) -> Result<Id, FrameError> {
        let len = self.byte()? as usize;
        let value = std::str::from_utf8(self.take(len)?).map_err(|_| FrameError::Malformed)?;
        Id::new(value).map_err(|_| FrameError::Malformed)
    }

    fn scope(&mut self) -> Result<Scope, FrameError> {
        Ok(Scope::new(
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
        ))
    }

    fn history_limits(&mut self) -> Result<(u64, usize, usize, usize), FrameError> {
        Ok((
            self.u64()?,
            usize::try_from(self.u64()?).map_err(|_| FrameError::Malformed)?,
            usize::try_from(self.u64()?).map_err(|_| FrameError::Malformed)?,
            usize::try_from(self.u64()?).map_err(|_| FrameError::Malformed)?,
        ))
    }

    fn wire_record(&mut self, max_record_bytes: usize) -> Result<Record, FrameError> {
        let position = self.u64()?;
        let id = self.id()?;
        let scope = self.scope()?;
        let len = u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| FrameError::Malformed)?,
        ) as usize;
        if len > max_record_bytes {
            return Err(FrameError::TooLarge);
        }
        let payload = self.take(len)?.to_vec();
        Ok(Record {
            position,
            id,
            scope,
            payload,
        })
    }

    fn catalogue_key(&mut self) -> Result<EntryKey, FrameError> {
        Ok(EntryKey {
            creation: self.u64()?,
            id: self.id()?,
        })
    }

    fn catalogue_pass(&mut self) -> Result<CataloguePass, FrameError> {
        let scope = self.scope()?;
        let completed = self.u64()?;
        let boundary = self.u64()?;
        let generation = self.u64()?;
        let cursor = match self.byte()? {
            0 => None,
            1 => Some(self.catalogue_key()?),
            _ => return Err(FrameError::Malformed),
        };
        Ok(CataloguePass {
            scope,
            completed,
            boundary,
            cursor,
            generation,
        })
    }

    fn manifest_entry(&mut self) -> Result<ManifestEntry, FrameError> {
        let key = self.catalogue_key()?;
        let revision = self.u64()?;
        let deleted = match self.byte()? {
            0 => false,
            1 => true,
            _ => return Err(FrameError::Malformed),
        };
        Ok(ManifestEntry {
            key,
            revision,
            deleted,
        })
    }

    fn artifact_key(&mut self) -> Result<ArtifactKey, FrameError> {
        Ok(ArtifactKey {
            scope: self.scope()?,
            id: self.id()?,
        })
    }

    fn artifact_manifest(&mut self) -> Result<ArtifactManifest, FrameError> {
        let key = self.artifact_key()?;
        let revision = self.u64()?;
        let state = match self.byte()? {
            0 => {
                let length = self.u64()?;
                let hash: [u8; 32] = self
                    .take(32)?
                    .try_into()
                    .map_err(|_| FrameError::Malformed)?;
                ArtifactState::Live(ContentIdentity {
                    length,
                    digest: Sha256Digest(hash),
                })
            }
            1 => ArtifactState::Deleted,
            _ => return Err(FrameError::Malformed),
        };
        Ok(ArtifactManifest {
            key,
            revision,
            state,
        })
    }

    fn chunk_request(&mut self) -> Result<ChunkRequest, FrameError> {
        let manifest = self.artifact_manifest()?;
        let offset = self.u64()?;
        let max_bytes = usize::try_from(self.u64()?).map_err(|_| FrameError::Malformed)?;
        Ok(ChunkRequest {
            manifest,
            offset,
            max_bytes,
        })
    }

    fn finish(self) -> Result<(), FrameError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(FrameError::Malformed)
        }
    }
}

fn put_catalogue_key(out: &mut Vec<u8>, key: &EntryKey) {
    out.extend_from_slice(&key.creation.to_be_bytes());
    put_id(out, &key.id);
}

fn put_catalogue_pass(out: &mut Vec<u8>, pass: &CataloguePass) {
    put_scope(out, &pass.scope);
    for value in [pass.completed, pass.boundary, pass.generation] {
        out.extend_from_slice(&value.to_be_bytes());
    }
    match &pass.cursor {
        Some(key) => {
            out.push(1);
            put_catalogue_key(out, key);
        }
        None => out.push(0),
    }
}

#[cfg(feature = "sqlite")]
fn put_manifest_entry(out: &mut Vec<u8>, entry: &ManifestEntry) {
    put_catalogue_key(out, &entry.key);
    out.extend_from_slice(&entry.revision.to_be_bytes());
    out.push(u8::from(entry.deleted));
}

fn put_artifact_key(out: &mut Vec<u8>, key: &ArtifactKey) {
    put_scope(out, &key.scope);
    put_id(out, &key.id);
}

fn put_artifact_manifest(out: &mut Vec<u8>, manifest: &ArtifactManifest) {
    put_artifact_key(out, &manifest.key);
    out.extend_from_slice(&manifest.revision.to_be_bytes());
    match manifest.state {
        ArtifactState::Live(content) => {
            out.push(0);
            out.extend_from_slice(&content.length.to_be_bytes());
            out.extend_from_slice(&content.digest.0);
        }
        ArtifactState::Deleted => out.push(1),
    }
}

fn put_chunk_request(out: &mut Vec<u8>, request: &ChunkRequest) {
    put_artifact_manifest(out, &request.manifest);
    out.extend_from_slice(&request.offset.to_be_bytes());
    out.extend_from_slice(&(request.max_bytes as u64).to_be_bytes());
}

#[cfg(feature = "sqlite")]
fn manifest_source_error_code(error: ManifestSourceError) -> u8 {
    match error {
        ManifestSourceError::Unavailable => 2,
        ManifestSourceError::Denied => 1,
        ManifestSourceError::ScopeChanged => 3,
        ManifestSourceError::Missing => 4,
    }
}

fn decode_manifest_source_error(code: u8) -> ManifestSourceError {
    match code {
        1 => ManifestSourceError::Denied,
        3 => ManifestSourceError::ScopeChanged,
        4 => ManifestSourceError::Missing,
        _ => ManifestSourceError::Unavailable,
    }
}

#[cfg(feature = "sqlite")]
fn chunk_source_error_code(error: ChunkSourceError) -> u8 {
    match error {
        ChunkSourceError::Unavailable => 2,
        ChunkSourceError::Denied => 1,
        ChunkSourceError::ScopeChanged => 3,
        ChunkSourceError::VersionChanged => 4,
        ChunkSourceError::Deleted => 5,
        ChunkSourceError::InvalidRequest => 6,
    }
}

fn decode_chunk_source_error(code: u8) -> ChunkSourceError {
    match code {
        1 => ChunkSourceError::Denied,
        3 => ChunkSourceError::ScopeChanged,
        4 => ChunkSourceError::VersionChanged,
        5 => ChunkSourceError::Deleted,
        6 => ChunkSourceError::InvalidRequest,
        _ => ChunkSourceError::Unavailable,
    }
}

#[cfg(feature = "sqlite")]
fn catalogue_error_code(error: CatalogueSourceError) -> u8 {
    match error {
        CatalogueSourceError::Unavailable => 2,
        CatalogueSourceError::IdentityChanged => 3,
        CatalogueSourceError::InvalidRequest => 4,
        CatalogueSourceError::OversizedEntry => 5,
    }
}

fn decode_catalogue_error(code: u8) -> CatalogueSourceError {
    match code {
        3 => CatalogueSourceError::IdentityChanged,
        4 => CatalogueSourceError::InvalidRequest,
        5 => CatalogueSourceError::OversizedEntry,
        _ => CatalogueSourceError::Unavailable,
    }
}

fn source_error_code(error: SourceError) -> u8 {
    match error {
        SourceError::Unavailable => 2,
        SourceError::Pruned => 3,
        SourceError::InvalidRequest => 4,
        SourceError::IdentityChanged => 5,
        SourceError::OversizedRecord => 6,
    }
}

fn decode_source_error(code: u8) -> SourceError {
    match code {
        3 => SourceError::Pruned,
        4 => SourceError::InvalidRequest,
        5 => SourceError::IdentityChanged,
        6 => SourceError::OversizedRecord,
        _ => SourceError::Unavailable,
    }
}

fn encode_record_page(request: &PageRequest, page: Page) -> Result<Vec<u8>, SourceError> {
    if page.request != *request {
        return Err(SourceError::IdentityChanged);
    }
    if page.records.is_empty() || page.records.len() > request.max_records {
        return Err(SourceError::InvalidRequest);
    }
    let count = u8::try_from(page.records.len()).map_err(|_| SourceError::InvalidRequest)?;
    let mut bytes = 0usize;
    let mut next = request.after;
    let mut ids = HashSet::new();
    for record in &page.records {
        if record.scope != request.scope || !ids.insert(&record.id) {
            return Err(SourceError::IdentityChanged);
        }
        next = next.checked_add(1).ok_or(SourceError::InvalidRequest)?;
        if record.position != next || next > request.target {
            return Err(SourceError::InvalidRequest);
        }
        if record.payload.len() > request.max_record_bytes {
            return Err(SourceError::OversizedRecord);
        }
        bytes = bytes
            .checked_add(record.payload.len())
            .ok_or(SourceError::OversizedRecord)?;
        if bytes > request.max_payload_bytes {
            return Err(SourceError::OversizedRecord);
        }
        u32::try_from(record.payload.len()).map_err(|_| SourceError::OversizedRecord)?;
    }
    let mut response = vec![0];
    put_scope(&mut response, &page.request.scope);
    for value in [
        page.request.after,
        page.request.target,
        page.request.max_records as u64,
        page.request.max_payload_bytes as u64,
        page.request.max_record_bytes as u64,
    ] {
        response.extend_from_slice(&value.to_be_bytes());
    }
    response.push(count);
    for record in &page.records {
        put_wire_record(&mut response, record);
    }
    if response.len() > MAX_FRAME_BYTES {
        return Err(SourceError::OversizedRecord);
    }
    Ok(response)
}

#[cfg(feature = "sqlite")]
fn history_error_code(error: HistorySourceError) -> u8 {
    match error {
        HistorySourceError::Unavailable => 2,
        HistorySourceError::ResetRequired => 3,
        HistorySourceError::InvalidRequest => 4,
        HistorySourceError::IdentityChanged => 5,
        HistorySourceError::OversizedRecord => 6,
    }
}

fn decode_history_error(code: u8) -> HistorySourceError {
    match code {
        3 => HistorySourceError::ResetRequired,
        4 => HistorySourceError::InvalidRequest,
        5 => HistorySourceError::IdentityChanged,
        6 => HistorySourceError::OversizedRecord,
        _ => HistorySourceError::Unavailable,
    }
}

/// Explicit local development credentials and reference-source identity.
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct LoopbackConfig {
    /// Source SQLite path, created only if absent.
    pub source_path: PathBuf,
    /// Optional, separate current-value catalogue SQLite source file.
    pub catalogue_source_path: Option<PathBuf>,
    /// Optional separate artifact-source SQLite file for the local lab.
    pub artifact_source_path: Option<PathBuf>,
    /// Optional one-shot development fault; never a production transport mode.
    pub catalogue_fault: Option<CatalogueFault>,
    /// Exact source origin.
    pub origin: Id,
    /// Exact source stream.
    pub stream: Id,
    /// Source incarnation.
    pub incarnation: Id,
    /// Payload schema identity.
    pub schema: Id,
    /// Current access epoch.
    pub access_epoch: Id,
    /// Development read credential; never inferred from receiver identity.
    pub read_token: Id,
    /// Explicit receivers allowed to use that credential in this local lab.
    pub allowed_receivers: Vec<Id>,
    /// Separate development append credential.
    pub write_token: Id,
}

#[cfg(feature = "sqlite")]
impl LoopbackConfig {
    fn open_source(&self) -> Result<SqliteReferenceSource, FrameError> {
        SqliteReferenceSource::open(
            &self.source_path,
            self.origin.clone(),
            self.stream.clone(),
            self.incarnation.clone(),
            self.schema.clone(),
        )
        .map_err(|_| FrameError::Malformed)
    }

    fn open_catalogue_source(&self) -> Result<SqliteCatalogueSource, FrameError> {
        let path = self
            .catalogue_source_path
            .as_ref()
            .ok_or(FrameError::Malformed)?;
        SqliteCatalogueSource::open(
            path,
            self.origin.clone(),
            self.stream.clone(),
            self.incarnation.clone(),
            self.schema.clone(),
        )
        .map_err(|_| FrameError::Malformed)
    }

    fn open_artifact_source(&self) -> Result<SqliteArtifactSource, FrameError> {
        let path = self
            .artifact_source_path
            .as_ref()
            .ok_or(FrameError::Malformed)?;
        SqliteArtifactSource::open(
            path,
            self.origin.clone(),
            self.stream.clone(),
            self.incarnation.clone(),
            self.schema.clone(),
        )
        .map_err(|_| FrameError::Malformed)
    }

    fn matches(&self, scope: &Scope) -> bool {
        scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
            && scope.access_epoch() == &self.access_epoch
    }
}

#[cfg(feature = "sqlite")]
#[derive(Default)]
struct ServerCounters {
    head_reads: AtomicU64,
    page_reads: AtomicU64,
    refused_reads: AtomicU64,
    tail_reads: AtomicU64,
    older_reads: AtomicU64,
    history_payload_bytes: AtomicU64,
    catalogue_head_reads: AtomicU64,
    catalogue_manifest_reads: AtomicU64,
    catalogue_resolve_reads: AtomicU64,
    catalogue_fault_claimed: AtomicBool,
}

/// Loopback source server with one independent thread and SQLite handle per
/// connection; a blocked receiver socket does not hold a source transaction.
#[cfg(feature = "sqlite")]
pub struct LoopbackServer {
    listener: TcpListener,
    config: Arc<LoopbackConfig>,
    wake: Arc<(Mutex<u64>, Condvar)>,
    counters: Arc<ServerCounters>,
}

#[cfg(feature = "sqlite")]
impl LoopbackServer {
    /// Binds only the IPv4 loopback address. Port zero requests an OS-chosen port.
    pub fn bind(port: u16, config: LoopbackConfig) -> io::Result<Self> {
        if config.read_token == config.write_token {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read and write credentials must differ",
            ));
        }
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            wake: Arc::new((Mutex::new(0), Condvar::new())),
            counters: Arc::new(ServerCounters::default()),
        })
    }

    /// Returns the local-only address bound by the server.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves connections until the process exits. Each connection has finite
    /// read/write timeouts; subscription waits carry only a coalesced generation.
    pub fn serve(self) -> io::Result<()> {
        for incoming in self.listener.incoming() {
            let stream = incoming?;
            let config = Arc::clone(&self.config);
            let wake = Arc::clone(&self.wake);
            let counters = Arc::clone(&self.counters);
            thread::spawn(move || {
                let _ = serve_one(stream, &config, &wake, &counters);
            });
        }
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
fn serve_one(
    mut stream: TcpStream,
    config: &LoopbackConfig,
    wake: &(Mutex<u64>, Condvar),
    counters: &ServerCounters,
) -> Result<(), FrameError> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let frame = read_frame(&mut stream)?;
    let mut input = Decoder::new(&frame);
    let operation = input.byte()?;
    let token = input.id()?;
    match operation {
        1..=3 => {
            let scope = input.scope()?;
            if token != config.read_token
                || !config.matches(&scope)
                || !config.allowed_receivers.contains(scope.receiver())
            {
                counters.refused_reads.fetch_add(1, Ordering::Relaxed);
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            if operation == 1 {
                input.finish()?;
                write_frame(&mut stream, &[0])?;
            } else if operation == 2 {
                input.finish()?;
                counters.head_reads.fetch_add(1, Ordering::Relaxed);
                let result = config.open_source()?.head(&scope);
                let mut response = Vec::with_capacity(9);
                match result {
                    Ok(head) => {
                        response.push(0);
                        response.extend_from_slice(&head.to_be_bytes());
                    }
                    Err(error) => response.push(source_error_code(error)),
                }
                write_frame(&mut stream, &response)?;
            } else {
                let after = input.u64()?;
                let target = input.u64()?;
                let count = input.u64()?;
                let bytes = input.u64()?;
                let record_bytes = input.u64()?;
                input.finish()?;
                if count == 0
                    || count > MAX_PAGE_RECORDS as u64
                    || bytes == 0
                    || bytes > MAX_PAGE_PAYLOAD as u64
                    || record_bytes == 0
                    || record_bytes > MAX_PAGE_PAYLOAD as u64
                {
                    write_frame(&mut stream, &[4])?;
                    return Ok(());
                }
                let request = PageRequest {
                    scope,
                    after,
                    target,
                    max_records: count as usize,
                    max_payload_bytes: bytes as usize,
                    max_record_bytes: record_bytes as usize,
                };
                counters.page_reads.fetch_add(1, Ordering::Relaxed);
                let result = config.open_source()?.page(&request);
                let response = match result {
                    Ok(page) => encode_record_page(&request, page)
                        .unwrap_or_else(|error| vec![source_error_code(error)]),
                    Err(error) => vec![source_error_code(error)],
                };
                write_frame(&mut stream, &response)?;
            }
        }
        4 => {
            let scope = input.scope()?;
            input.finish()?;
            if token != config.read_token
                || !config.matches(&scope)
                || !config.allowed_receivers.contains(scope.receiver())
            {
                counters.refused_reads.fetch_add(1, Ordering::Relaxed);
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let (lock, signal) = wake;
            // Register the generation before acknowledging subscription.
            // A commit after the ACK cannot fall between the host's head
            // check and the server's subscription baseline.
            let seen = *lock.lock().map_err(|_| FrameError::Malformed)?;
            write_frame(&mut stream, &[0])?;
            loop {
                let guard = lock.lock().map_err(|_| FrameError::Malformed)?;
                let (guard, wait) = signal
                    .wait_timeout_while(guard, Duration::from_secs(30), |generation| {
                        *generation == seen
                    })
                    .map_err(|_| FrameError::Malformed)?;
                if *guard != seen {
                    drop(guard);
                    write_frame(&mut stream, &[7])?;
                    // One subscription delivers at most one wake. The host
                    // catches up, then re-subscribes before its next head
                    // check. Bursts cannot queue one hint per source append.
                    return Ok(());
                } else if wait.timed_out() {
                    drop(guard);
                    // A bounded keepalive detects a peer that vanished while
                    // no facts changed, releasing this subscription thread.
                    write_frame(&mut stream, &[8])?;
                }
            }
        }
        5 => {
            let id = input.id()?;
            let len = u32::from_be_bytes(
                input
                    .take(4)?
                    .try_into()
                    .map_err(|_| FrameError::Malformed)?,
            ) as usize;
            let payload = input.take(len)?;
            input.finish()?;
            if token != config.write_token {
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let mut response = Vec::new();
            match config.open_source()?.append(&id, payload) {
                Ok(position) => {
                    response.push(0);
                    response.extend_from_slice(&position.to_be_bytes());
                    let (lock, signal) = wake;
                    let mut generation = lock.lock().map_err(|_| FrameError::Malformed)?;
                    *generation = generation.wrapping_add(1);
                    signal.notify_all();
                }
                Err(_) => response.push(2),
            }
            write_frame(&mut stream, &response)?;
        }
        6 => {
            input.finish()?;
            if token != config.write_token {
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let mut response = vec![0];
            for value in [
                &counters.head_reads,
                &counters.page_reads,
                &counters.refused_reads,
            ] {
                response.extend_from_slice(&value.load(Ordering::Relaxed).to_be_bytes());
            }
            write_frame(&mut stream, &response)?;
        }
        7 | 8 => {
            let scope = input.scope()?;
            if token != config.read_token
                || !config.matches(&scope)
                || !config.allowed_receivers.contains(scope.receiver())
            {
                counters.refused_reads.fetch_add(1, Ordering::Relaxed);
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let before = if operation == 8 {
                Some(input.u64()?)
            } else {
                None
            };
            let (generation, count, bytes, record_bytes) = input.history_limits()?;
            input.finish()?;
            if generation == 0
                || count == 0
                || count > MAX_PAGE_RECORDS
                || bytes == 0
                || bytes > MAX_PAGE_PAYLOAD
                || record_bytes == 0
                || record_bytes > MAX_PAGE_PAYLOAD
            {
                write_frame(&mut stream, &[4])?;
                return Ok(());
            }
            let mut source = config.open_source()?;
            let mut response = Vec::new();
            if let Some(before) = before {
                counters.older_reads.fetch_add(1, Ordering::Relaxed);
                let request = OlderRequest {
                    scope,
                    generation,
                    before,
                    max_records: count,
                    max_payload_bytes: bytes,
                    max_record_bytes: record_bytes,
                };
                match source.older(&request) {
                    Ok(page) => {
                        response.push(0);
                        put_scope(&mut response, &page.request.scope);
                        response.extend_from_slice(&page.request.before.to_be_bytes());
                        put_history_limits(
                            &mut response,
                            page.request.generation,
                            page.request.max_records,
                            page.request.max_payload_bytes,
                            page.request.max_record_bytes,
                        );
                        response.extend_from_slice(&page.oldest_available.to_be_bytes());
                        response.push(page.records.len() as u8);
                        for record in &page.records {
                            put_wire_record(&mut response, record);
                        }
                        counters.history_payload_bytes.fetch_add(
                            page.records
                                .iter()
                                .map(|record| record.payload.len() as u64)
                                .sum(),
                            Ordering::Relaxed,
                        );
                    }
                    Err(error) => response.push(history_error_code(error)),
                }
            } else {
                counters.tail_reads.fetch_add(1, Ordering::Relaxed);
                let request = TailRequest {
                    scope,
                    generation,
                    max_records: count,
                    max_payload_bytes: bytes,
                    max_record_bytes: record_bytes,
                };
                match source.tail(&request) {
                    Ok(snapshot) => {
                        response.push(0);
                        put_scope(&mut response, &snapshot.request.scope);
                        put_history_limits(
                            &mut response,
                            snapshot.request.generation,
                            snapshot.request.max_records,
                            snapshot.request.max_payload_bytes,
                            snapshot.request.max_record_bytes,
                        );
                        for value in [
                            snapshot.watermark,
                            snapshot.first,
                            snapshot.oldest_available,
                        ] {
                            response.extend_from_slice(&value.to_be_bytes());
                        }
                        response.push(snapshot.records.len() as u8);
                        for record in &snapshot.records {
                            put_wire_record(&mut response, record);
                        }
                        counters.history_payload_bytes.fetch_add(
                            snapshot
                                .records
                                .iter()
                                .map(|record| record.payload.len() as u64)
                                .sum(),
                            Ordering::Relaxed,
                        );
                    }
                    Err(error) => response.push(history_error_code(error)),
                }
            }
            write_frame(&mut stream, &response)?;
        }
        9 => {
            input.finish()?;
            if token != config.write_token {
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let mut response = vec![0];
            for value in [
                &counters.tail_reads,
                &counters.older_reads,
                &counters.history_payload_bytes,
            ] {
                response.extend_from_slice(&value.load(Ordering::Relaxed).to_be_bytes());
            }
            write_frame(&mut stream, &response)?;
        }
        10 => {
            let position = input.u64()?;
            input.finish()?;
            if token != config.write_token {
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let result = config.open_source()?.prune_through(position);
            write_frame(&mut stream, &[if result.is_ok() { 0 } else { 2 }])?;
        }
        11..=13 => {
            let scope = if operation == 12 {
                let pass = input.catalogue_pass()?;
                let count = input.u64()?;
                input.finish()?;
                if !catalogue_access(config, &token, &pass.scope, counters, &mut stream)? {
                    return Ok(());
                }
                if count == 0
                    || count > MAX_CATALOGUE_ENTRIES as u64
                    || pass.generation == 0
                    || pass.boundary <= pass.completed
                {
                    write_frame(&mut stream, &[4])?;
                    return Ok(());
                }
                let request = ManifestRequest {
                    pass,
                    max_entries: count as usize,
                };
                counters
                    .catalogue_manifest_reads
                    .fetch_add(1, Ordering::Relaxed);
                let result = config.open_catalogue_source()?.manifest(&request);
                let mut response = Vec::new();
                match result {
                    Ok(page) => {
                        response.push(0);
                        put_catalogue_pass(&mut response, &page.request.pass);
                        response
                            .extend_from_slice(&(page.request.max_entries as u64).to_be_bytes());
                        response.push(u8::from(page.has_more));
                        response.extend_from_slice(&(page.entries.len() as u16).to_be_bytes());
                        for entry in &page.entries {
                            put_manifest_entry(&mut response, entry);
                        }
                    }
                    Err(error) => response.push(catalogue_error_code(error)),
                }
                write_catalogue_frame(&mut stream, &response, config, counters)?;
                return Ok(());
            } else if operation == 13 {
                let pass = input.catalogue_pass()?;
                let id = input.id()?;
                let max = input.u64()?;
                input.finish()?;
                let scope = pass.scope.clone();
                if !catalogue_access(config, &token, &scope, counters, &mut stream)? {
                    return Ok(());
                }
                if max == 0
                    || max > MAX_PAGE_PAYLOAD as u64
                    || pass.generation == 0
                    || pass.boundary <= pass.completed
                {
                    write_frame(&mut stream, &[4])?;
                    return Ok(());
                }
                counters
                    .catalogue_resolve_reads
                    .fetch_add(1, Ordering::Relaxed);
                let mut response = Vec::new();
                match config
                    .open_catalogue_source()?
                    .resolve(&pass, &id, max as usize)
                {
                    Ok(entry) => {
                        response.push(0);
                        put_catalogue_pass(&mut response, &pass);
                        put_id(&mut response, &id);
                        response.extend_from_slice(&max.to_be_bytes());
                        put_manifest_entry(&mut response, &entry.manifest);
                        response.extend_from_slice(&(entry.payload.len() as u32).to_be_bytes());
                        response.extend_from_slice(&entry.payload);
                    }
                    Err(error) => response.push(catalogue_error_code(error)),
                }
                write_catalogue_frame(&mut stream, &response, config, counters)?;
                return Ok(());
            } else {
                input.scope()?
            };
            input.finish()?;
            if !catalogue_access(config, &token, &scope, counters, &mut stream)? {
                return Ok(());
            }
            let mut response = Vec::new();
            counters
                .catalogue_head_reads
                .fetch_add(1, Ordering::Relaxed);
            match config.open_catalogue_source()?.head(&scope) {
                Ok(head) => {
                    response.push(0);
                    put_scope(&mut response, &scope);
                    response.extend_from_slice(&head.to_be_bytes());
                }
                Err(error) => response.push(catalogue_error_code(error)),
            }
            write_catalogue_frame(&mut stream, &response, config, counters)?;
        }
        14 => {
            input.finish()?;
            if token != config.write_token {
                write_frame(&mut stream, &[1])?;
                return Ok(());
            }
            let mut response = vec![0];
            for value in [
                &counters.catalogue_head_reads,
                &counters.catalogue_manifest_reads,
                &counters.catalogue_resolve_reads,
            ] {
                response.extend_from_slice(&value.load(Ordering::Relaxed).to_be_bytes());
            }
            write_frame(&mut stream, &response)?;
        }
        15 => {
            let key = input.artifact_key()?;
            input.finish()?;
            if !artifact_access(config, &token, &key.scope, counters, &mut stream)? {
                return Ok(());
            }
            let mut response = Vec::new();
            let request = ArtifactManifestRequest { key };
            match config.open_artifact_source()?.manifest(&request) {
                Ok(reply) => {
                    response.push(0);
                    put_artifact_key(&mut response, &reply.request.key);
                    put_artifact_manifest(&mut response, &reply.manifest);
                }
                Err(error) => response.push(manifest_source_error_code(error)),
            }
            write_frame(&mut stream, &response)?;
        }
        16 => {
            let request = input.chunk_request()?;
            input.finish()?;
            if !artifact_access(
                config,
                &token,
                &request.manifest.key.scope,
                counters,
                &mut stream,
            )? {
                return Ok(());
            }
            if request.max_bytes == 0 || request.max_bytes > MAX_CHUNK_BYTES {
                write_frame(&mut stream, &[6])?;
                return Ok(());
            }
            let mut response = Vec::new();
            match config.open_artifact_source()?.chunk(&request) {
                Ok(reply) => {
                    response.push(0);
                    put_chunk_request(&mut response, &reply.request);
                    response.extend_from_slice(&(reply.bytes.len() as u32).to_be_bytes());
                    response.extend_from_slice(&reply.bytes);
                }
                Err(error) => response.push(chunk_source_error_code(error)),
            }
            write_frame(&mut stream, &response)?;
        }
        _ => return Err(FrameError::Malformed),
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn artifact_access(
    config: &LoopbackConfig,
    token: &Id,
    scope: &Scope,
    counters: &ServerCounters,
    stream: &mut TcpStream,
) -> Result<bool, FrameError> {
    if *token != config.read_token
        || !config.matches(scope)
        || !config.allowed_receivers.contains(scope.receiver())
        || config.artifact_source_path.is_none()
    {
        counters.refused_reads.fetch_add(1, Ordering::Relaxed);
        write_frame(stream, &[1])?;
        return Ok(false);
    }
    Ok(true)
}

#[cfg(feature = "sqlite")]
fn write_catalogue_frame(
    stream: &mut TcpStream,
    body: &[u8],
    config: &LoopbackConfig,
    counters: &ServerCounters,
) -> Result<(), FrameError> {
    if let Some(fault) = config.catalogue_fault {
        if counters
            .catalogue_fault_claimed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if fault == CatalogueFault::TruncateFirstReply {
                stream.write_all(&(body.len() as u32).to_be_bytes())?;
                stream.write_all(&body[..1])?;
            }
            return Ok(());
        }
    }
    write_frame(stream, body)
}

#[cfg(feature = "sqlite")]
fn catalogue_access(
    config: &LoopbackConfig,
    token: &Id,
    scope: &Scope,
    counters: &ServerCounters,
    stream: &mut TcpStream,
) -> Result<bool, FrameError> {
    if *token != config.read_token
        || !config.matches(scope)
        || !config.allowed_receivers.contains(scope.receiver())
        || config.catalogue_source_path.is_none()
    {
        counters.refused_reads.fetch_add(1, Ordering::Relaxed);
        write_frame(stream, &[1])?;
        return Ok(false);
    }
    Ok(true)
}

/// Client-side wire accounting. Record payload and framing are separate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireCounters {
    /// Framing, metadata and request bytes after subtracting counted payload
    /// and catalogue descriptor bytes from complete frames.
    pub protocol_bytes: u64,
    /// Source record payload bytes received, including retries.
    pub payload_bytes: u64,
    /// Payload bytes repeated at a position already received by this process.
    /// A fresh process has no memory of earlier wire traffic.
    pub duplicate_bytes: u64,
    /// Explicit remote head checks.
    pub head_checks: u64,
    /// Catalogue descriptor bytes received, including repeated pages.
    pub catalogue_manifest_bytes: u64,
    /// Catalogue current-value bytes received, including repeated responses.
    pub catalogue_payload_bytes: u64,
    /// Repeated catalogue payload bytes observed by this client process.
    pub catalogue_duplicate_bytes: u64,
    /// Artifact content bytes received in complete chunks, including repeats.
    pub artifact_payload_bytes: u64,
    /// Repeated artifact chunk bytes observed by this client process.
    pub artifact_duplicate_bytes: u64,
}

/// One local receiver's bounded network source. This adapter uses a new TCP
/// connection per read so a failed/truncated reply does not poison later reads.
pub struct LoopbackClient {
    address: SocketAddrV4,
    token: Id,
    counters: WireCounters,
    seen_positions: HashMap<Scope, u64>,
    seen_catalogue_revisions: HashMap<(Scope, Id), u64>,
    seen_artifact_chunks: HashSet<(ArtifactKey, u64, u64)>,
}

impl LoopbackClient {
    /// Creates a development client; addresses outside IPv4 loopback are refused.
    pub fn new(address: SocketAddrV4, token: Id) -> io::Result<Self> {
        if *address.ip() != Ipv4Addr::LOCALHOST {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "loopback only",
            ));
        }
        Ok(Self {
            address,
            token,
            counters: WireCounters::default(),
            seen_positions: HashMap::new(),
            seen_catalogue_revisions: HashMap::new(),
            seen_artifact_chunks: HashSet::new(),
        })
    }

    /// Returns accounting accumulated by this client process.
    pub fn counters(&self) -> WireCounters {
        self.counters
    }

    fn exchange(&mut self, request: &[u8]) -> Result<Vec<u8>, FrameError> {
        let mut stream = TcpStream::connect_timeout(&self.address.into(), CONNECT_TIMEOUT)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(&mut stream, request)?;
        self.counters.protocol_bytes += (request.len() + 4) as u64;
        let response = read_frame(&mut stream)?;
        self.counters.protocol_bytes += (response.len() + 4) as u64;
        Ok(response)
    }

    fn request(&self, operation: u8, scope: &Scope) -> Vec<u8> {
        let mut request = vec![operation];
        put_id(&mut request, &self.token);
        put_scope(&mut request, scope);
        request
    }

    /// Opens a coalesced wake stream. Subscribe before catch-up and recheck
    /// head before waiting; an EOF means reconnect and check the head again.
    pub fn subscribe(&mut self, scope: &Scope) -> Result<TcpStream, FrameError> {
        let mut stream = TcpStream::connect_timeout(&self.address.into(), CONNECT_TIMEOUT)?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        let request = self.request(4, scope);
        write_frame(&mut stream, &request)?;
        self.counters.protocol_bytes += (request.len() + 4) as u64;
        let response = read_frame(&mut stream)?;
        self.counters.protocol_bytes += (response.len() + 4) as u64;
        match response.as_slice() {
            [0] => Ok(stream),
            _ => Err(FrameError::Malformed),
        }
    }

    /// Appends a development fact with a separately configured write credential.
    pub fn append(&mut self, id: &Id, payload: &[u8]) -> Result<u64, FrameError> {
        let mut request = vec![5];
        put_id(&mut request, &self.token);
        put_id(&mut request, id);
        let len = u32::try_from(payload.len()).map_err(|_| FrameError::TooLarge)?;
        request.extend_from_slice(&len.to_be_bytes());
        request.extend_from_slice(payload);
        let response = self.exchange(&request)?;
        let mut decoder = Decoder::new(&response);
        if decoder.byte()? != 0 {
            return Err(FrameError::Malformed);
        }
        let position = decoder.u64()?;
        decoder.finish()?;
        Ok(position)
    }

    /// Reads server-side source/refusal counters using the write credential.
    pub fn server_counters(&mut self) -> Result<(u64, u64, u64), FrameError> {
        let mut request = vec![6];
        put_id(&mut request, &self.token);
        let response = self.exchange(&request)?;
        let mut decoder = Decoder::new(&response);
        if decoder.byte()? != 0 {
            return Err(FrameError::Malformed);
        }
        let result = (decoder.u64()?, decoder.u64()?, decoder.u64()?);
        decoder.finish()?;
        Ok(result)
    }

    /// Reads development history-request and payload counters.
    pub fn history_counters(&mut self) -> Result<(u64, u64, u64), FrameError> {
        let mut request = vec![9];
        put_id(&mut request, &self.token);
        let response = self.exchange(&request)?;
        let mut decoder = Decoder::new(&response);
        if decoder.byte()? != 0 {
            return Err(FrameError::Malformed);
        }
        let result = (decoder.u64()?, decoder.u64()?, decoder.u64()?);
        decoder.finish()?;
        Ok(result)
    }

    /// Reads development catalogue-operation counters with the write credential.
    pub fn catalogue_counters(&mut self) -> Result<(u64, u64, u64), FrameError> {
        let mut request = vec![14];
        put_id(&mut request, &self.token);
        let response = self.exchange(&request)?;
        let mut decoder = Decoder::new(&response);
        if decoder.byte()? != 0 {
            return Err(FrameError::Malformed);
        }
        let result = (decoder.u64()?, decoder.u64()?, decoder.u64()?);
        decoder.finish()?;
        Ok(result)
    }

    /// Advances the development source's historical floor. Physical records
    /// remain for immutable ID deduplication; only reads below the floor stop.
    pub fn prune_through(&mut self, position: u64) -> Result<(), FrameError> {
        let mut request = vec![10];
        put_id(&mut request, &self.token);
        request.extend_from_slice(&position.to_be_bytes());
        if self.exchange(&request)? == [0] {
            Ok(())
        } else {
            Err(FrameError::Malformed)
        }
    }
}

impl CatalogueSource for LoopbackClient {
    fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError> {
        let response = self
            .exchange(&self.request(11, scope))
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_catalogue_error(code));
        }
        let echoed = decoder
            .scope()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let head = decoder
            .u64()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        decoder
            .finish()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if echoed != *scope {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        Ok(head)
    }

    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestPage, CatalogueSourceError> {
        if request.max_entries == 0 || request.max_entries > MAX_CATALOGUE_ENTRIES {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let mut body = vec![12];
        put_id(&mut body, &self.token);
        put_catalogue_pass(&mut body, &request.pass);
        body.extend_from_slice(&(request.max_entries as u64).to_be_bytes());
        let response = self
            .exchange(&body)
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_catalogue_error(code));
        }
        let echoed = ManifestRequest {
            pass: decoder
                .catalogue_pass()
                .map_err(|_| CatalogueSourceError::Unavailable)?,
            max_entries: usize::try_from(
                decoder
                    .u64()
                    .map_err(|_| CatalogueSourceError::Unavailable)?,
            )
            .map_err(|_| CatalogueSourceError::InvalidRequest)?,
        };
        if echoed != *request {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let has_more = match decoder
            .byte()
            .map_err(|_| CatalogueSourceError::Unavailable)?
        {
            0 => false,
            1 => true,
            _ => return Err(CatalogueSourceError::Unavailable),
        };
        let count = u16::from_be_bytes(
            decoder
                .take(2)
                .map_err(|_| CatalogueSourceError::Unavailable)?
                .try_into()
                .map_err(|_| CatalogueSourceError::Unavailable)?,
        ) as usize;
        if count > request.max_entries {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let mut entries = Vec::with_capacity(count);
        let mut descriptor_bytes = 0u64;
        for _ in 0..count {
            let entry = decoder
                .manifest_entry()
                .map_err(|_| CatalogueSourceError::Unavailable)?;
            descriptor_bytes += entry.key.id.as_str().len() as u64 + 17;
            entries.push(entry);
        }
        decoder
            .finish()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let page = ManifestPage {
            request: echoed,
            entries,
            has_more,
        };
        crate::replication::catalogue::validate_manifest(request, &page, MAX_CATALOGUE_ENTRIES)
            .map_err(|_| CatalogueSourceError::InvalidRequest)?;
        self.counters.catalogue_manifest_bytes += descriptor_bytes;
        self.counters.protocol_bytes = self
            .counters
            .protocol_bytes
            .saturating_sub(descriptor_bytes);
        Ok(page)
    }

    fn resolve(
        &mut self,
        pass: &CataloguePass,
        id: &Id,
        max_payload_bytes: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        if max_payload_bytes == 0 || max_payload_bytes > MAX_PAGE_PAYLOAD {
            return Err(CatalogueSourceError::InvalidRequest);
        }
        let mut body = vec![13];
        put_id(&mut body, &self.token);
        put_catalogue_pass(&mut body, pass);
        put_id(&mut body, id);
        body.extend_from_slice(&(max_payload_bytes as u64).to_be_bytes());
        let response = self
            .exchange(&body)
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_catalogue_error(code));
        }
        let echoed_pass = decoder
            .catalogue_pass()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let echoed_id = decoder
            .id()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let echoed_max = decoder
            .u64()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if echoed_pass != *pass || echoed_id != *id || echoed_max != max_payload_bytes as u64 {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        let manifest = decoder
            .manifest_entry()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        let len = u32::from_be_bytes(
            decoder
                .take(4)
                .map_err(|_| CatalogueSourceError::Unavailable)?
                .try_into()
                .map_err(|_| CatalogueSourceError::Unavailable)?,
        ) as usize;
        if len > max_payload_bytes {
            return Err(CatalogueSourceError::OversizedEntry);
        }
        let payload = decoder
            .take(len)
            .map_err(|_| CatalogueSourceError::Unavailable)?
            .to_vec();
        decoder
            .finish()
            .map_err(|_| CatalogueSourceError::Unavailable)?;
        if manifest.key.id != *id || (manifest.deleted && !payload.is_empty()) {
            return Err(CatalogueSourceError::IdentityChanged);
        }
        self.counters.catalogue_payload_bytes += len as u64;
        self.counters.protocol_bytes = self.counters.protocol_bytes.saturating_sub(len as u64);
        let key = (pass.scope.clone(), id.clone());
        if self
            .seen_catalogue_revisions
            .get(&key)
            .is_some_and(|seen| *seen >= manifest.revision)
        {
            self.counters.catalogue_duplicate_bytes += len as u64;
        }
        self.seen_catalogue_revisions.insert(key, manifest.revision);
        Ok(ResolvedEntry { manifest, payload })
    }
}

impl ManifestSource for LoopbackClient {
    fn manifest(
        &mut self,
        request: &ArtifactManifestRequest,
    ) -> Result<ArtifactManifestReply, ManifestSourceError> {
        let mut body = vec![15];
        put_id(&mut body, &self.token);
        put_artifact_key(&mut body, &request.key);
        let response = self
            .exchange(&body)
            .map_err(|_| ManifestSourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| ManifestSourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_manifest_source_error(code));
        }
        let echoed = ArtifactManifestRequest {
            key: decoder
                .artifact_key()
                .map_err(|_| ManifestSourceError::Unavailable)?,
        };
        let manifest = decoder
            .artifact_manifest()
            .map_err(|_| ManifestSourceError::Unavailable)?;
        decoder
            .finish()
            .map_err(|_| ManifestSourceError::Unavailable)?;
        let reply = ArtifactManifestReply {
            request: echoed,
            manifest,
        };
        crate::replication::artifacts::validate_manifest(request, &reply, None)
            .map_err(|_| ManifestSourceError::ScopeChanged)?;
        Ok(reply)
    }
}

impl ChunkSource for LoopbackClient {
    fn chunk(&mut self, request: &ChunkRequest) -> Result<ChunkReply, ChunkSourceError> {
        if request.max_bytes == 0 || request.max_bytes > MAX_CHUNK_BYTES {
            return Err(ChunkSourceError::InvalidRequest);
        }
        let mut body = vec![16];
        put_id(&mut body, &self.token);
        put_chunk_request(&mut body, request);
        let response = self
            .exchange(&body)
            .map_err(|_| ChunkSourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder.byte().map_err(|_| ChunkSourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_chunk_source_error(code));
        }
        let echoed = decoder
            .chunk_request()
            .map_err(|_| ChunkSourceError::Unavailable)?;
        if echoed != *request {
            return Err(ChunkSourceError::VersionChanged);
        }
        let length = u32::from_be_bytes(
            decoder
                .take(4)
                .map_err(|_| ChunkSourceError::Unavailable)?
                .try_into()
                .map_err(|_| ChunkSourceError::Unavailable)?,
        ) as usize;
        if length > MAX_CHUNK_BYTES {
            return Err(ChunkSourceError::InvalidRequest);
        }
        let bytes = decoder
            .take(length)
            .map_err(|_| ChunkSourceError::Unavailable)?
            .to_vec();
        decoder
            .finish()
            .map_err(|_| ChunkSourceError::Unavailable)?;
        let reply = ChunkReply {
            request: echoed,
            bytes,
        };
        validate_chunk(request, &reply).map_err(|_| ChunkSourceError::InvalidRequest)?;
        self.counters.artifact_payload_bytes += length as u64;
        self.counters.protocol_bytes = self.counters.protocol_bytes.saturating_sub(length as u64);
        if !self.seen_artifact_chunks.insert((
            request.manifest.key.clone(),
            request.manifest.revision,
            request.offset,
        )) {
            self.counters.artifact_duplicate_bytes += length as u64;
        }
        Ok(reply)
    }
}

fn decode_history_records(
    decoder: &mut Decoder<'_>,
    max_records: usize,
    max_payload_bytes: usize,
    max_record_bytes: usize,
) -> Result<(Vec<Record>, usize), HistorySourceError> {
    let count = decoder
        .byte()
        .map_err(|_| HistorySourceError::Unavailable)? as usize;
    if count > max_records {
        return Err(HistorySourceError::InvalidRequest);
    }
    let mut records = Vec::with_capacity(count);
    let mut bytes = 0_usize;
    for _ in 0..count {
        let record = decoder
            .wire_record(max_record_bytes)
            .map_err(|_| HistorySourceError::Unavailable)?;
        bytes = bytes
            .checked_add(record.payload.len())
            .ok_or(HistorySourceError::InvalidRequest)?;
        if bytes > max_payload_bytes {
            return Err(HistorySourceError::InvalidRequest);
        }
        records.push(record);
    }
    Ok((records, bytes))
}

impl HistorySource for LoopbackClient {
    fn tail(&mut self, request: &TailRequest) -> Result<TailSnapshot, HistorySourceError> {
        if request.max_records > MAX_PAGE_RECORDS
            || request.max_payload_bytes > MAX_PAGE_PAYLOAD
            || request.max_record_bytes > MAX_PAGE_PAYLOAD
        {
            return Err(HistorySourceError::InvalidRequest);
        }
        let mut body = self.request(7, &request.scope);
        put_history_limits(
            &mut body,
            request.generation,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        );
        let response = self
            .exchange(&body)
            .map_err(|_| HistorySourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| HistorySourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_history_error(code));
        }
        let scope = decoder
            .scope()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let (generation, max_records, max_payload_bytes, max_record_bytes) = decoder
            .history_limits()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let watermark = decoder.u64().map_err(|_| HistorySourceError::Unavailable)?;
        let first = decoder.u64().map_err(|_| HistorySourceError::Unavailable)?;
        let oldest_available = decoder.u64().map_err(|_| HistorySourceError::Unavailable)?;
        let (records, bytes) = decode_history_records(
            &mut decoder,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        )?;
        decoder
            .finish()
            .map_err(|_| HistorySourceError::Unavailable)?;
        self.counters.payload_bytes += bytes as u64;
        self.counters.protocol_bytes = self.counters.protocol_bytes.saturating_sub(bytes as u64);
        Ok(TailSnapshot {
            request: TailRequest {
                scope,
                generation,
                max_records,
                max_payload_bytes,
                max_record_bytes,
            },
            watermark,
            first,
            oldest_available,
            records,
        })
    }

    fn older(&mut self, request: &OlderRequest) -> Result<OlderPage, HistorySourceError> {
        if request.max_records > MAX_PAGE_RECORDS
            || request.max_payload_bytes > MAX_PAGE_PAYLOAD
            || request.max_record_bytes > MAX_PAGE_PAYLOAD
        {
            return Err(HistorySourceError::InvalidRequest);
        }
        let mut body = self.request(8, &request.scope);
        body.extend_from_slice(&request.before.to_be_bytes());
        put_history_limits(
            &mut body,
            request.generation,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        );
        let response = self
            .exchange(&body)
            .map_err(|_| HistorySourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder
            .byte()
            .map_err(|_| HistorySourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_history_error(code));
        }
        let scope = decoder
            .scope()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let before = decoder.u64().map_err(|_| HistorySourceError::Unavailable)?;
        let (generation, max_records, max_payload_bytes, max_record_bytes) = decoder
            .history_limits()
            .map_err(|_| HistorySourceError::Unavailable)?;
        let oldest_available = decoder.u64().map_err(|_| HistorySourceError::Unavailable)?;
        let (records, bytes) = decode_history_records(
            &mut decoder,
            request.max_records,
            request.max_payload_bytes,
            request.max_record_bytes,
        )?;
        decoder
            .finish()
            .map_err(|_| HistorySourceError::Unavailable)?;
        self.counters.payload_bytes += bytes as u64;
        self.counters.protocol_bytes = self.counters.protocol_bytes.saturating_sub(bytes as u64);
        Ok(OlderPage {
            request: OlderRequest {
                scope,
                generation,
                before,
                max_records,
                max_payload_bytes,
                max_record_bytes,
            },
            oldest_available,
            records,
        })
    }
}

impl ScopeAuthorizer for LoopbackClient {
    fn authorize(&mut self, scope: &Scope) -> Access {
        match self.exchange(&self.request(1, scope)) {
            Ok(response) if response == [0] => Access::Allowed(scope.clone()),
            Ok(response) if response == [1] => Access::Denied,
            _ => Access::Unverifiable,
        }
    }
}

impl RecordSource for LoopbackClient {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.counters.head_checks += 1;
        let response = self
            .exchange(&self.request(2, scope))
            .map_err(|_| SourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder.byte().map_err(|_| SourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_source_error(code));
        }
        let result = decoder.u64().map_err(|_| SourceError::Unavailable)?;
        decoder.finish().map_err(|_| SourceError::Unavailable)?;
        Ok(result)
    }

    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        if request.max_records > MAX_PAGE_RECORDS
            || request.max_payload_bytes > MAX_PAGE_PAYLOAD
            || request.max_record_bytes > MAX_PAGE_PAYLOAD
        {
            return Err(SourceError::InvalidRequest);
        }
        let mut body = self.request(3, &request.scope);
        for value in [
            request.after,
            request.target,
            request.max_records as u64,
            request.max_payload_bytes as u64,
            request.max_record_bytes as u64,
        ] {
            body.extend_from_slice(&value.to_be_bytes());
        }
        let response = self.exchange(&body).map_err(|_| SourceError::Unavailable)?;
        let mut decoder = Decoder::new(&response);
        let code = decoder.byte().map_err(|_| SourceError::Unavailable)?;
        if code != 0 {
            return Err(decode_source_error(code));
        }
        let echoed = PageRequest {
            scope: decoder.scope().map_err(|_| SourceError::Unavailable)?,
            after: decoder.u64().map_err(|_| SourceError::Unavailable)?,
            target: decoder.u64().map_err(|_| SourceError::Unavailable)?,
            max_records: usize::try_from(decoder.u64().map_err(|_| SourceError::Unavailable)?)
                .map_err(|_| SourceError::InvalidRequest)?,
            max_payload_bytes: usize::try_from(
                decoder.u64().map_err(|_| SourceError::Unavailable)?,
            )
            .map_err(|_| SourceError::InvalidRequest)?,
            max_record_bytes: usize::try_from(decoder.u64().map_err(|_| SourceError::Unavailable)?)
                .map_err(|_| SourceError::InvalidRequest)?,
        };
        let count = decoder.byte().map_err(|_| SourceError::Unavailable)? as usize;
        if count > request.max_records {
            return Err(SourceError::InvalidRequest);
        }
        let mut records = Vec::with_capacity(count);
        let mut bytes = 0usize;
        let mut duplicate = 0usize;
        let seen = self
            .seen_positions
            .get(&request.scope)
            .copied()
            .unwrap_or(0);
        let mut highest = seen;
        for _ in 0..count {
            let position = decoder.u64().map_err(|_| SourceError::Unavailable)?;
            let id = decoder.id().map_err(|_| SourceError::Unavailable)?;
            let record_scope = decoder.scope().map_err(|_| SourceError::Unavailable)?;
            let len: [u8; 4] = decoder
                .take(4)
                .map_err(|_| SourceError::Unavailable)?
                .try_into()
                .map_err(|_| SourceError::Unavailable)?;
            let len = u32::from_be_bytes(len) as usize;
            bytes = bytes.checked_add(len).ok_or(SourceError::InvalidRequest)?;
            if len > request.max_record_bytes || bytes > request.max_payload_bytes {
                return Err(SourceError::InvalidRequest);
            }
            let payload = decoder
                .take(len)
                .map_err(|_| SourceError::Unavailable)?
                .to_vec();
            if position <= seen {
                duplicate += len;
            }
            highest = highest.max(position);
            records.push(Record {
                position,
                id,
                scope: record_scope,
                payload,
            });
        }
        decoder.finish().map_err(|_| SourceError::Unavailable)?;
        self.counters.payload_bytes += bytes as u64;
        self.counters.duplicate_bytes += duplicate as u64;
        self.counters.protocol_bytes = self.counters.protocol_bytes.saturating_sub(bytes as u64);
        self.seen_positions.insert(request.scope.clone(), highest);
        Ok(Page {
            request: echoed,
            records,
        })
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }
    fn test_scope() -> Scope {
        Scope::new(
            id("receiver"),
            id("origin"),
            id("index"),
            id("first"),
            id("opaque"),
            id("epoch"),
        )
    }
    fn fake_reply(response: Vec<u8>) -> LoopbackClient {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = match listener.local_addr().unwrap() {
            std::net::SocketAddr::V4(address) => address,
            _ => unreachable!(),
        };
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream).unwrap();
            write_frame(&mut stream, &response).unwrap();
        });
        LoopbackClient::new(address, id("read")).unwrap()
    }

    #[test]
    fn oversized_header_is_rejected_without_reading_a_body() {
        let bytes = ((MAX_FRAME_BYTES + 1) as u32).to_be_bytes();
        assert!(matches!(
            read_frame(&mut bytes.as_slice()),
            Err(FrameError::TooLarge)
        ));
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let bytes = [0, 0, 0, 4, 1, 2];
        assert!(matches!(
            read_frame(&mut bytes.as_slice()),
            Err(FrameError::Io(_))
        ));
    }

    #[test]
    fn read_and_write_credential_must_differ() {
        let value = |text| Id::new(text).unwrap();
        let config = LoopbackConfig {
            source_path: PathBuf::from("unused.db"),
            catalogue_source_path: None,
            artifact_source_path: None,
            catalogue_fault: None,
            origin: value("origin"),
            stream: value("stream"),
            incarnation: value("first"),
            schema: value("schema"),
            access_epoch: value("epoch"),
            read_token: value("same"),
            write_token: value("same"),
            allowed_receivers: vec![value("receiver")],
        };
        assert_eq!(
            LoopbackServer::bind(0, config).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn catalogue_wire_rejects_stale_pass_echo() {
        let request = ManifestRequest {
            pass: CataloguePass {
                scope: test_scope(),
                completed: 0,
                boundary: 2,
                cursor: None,
                generation: 2,
            },
            max_entries: 40,
        };
        let mut response = vec![0];
        let mut stale = request.pass.clone();
        stale.generation = 1;
        put_catalogue_pass(&mut response, &stale);
        response.extend_from_slice(&40_u64.to_be_bytes());
        response.push(0);
        response.extend_from_slice(&0_u16.to_be_bytes());
        let mut client = fake_reply(response);
        assert_eq!(
            CatalogueSource::manifest(&mut client, &request),
            Err(CatalogueSourceError::IdentityChanged)
        );
    }

    #[test]
    fn catalogue_wire_rejects_foreign_and_invalid_deletion_payloads() {
        let pass = CataloguePass {
            scope: test_scope(),
            completed: 0,
            boundary: 1,
            cursor: None,
            generation: 1,
        };
        for (echoed_id, deleted, payload) in [
            ("other", false, b"a".as_slice()),
            ("item", true, b"nonempty".as_slice()),
        ] {
            let mut response = vec![0];
            put_catalogue_pass(&mut response, &pass);
            put_id(&mut response, &id("item"));
            response.extend_from_slice(&128_u64.to_be_bytes());
            put_manifest_entry(
                &mut response,
                &ManifestEntry {
                    key: EntryKey {
                        creation: 1,
                        id: id(echoed_id),
                    },
                    revision: 1,
                    deleted,
                },
            );
            response.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            response.extend_from_slice(payload);
            let mut client = fake_reply(response);
            assert_eq!(
                client.resolve(&pass, &id("item"), 128),
                Err(CatalogueSourceError::IdentityChanged)
            );
        }
    }

    #[test]
    fn catalogue_wire_rejects_stale_payload_pass() {
        let pass = CataloguePass {
            scope: test_scope(),
            completed: 0,
            boundary: 2,
            cursor: None,
            generation: 2,
        };
        let mut stale = pass.clone();
        stale.generation = 1;
        let mut response = vec![0];
        put_catalogue_pass(&mut response, &stale);
        put_id(&mut response, &id("item"));
        response.extend_from_slice(&128_u64.to_be_bytes());
        put_manifest_entry(
            &mut response,
            &ManifestEntry {
                key: EntryKey {
                    creation: 1,
                    id: id("item"),
                },
                revision: 1,
                deleted: false,
            },
        );
        response.extend_from_slice(&1_u32.to_be_bytes());
        response.push(b'x');
        let mut client = fake_reply(response);
        assert_eq!(
            client.resolve(&pass, &id("item"), 128),
            Err(CatalogueSourceError::IdentityChanged)
        );
    }

    #[test]
    fn artifact_wire_rejects_foreign_manifest_and_chunk_echoes() {
        let key = ArtifactKey {
            scope: test_scope(),
            id: id("artifact"),
        };
        let manifest = ArtifactManifest {
            key: key.clone(),
            revision: 1,
            state: ArtifactState::Live(ContentIdentity::of(b"four")),
        };
        let request = ArtifactManifestRequest { key: key.clone() };
        let mut manifest_reply = vec![0];
        put_artifact_key(&mut manifest_reply, &request.key);
        let mut foreign = manifest.clone();
        foreign.key.id = id("other");
        put_artifact_manifest(&mut manifest_reply, &foreign);
        let mut client = fake_reply(manifest_reply);
        assert_eq!(
            ManifestSource::manifest(&mut client, &request),
            Err(ManifestSourceError::ScopeChanged)
        );

        let chunk = ChunkRequest {
            manifest,
            offset: 0,
            max_bytes: 4,
        };
        let mut chunk_reply = vec![0];
        let mut stale = chunk.clone();
        stale.manifest.revision = 2;
        put_chunk_request(&mut chunk_reply, &stale);
        chunk_reply.extend_from_slice(&4_u32.to_be_bytes());
        chunk_reply.extend_from_slice(b"four");
        let mut client = fake_reply(chunk_reply);
        assert_eq!(client.chunk(&chunk), Err(ChunkSourceError::VersionChanged));
    }
}
