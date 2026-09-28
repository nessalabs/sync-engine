//! Development-only, loopback-bound framed transport. The server authorizes
//! each request before opening the reference source. No public pairing, TLS,
//! relay, or remote exposure is provided by this adapter.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::replication::application::{Access, RecordSource, ScopeAuthorizer, SourceError};
use crate::replication::domain::{Id, Page, PageRequest, Record, Scope};

use super::SqliteReferenceSource;

/// Maximum complete wire body, checked before allocating a read buffer.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_PAGE_PAYLOAD: usize = 512 * 1024;
const MAX_PAGE_RECORDS: usize = 64;
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

    fn finish(self) -> Result<(), FrameError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(FrameError::Malformed)
        }
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

/// Explicit local development credentials and reference-source identity.
#[derive(Clone)]
pub struct LoopbackConfig {
    /// Source SQLite path, created only if absent.
    pub source_path: PathBuf,
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

    fn matches(&self, scope: &Scope) -> bool {
        scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
            && scope.access_epoch() == &self.access_epoch
    }
}

#[derive(Default)]
struct ServerCounters {
    head_reads: AtomicU64,
    page_reads: AtomicU64,
    refused_reads: AtomicU64,
}

/// Loopback source server with one independent thread and SQLite handle per
/// connection; a blocked receiver socket does not hold a source transaction.
pub struct LoopbackServer {
    listener: TcpListener,
    config: Arc<LoopbackConfig>,
    wake: Arc<(Mutex<u64>, Condvar)>,
    counters: Arc<ServerCounters>,
}

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
                let mut response = Vec::new();
                match result {
                    Ok(page) => {
                        response.push(0);
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
                        response.push(page.records.len() as u8);
                        for record in page.records {
                            response.extend_from_slice(&record.position.to_be_bytes());
                            put_id(&mut response, &record.id);
                            put_scope(&mut response, &record.scope);
                            response
                                .extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
                            response.extend_from_slice(&record.payload);
                        }
                    }
                    Err(error) => response.push(source_error_code(error)),
                }
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
        _ => return Err(FrameError::Malformed),
    }
    Ok(())
}

/// Client-side wire accounting. Record payload and framing are separate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireCounters {
    /// Complete request and response framing bytes sent or received.
    pub protocol_bytes: u64,
    /// Source record payload bytes received, including retries.
    pub payload_bytes: u64,
    /// Payload bytes repeated at a position already received by this process.
    /// A fresh process has no memory of earlier wire traffic.
    pub duplicate_bytes: u64,
    /// Explicit remote head checks.
    pub head_checks: u64,
}

/// One local receiver's bounded network source. This adapter uses a new TCP
/// connection per read so a failed/truncated reply does not poison later reads.
pub struct LoopbackClient {
    address: SocketAddrV4,
    token: Id,
    counters: WireCounters,
    seen_positions: HashMap<Scope, u64>,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
