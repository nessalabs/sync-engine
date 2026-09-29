//! Read-only development server for an application-owned record source.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::replication::application::{RecordSource, SourceError};
use crate::replication::domain::{Id, PageRequest, Scope};

use super::{
    encode_record_page, read_frame, source_error_code, write_frame, Decoder, FrameError,
    IO_TIMEOUT, MAX_PAGE_PAYLOAD, MAX_PAGE_RECORDS,
};

/// Exact development read authority for one record source identity.
///
/// The credential grants only authorization, head, and bounded page reads to
/// listed receivers. It is not a production pairing or remote access policy.
#[derive(Clone)]
pub struct LoopbackReadConfig {
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
    /// Development read credential independent of receiver identity.
    pub read_token: Id,
    /// Receivers allowed to use the read credential.
    pub allowed_receivers: Vec<Id>,
}

impl LoopbackReadConfig {
    fn permits(&self, token: &Id, scope: &Scope) -> bool {
        token == &self.read_token
            && scope.origin() == &self.origin
            && scope.stream() == &self.stream
            && scope.incarnation() == &self.incarnation
            && scope.schema() == &self.schema
            && scope.access_epoch() == &self.access_epoch
            && self.allowed_receivers.contains(scope.receiver())
    }
}

/// Loopback-only server for host-owned `RecordSource` implementations.
///
/// `open_source` is called once per authorized head or page request, after
/// request validation. The source is dropped before socket response writes, so
/// a stalled receiver never retains a source handle or transaction. Each
/// connection runs on an independent thread with bounded socket timeouts.
/// `serve` runs until its process exits or the listener fails.
pub struct LoopbackRecordServer<F> {
    listener: TcpListener,
    config: Arc<LoopbackReadConfig>,
    open_source: Arc<F>,
}

impl<F, S> LoopbackRecordServer<F>
where
    F: Fn() -> Result<S, SourceError> + Send + Sync + 'static,
    S: RecordSource + Send + 'static,
{
    /// Binds IPv4 loopback only. Port zero requests an OS-selected port.
    pub fn bind(port: u16, config: LoopbackReadConfig, open_source: F) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            open_source: Arc::new(open_source),
        })
    }

    /// Returns the address bound by this server.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves independent connections until the listener fails or process exits.
    pub fn serve(self) -> io::Result<()> {
        for incoming in self.listener.incoming() {
            let stream = incoming?;
            let config = Arc::clone(&self.config);
            let open_source = Arc::clone(&self.open_source);
            thread::spawn(move || {
                let _ = serve_one(stream, &config, &*open_source);
            });
        }
        Ok(())
    }
}

fn serve_one<F, S>(
    mut stream: TcpStream,
    config: &LoopbackReadConfig,
    open_source: &F,
) -> Result<(), FrameError>
where
    F: Fn() -> Result<S, SourceError>,
    S: RecordSource,
{
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let frame = read_frame(&mut stream)?;
    let mut input = Decoder::new(&frame);
    let operation = input.byte()?;
    let token = input.id()?;
    if !matches!(operation, 1..=3) {
        return Err(FrameError::Malformed);
    }
    let scope = input.scope()?;
    if !config.permits(&token, &scope) {
        write_frame(&mut stream, &[1])?;
        return Ok(());
    }
    let response = match operation {
        1 => {
            input.finish()?;
            vec![0]
        }
        2 => {
            input.finish()?;
            let result = open_source().and_then(|mut source| source.head(&scope));
            match result {
                Ok(head) => {
                    let mut response = vec![0];
                    response.extend_from_slice(&head.to_be_bytes());
                    response
                }
                Err(error) => vec![source_error_code(error)],
            }
        }
        3 => {
            let after = input.u64()?;
            let target = input.u64()?;
            let count = input.u64()?;
            let bytes = input.u64()?;
            let record_bytes = input.u64()?;
            input.finish()?;
            if after >= target
                || count == 0
                || count > MAX_PAGE_RECORDS as u64
                || bytes == 0
                || bytes > MAX_PAGE_PAYLOAD as u64
                || record_bytes == 0
                || record_bytes > MAX_PAGE_PAYLOAD as u64
            {
                vec![source_error_code(SourceError::InvalidRequest)]
            } else {
                let request = PageRequest {
                    scope,
                    after,
                    target,
                    max_records: count as usize,
                    max_payload_bytes: bytes as usize,
                    max_record_bytes: record_bytes as usize,
                };
                let result = open_source().and_then(|mut source| source.page(&request));
                match result {
                    Ok(page) => encode_record_page(&request, page)
                        .unwrap_or_else(|error| vec![source_error_code(error)]),
                    Err(error) => vec![source_error_code(error)],
                }
            }
        }
        _ => unreachable!(),
    };
    write_frame(&mut stream, &response)?;
    Ok(())
}
