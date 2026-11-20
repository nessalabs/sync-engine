//! Host-composed TLS 1.3 for the existing serialized channel. The trust
//! store, exact peers and route are chosen by the host, not by wire metadata.
use super::*;
use rustls::{
    client::{
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        Resumption, WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    server::WebPkiClientVerifier,
    CertificateError, ClientConfig, ClientConnection, DigitallySignedStruct, DistinguishedName,
    RootCertStore, ServerConfig, ServerConnection, SignatureScheme, StreamOwned,
};
use std::time::{Duration, Instant};

const PROTOCOL: &[u8] = b"nessa-sync-session/1";

/// Host-supplied TLS certificate chain and private key. Hosts own issuance,
/// storage and rotation; the session does not enumerate or log credentials.
pub struct TlsIdentity {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}
impl TlsIdentity {
    /// Construct a local identity; Rustls checks shape and key consistency
    /// when building the verifier/connect configuration.
    pub fn new(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Self {
        Self { chain, key }
    }
}

/// Host-supplied trust roots and the exact leaf authorized for this endpoint.
/// A trusted sibling leaf is not a matching application peer. Compare DER
/// only after Rustls verifies the chain and handshake proof of possession.
pub struct TlsPeer {
    roots: RootCertStore,
    leaf: CertificateDer<'static>,
}
impl TlsPeer {
    /// Use these roots and this expected leaf, independent of the network route.
    pub fn new(roots: RootCertStore, leaf: CertificateDer<'static>) -> Self {
        Self { roots, leaf }
    }
}

enum Stream {
    Client(StreamOwned<ClientConnection, TcpSessionIo>),
    Server(StreamOwned<ServerConnection, TcpSessionIo>),
}

/// A fully admitted TLS 1.3 session stream. The original TCP interrupt
/// handle fences and shuts down the socket without taking the TLS or RPC
/// lock. This intentionally implements no split/Follow transport.
///
/// ```compile_fail
/// use nessa_sync::replication::infrastructure::session::{DuplexSessionIo, TlsSessionIo};
/// fn requires_split(_: impl DuplexSessionIo) {}
/// fn tls_is_serial_only(tls: TlsSessionIo) { requires_split(tls); }
/// ```
pub struct TlsSessionIo {
    stream: Stream,
}

fn invalid_local(_: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid session TLS configuration",
    )
}

fn verify_peer(
    chain: Option<&[CertificateDer<'_>]>,
    alpn: Option<&[u8]>,
    expected: &CertificateDer<'_>,
) -> io::Result<()> {
    if chain.and_then(|certificates| certificates.first()) != Some(expected)
        || alpn != Some(PROTOCOL)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unexpected session TLS peer or protocol",
        ));
    }
    Ok(())
}

// Rustls calls verify_server_cert before it emits a client certificate in a
// TLS 1.3 handshake. Add the exact host leaf rule here, not after Finish.
struct PinnedServerVerifier {
    standard: Arc<WebPkiServerVerifier>,
    expected_leaf: CertificateDer<'static>,
}

impl std::fmt::Debug for PinnedServerVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PinnedServerVerifier")
    }
}

impl ServerCertVerifier for PinnedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let verified =
            self.standard
                .verify_server_cert(end_entity, intermediates, name, ocsp, now)?;
        if end_entity != &self.expected_leaf {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signed: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.standard.verify_tls12_signature(message, cert, signed)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signed: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.standard.verify_tls13_signature(message, cert, signed)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.standard.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.standard.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        self.standard.root_hint_subjects()
    }
}

impl TlsSessionIo {
    /// Connect with standard server-chain and expected DNS/IP verification,
    /// the exact authorized server leaf, and a required local client identity.
    /// Existing socket read/write timeouts must be finite. An absolute handshake
    /// deadline caps each operation and the prior socket options are restored.
    /// No application stream is exposed on handshake/admission error.
    pub fn connect(
        mut tcp: TcpSessionIo,
        expected_name: ServerName<'static>,
        local: TlsIdentity,
        remote: TlsPeer,
        deadline: Instant,
    ) -> io::Result<Self> {
        admit_tcp(&mut tcp, |tcp| {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let standard = WebPkiServerVerifier::builder_with_provider(
                Arc::new(remote.roots),
                provider.clone(),
            )
            .build()
            .map_err(invalid_local)?;
            let pin = Arc::new(PinnedServerVerifier {
                standard,
                expected_leaf: remote.leaf.clone(),
            });
            let mut config = ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(invalid_local)?
                // This internal adapter only tightens the built-in verifier.
                // No custom or unverified verifier is accepted from the caller.
                .dangerous()
                .with_custom_certificate_verifier(pin)
                .with_client_auth_cert(local.chain, local.key)
                .map_err(invalid_local)?;
            config.alpn_protocols = vec![PROTOCOL.to_vec()];
            config.resumption = Resumption::disabled();
            config.enable_early_data = false;
            let mut conn =
                ClientConnection::new(Arc::new(config), expected_name).map_err(invalid_local)?;
            handshake(tcp, deadline, |io| {
                while conn.is_handshaking() {
                    conn.complete_io(io)?;
                }
                Ok(())
            })?;
            verify_peer(conn.peer_certificates(), conn.alpn_protocol(), &remote.leaf)?;
            Ok(conn)
        })
        .map(|conn| Self {
            stream: Stream::Client(StreamOwned::new(conn, tcp)),
        })
    }

    /// Accept a client only after standard chain/proof verification, required
    /// client authentication, and exact host peer-leaf authorization. A failed
    /// handshake exposes no application stream, policy callback or source.
    /// The host still ties subsequent current scope authorization to this peer.
    pub fn accept(
        mut tcp: TcpSessionIo,
        local: TlsIdentity,
        remote: TlsPeer,
        deadline: Instant,
    ) -> io::Result<Self> {
        admit_tcp(&mut tcp, |tcp| {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let client_verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(remote.roots),
                provider.clone(),
            )
            .build()
            .map_err(invalid_local)?;
            let mut config = ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(invalid_local)?
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(local.chain, local.key)
                .map_err(invalid_local)?;
            config.alpn_protocols = vec![PROTOCOL.to_vec()];
            config.send_tls13_tickets = 0;
            config.max_early_data_size = 0;
            let mut conn = ServerConnection::new(Arc::new(config)).map_err(invalid_local)?;
            handshake(tcp, deadline, |io| {
                while conn.is_handshaking() {
                    conn.complete_io(io)?;
                }
                Ok(())
            })?;
            verify_peer(conn.peer_certificates(), conn.alpn_protocol(), &remote.leaf)?;
            Ok(conn)
        })
        .map(|conn| Self {
            stream: Stream::Server(StreamOwned::new(conn, tcp)),
        })
    }
}

fn admit_tcp<T>(
    tcp: &mut TcpSessionIo,
    admit: impl FnOnce(&mut TcpSessionIo) -> io::Result<T>,
) -> io::Result<T> {
    let result = admit(tcp);
    if result.is_err() {
        tcp.interrupt_handle().interrupt()?;
    }
    result
}

struct HandshakeIo<'a> {
    tcp: &'a mut TcpSessionIo,
    deadline: Instant,
    read_limit: Duration,
    write_limit: Duration,
}

fn remaining(deadline: Instant, limit: Duration) -> io::Result<Duration> {
    let budget = deadline
        .saturating_duration_since(Instant::now())
        .min(limit);
    if budget.is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "session TLS handshake deadline expired",
        ))
    } else {
        Ok(budget)
    }
}
impl Read for HandshakeIo<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.tcp
            .socket
            .set_read_timeout(Some(remaining(self.deadline, self.read_limit)?))?;
        self.tcp.read(out)
    }
}
impl Write for HandshakeIo<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.tcp
            .socket
            .set_write_timeout(Some(remaining(self.deadline, self.write_limit)?))?;
        self.tcp.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        remaining(self.deadline, self.write_limit)?;
        self.tcp.flush()
    }
}

fn handshake(
    tcp: &mut TcpSessionIo,
    deadline: Instant,
    perform: impl FnOnce(&mut HandshakeIo<'_>) -> io::Result<()>,
) -> io::Result<()> {
    let (Some(read_limit), Some(write_limit)) =
        (tcp.socket.read_timeout()?, tcp.socket.write_timeout()?)
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session TLS requires finite socket timeouts",
        ));
    };
    let result = perform(&mut HandshakeIo {
        tcp,
        deadline,
        read_limit,
        write_limit,
    })
    .and_then(|()| remaining(deadline, read_limit).map(|_| ()));
    let read_restored = tcp.socket.set_read_timeout(Some(read_limit));
    let write_restored = tcp.socket.set_write_timeout(Some(write_limit));
    result.and(read_restored).and(write_restored)
}

impl TlsSessionIo {
    fn tcp(&self) -> &TcpSessionIo {
        match &self.stream {
            Stream::Client(io) => &io.sock,
            Stream::Server(io) => &io.sock,
        }
    }
}

impl Read for TlsSessionIo {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Rustls may otherwise return already-buffered bytes without touching
        // the socket. Every operation observes the permanent transport fence.
        self.tcp().interrupt.open()?;
        match &mut self.stream {
            Stream::Client(io) => io.read(out),
            Stream::Server(io) => io.read(out),
        }
    }
}
impl Write for TlsSessionIo {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.tcp().interrupt.open()?;
        match &mut self.stream {
            Stream::Client(io) => io.write(bytes),
            Stream::Server(io) => io.write(bytes),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.tcp().interrupt.open()?;
        match &mut self.stream {
            Stream::Client(io) => io.flush(),
            Stream::Server(io) => io.flush(),
        }
    }
}
impl InterruptibleSessionIo for TlsSessionIo {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        self.tcp().interrupt_handle()
    }
}
