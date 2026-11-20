#![cfg(all(feature = "tls", feature = "sqlite"))]

use nessa_sync::replication::{
    application::{begin_pass, finish_pass, Access, ScopeAuthorizer, SourceError},
    domain::{Id, Limits, Scope},
    infrastructure::{
        session::{
            serve_record_session, InterruptibleSessionIo, RecordSession, RecordSessionError,
            SessionLimits, TcpSessionIo, TlsIdentity, TlsPeer, TlsSessionIo,
        },
        SqliteReferenceSource, SqliteReplicaStore,
    },
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore,
};
use std::io::{Read, Write};
use std::{
    fs, io,
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const LIMITS: SessionLimits = SessionLimits {
    control_bytes: 2048,
    bindings: 1,
};
const DEADLINE: Duration = Duration::from_secs(4);
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sync-session-tls-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        #[cfg(unix)]
        let directory = {
            use std::os::unix::fs::DirBuilderExt;
            let mut directory = fs::DirBuilder::new();
            directory.mode(0o700);
            directory
        };
        #[cfg(not(unix))]
        let directory = fs::DirBuilder::new();
        directory.create(&path).unwrap();
        let fixture = Self(path);
        fixture.openssl(&[
            "req",
            "-new",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-subj",
            "/CN=Local session fixture CA",
            "-days",
            "1",
        ]);
        fixture.openssl(&["x509", "-in", "ca.pem", "-outform", "DER", "-out", "ca.der"]);
        for name in ["server", "server-sibling", "client", "sibling"] {
            fixture.leaf(name);
        }
        fixture
    }

    fn openssl(&self, args: &[&str]) {
        let output = Command::new("openssl")
            .current_dir(&self.0)
            .args(args)
            .output()
            .expect("fixture requires OpenSSL CLI");
        assert!(
            output.status.success(),
            "openssl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn leaf(&self, name: &str) {
        let key = format!("{name}.key");
        let req = format!("{name}.req");
        let der = format!("{name}.der");
        let key_der = format!("{name}.key.der");
        let ext = format!("{name}.ext");
        let e_ku = if name.starts_with("server") {
            "serverAuth"
        } else {
            "clientAuth"
        };
        fs::write(
            self.0.join(&ext),
            format!(
                "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage={e_ku}\nsubjectAltName=DNS:server.fixture.test\n"
            ),
        )
        .unwrap();
        self.openssl(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            &key,
            "-out",
            &req,
            "-subj",
            &format!("/CN={name}"),
        ]);
        self.openssl(&[
            "x509",
            "-req",
            "-in",
            &req,
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-extfile",
            &ext,
            "-days",
            "1",
            "-outform",
            "DER",
            "-out",
            &der,
        ]);
        self.openssl(&[
            "pkcs8", "-topk8", "-nocrypt", "-in", &key, "-outform", "DER", "-out", &key_der,
        ]);
    }

    fn cert(&self, name: &str) -> CertificateDer<'static> {
        fs::read(self.0.join(format!("{name}.der"))).unwrap().into()
    }

    fn key(&self, name: &str) -> PrivateKeyDer<'static> {
        let key = fs::read(self.0.join(format!("{name}.key.der"))).unwrap();
        PrivateKeyDer::try_from(key).unwrap()
    }

    fn identity(&self, name: &str) -> TlsIdentity {
        TlsIdentity::new(vec![self.cert(name)], self.key(name))
    }

    fn peer(&self, name: &str) -> TlsPeer {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert("ca")).unwrap();
        TlsPeer::new(roots, self.cert(name))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}

fn scope() -> Scope {
    Scope::new(
        id("peer"),
        id("origin"),
        id("transcript"),
        id("first"),
        id("text"),
        id("epoch"),
    )
}
fn source(path: &Path) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        path,
        id("origin"),
        id("transcript"),
        id("first"),
        id("text"),
    )
    .unwrap()
}
struct Policy;
impl ScopeAuthorizer for Policy {
    fn authorize(&mut self, s: &Scope) -> Access {
        if s == &scope() {
            Access::Allowed(scope())
        } else {
            Access::Denied
        }
    }
}
fn tcp(socket: TcpStream) -> TcpSessionIo {
    socket.set_nodelay(true).unwrap();
    socket.set_read_timeout(Some(DEADLINE)).unwrap();
    socket.set_write_timeout(Some(DEADLINE)).unwrap();
    TcpSessionIo::new(socket).unwrap()
}
fn endpoint() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}

fn pump(mut from: TcpStream, mut to: TcpStream) -> io::Result<u64> {
    let result = io::copy(&mut from, &mut to);
    let _ = to.shutdown(Shutdown::Write);
    result
}
fn relay(target: SocketAddr) -> (SocketAddr, thread::JoinHandle<(u64, u64)>) {
    let socket = endpoint();
    let addr = socket.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (incoming, _) = socket.accept().unwrap();
        let outgoing = TcpStream::connect(target).unwrap();
        for socket in [&incoming, &outgoing] {
            socket.set_read_timeout(Some(DEADLINE)).unwrap();
            socket.set_write_timeout(Some(DEADLINE)).unwrap();
            socket.set_nodelay(true).unwrap();
        }
        let (incoming_copy, outgoing_copy) =
            (incoming.try_clone().unwrap(), outgoing.try_clone().unwrap());
        let up = thread::spawn(move || pump(incoming_copy, outgoing_copy).unwrap());
        let down = pump(outgoing, incoming).unwrap();
        (up.join().unwrap(), down)
    });
    (addr, worker)
}

#[test]
fn mutual_tls_direct_and_opaque_relay_reopen_identically() {
    let fixture = Fixture::new();
    let mut rows = Vec::new();
    for relayed in [false, true] {
        let path = fixture.0.join("source.db");
        if !relayed {
            let mut store = source(&path);
            for n in 1..=3 {
                store
                    .append(&id(&format!("record-{n}")), &vec![n; 1000])
                    .unwrap();
            }
        }
        let server = endpoint();
        let target = server.local_addr().unwrap();
        let (addr, relay) = if relayed {
            let (addr, relay) = relay(target);
            (addr, Some(relay))
        } else {
            (target, None)
        };
        let local = fixture.identity("server");
        let peer = fixture.peer("client");
        let server = thread::spawn(move || {
            let (socket, _) = server.accept().unwrap();
            let mut io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let result =
                serve_record_session(&mut io, 1, LIMITS, &mut Policy, move |_| Ok(source(&path)));
            // Explicit RecordSession close interrupts at the transport layer;
            // it does not promise a graceful TLS close_notify exchange.
            match result {
                Ok(()) => {}
                Err(RecordSessionError::Io(error))
                    if error.kind() == io::ErrorKind::UnexpectedEof => {}
                result => panic!("unexpected server shutdown: {result:?}"),
            }
        });
        let io = TlsSessionIo::connect(
            tcp(TcpStream::connect(addr).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            fixture.identity("client"),
            fixture.peer("server"),
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let session = RecordSession::connect_open_interruptible(io, scope(), LIMITS).unwrap();
        let (mut auth, mut records) = session.parts();
        let cache = fixture
            .0
            .join(if relayed { "relayed.db" } else { "direct.db" });
        let mut store = SqliteReplicaStore::open(&cache).unwrap();
        let limits = Limits::new(3, 3000, 1000).unwrap();
        let mut pass = begin_pass(&scope(), &mut auth, &mut records, &mut store).unwrap();
        finish_pass(&mut pass, limits, &mut auth, &mut records, &mut store).unwrap();
        let wire = session.counters().unwrap();
        assert_eq!(wire.payload_bytes, 3000);
        rows.push((wire.exchanges, wire.protocol_bytes, wire.payload_bytes));
        session.close().unwrap();
        server.join().unwrap();
        if let Some(relay) = relay {
            let (up, down) = relay.join().unwrap();
            assert!(up > 0 && down > 0);
            assert!(up + down > wire.protocol_bytes + wire.payload_bytes);
            eprintln!("TLS relay bytes: client→server {up}, server→client {down}");
        }
        drop(store);
        let actual = SqliteReplicaStore::open(&cache)
            .unwrap()
            .read_after(&scope(), 0, 3, 3000)
            .unwrap();
        assert_eq!(actual.len(), 3);
        for (index, record) in actual.into_iter().enumerate() {
            let n = index + 1;
            assert_eq!(record.position, n as u64);
            assert_eq!(record.id, id(&format!("record-{n}")));
            assert_eq!(record.payload, vec![n as u8; 1000]);
        }
    }
    assert_eq!(rows[0], rows[1]);
    eprintln!("Identical record counts: {:?}", rows[0]);
}

#[test]
fn wrong_dns_and_trusted_sibling_do_not_enter_record_factory() {
    let fixture = Fixture::new();
    for (client_identity, dns) in [
        ("client", "different.fixture.test"),
        ("sibling", "server.fixture.test"),
    ] {
        let listener = endpoint();
        let addr = listener.local_addr().unwrap();
        let local = fixture.identity("server");
        let expected = fixture.peer("client");
        let factory_calls = Arc::new(AtomicU64::new(0));
        let count = factory_calls.clone();
        let worker = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let result =
                TlsSessionIo::accept(tcp(socket), local, expected, Instant::now() + DEADLINE);
            if let Ok(mut io) = result {
                let _ = serve_record_session(
                    &mut io,
                    2,
                    LIMITS,
                    &mut Policy,
                    |_| -> Result<SqliteReferenceSource, SourceError> {
                        count.fetch_add(1, Ordering::SeqCst);
                        Err(SourceError::Unavailable)
                    },
                );
                false
            } else {
                true
            }
        });
        let result = TlsSessionIo::connect(
            tcp(TcpStream::connect(addr).unwrap()),
            ServerName::try_from(dns).unwrap(),
            fixture.identity(client_identity),
            fixture.peer("server"),
            Instant::now() + DEADLINE,
        );
        // A client can finish its part of the handshake before the server's
        // decision arrives; it must still fail the first app-layer exchange.
        if let Ok(io) = result {
            assert!(RecordSession::connect_open_interruptible(io, scope(), LIMITS).is_err());
        }
        assert!(worker.join().unwrap());
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn silent_peer_is_bounded_by_explicit_handshake_deadline() {
    let fixture = Fixture::new();
    let listener = endpoint();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (silent, _) = listener.accept().unwrap();
    let start = Instant::now();
    let result = TlsSessionIo::connect(
        tcp(client),
        ServerName::try_from("server.fixture.test").unwrap(),
        fixture.identity("client"),
        fixture.peer("server"),
        start + Duration::from_millis(150),
    );
    assert!(result.is_err());
    // The input TCP deadline is 4s. This loose bound detects fallback to
    // that socket deadline without depending on tight scheduler timings.
    assert!(start.elapsed() < Duration::from_secs(3));
    drop(silent);
}

#[test]
fn interrupted_tls_does_not_return_buffered_plaintext() {
    let fixture = Fixture::new();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let (release, wait) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io =
            TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        io.write_all(b"fixture bytes").unwrap();
        io.flush().unwrap();
        let _ = wait.recv_timeout(DEADLINE);
    });
    let mut io = TlsSessionIo::connect(
        tcp(TcpStream::connect(addr).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        fixture.identity("client"),
        fixture.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let mut first = [0];
    io.read_exact(&mut first).unwrap();
    assert_eq!(first, *b"f");
    io.interrupt_handle().interrupt().unwrap();
    assert_eq!(
        io.read(&mut first).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    release.send(()).unwrap();
    server.join().unwrap();
}

#[test]
fn missing_client_certificate_cannot_enter_the_session() {
    let fixture = Fixture::new();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        assert!(TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).is_err());
    });
    // Ordinary TLS fixture without client credentials. It has no application
    // session and sends no application data on this rejected connection.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(fixture.cert("ca")).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"nessa-sync-session/1".to_vec()];
    let mut tls = rustls::ClientConnection::new(
        Arc::new(config),
        ServerName::try_from("server.fixture.test").unwrap(),
    )
    .unwrap();
    let mut socket = tcp(TcpStream::connect(addr).unwrap());
    while tls.is_handshaking() {
        if tls.complete_io(&mut socket).is_err() {
            break;
        }
    }
    worker.join().unwrap();
}

#[test]
fn authenticated_peer_still_needs_current_scope_authorization() {
    let fixture = Fixture::new();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io =
            TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        serve_record_session(
            &mut io,
            3,
            LIMITS,
            &mut Policy,
            |_| -> Result<SqliteReferenceSource, SourceError> {
                panic!("a denied scope must not enter the source factory");
            },
        )
        .unwrap();
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(addr).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        fixture.identity("client"),
        fixture.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let denied = Scope::new(
        id("peer"),
        id("origin"),
        id("not-granted"),
        id("first"),
        id("text"),
        id("epoch"),
    );
    assert!(matches!(
        RecordSession::connect_open_interruptible(io, denied, LIMITS),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
    worker.join().unwrap();
}

#[test]
fn trusted_same_name_but_wrong_server_leaf_gets_no_client_identity() {
    let fixture = Fixture::new();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(fixture.cert("ca")).unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .unwrap();
    // The alternate fixture server is a normal trusted server for the same
    // DNS name, but the host did not authorize its leaf for this session.
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![fixture.cert("server-sibling")],
            fixture.key("server-sibling"),
        )
        .unwrap();
    config.alpn_protocols = vec![b"nessa-sync-session/1".to_vec()];
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut tcp = tcp(socket);
        let mut tls = rustls::ServerConnection::new(Arc::new(config)).unwrap();
        while tls.is_handshaking() {
            if tls.complete_io(&mut tcp).is_err() {
                break;
            }
        }
        assert!(tls.peer_certificates().is_none());
    });
    let result = TlsSessionIo::connect(
        tcp(TcpStream::connect(addr).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        fixture.identity("client"),
        fixture.peer("server"),
        Instant::now() + DEADLINE,
    );
    assert!(result.is_err());
    worker.join().unwrap();
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires the standalone SwiftTLSFixture package; run clients/swift/e2e/run-tls.sh"]
fn swift_tls_open_head_page_against_rust() {
    use nessa_sync::replication::infrastructure::session::serve_capability_record_session;
    use std::process::Stdio;

    struct ObservedPolicy(u64);
    impl ScopeAuthorizer for ObservedPolicy {
        fn authorize(&mut self, requested: &Scope) -> Access {
            self.0 += 1;
            Policy.authorize(requested)
        }
    }

    let swift = std::env::var_os("NESSA_SWIFT_TLS_FIXTURE").expect("set the Swift fixture path");
    let fixture = Fixture::new();
    let path = fixture.0.join("source.db");
    let mut store = source(&path);
    for n in 1..=3 {
        store
            .append(&id(&format!("record-{n}")), &vec![n; 1000])
            .unwrap();
    }
    drop(store);

    for (mode, server, expected) in [
        ("healthy", "server", "tls13-open-head-page:3:3000"),
        ("denied", "server", "typed-scope-denied"),
        (
            "peer-mismatch",
            "server-sibling",
            "peer-mismatch-before-record-dispatch",
        ),
    ] {
        let listener = endpoint();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let local = fixture.identity(server);
        let expected_client = fixture.peer("client");
        let source_path = path.clone();
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + DEADLINE;
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "Swift fixture never reached listener"
                        );
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("TLS fixture connect: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            let accepted = TlsSessionIo::accept(
                tcp(socket),
                local,
                expected_client,
                Instant::now() + DEADLINE,
            );
            let Ok(mut channel) = accepted else {
                return (false, 0, 0);
            };
            let mut policy = ObservedPolicy(0);
            let mut factory_calls = 0;
            let result =
                serve_capability_record_session(&mut channel, 1, LIMITS, &mut policy, |_| {
                    factory_calls += 1;
                    Ok(source(&source_path))
                });
            // The host-owned fixture is allowed to cancel the socket after its
            // operation; graceful close_notify is not part of this test.
            match result {
                Ok(()) => {}
                Err(RecordSessionError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                    ) => {}
                result => panic!("unexpected fixture server shutdown: {result:?}"),
            }
            (true, policy.0, factory_calls)
        });
        let mut child = Command::new(&swift)
            .arg(addr.port().to_string())
            .arg(&fixture.0)
            .arg(mode)
            .stdout(Stdio::piped())
            .spawn()
            .expect("start Swift TLS fixture");
        let child_deadline = Instant::now() + Duration::from_secs(8);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= child_deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("Swift TLS fixture exceeded deadline: {mode}");
            }
            thread::sleep(Duration::from_millis(5));
        }
        let output = child.wait_with_output().unwrap();
        let observed = worker.join().unwrap();
        assert!(output.status.success(), "Swift TLS fixture failed: {mode}");
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), expected);
        match mode {
            "healthy" => assert!(observed.0 && observed.1 > 0 && observed.2 >= 3),
            "denied" => assert!(observed.0 && observed.1 > 0 && observed.2 == 0),
            _ => assert_eq!((observed.1, observed.2), (0, 0)),
        }
        println!(
            "Swift TLS fixture: {mode}; policy calls {}; source acquisitions {}",
            observed.1, observed.2
        );
    }
}
