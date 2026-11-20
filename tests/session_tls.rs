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

mod epochs {
    use super::*;
    use nessa_sync::replication::{
        domain::{Checkpoint, PageRequest},
        history::{OlderRequest, TailRequest},
        infrastructure::{
            session::{
                serve_epoch_record_session, EpochRecordSession, EpochSessionAdmission,
                EpochSessionBinding, EpochSessionError,
            },
            EpochBinding, EpochHistoryError, EpochReplicaError, SqliteEpochRecordSource,
            SqliteEpochReplicaStore, SqliteOriginAuthority,
        },
        origin_epoch::{OriginAuthorityKey, OriginEpoch},
    };

    fn key(name: &str) -> OriginAuthorityKey {
        OriginAuthorityKey::new(id(name))
    }

    fn approved(
        fixture: &Fixture,
        authority: &Path,
        key: OriginAuthorityKey,
        epoch: OriginEpoch,
        expected_leaf: &str,
    ) -> EpochSessionBinding {
        EpochSessionBinding::new(
            scope(),
            key,
            epoch,
            SqliteOriginAuthority::open(authority).unwrap(),
            fixture.peer(expected_leaf),
        )
    }

    fn connect(
        fixture: &Fixture,
        address: SocketAddr,
        binding: EpochSessionBinding,
    ) -> Result<EpochRecordSession, EpochSessionError> {
        EpochRecordSession::connect(
            tcp(TcpStream::connect(address).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            fixture.identity("client"),
            binding,
            LIMITS,
            Instant::now() + DEADLINE,
        )
    }

    fn end(result: Result<(), RecordSessionError>) {
        match result {
            Ok(()) => {}
            Err(RecordSessionError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {}
            failure => panic!("unexpected protected server shutdown: {failure:?}"),
        }
    }

    struct GatePolicy {
        armed: Arc<std::sync::atomic::AtomicBool>,
        arrived: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl ScopeAuthorizer for GatePolicy {
        fn authorize(&mut self, requested: &Scope) -> Access {
            let allowed = Policy.authorize(requested);
            if self.armed.swap(false, Ordering::AcqRel) {
                self.arrived.send(()).unwrap();
                self.release.recv_timeout(DEADLINE).unwrap();
            }
            allowed
        }
    }

    fn listen(
        fixture: &Fixture,
        leaf: &str,
        mut producer: SqliteEpochRecordSource,
        deny_first_pin_attempt: bool,
        mut policy: impl ScopeAuthorizer + Send + 'static,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = endpoint();
        let address = listener.local_addr().unwrap();
        let denied =
            deny_first_pin_attempt.then(|| (fixture.identity(leaf), fixture.peer("client")));
        let local = fixture.identity(leaf);
        let peer = fixture.peer("client");
        let thread = thread::spawn(move || {
            if let Some((local, peer)) = denied {
                let (socket, _) = listener.accept().unwrap();
                assert!(
                    TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE,)
                        .is_err()
                );
            }
            let (socket, _) = listener.accept().unwrap();
            let mut stream =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            end(serve_epoch_record_session(
                &mut stream,
                1,
                LIMITS,
                &scope(),
                &mut policy,
                &mut producer,
            ));
        });
        (address, thread)
    }

    #[test]
    fn protected_tls_verified_handoff_refuses_delayed_page_and_resumes_from_saved_progress() {
        let fixture = Fixture::new();
        let source_file = fixture.0.join("qualified-source");
        let authority = fixture.0.join("outside-source-backup-claims");
        let cache = fixture.0.join("qualified-cache");
        let key = key("stable-origin");
        let scope = scope();
        let mut writer = source(&source_file);
        writer.append(&id("one"), b"one").unwrap();
        writer.append(&id("two"), b"two").unwrap();
        let first = SqliteEpochRecordSource::claim(
            source(&source_file),
            SqliteOriginAuthority::provision(&authority).unwrap(),
            key.clone(),
            None,
        )
        .unwrap();
        let e1 = first.epoch();
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (arrived_tx, arrived_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (address1, server1) = listen(
            &fixture,
            "server",
            first,
            false,
            GatePolicy {
                armed: armed.clone(),
                arrived: arrived_tx,
                release: release_rx,
            },
        );
        let session1 = connect(
            &fixture,
            address1,
            approved(&fixture, &authority, key.clone(), e1, "server"),
        )
        .unwrap();
        let binding = EpochBinding::new(
            scope.receiver().clone(),
            scope.origin().clone(),
            key.clone(),
        );
        let mut receiver = SqliteEpochReplicaStore::provision(
            &cache,
            binding.clone(),
            EpochSessionAdmission::new(Policy),
        )
        .unwrap();
        assert_eq!(receiver.observe_head(session1.head().unwrap()), Ok(e1));
        assert_eq!(receiver.checkpoint(&scope), Ok(None));
        let limits = Limits::new(1, 4, 4).unwrap();
        let c0 = Checkpoint::new(scope.clone(), 0);
        let c1 = Checkpoint::new(scope.clone(), 1);
        let c2 = Checkpoint::new(scope.clone(), 2);
        let first = PageRequest {
            scope: scope.clone(),
            after: 0,
            target: 2,
            max_records: 1,
            max_payload_bytes: 4,
            max_record_bytes: 4,
        };
        receiver
            .apply(session1.page(&c0, &first, limits).unwrap())
            .unwrap();
        let next = PageRequest { after: 1, ..first };
        let held = session1.page(&c1, &next, limits).unwrap();
        assert_eq!(receiver.checkpoint(&scope), Ok(Some(c1.clone())));

        let checkpoint = c1.clone();
        let requested = next.clone();
        armed.store(true, Ordering::Release);
        let old_attempt = thread::spawn(move || {
            let result = session1.page(&checkpoint, &requested, limits);
            (session1, result)
        });
        arrived_rx.recv_timeout(DEADLINE).unwrap();
        let second = SqliteEpochRecordSource::claim(
            source(&source_file),
            SqliteOriginAuthority::open(&authority).unwrap(),
            key.clone(),
            Some(e1),
        )
        .unwrap();
        let e2 = second.epoch();
        assert_eq!(e2, OriginEpoch::new(2).unwrap());
        release_tx.send(()).unwrap();
        let (session1, refused) = old_attempt.join().unwrap();
        assert!(matches!(
            refused,
            Err(EpochSessionError::Record(RecordSessionError::Source(
                SourceError::IdentityChanged
            )))
        ));
        let (address2, server2) = listen(&fixture, "server-sibling", second, true, Policy);
        assert!(matches!(
            connect(
                &fixture,
                address2,
                approved(&fixture, &authority, key.clone(), e2, "server")
            ),
            Err(EpochSessionError::Record(RecordSessionError::Io(_)))
        ));
        let session2 = connect(
            &fixture,
            address2,
            approved(&fixture, &authority, key, e2, "server-sibling"),
        )
        .unwrap();
        assert_eq!(receiver.observe_head(session2.head().unwrap()), Ok(e2));
        drop(receiver);
        let mut receiver = SqliteEpochReplicaStore::open(
            &cache,
            binding.clone(),
            EpochSessionAdmission::new(Policy),
        )
        .unwrap();
        assert_eq!(receiver.apply(held), Err(EpochReplicaError::Fenced));
        assert_eq!(receiver.checkpoint(&scope), Ok(Some(c1.clone())));
        assert_eq!(receiver.read_after(&scope, 0, 2, 8).unwrap().len(), 1);
        receiver
            .apply(session2.page(&c1, &next, limits).unwrap())
            .unwrap();
        drop(receiver);
        let mut receiver =
            SqliteEpochReplicaStore::open(&cache, binding, EpochSessionAdmission::new(Policy))
                .unwrap();
        assert_eq!(receiver.checkpoint(&scope), Ok(Some(c2)));
        let payloads = receiver
            .read_after(&scope, 0, 2, 8)
            .unwrap()
            .into_iter()
            .map(|r| r.payload)
            .collect::<Vec<_>>();
        assert_eq!(payloads, [b"one".to_vec(), b"two".to_vec()]);
        let old = session1.counters().unwrap();
        let new = session2.counters().unwrap();
        assert_eq!(old.payload_bytes, 6);
        assert_eq!((new.exchanges, new.payload_bytes), (5, 3));
        eprintln!(
            "protected fixture app exchanges/protocol bytes/payload old={},{},{}; new={},{},{}",
            old.exchanges,
            old.protocol_bytes,
            old.payload_bytes,
            new.exchanges,
            new.protocol_bytes,
            new.payload_bytes
        );
        session2.close().unwrap();
        server1.join().unwrap();
        server2.join().unwrap();
    }

    #[test]
    fn protected_tls_history_handoff_preserves_durable_coverage_until_new_claim_applies() {
        let fixture = Fixture::new();
        let source_file = fixture.0.join("history-source");
        let authority = fixture.0.join("history-claim-ledger");
        let cache = fixture.0.join("history-cache");
        let key = key("history-origin");
        let scope = scope();
        let mut writer = source(&source_file);
        for (name, payload) in [("one", b"a"), ("two", b"b"), ("three", b"c")] {
            writer.append(&id(name), payload).unwrap();
        }
        let first = SqliteEpochRecordSource::claim(
            source(&source_file),
            SqliteOriginAuthority::provision(&authority).unwrap(),
            key.clone(),
            None,
        )
        .unwrap();
        let e1 = first.epoch();
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (arrived_tx, arrived_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (addr1, server1) = listen(
            &fixture,
            "server",
            first,
            false,
            GatePolicy {
                armed: armed.clone(),
                arrived: arrived_tx,
                release: release_rx,
            },
        );
        let old = connect(
            &fixture,
            addr1,
            approved(&fixture, &authority, key.clone(), e1, "server"),
        )
        .unwrap();
        let binding = EpochBinding::new(
            scope.receiver().clone(),
            scope.origin().clone(),
            key.clone(),
        );
        let mut receiver = SqliteEpochReplicaStore::provision(
            &cache,
            binding.clone(),
            EpochSessionAdmission::new(Policy),
        )
        .unwrap();
        assert_eq!(receiver.observe_head(old.head().unwrap()), Ok(e1));
        let limits = Limits::new(1, 1, 1).unwrap();
        let initial = TailRequest {
            scope: scope.clone(),
            generation: 1,
            max_records: 1,
            max_payload_bytes: 1,
            max_record_bytes: 1,
        };
        let saved = receiver
            .install_tail(old.tail(&initial, limits).unwrap())
            .unwrap();
        assert_eq!(
            (saved.generation, saved.lower_bound, saved.live_head),
            (1, 3, 3)
        );
        let before = OlderRequest {
            scope: scope.clone(),
            generation: 1,
            before: 3,
            max_records: 1,
            max_payload_bytes: 1,
            max_record_bytes: 1,
        };
        // The wire can accept these budgets, but this application's
        // budget cannot. Neither operation may reach source authorization.
        let oversized_tail = TailRequest {
            max_payload_bytes: 2,
            ..initial.clone()
        };
        let oversized_older = OlderRequest {
            max_payload_bytes: 2,
            ..before.clone()
        };
        let counters = old.counters().unwrap();
        armed.store(true, Ordering::Release);
        assert!(matches!(
            old.tail(&oversized_tail, limits),
            Err(EpochSessionError::History(_))
        ));
        assert!(matches!(
            old.older(&oversized_older, limits),
            Err(EpochSessionError::History(_))
        ));
        assert_eq!(old.counters().unwrap(), counters);
        assert!(armed.swap(false, Ordering::AcqRel));
        assert!(arrived_rx.try_recv().is_err());
        let delayed_older = old.older(&before, limits).unwrap();
        writer.append(&id("four"), b"d").unwrap();
        let replacement = TailRequest {
            generation: 2,
            ..initial
        };
        let delayed_tail = old.tail(&replacement, limits).unwrap();
        let parked = before.clone();
        armed.store(true, Ordering::Release);
        let attempt = thread::spawn(move || {
            let result = old.older(&parked, limits);
            (old, result)
        });
        arrived_rx.recv_timeout(DEADLINE).unwrap();
        let second = SqliteEpochRecordSource::claim(
            source(&source_file),
            SqliteOriginAuthority::open(&authority).unwrap(),
            key.clone(),
            Some(e1),
        )
        .unwrap();
        let e2 = second.epoch();
        release_tx.send(()).unwrap();
        let (old, refused) = attempt.join().unwrap();
        assert!(matches!(
            refused,
            Err(EpochSessionError::Record(RecordSessionError::Source(
                SourceError::IdentityChanged
            )))
        ));
        let (addr2, server2) = listen(&fixture, "server-sibling", second, false, Policy);
        let new = connect(
            &fixture,
            addr2,
            approved(&fixture, &authority, key, e2, "server-sibling"),
        )
        .unwrap();
        assert_eq!(
            receiver.install_tail(new.tail(&replacement, limits).unwrap()),
            Err(EpochHistoryError::Epoch(EpochReplicaError::Unobserved))
        );
        assert_eq!(receiver.observe_head(new.head().unwrap()), Ok(e2));
        drop(receiver);
        let mut receiver = SqliteEpochReplicaStore::open(
            &cache,
            binding.clone(),
            EpochSessionAdmission::new(Policy),
        )
        .unwrap();
        assert_eq!(
            receiver.install_older(delayed_older),
            Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
        );
        assert_eq!(
            receiver.install_tail(delayed_tail),
            Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
        );
        assert_eq!(receiver.history_progress(&scope), Ok(Some(saved)));
        assert_eq!(
            receiver.checkpoint(&scope),
            Ok(Some(Checkpoint::new(scope.clone(), 3)))
        );
        assert_eq!(
            receiver.read_after(&scope, 2, 2, 2).unwrap()[0].payload,
            b"c"
        );
        let installed = receiver
            .install_tail(new.tail(&replacement, limits).unwrap())
            .unwrap();
        assert_eq!(
            (
                installed.generation,
                installed.lower_bound,
                installed.live_head
            ),
            (2, 4, 4)
        );
        let extended = receiver
            .install_older(
                new.older(
                    &OlderRequest {
                        generation: 2,
                        before: 4,
                        ..before
                    },
                    limits,
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            (
                extended.generation,
                extended.lower_bound,
                extended.live_head
            ),
            (2, 3, 4)
        );
        drop(receiver);
        let mut receiver =
            SqliteEpochReplicaStore::open(&cache, binding, EpochSessionAdmission::new(Policy))
                .unwrap();
        assert_eq!(receiver.history_progress(&scope), Ok(Some(extended)));
        assert_eq!(
            receiver.checkpoint(&scope),
            Ok(Some(Checkpoint::new(scope.clone(), 4)))
        );
        let rows = receiver.read_after(&scope, 2, 2, 2).unwrap();
        assert_eq!(
            rows.into_iter().map(|row| row.payload).collect::<Vec<_>>(),
            [b"c".to_vec(), b"d".to_vec()]
        );
        for (phase, session) in [("old", old), ("new", new)] {
            let counters = session.counters().unwrap();
            eprintln!(
                "protected history {phase}: {} exchanges, {} protocol bytes, {} payload bytes",
                counters.exchanges, counters.protocol_bytes, counters.payload_bytes
            );
            session.close().unwrap();
        }
        server1.join().unwrap();
        server2.join().unwrap();
    }

    #[test]
    fn verified_tls_without_protected_version_or_matching_claim_never_admits_evidence() {
        let fixture = Fixture::new();
        let authority = fixture.0.join("outside-source-backup-claims");
        let mut ledger = SqliteOriginAuthority::provision(&authority).unwrap();
        let expected_key = key("host-configured");
        let e = ledger.compare_and_claim(&expected_key, None).unwrap();

        let listener = endpoint();
        let local = fixture.identity("server");
        let peer = fixture.peer("client");
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let source_calls = calls.clone();
        let legacy = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let result = serve_record_session::<_, _, _, SqliteReferenceSource>(
                &mut io,
                1,
                LIMITS,
                &mut Policy,
                move |_| {
                    source_calls.fetch_add(1, Ordering::Relaxed);
                    Err(SourceError::Unavailable)
                },
            );
            assert!(matches!(result, Err(RecordSessionError::TooLarge)));
        });
        assert!(connect(
            &fixture,
            address,
            approved(&fixture, &authority, expected_key.clone(), e, "server")
        )
        .is_err());
        legacy.join().unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        let source_file = fixture.0.join("different-qualified-source");
        let mut writer = source(&source_file);
        writer.append(&id("one"), b"one").unwrap();
        let different = SqliteEpochRecordSource::claim(
            source(&source_file),
            SqliteOriginAuthority::open(&authority).unwrap(),
            key("peer-configured"),
            None,
        )
        .unwrap();
        let (address, server) = listen(&fixture, "server", different, false, Policy);
        let wrong_binding = connect(
            &fixture,
            address,
            approved(&fixture, &authority, expected_key.clone(), e, "server"),
        )
        .unwrap();
        assert!(matches!(
            wrong_binding.head(),
            Err(EpochSessionError::WrongClaim)
        ));
        server.join().unwrap();

        let unused = endpoint();
        assert!(matches!(
            connect(
                &fixture,
                unused.local_addr().unwrap(),
                approved(
                    &fixture,
                    &authority,
                    expected_key,
                    OriginEpoch::new(2).unwrap(),
                    "server",
                )
            ),
            Err(EpochSessionError::WrongClaim)
        ));
    }
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
