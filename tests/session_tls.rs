#![cfg(all(feature = "tls", feature = "sqlite"))]

use nessa_sync::replication::{
    application::{
        begin_pass, finish_pass, Access, ScopeAuthorizer, SourceError, MAX_RECORD_BYTES,
    },
    domain::{Id, Limits, Scope},
    infrastructure::{
        session::{
            read_control, serve_record_session, serve_secure_follow, write_control,
            BindingMetadata, ControlFrame, Correlation, FollowError, HintSignal,
            InterruptibleSessionIo, RecordSession, RecordSessionError, SecureFollow,
            SecureFollowStop, SessionLimits, TcpSessionIo, TlsIdentity, TlsPeer, TlsSessionIo,
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

    #[path = "protected_process.rs"]
    mod protected_process;

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

#[test]
fn secure_follow_hints_are_advisory_and_reconnect_fetches_durable_checkpoint() {
    for relayed in [false, true] {
        let f = Fixture::new();
        let source_path = f.0.join("secure-follow-source.db");
        source(&source_path).append(&id("one"), b"one").unwrap();
        let cache_path = f.0.join("secure-follow-cache.db");
        let mut cache = SqliteReplicaStore::open(&cache_path).unwrap();
        let mut fetch_counts = Vec::new();
        let mut hint_counts = Vec::new();
        for round in 0..3 {
            if round > 0 {
                let listener = endpoint();
                let (addr, relay_worker) = if relayed {
                    let (a, r) = relay(listener.local_addr().unwrap());
                    (a, Some(r))
                } else {
                    (listener.local_addr().unwrap(), None)
                };
                let server_identity = f.identity("server");
                let server_peer = f.peer("client");
                let signal = HintSignal::new();
                let server_signal = signal.clone();
                let server_path = source_path.clone();
                let (sender, receiver) = std::sync::mpsc::channel();
                let worker = thread::spawn(move || {
                    let (socket, _) = listener.accept().unwrap();
                    let io = TlsSessionIo::accept(
                        tcp(socket),
                        server_identity,
                        server_peer,
                        Instant::now() + DEADLINE,
                    )
                    .unwrap();
                    let stop = Arc::new(SecureFollowStop::new(&io, server_signal));
                    sender.send(stop.clone()).unwrap();
                    let result = serve_secure_follow(
                        io,
                        10 + round,
                        LIMITS,
                        Policy,
                        move |_| Ok(source(&server_path)),
                        &stop,
                        Duration::from_secs(30),
                    );
                    assert!(result.is_ok() || matches!(result, Err(RecordSessionError::Io(_))));
                });
                let io = TlsSessionIo::connect(
                    tcp(TcpStream::connect(addr).unwrap()),
                    ServerName::try_from("server.fixture.test").unwrap(),
                    f.identity("client"),
                    f.peer("server"),
                    Instant::now() + DEADLINE,
                )
                .unwrap();
                let mut hints = SecureFollow::connect(io, scope(), LIMITS).unwrap();
                let stop = receiver.recv_timeout(DEADLINE).unwrap();
                let record_name = if round == 1 { "two" } else { "six" };
                source(&source_path)
                    .append(&id(record_name), b"new")
                    .unwrap();
                signal.notify().unwrap();
                assert_eq!(hints.next_hint().unwrap(), round + 1);
                assert_eq!(
                    cache.checkpoint(&scope()).unwrap().unwrap().position(),
                    round
                );
                let stats = hints.counters().unwrap();
                hint_counts.push((stats.exchanges, stats.protocol_bytes, stats.payload_bytes));
                hints.close().unwrap();
                stop.stop().unwrap();
                worker.join().unwrap();
                if let Some(r) = relay_worker {
                    let _ = r.join().unwrap();
                }
            }
            let listener = endpoint();
            let (addr, relay_worker) = if relayed {
                let (a, r) = relay(listener.local_addr().unwrap());
                (a, Some(r))
            } else {
                (listener.local_addr().unwrap(), None)
            };
            let local = f.identity("server");
            let peer = f.peer("client");
            let path = source_path.clone();
            let worker = thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                let mut io =
                    TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE)
                        .unwrap();
                let _ = serve_record_session(&mut io, 30 + round, LIMITS, &mut Policy, move |_| {
                    Ok(source(&path))
                });
            });
            let io = TlsSessionIo::connect(
                tcp(TcpStream::connect(addr).unwrap()),
                ServerName::try_from("server.fixture.test").unwrap(),
                f.identity("client"),
                f.peer("server"),
                Instant::now() + DEADLINE,
            )
            .unwrap();
            let records = RecordSession::connect_open_interruptible(io, scope(), LIMITS).unwrap();
            let (mut auth, mut read) = records.parts();
            let mut pass = begin_pass(&scope(), &mut auth, &mut read, &mut cache).unwrap();
            assert_eq!(pass.position(), round);
            finish_pass(
                &mut pass,
                Limits::new(3, 3000, 1000).unwrap(),
                &mut auth,
                &mut read,
                &mut cache,
            )
            .unwrap();
            assert_eq!(pass.position(), round + 1);
            let stats = records.counters().unwrap();
            fetch_counts.push((stats.exchanges, stats.protocol_bytes, stats.payload_bytes));
            records.close().unwrap();
            drop(read);
            drop(auth);
            drop(records);
            worker.join().unwrap();
            if let Some(r) = relay_worker {
                let _ = r.join().unwrap();
            }
            drop(cache);
            cache = SqliteReplicaStore::open(&cache_path).unwrap();
            assert_eq!(
                cache.checkpoint(&scope()).unwrap().unwrap().position(),
                round + 1
            );
            assert_eq!(cache.count(&scope()).unwrap(), round + 1);
            let records = cache.read_after(&scope(), 0, 3, 3000).unwrap();
            let expected = [
                ("one", b"one".as_slice()),
                ("two", b"new".as_slice()),
                ("six", b"new".as_slice()),
            ];
            for (index, record) in records.iter().enumerate() {
                assert_eq!(record.position, index as u64 + 1);
                assert_eq!(record.id, id(expected[index].0));
                assert_eq!(record.payload, expected[index].1);
            }
        }
        assert_eq!(fetch_counts[0], fetch_counts[1]);
        assert_eq!(fetch_counts[1], fetch_counts[2]);
        assert_eq!(hint_counts[0], hint_counts[1]);
        eprintln!("secure-follow relay={relayed} fetch={fetch_counts:?} hint_setup_and_one_hint={hint_counts:?}");
    }
}

#[test]
fn secure_follow_rejects_malformed_stale_and_eof_on_authenticated_fixture() {
    let f = Fixture::new();
    for case in 0..13 {
        let listener = endpoint();
        let address = listener.local_addr().unwrap();
        let local = f.identity("server");
        let peer = f.peer("client");
        let worker = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let ControlFrame::FollowOpen { scope, request, .. } =
                read_control(&mut io, 2048).unwrap()
            else {
                panic!("expected FollowOpen")
            };
            write_control(
                &mut io,
                &ControlFrame::Welcome {
                    session: 41,
                    limits: LIMITS,
                    record_bytes: MAX_RECORD_BYTES as u32,
                },
                2048,
            )
            .unwrap();
            write_control(
                &mut io,
                &ControlFrame::Bound {
                    correlation: Correlation {
                        session: 41,
                        request,
                    },
                    binding: BindingMetadata {
                        handle: 1,
                        generation: 1,
                        scope,
                        head: 0,
                    },
                },
                2048,
            )
            .unwrap();
            io.flush().unwrap();
            if case == 6 {
                return;
            }
            if case == 10 {
                io.write_all(&2049_u32.to_be_bytes()).unwrap();
                io.flush().unwrap();
                return;
            }
            if case == 11 {
                io.write_all(&[0, 0]).unwrap();
                io.flush().unwrap();
                return;
            }
            let mut body = Vec::new();
            body.extend_from_slice(b"NSR1");
            body.extend_from_slice(&1_u16.to_be_bytes());
            body.push(if case == 4 { 2 } else { 64 });
            body.push(if case == 5 {
                255
            } else if case == 12 {
                1
            } else {
                0
            });
            body.extend_from_slice(&(if case == 0 { 42_u64 } else { 41 }).to_be_bytes());
            body.extend_from_slice(&(if case == 1 { 2_u64 } else { request }).to_be_bytes());
            body.extend_from_slice(&(if case == 2 { 2_u32 } else { 1 }).to_be_bytes());
            body.extend_from_slice(&(if case == 3 { 2_u64 } else { 1 }).to_be_bytes());
            body.extend_from_slice(&(if case == 9 { 9_u32 } else { 8 }).to_be_bytes());
            body.extend_from_slice(&(if case == 8 { 1_u32 } else { 0 }).to_be_bytes());
            body.extend_from_slice(&3_u64.to_be_bytes());
            io.write_all(&(body.len() as u32).to_be_bytes()).unwrap();
            io.write_all(&body).unwrap();
            io.flush().unwrap();
            if case == 7 {
                io.write_all(&(body.len() as u32).to_be_bytes()).unwrap();
                io.write_all(&body).unwrap();
                io.flush().unwrap();
            }
        });
        let io = TlsSessionIo::connect(
            tcp(TcpStream::connect(address).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            f.identity("client"),
            f.peer("server"),
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let mut hints = SecureFollow::connect(io, scope(), LIMITS).unwrap();
        if case == 7 {
            assert_eq!(hints.next_hint().unwrap(), 3);
        }
        let error = hints.next_hint().unwrap_err();
        if case == 6 || case == 11 {
            assert!(matches!(
                error,
                FollowError::Closed | FollowError::Unavailable
            ));
        } else {
            assert_eq!(error, FollowError::Protocol, "case {case}");
        }
        assert!(hints.next_hint().is_err());
        worker.join().unwrap();
    }
}

#[test]
fn secure_follow_denial_and_idle_stop_fence_the_connection() {
    use std::sync::atomic::{AtomicU8, Ordering};
    struct SwitchPolicy(Arc<AtomicU8>);
    impl ScopeAuthorizer for SwitchPolicy {
        fn authorize(&mut self, scope: &Scope) -> Access {
            if self.0.load(Ordering::SeqCst) == 0 {
                Access::Allowed(scope.clone())
            } else {
                Access::Denied
            }
        }
    }
    let f = Fixture::new();
    for denied in [true, false] {
        let path = f.0.join("fenced-source.db");
        drop(source(&path));
        let listener = endpoint();
        let addr = listener.local_addr().unwrap();
        let local = f.identity("server");
        let peer = f.peer("client");
        let signal = HintSignal::new();
        let signal_for_server = signal.clone();
        let state = Arc::new(AtomicU8::new(0));
        let server_state = state.clone();
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        let (send, recv) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let stop = Arc::new(SecureFollowStop::new(&io, signal_for_server));
            send.send(stop.clone()).unwrap();
            serve_secure_follow(
                io,
                50,
                LIMITS,
                SwitchPolicy(server_state),
                move |_| {
                    server_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(source(&path))
                },
                &stop,
                Duration::from_secs(30),
            )
        });
        let io = TlsSessionIo::connect(
            tcp(TcpStream::connect(addr).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            f.identity("client"),
            f.peer("server"),
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let mut client = SecureFollow::connect(io, scope(), LIMITS).unwrap();
        let stop = recv.recv_timeout(DEADLINE).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if denied {
            state.store(1, Ordering::SeqCst);
            signal.notify().unwrap();
            assert_eq!(
                client.next_hint(),
                Err(FollowError::Refused(SourceError::Denied))
            );
            assert!(client.next_hint().is_err());
            assert!(matches!(
                server.join().unwrap(),
                Err(RecordSessionError::Source(SourceError::Denied))
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            stop.stop().unwrap();
            assert!(client.next_hint().is_err());
            let shutdown = server.join().unwrap();
            assert!(shutdown.is_ok() || matches!(shutdown, Err(RecordSessionError::Io(_))));
        }
    }
}

#[test]
fn secure_follow_tls_peer_mismatch_never_invokes_policy_or_factory() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountPolicy(Arc<AtomicUsize>);
    impl ScopeAuthorizer for CountPolicy {
        fn authorize(&mut self, scope: &Scope) -> Access {
            self.0.fetch_add(1, Ordering::SeqCst);
            Access::Allowed(scope.clone())
        }
    }
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let expected = f.peer("client");
    let policies = Arc::new(AtomicUsize::new(0));
    let p = policies.clone();
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        if let Ok(io) =
            TlsSessionIo::accept(tcp(socket), local, expected, Instant::now() + DEADLINE)
        {
            let stop = SecureFollowStop::new(&io, HintSignal::new());
            let _ = serve_secure_follow(
                io,
                2,
                LIMITS,
                CountPolicy(p),
                move |_| -> Result<SqliteReferenceSource, SourceError> {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(SourceError::Unavailable)
                },
                &stop,
                Duration::from_secs(30),
            );
            false
        } else {
            true
        }
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("sibling"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    );
    if let Ok(io) = io {
        assert!(SecureFollow::connect(io, scope(), LIMITS).is_err());
    }
    assert!(worker.join().unwrap());
    assert_eq!(policies.load(Ordering::SeqCst), 0);
    assert_eq!(factories.load(Ordering::SeqCst), 0);
}

#[test]
fn secure_follow_rechecks_policy_after_capturing_head_before_egress() {
    use nessa_sync::replication::{
        application::RecordSource,
        domain::{Page, PageRequest},
    };
    use std::sync::atomic::{AtomicU8, Ordering};
    struct SwitchingPolicy(Arc<AtomicU8>);
    impl ScopeAuthorizer for SwitchingPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            if self.0.load(Ordering::SeqCst) == 0 {
                Access::Allowed(s.clone())
            } else {
                Access::Denied
            }
        }
    }
    struct SwitchingSource {
        state: Arc<AtomicU8>,
        revoke: bool,
    }
    impl RecordSource for SwitchingSource {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            if self.revoke {
                self.state.store(1, Ordering::SeqCst);
            }
            Ok(42)
        }
        fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
            panic!("no page")
        }
    }
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let peer = f.peer("client");
    let state = Arc::new(AtomicU8::new(0));
    let server_state = state.clone();
    let signal = HintSignal::new();
    let server_signal = signal.clone();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = SecureFollowStop::new(&io, server_signal);
        let mut opens = 0;
        let factory_state = server_state.clone();
        serve_secure_follow(
            io,
            71,
            LIMITS,
            SwitchingPolicy(server_state),
            move |_| {
                opens += 1;
                Ok(SwitchingSource {
                    state: factory_state.clone(),
                    revoke: opens > 1,
                })
            },
            &stop,
            Duration::from_secs(30),
        )
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let mut client = SecureFollow::connect(io, scope(), LIMITS).unwrap();
    signal.notify().unwrap();
    assert_eq!(
        client.next_hint(),
        Err(FollowError::Refused(SourceError::Denied))
    );
    assert!(matches!(
        server.join().unwrap(),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
}

#[test]
fn secure_follow_stop_handle_is_bound_to_its_original_tls_connection() {
    struct CountPolicy(Arc<AtomicU64>);
    impl ScopeAuthorizer for CountPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            self.0.fetch_add(1, Ordering::SeqCst);
            Access::Allowed(s.clone())
        }
    }
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let first_local = f.identity("server");
    let first_peer = f.peer("client");
    let second_local = f.identity("server");
    let second_peer = f.peer("client");
    let calls = Arc::new(AtomicU64::new(0));
    let p = calls.clone();
    let (ack_tx, ack_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (first, _) = listener.accept().unwrap();
        let mut first = TlsSessionIo::accept(
            tcp(first),
            first_local,
            first_peer,
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let stop = SecureFollowStop::new(&first, HintSignal::new());
        let (second, _) = listener.accept().unwrap();
        let second = TlsSessionIo::accept(
            tcp(second),
            second_local,
            second_peer,
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let result = serve_secure_follow(
            second,
            1,
            LIMITS,
            CountPolicy(p),
            |_| -> Result<SqliteReferenceSource, SourceError> {
                panic!("mismatched stop must not acquire a source")
            },
            &stop,
            Duration::from_secs(30),
        );
        assert!(matches!(result, Err(RecordSessionError::Protocol)));
        first.write_all(b"still-open").unwrap();
        first.flush().unwrap();
        ack_rx.recv_timeout(DEADLINE).unwrap();
        stop.stop().unwrap();
    });
    let mut first = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let second = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let mut proof = [0; 10];
    first.read_exact(&mut proof).unwrap();
    assert_eq!(&proof, b"still-open");
    ack_tx.send(()).unwrap();
    worker.join().unwrap();
    drop(first);
    drop(second);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn secure_follow_keeps_commit_wake_during_binding() {
    use nessa_sync::replication::{
        application::RecordSource,
        domain::{Page, PageRequest},
    };
    use std::sync::atomic::AtomicBool;
    struct DuringBinding {
        signal: HintSignal,
        fired: Arc<AtomicBool>,
    }
    impl RecordSource for DuringBinding {
        fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                self.signal.notify().unwrap();
            }
            Ok(42)
        }
        fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
            panic!("no page")
        }
    }
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let peer = f.peer("client");
    let signal = HintSignal::new();
    let server_signal = signal.clone();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = Arc::new(SecureFollowStop::new(&io, server_signal.clone()));
        stop_tx.send(stop.clone()).unwrap();
        let fired = Arc::new(AtomicBool::new(false));
        serve_secure_follow(
            io,
            91,
            LIMITS,
            Policy,
            move |_| {
                Ok(DuringBinding {
                    signal: server_signal.clone(),
                    fired: fired.clone(),
                })
            },
            &stop,
            Duration::from_secs(30),
        )
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let interrupt = io.interrupt_handle();
    let mut client = SecureFollow::connect(io, scope(), LIMITS).unwrap();
    let stop = stop_rx.recv_timeout(DEADLINE).unwrap();
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    let reader = thread::spawn(move || {
        let _ = reply_tx.send(client.next_hint());
    });
    let result = reply_rx.recv_timeout(Duration::from_secs(2));
    stop.stop().unwrap();
    interrupt.interrupt().unwrap();
    reader.join().unwrap();
    assert_eq!(result.unwrap(), Ok(42));
    let shutdown = worker.join().unwrap();
    assert!(shutdown.is_ok() || matches!(shutdown, Err(RecordSessionError::Io(_))));
}

#[test]
fn secure_follow_denied_scope_never_acquires_source() {
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let peer = f.peer("client");
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = SecureFollowStop::new(&io, HintSignal::new());
        serve_secure_follow(
            io,
            77,
            LIMITS,
            Policy,
            move |_| -> Result<SqliteReferenceSource, SourceError> {
                count.fetch_add(1, Ordering::SeqCst);
                Err(SourceError::Unavailable)
            },
            &stop,
            Duration::from_secs(30),
        )
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let wrong = Scope::new(
        id("peer"),
        id("origin"),
        id("other"),
        id("first"),
        id("text"),
        id("epoch"),
    );
    assert!(matches!(
        SecureFollow::connect(io, wrong, LIMITS),
        Err(RecordSessionError::Source(SourceError::Denied))
    ));
    assert!(worker.join().unwrap().is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn secure_follow_zero_session_is_protocol_error_before_policy() {
    struct CountPolicy(Arc<AtomicU64>);
    impl ScopeAuthorizer for CountPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            self.0.fetch_add(1, Ordering::SeqCst);
            Access::Allowed(s.clone())
        }
    }
    let f = Fixture::new();
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let peer = f.peer("client");
    let calls = Arc::new(AtomicU64::new(0));
    let p = calls.clone();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = SecureFollowStop::new(&io, HintSignal::new());
        assert!(matches!(
            serve_secure_follow(
                io,
                0,
                LIMITS,
                CountPolicy(p),
                |_| -> Result<SqliteReferenceSource, SourceError> {
                    panic!("invalid session must not acquire a source")
                },
                &stop,
                Duration::from_secs(30)
            ),
            Err(RecordSessionError::Protocol)
        ));
    });
    let client = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    server.join().unwrap();
    drop(client);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn secure_follow_stop_during_binding_prevents_later_source_acquisition() {
    struct StopPolicy(Arc<SecureFollowStop>, Arc<AtomicU64>);
    impl ScopeAuthorizer for StopPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.stop().unwrap();
            Access::Allowed(s.clone())
        }
    }
    let f = Fixture::new();
    let path = f.0.join("stopped-binding-source.db");
    drop(source(&path));
    let listener = endpoint();
    let address = listener.local_addr().unwrap();
    let local = f.identity("server");
    let peer = f.peer("client");
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let policy_calls = Arc::new(AtomicU64::new(0));
    let policy_count = policy_calls.clone();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = Arc::new(SecureFollowStop::new(&io, HintSignal::new()));
        let _ = serve_secure_follow(
            io,
            81,
            LIMITS,
            StopPolicy(stop.clone(), policy_count),
            move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(source(&path))
            },
            &stop,
            Duration::from_secs(30),
        );
    });
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    assert!(SecureFollow::connect(io, scope(), LIMITS).is_err());
    server.join().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(policy_calls.load(Ordering::SeqCst), 1);
}

fn secure_follow_terminal_close_with_retained_token(relayed: bool, deny_binding: bool) {
    use std::sync::atomic::AtomicBool;
    struct DynamicPolicy(Arc<AtomicBool>);
    impl ScopeAuthorizer for DynamicPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            if self.0.load(Ordering::SeqCst) {
                Access::Denied
            } else {
                Access::Allowed(s.clone())
            }
        }
    }
    let f = Fixture::new();
    let path = f.0.join("terminal-source.db");
    drop(source(&path));
    let listener = endpoint();
    let (address, relay_worker) = if relayed {
        let (address, worker) = relay(listener.local_addr().unwrap());
        (address, Some(worker))
    } else {
        (listener.local_addr().unwrap(), None)
    };
    let local = f.identity("server");
    let peer = f.peer("client");
    let state = Arc::new(AtomicBool::new(deny_binding));
    let server_state = state.clone();
    let signal = HintSignal::new();
    let server_signal = signal.clone();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let stop = Arc::new(SecureFollowStop::new(&io, server_signal));
        stop_tx.send(stop.clone()).unwrap();
        serve_secure_follow(
            io,
            81,
            LIMITS,
            DynamicPolicy(server_state),
            move |_| Ok(source(&path)),
            &stop,
            Duration::from_secs(30),
        )
    });
    let mut io = TlsSessionIo::connect(
        tcp(TcpStream::connect(address).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        f.identity("client"),
        f.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    write_control(
        &mut io,
        &ControlFrame::FollowOpen {
            limits: LIMITS,
            request: 1,
            scope: scope(),
        },
        2048,
    )
    .unwrap();
    io.flush().unwrap();
    let retained_stop = stop_rx.recv_timeout(DEADLINE).unwrap();
    assert!(matches!(
        read_control(&mut io, 2048).unwrap(),
        ControlFrame::Welcome { .. }
    ));
    let binding = read_control(&mut io, 2048).unwrap();
    if deny_binding {
        assert!(matches!(
            binding,
            ControlFrame::Refused {
                reason: SourceError::Denied,
                ..
            }
        ));
        assert!(server.join().unwrap().is_ok());
    } else {
        assert!(matches!(binding, ControlFrame::Bound { .. }));
        state.store(true, Ordering::SeqCst);
        signal.notify().unwrap();
        let mut prefix = [0_u8; 4];
        io.read_exact(&mut prefix).unwrap();
        let mut frame = vec![0_u8; u32::from_be_bytes(prefix) as usize];
        io.read_exact(&mut frame).unwrap();
        assert_eq!(frame.len(), 44);
        assert_eq!(frame[6], 64);
        assert_eq!(frame[7], 1);
        assert!(matches!(
            server.join().unwrap(),
            Err(RecordSessionError::Source(SourceError::Denied))
        ));
    }
    let mut byte = [0_u8; 1];
    let terminal = io.read(&mut byte);
    assert!(
        !matches!(&terminal, Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)),
        "server returned while peer remains open with retained token"
    );
    assert!(!matches!(terminal, Ok(n) if n > 0));
    drop(retained_stop);
    drop(io);
    if let Some(worker) = relay_worker {
        let _ = worker.join().unwrap();
    }
}
#[test]
fn secure_follow_terminal_binding_refusal_closes_retained_socket() {
    for relayed in [false, true] {
        secure_follow_terminal_close_with_retained_token(relayed, true);
    }
}
#[test]
fn secure_follow_terminal_hint_refusal_closes_retained_socket() {
    for relayed in [false, true] {
        secure_follow_terminal_close_with_retained_token(relayed, false);
    }
}

#[test]
fn secure_follow_stop_during_final_policy_blocks_output_direct_and_relay() {
    struct GatedPolicy {
        calls: usize,
        target: usize,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl ScopeAuthorizer for GatedPolicy {
        fn authorize(&mut self, s: &Scope) -> Access {
            self.calls += 1;
            if self.calls == self.target {
                self.entered.send(()).unwrap();
                self.release.recv_timeout(DEADLINE).unwrap();
            }
            Access::Allowed(s.clone())
        }
    }
    for relayed in [false, true] {
        for bind in [true, false] {
            let f = Fixture::new();
            let path = f.0.join("gated-policy-source.db");
            drop(source(&path));
            let listener = endpoint();
            let (address, relay_worker) = if relayed {
                let (address, worker) = relay(listener.local_addr().unwrap());
                (address, Some(worker))
            } else {
                (listener.local_addr().unwrap(), None)
            };
            let local = f.identity("server");
            let peer = f.peer("client");
            let signal = HintSignal::new();
            let server_signal = signal.clone();
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let (socket, _) = listener.accept().unwrap();
                let io = TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE)
                    .unwrap();
                let stop = Arc::new(SecureFollowStop::new(&io, server_signal));
                stop_tx.send(stop.clone()).unwrap();
                serve_secure_follow(
                    io,
                    93,
                    LIMITS,
                    GatedPolicy {
                        calls: 0,
                        target: if bind { 2 } else { 4 },
                        entered: entered_tx,
                        release: release_rx,
                    },
                    move |_| Ok(source(&path)),
                    &stop,
                    Duration::from_secs(30),
                )
            });
            let mut io = TlsSessionIo::connect(
                tcp(TcpStream::connect(address).unwrap()),
                ServerName::try_from("server.fixture.test").unwrap(),
                f.identity("client"),
                f.peer("server"),
                Instant::now() + DEADLINE,
            )
            .unwrap();
            write_control(
                &mut io,
                &ControlFrame::FollowOpen {
                    limits: LIMITS,
                    request: 1,
                    scope: scope(),
                },
                2048,
            )
            .unwrap();
            io.flush().unwrap();
            let stop = stop_rx.recv_timeout(DEADLINE).unwrap();
            assert!(matches!(
                read_control(&mut io, 2048).unwrap(),
                ControlFrame::Welcome { .. }
            ));
            if !bind {
                assert!(matches!(
                    read_control(&mut io, 2048).unwrap(),
                    ControlFrame::Bound { .. }
                ));
                signal.notify().unwrap();
            }
            entered_rx.recv_timeout(DEADLINE).unwrap();
            let stop_clone = stop.clone();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            let stopper = thread::spawn(move || {
                let _ = result_tx.send(stop_clone.stop());
            });
            let stopped = result_rx.recv_timeout(Duration::from_secs(2));
            release_tx.send(()).unwrap();
            stopper.join().unwrap();
            assert!(
                stopped.unwrap().is_ok(),
                "stop must not wait on host policy"
            );
            if bind {
                assert!(read_control(&mut io, 2048).is_err());
            } else {
                let mut byte = [0];
                assert!(!matches!(io.read(&mut byte), Ok(n) if n > 0));
            }
            assert!(matches!(
                server.join().unwrap(),
                Err(RecordSessionError::Io(_))
            ));
            drop(stop);
            drop(io);
            if let Some(worker) = relay_worker {
                let _ = worker.join().unwrap();
            }
        }
    }
}

#[test]
fn secure_follow_finite_lease_closes_idle_retained_connection_direct_and_relay() {
    for relayed in [false, true] {
        let f = Fixture::new();
        let path = f.0.join("lease-source.db");
        drop(source(&path));
        let listener = endpoint();
        let (address, relay_worker) = if relayed {
            let (address, worker) = relay(listener.local_addr().unwrap());
            (address, Some(worker))
        } else {
            (listener.local_addr().unwrap(), None)
        };
        let local = f.identity("server");
        let peer = f.peer("client");
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let stop = Arc::new(SecureFollowStop::new(&io, HintSignal::new()));
            stop_tx.send(stop.clone()).unwrap();
            serve_secure_follow(
                io,
                94,
                LIMITS,
                Policy,
                move |_| Ok(source(&path)),
                &stop,
                Duration::from_millis(200),
            )
        });
        let mut io = TlsSessionIo::connect(
            tcp(TcpStream::connect(address).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            f.identity("client"),
            f.peer("server"),
            Instant::now() + DEADLINE,
        )
        .unwrap();
        write_control(
            &mut io,
            &ControlFrame::FollowOpen {
                limits: LIMITS,
                request: 1,
                scope: scope(),
            },
            2048,
        )
        .unwrap();
        io.flush().unwrap();
        let retained = stop_rx.recv_timeout(DEADLINE).unwrap();
        assert!(matches!(
            read_control(&mut io, 2048).unwrap(),
            ControlFrame::Welcome { .. }
        ));
        assert!(matches!(
            read_control(&mut io, 2048).unwrap(),
            ControlFrame::Bound { .. }
        ));
        let mut byte = [0];
        let terminal = io.read(&mut byte);
        let timed_out = matches!(&terminal, Err(e) if matches!(e.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock));
        if timed_out {
            let _ = retained.stop();
        }
        let result = server.join().unwrap();
        drop(retained);
        drop(io);
        if let Some(worker) = relay_worker {
            let _ = worker.join().unwrap();
        }
        assert!(
            !timed_out,
            "lease must release an idle peer even while token is retained"
        );
        assert!(!matches!(terminal, Ok(n) if n > 0));
        assert!(result.is_ok());
    }
}

#[test]
fn secure_follow_terminal_invalid_setup_closes_retained_socket() {
    for relayed in [false, true] {
        let f = Fixture::new();
        let listener = endpoint();
        let (address, relay_worker) = if relayed {
            let (address, worker) = relay(listener.local_addr().unwrap());
            (address, Some(worker))
        } else {
            (listener.local_addr().unwrap(), None)
        };
        let local = f.identity("server");
        let peer = f.peer("client");
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let stop = Arc::new(SecureFollowStop::new(&io, HintSignal::new()));
            stop_tx.send(stop.clone()).unwrap();
            serve_secure_follow(
                io,
                95,
                LIMITS,
                Policy,
                |_| -> Result<SqliteReferenceSource, SourceError> {
                    panic!("invalid setup must not reach source")
                },
                &stop,
                Duration::from_secs(30),
            )
        });
        let mut io = TlsSessionIo::connect(
            tcp(TcpStream::connect(address).unwrap()),
            ServerName::try_from("server.fixture.test").unwrap(),
            f.identity("client"),
            f.peer("server"),
            Instant::now() + DEADLINE,
        )
        .unwrap();
        let retained = stop_rx.recv_timeout(DEADLINE).unwrap();
        write_control(&mut io, &ControlFrame::Hello(LIMITS), 2048).unwrap();
        io.flush().unwrap();
        assert!(matches!(
            worker.join().unwrap(),
            Err(RecordSessionError::Protocol)
        ));
        let mut byte = [0];
        let terminal = io.read(&mut byte);
        assert!(
            !matches!(&terminal, Err(e) if matches!(e.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock))
        );
        assert!(!matches!(terminal, Ok(n) if n > 0));
        drop(retained);
        drop(io);
        if let Some(worker) = relay_worker {
            let _ = worker.join().unwrap();
        }
    }
}
