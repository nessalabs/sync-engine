#![cfg(all(feature = "tls", feature = "sqlite"))]

use nessa_sync::replication::{
    application::{Access, RecordSource, ScopeAuthorizer, SourceError},
    artifacts::{
        ArtifactKey, ArtifactState, ContentIdentity, ManifestReply, ManifestRequest,
        ManifestSource, ManifestSourceError, Sha256Digest,
    },
    domain::{Id, PageRequest, Scope},
    infrastructure::{
        session::{
            serve_record_session, serve_tls_artifact_manifest_session, ArtifactManifestAccess,
            RecordSession, RecordSessionError, SessionLimits, TcpSessionIo, TlsIdentity, TlsPeer,
            TlsSessionIo,
        },
        SqliteArtifactSource, SqliteReferenceSource,
    },
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore,
};

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
            "sync-session-artifact-manifest-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
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

fn artifact_source(path: &Path) -> SqliteArtifactSource {
    SqliteArtifactSource::open(
        path,
        id("origin"),
        id("transcript"),
        id("first"),
        id("text"),
    )
    .unwrap()
}
fn request(name: &str) -> ManifestRequest {
    ManifestRequest {
        key: ArtifactKey {
            scope: scope(),
            id: id(name),
        },
    }
}
fn session(fixture: &Fixture, addr: SocketAddr) -> RecordSession<TlsSessionIo> {
    let io = TlsSessionIo::connect(
        tcp(TcpStream::connect(addr).unwrap()),
        ServerName::try_from("server.fixture.test").unwrap(),
        fixture.identity("client"),
        fixture.peer("server"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    RecordSession::connect_open_interruptible(io, scope(), LIMITS).unwrap()
}
fn accept(listener: TcpListener, fixture: &Fixture) -> thread::JoinHandle<()> {
    let manifest_db = fixture.0.join("artifacts.db");
    let records_db = fixture.0.join("records.db");
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let certificate = Sha256Digest::of(fixture.cert("client").as_ref());
    thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io =
            TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let result = serve_tls_artifact_manifest_session(
            &mut io,
            1,
            LIMITS,
            &mut Policy,
            move |_| Ok(source(&records_db)),
            move |leaf, _| {
                assert_eq!(leaf, certificate);
                ArtifactManifestAccess::Allowed
            },
            move |_| Ok(artifact_source(&manifest_db)),
        );
        finish_server(result);
    })
}
fn finish_server(result: Result<(), RecordSessionError>) {
    match result {
        Ok(()) => {}
        Err(RecordSessionError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {}
        result => panic!("unexpected server termination: {result:?}"),
    }
}

#[derive(Default)]
struct Observation {
    replies: Vec<Result<ManifestReply, ManifestSourceError>>,
    exchanges: u64,
    protocol_bytes: u64,
    tls_bytes: u64,
}
fn observe(fixture: &Fixture, requests: &[&str], relayed: bool, persistent: bool) -> Observation {
    let mut output = Observation::default();
    for names in requests.chunks(if persistent { requests.len() } else { 1 }) {
        let listener = endpoint();
        let target = listener.local_addr().unwrap();
        let (address, proxy) = if relayed {
            let (address, worker) = relay(target);
            (address, Some(worker))
        } else {
            (target, None)
        };
        let server = accept(listener, fixture);
        let session = session(fixture, address);
        let mut manifests = session.artifact_manifest_source();
        for name in names {
            let request = request(name);
            let result = manifests.manifest(&request);
            if let Ok(reply) = &result {
                assert_eq!(reply.request, request);
                assert_eq!(reply.manifest.key, request.key);
            }
            output.replies.push(result);
        }
        let counters = session.counters().unwrap();
        output.exchanges += counters.exchanges;
        output.protocol_bytes += counters.protocol_bytes;
        assert_eq!(counters.artifact_payload_bytes, 0);
        assert_eq!(counters.payload_bytes, 0);
        session.close().unwrap();
        server.join().unwrap();
        if let Some(proxy) = proxy {
            let (up, down) = proxy.join().unwrap();
            assert!(up > 0 && down > 0);
            output.tls_bytes += up + down;
        }
    }
    output
}

#[test]
fn pinned_tls_manifest_workload_matches_reused_fresh_direct_and_relay() {
    let fixture = Fixture::new();
    let mut artifacts = artifact_source(&fixture.0.join("artifacts.db"));
    artifacts.upsert(&id("live"), b"with bytes").unwrap();
    artifacts.upsert(&id("empty"), b"").unwrap();
    artifacts.upsert(&id("deleted"), b"remove me").unwrap();
    artifacts.delete(&id("deleted")).unwrap();
    drop(artifacts);
    let names = ["live", "empty", "deleted", "missing"];
    let reused = observe(&fixture, &names, false, true);
    let reused_relay = observe(&fixture, &names, true, true);
    let fresh = observe(&fixture, &names, false, false);
    let fresh_relay = observe(&fixture, &names, true, false);
    for result in [&reused_relay, &fresh, &fresh_relay] {
        assert_eq!(reused.replies, result.replies);
    }
    for (result, state) in reused.replies.iter().take(3).zip([
        ArtifactState::Live(ContentIdentity::of(b"with bytes")),
        ArtifactState::Live(ContentIdentity::of(b"")),
        ArtifactState::Deleted,
    ]) {
        assert_eq!(result.as_ref().unwrap().manifest.state, state);
    }
    assert_eq!(reused.replies[3], Err(ManifestSourceError::Missing));
    assert_eq!(reused.exchanges, reused_relay.exchanges);
    assert_eq!(fresh.exchanges, fresh_relay.exchanges);
    assert_eq!(reused.protocol_bytes, reused_relay.protocol_bytes);
    assert_eq!(fresh.protocol_bytes, fresh_relay.protocol_bytes);
    assert!(reused.exchanges < fresh.exchanges);
    assert!(reused.protocol_bytes < fresh.protocol_bytes);
    assert!(reused_relay.tls_bytes > reused.protocol_bytes);
    assert!(fresh_relay.tls_bytes > reused_relay.tls_bytes);
    eprintln!(
        "Manifest-only four reads: persistent {} exchanges / {} app bytes / {} raw TLS relay bytes; fresh {} / {} / {}.",
        reused.exchanges, reused.protocol_bytes, reused_relay.tls_bytes,
        fresh.exchanges, fresh.protocol_bytes, fresh_relay.tls_bytes
    );
}

#[test]
fn capability_and_exact_authority_precede_artifact_factory() {
    let fixture = Fixture::new();
    let mut backing = artifact_source(&fixture.0.join("artifacts.db"));
    backing
        .upsert(&id("known"), b"private only to fixture")
        .unwrap();
    drop(backing);
    let mut backing_records = source(&fixture.0.join("records.db"));
    backing_records
        .append(&id("record"), b"ordinary record")
        .unwrap();
    drop(backing_records);

    let calls = Arc::new(AtomicU64::new(0));
    for enabled in [true, false] {
        let listener = endpoint();
        let addr = listener.local_addr().unwrap();
        let local = fixture.identity("server");
        let peer = fixture.peer("client");
        let manifests_path = fixture.0.join("artifacts.db");
        let records_path = fixture.0.join("records.db");
        let count = calls.clone();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            let result = if enabled {
                serve_tls_artifact_manifest_session(
                    &mut io,
                    2,
                    LIMITS,
                    &mut Policy,
                    move |_| Ok(source(&records_path)),
                    |_, _| ArtifactManifestAccess::Denied,
                    move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(artifact_source(&manifests_path))
                    },
                )
            } else {
                serve_record_session(&mut io, 2, LIMITS, &mut Policy, move |_| {
                    Ok(source(&records_path))
                })
            };
            if enabled {
                finish_server(result);
            } else {
                assert!(matches!(result, Err(RecordSessionError::Protocol)));
            }
        });
        let client = session(&fixture, addr);
        let (_, mut records) = client.parts();
        assert_eq!(records.head(&scope()).unwrap(), 1);
        let record = records
            .page(&PageRequest {
                scope: scope(),
                after: 0,
                target: 1,
                max_records: 1,
                max_payload_bytes: 100,
                max_record_bytes: 100,
            })
            .unwrap();
        assert_eq!(record.records[0].id, id("record"));
        assert_eq!(record.records[0].payload, b"ordinary record");
        let mut remote = client.artifact_manifest_source();
        let before = client.counters().unwrap();
        let mut foreign = request("known");
        foreign.key.scope = Scope::new(
            id("other-peer"),
            id("origin"),
            id("transcript"),
            id("first"),
            id("text"),
            id("epoch"),
        );
        assert_eq!(
            remote.manifest(&foreign),
            Err(ManifestSourceError::ScopeChanged)
        );
        assert_eq!(client.counters().unwrap().exchanges, before.exchanges);
        let known = remote.manifest(&request("known"));
        let unknown = remote.manifest(&request("unknown"));
        assert!(known.is_err());
        assert_eq!(known, unknown);
        if enabled {
            assert_eq!(known, Err(ManifestSourceError::Denied));
            assert_eq!(records.head(&scope()).unwrap(), 1);
        } else {
            assert_eq!(known, Err(ManifestSourceError::Unavailable));
            assert_eq!(records.head(&scope()), Err(SourceError::Unavailable));
        }
        client.close().unwrap();
        server.join().unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct RevokingManifest {
    inner: SqliteArtifactSource,
    mode: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
}
impl ManifestSource for RevokingManifest {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.manifest(request);
        self.mode.store(1, Ordering::SeqCst);
        result
    }
}

#[test]
fn current_artifact_policy_fences_acquisition_and_lookup_results() {
    let fixture = Fixture::new();
    let mut backing = artifact_source(&fixture.0.join("artifacts.db"));
    backing
        .upsert(&id("known"), b"authoritative fixture")
        .unwrap();
    drop(backing);
    for (revoke_on_open, name) in [(true, "known"), (false, "known"), (false, "missing")] {
        let listener = endpoint();
        let addr = listener.local_addr().unwrap();
        let local = fixture.identity("server");
        let peer = fixture.peer("client");
        let manifests_path = fixture.0.join("artifacts.db");
        let records_path = fixture.0.join("records.db");
        let mode = Arc::new(AtomicU64::new(0));
        let read_mode = mode.clone();
        let reads = Arc::new(AtomicU64::new(0));
        let server_reads = reads.clone();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut io =
                TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
            finish_server(serve_tls_artifact_manifest_session(
                &mut io,
                3,
                LIMITS,
                &mut Policy,
                move |_| Ok(source(&records_path)),
                |_, _| {
                    if mode.load(Ordering::SeqCst) == 0 {
                        ArtifactManifestAccess::Allowed
                    } else {
                        ArtifactManifestAccess::Denied
                    }
                },
                move |_| {
                    let inner = artifact_source(&manifests_path);
                    if revoke_on_open {
                        read_mode.store(1, Ordering::SeqCst);
                    }
                    Ok(RevokingManifest {
                        inner,
                        mode: read_mode.clone(),
                        reads: server_reads.clone(),
                    })
                },
            ));
        });
        let client = session(&fixture, addr);
        let mut remote = client.artifact_manifest_source();
        assert_eq!(
            remote.manifest(&request(name)),
            Err(ManifestSourceError::Denied)
        );
        assert_eq!(reads.load(Ordering::SeqCst), u64::from(!revoke_on_open));
        assert_eq!(client.counters().unwrap().artifact_payload_bytes, 0);
        client.close().unwrap();
        server.join().unwrap();
    }
}

struct HeldManifest {
    inner: SqliteArtifactSource,
    reached: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
impl ManifestSource for HeldManifest {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        let reply = self.inner.manifest(request)?;
        self.reached.send(()).unwrap();
        self.release.recv_timeout(DEADLINE).unwrap();
        Ok(reply)
    }
}

#[test]
fn interrupt_during_manifest_discards_reply_and_fresh_session_reads_same_source() {
    let fixture = Fixture::new();
    let manifest_db = fixture.0.join("artifacts.db");
    let records_db = fixture.0.join("records.db");
    let mut backing = artifact_source(&manifest_db);
    backing
        .upsert(&id("known"), b"still authoritative")
        .unwrap();
    drop(backing);
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io =
            TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let mut channels = Some((reached_tx, release_rx));
        let result = serve_tls_artifact_manifest_session(
            &mut io,
            4,
            LIMITS,
            &mut Policy,
            move |_| Ok(source(&records_db)),
            |_, _| ArtifactManifestAccess::Allowed,
            move |_| {
                let (reached, release) = channels.take().expect("exactly one read");
                Ok(HeldManifest {
                    inner: artifact_source(&manifest_db),
                    reached,
                    release,
                })
            },
        );
        match result {
            Ok(()) => {}
            Err(RecordSessionError::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::NotConnected
                ) => {}
            other => panic!("unexpected server termination: {other:?}"),
        }
    });
    let client = session(&fixture, addr);
    let mut remote = client.artifact_manifest_source();
    let read = thread::spawn(move || remote.manifest(&request("known")));
    reached_rx.recv_timeout(DEADLINE).unwrap();
    client.close().unwrap();
    let result = read.join().unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(result, Err(ManifestSourceError::Unavailable));
    server.join().unwrap();

    let fresh = observe(&fixture, &["known"], false, true);
    assert_eq!(
        fresh.replies[0].as_ref().unwrap().manifest.state,
        ArtifactState::Live(ContentIdentity::of(b"still authoritative"))
    );
}
