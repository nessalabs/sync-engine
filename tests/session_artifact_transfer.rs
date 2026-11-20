#![cfg(all(feature = "tls", feature = "sqlite"))]

use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer},
    artifacts::{
        publish_if_current, transfer_one, ArtifactKey, ChunkReply, ChunkRequest, ChunkSource,
        ChunkSourceError, ManifestReply, ManifestRequest, ManifestSource, ManifestSourceError,
        Sha256Digest, TransferError, TransferStore,
    },
    domain::{Id, Scope},
    infrastructure::{
        session::{
            serve_tls_artifact_transfer_session, ArtifactSessionAccess, ArtifactSessionOperation,
            RecordSession, RecordSessionError, SessionLimits, TcpSessionIo, TlsIdentity, TlsPeer,
            TlsSessionIo,
        },
        SqliteArtifactCache, SqliteArtifactSource, SqliteReferenceSource,
    },
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore,
};

use std::{
    fs,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
            "sync-session-artifact-transfer-{}-{}",
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

fn disconnected(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
    )
}
fn pump(mut from: TcpStream, mut to: TcpStream, bytes: &AtomicU64) -> io::Result<()> {
    let result = (|| {
        let mut buffer = [0; 4096];
        loop {
            let n = match from.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(n) => n,
                Err(e) if disconnected(&e) => return Ok(()),
                Err(e) => return Err(e),
            };
            let mut sent = 0;
            while sent < n {
                match to.write(&buffer[sent..n]) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(written) => {
                        bytes.fetch_add(written as u64, Ordering::SeqCst);
                        sent += written;
                    }
                    Err(e) if disconnected(&e) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        }
    })();
    let _ = to.shutdown(Shutdown::Write);
    result
}
struct Relay {
    bytes_down: Arc<AtomicU64>,
    sockets: std::sync::mpsc::Receiver<(TcpStream, TcpStream)>,
    worker: thread::JoinHandle<(u64, u64)>,
}
impl Relay {
    fn cut(&self) {
        let (client, server) = self.sockets.recv_timeout(DEADLINE).unwrap();
        for socket in [client, server] {
            socket.shutdown(Shutdown::Both).unwrap();
        }
    }
}
fn relay(target: SocketAddr) -> (SocketAddr, Relay) {
    let socket = endpoint();
    let addr = socket.local_addr().unwrap();
    let bytes_down = Arc::new(AtomicU64::new(0));
    let downstream = bytes_down.clone();
    let (ready, sockets) = std::sync::mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let (incoming, _) = socket.accept().unwrap();
        let outgoing = TcpStream::connect(target).unwrap();
        for socket in [&incoming, &outgoing] {
            socket.set_read_timeout(Some(DEADLINE)).unwrap();
            socket.set_write_timeout(Some(DEADLINE)).unwrap();
            socket.set_nodelay(true).unwrap();
        }
        ready
            .send((incoming.try_clone().unwrap(), outgoing.try_clone().unwrap()))
            .unwrap();
        let (incoming_copy, outgoing_copy) =
            (incoming.try_clone().unwrap(), outgoing.try_clone().unwrap());
        let up = thread::spawn(move || {
            let bytes = AtomicU64::new(0);
            pump(incoming_copy, outgoing_copy, &bytes).unwrap();
            bytes.load(Ordering::SeqCst)
        });
        pump(outgoing, incoming, &downstream).unwrap();
        (up.join().unwrap(), downstream.load(Ordering::SeqCst))
    });
    (
        addr,
        Relay {
            bytes_down,
            sockets,
            worker,
        },
    )
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
const CHUNK: usize = 8192;
const BODY: usize = 2 * CHUNK + 91;

struct MeasuredSource {
    inner: SqliteArtifactSource,
    acquired: Arc<AtomicU64>,
    last_offset: Arc<AtomicU64>,
    reply_ready: Arc<AtomicBool>,
}
impl ManifestSource for MeasuredSource {
    fn manifest(&mut self, key: &ManifestRequest) -> Result<ManifestReply, ManifestSourceError> {
        self.inner.manifest(key)
    }
}
impl ChunkSource for MeasuredSource {
    fn chunk(&mut self, request: &ChunkRequest) -> Result<ChunkReply, ChunkSourceError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        self.last_offset.store(request.offset, Ordering::SeqCst);
        let reply = self.inner.chunk(request);
        self.reply_ready.store(reply.is_ok(), Ordering::SeqCst);
        reply
    }
}

fn normal_or_peer_closed(result: Result<(), RecordSessionError>) {
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
        other => panic!("unexpected server termination {other:?}"),
    }
}

#[derive(Default, Clone)]
struct Measurements {
    acquired: Arc<AtomicU64>,
    factories: Arc<AtomicU64>,
    last_offset: Arc<AtomicU64>,
    reply_ready: Arc<AtomicBool>,
}
type Pause = (
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
    bool,
);

fn serve(
    listener: TcpListener,
    fixture: &Fixture,
    measure: Measurements,
    deny_chunk: bool,
    pause: Option<Pause>,
) -> thread::JoinHandle<()> {
    let manifest_db = fixture.0.join("artifacts.db");
    let record_db = fixture.0.join("records.db");
    let local = fixture.identity("server");
    let peer = fixture.peer("client");
    let certificate = Sha256Digest::of(fixture.cert("client").as_ref());
    thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io =
            TlsSessionIo::accept(tcp(socket), local, peer, Instant::now() + DEADLINE).unwrap();
        let access = measure.clone();
        let mut after_read_checks = 0;
        let mut pause = pause;
        normal_or_peer_closed(serve_tls_artifact_transfer_session(
            &mut io,
            1,
            LIMITS,
            &mut Policy,
            move |_| Ok(source(&record_db)),
            move |leaf, operation| {
                assert_eq!(leaf, certificate);
                match operation {
                    ArtifactSessionOperation::Manifest(_) => ArtifactSessionAccess::Allowed,
                    ArtifactSessionOperation::Chunk(chunk) => {
                        if deny_chunk {
                            return ArtifactSessionAccess::Denied;
                        }
                        if chunk.offset == CHUNK as u64
                            && access.reply_ready.load(Ordering::SeqCst)
                            && access.last_offset.load(Ordering::SeqCst) == chunk.offset
                        {
                            after_read_checks += 1;
                            if after_read_checks == 2 {
                                if let Some((reached, release, revoke)) = pause.take() {
                                    reached.send(()).unwrap();
                                    release.recv_timeout(DEADLINE).unwrap();
                                    if revoke {
                                        return ArtifactSessionAccess::Denied;
                                    }
                                }
                            }
                        }
                        ArtifactSessionAccess::Allowed
                    }
                }
            },
            move |_| {
                measure.factories.fetch_add(1, Ordering::SeqCst);
                measure.reply_ready.store(false, Ordering::SeqCst);
                Ok(MeasuredSource {
                    inner: artifact_source(&manifest_db),
                    acquired: measure.acquired.clone(),
                    last_offset: measure.last_offset.clone(),
                    reply_ready: measure.reply_ready.clone(),
                })
            },
        ));
    })
}

fn connect_path(listener: &TcpListener, relayed: bool) -> (SocketAddr, Option<Relay>) {
    let addr = listener.local_addr().unwrap();
    if relayed {
        let (proxy, thread) = relay(addr);
        (proxy, Some(thread))
    } else {
        (addr, None)
    }
}

#[test]
fn pinned_tls_interrupted_chunk_reopens_at_durable_offset_and_publishes_exact_hash() {
    for relayed in [false, true] {
        let fixture = Fixture::new();
        let bytes: Vec<u8> = (0..BODY).map(|n| (n % 239) as u8).collect();
        artifact_source(&fixture.0.join("artifacts.db"))
            .upsert(&id("recording"), &bytes)
            .unwrap();
        let listener = endpoint();
        let (addr, proxy) = connect_path(&listener, relayed);
        let measure = Measurements::default();
        let (hit, reached) = std::sync::mpsc::sync_channel(0);
        let (release, wait) = std::sync::mpsc::sync_channel(0);
        let worker = serve(
            listener,
            &fixture,
            measure.clone(),
            false,
            Some((hit, wait, false)),
        );
        let client = session(&fixture, addr);
        let mut wire = client.artifact_transfer_source();
        let manifest = wire.manifest(&request("recording")).unwrap().manifest;
        let db = fixture.0.join("cache.db");
        let mut store = SqliteArtifactCache::open(&db).unwrap();
        let first = transfer_one(&manifest, CHUNK, &mut wire, &mut store).unwrap();
        assert_eq!(first.next_offset, CHUNK as u64);
        assert!(!first.verified);
        let before = client.counters().unwrap();
        assert_eq!(before.artifact_payload_bytes, CHUNK as u64);
        let relay_before = proxy.as_ref().map(|p| p.bytes_down.load(Ordering::SeqCst));
        let needed = manifest.clone();
        let attempt = thread::spawn(move || transfer_one(&needed, CHUNK, &mut wire, &mut store));
        reached.recv_timeout(DEADLINE).unwrap();
        if let (Some(proxy), Some(start)) = (&proxy, relay_before) {
            let deadline = Instant::now() + DEADLINE;
            while proxy.bytes_down.load(Ordering::SeqCst) - start < 2048 {
                assert!(
                    Instant::now() < deadline,
                    "relay did not forward the first TLS fragment"
                );
                thread::yield_now();
            }
            eprintln!(
                "Interrupted range ciphertext forwarded before cut: {} B (includes TLS framing)",
                proxy.bytes_down.load(Ordering::SeqCst) - start
            );
        }
        if let Some(proxy) = &proxy {
            proxy.cut();
        } else {
            client.close().unwrap();
        }
        assert_eq!(
            attempt.join().unwrap(),
            Err(TransferError::ChunkSource(ChunkSourceError::Unavailable))
        );
        client.close().unwrap();
        let interrupted = client.counters().unwrap();
        assert_eq!(
            interrupted.artifact_payload_bytes,
            before.artifact_payload_bytes
        );
        assert_eq!(interrupted.exchanges, before.exchanges);
        eprintln!(
            "Interrupted relayed={relayed}: {} additional unclassified app wire bytes, no complete chunk or commit",
            interrupted.protocol_bytes - before.protocol_bytes
        );
        release.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(measure.acquired.load(Ordering::SeqCst), 2);
        assert_eq!(measure.last_offset.load(Ordering::SeqCst), CHUNK as u64);
        if let Some(proxy) = proxy {
            let (up, down) = proxy.worker.join().unwrap();
            eprintln!("Interrupted opaque TLS connection raw bytes: {up} up / {down} down (all TLS records, not decoded chunk prefix)");
        }

        let mut store = SqliteArtifactCache::open(&db).unwrap();
        let reopened = store.progress(&manifest).unwrap().unwrap();
        assert_eq!(reopened, first);
        assert_eq!(store.read_verified(&manifest).unwrap(), None);
        let listener = endpoint();
        let (addr, proxy) = connect_path(&listener, relayed);
        let measure = Measurements::default();
        let worker = serve(listener, &fixture, measure.clone(), false, None);
        let fresh = session(&fixture, addr);
        let mut wire = fresh.artifact_transfer_source();
        let retried = transfer_one(&manifest, CHUNK, &mut wire, &mut store).unwrap();
        assert_eq!(measure.last_offset.load(Ordering::SeqCst), CHUNK as u64);
        assert_eq!(retried.next_offset, (2 * CHUNK) as u64);
        let complete = transfer_one(&manifest, CHUNK, &mut wire, &mut store).unwrap();
        assert_eq!(complete.next_offset, BODY as u64);
        assert!(!complete.verified);
        assert!(
            publish_if_current(&manifest, &mut wire, &mut store)
                .unwrap()
                .verified
        );
        let after = fresh.counters().unwrap();
        assert_eq!(after.artifact_payload_bytes, (BODY - CHUNK) as u64);
        assert_eq!(measure.acquired.load(Ordering::SeqCst), 2);
        eprintln!("Resumed relayed={relayed}: {} exchanges / {} app protocol / {} app payload; interrupted and retried range each requested {CHUNK} B, only retry committed", after.exchanges, after.protocol_bytes, after.artifact_payload_bytes);
        fresh.close().unwrap();
        worker.join().unwrap();
        if let Some(proxy) = proxy {
            let (up, down) = proxy.worker.join().unwrap();
            eprintln!("Resumed opaque TLS connection raw bytes: {up} up / {down} down");
        }
        drop(store);
        assert_eq!(
            SqliteArtifactCache::open(&db)
                .unwrap()
                .read_verified(&manifest)
                .unwrap(),
            Some(bytes)
        );
    }
}

#[test]
fn manifest_allow_does_not_grant_range_or_advance_staging() {
    let fixture = Fixture::new();
    artifact_source(&fixture.0.join("artifacts.db"))
        .upsert(&id("recording"), b"content")
        .unwrap();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let measure = Measurements::default();
    let worker = serve(listener, &fixture, measure.clone(), true, None);
    let client = session(&fixture, addr);
    let mut wire = client.artifact_transfer_source();
    let manifest = wire.manifest(&request("recording")).unwrap().manifest;
    let db = fixture.0.join("cache.db");
    let mut store = SqliteArtifactCache::open(&db).unwrap();
    let empty = store.accept_manifest(&manifest).unwrap();
    assert_eq!(
        transfer_one(&manifest, CHUNK, &mut wire, &mut store),
        Err(TransferError::ChunkSource(ChunkSourceError::Denied))
    );
    let mut absent = manifest.clone();
    absent.key.id = id("missing");
    assert_eq!(
        wire.chunk(&ChunkRequest {
            manifest: absent,
            offset: 0,
            max_bytes: CHUNK,
        }),
        Err(ChunkSourceError::Denied)
    );
    assert_eq!(measure.acquired.load(Ordering::SeqCst), 0);
    assert_eq!(measure.factories.load(Ordering::SeqCst), 1);
    assert_eq!(client.counters().unwrap().artifact_payload_bytes, 0);
    drop(store);
    assert_eq!(
        SqliteArtifactCache::open(&db)
            .unwrap()
            .progress(&manifest)
            .unwrap(),
        Some(empty)
    );
    client.close().unwrap();
    worker.join().unwrap();
}

#[test]
fn stale_and_deleted_source_versions_refuse_old_range_without_stage_changes() {
    let fixture = Fixture::new();
    let mut source = artifact_source(&fixture.0.join("artifacts.db"));
    source.upsert(&id("recording"), b"original").unwrap();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let worker = serve(listener, &fixture, Measurements::default(), false, None);
    let client = session(&fixture, addr);
    let mut wire = client.artifact_transfer_source();
    let manifest = wire.manifest(&request("recording")).unwrap().manifest;
    let mut store = SqliteArtifactCache::open(fixture.0.join("cache.db")).unwrap();
    let empty = store.accept_manifest(&manifest).unwrap();
    source.upsert(&id("recording"), b"new version").unwrap();
    assert_eq!(
        transfer_one(&manifest, CHUNK, &mut wire, &mut store),
        Err(TransferError::ChunkSource(ChunkSourceError::VersionChanged))
    );
    assert_eq!(store.progress(&manifest).unwrap(), Some(empty.clone()));
    source.delete(&id("recording")).unwrap();
    assert_eq!(
        transfer_one(&manifest, CHUNK, &mut wire, &mut store),
        Err(TransferError::ChunkSource(ChunkSourceError::Deleted))
    );
    assert_eq!(store.progress(&manifest).unwrap(), Some(empty));
    assert_eq!(client.counters().unwrap().artifact_payload_bytes, 0);
    client.close().unwrap();
    worker.join().unwrap();
}

#[test]
fn revocation_after_first_fragment_cannot_stage_a_partial_range() {
    let fixture = Fixture::new();
    let bytes = vec![42; BODY];
    artifact_source(&fixture.0.join("artifacts.db"))
        .upsert(&id("recording"), &bytes)
        .unwrap();
    let listener = endpoint();
    let addr = listener.local_addr().unwrap();
    let (hit, reached) = std::sync::mpsc::sync_channel(0);
    let (release, wait) = std::sync::mpsc::sync_channel(0);
    let worker = serve(
        listener,
        &fixture,
        Measurements::default(),
        false,
        Some((hit, wait, true)),
    );
    let client = session(&fixture, addr);
    let mut wire = client.artifact_transfer_source();
    let manifest = wire.manifest(&request("recording")).unwrap().manifest;
    let db = fixture.0.join("cache.db");
    let mut store = SqliteArtifactCache::open(&db).unwrap();
    let first = transfer_one(&manifest, CHUNK, &mut wire, &mut store).unwrap();
    let retained = manifest.clone();
    let read = thread::spawn(move || transfer_one(&retained, CHUNK, &mut wire, &mut store));
    reached.recv_timeout(DEADLINE).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        read.join().unwrap(),
        Err(TransferError::ChunkSource(ChunkSourceError::Denied))
    );
    let counters = client.counters().unwrap();
    assert_eq!(counters.artifact_payload_bytes, CHUNK as u64);
    let mut reopened = SqliteArtifactCache::open(&db).unwrap();
    assert_eq!(reopened.progress(&manifest).unwrap(), Some(first));
    assert_eq!(reopened.read_verified(&manifest).unwrap(), None);
    client.close().unwrap();
    worker.join().unwrap();
}
