use super::*;
use std::{
    mem::ManuallyDrop,
    process::{Child, Stdio},
};

const WORKER: &str = "epochs::protected_process::protected_process_worker";
const STARTUP: Duration = Duration::from_secs(15);

fn ready(path: &Path, bytes: impl AsRef<[u8]>) {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).unwrap();
    fs::rename(tmp, path).unwrap();
}

fn wait_file(path: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        match fs::read_to_string(path) {
            Ok(value) => return value,
            Err(err) if err.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("waiting for {}: {err}", path.display()),
        }
    }
}

struct Worker(Child);

impl Worker {
    fn spawn(path: &Path, role: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", WORKER, "--nocapture"])
                .env("NESSA_SYNC_TEST_EPOCH_PROCESS_DIR", path)
                .env("NESSA_SYNC_TEST_EPOCH_PROCESS_ROLE", role)
                .stdin(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    fn wait(mut self) {
        let deadline = Instant::now() + STARTUP;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "fixture worker: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "fixture worker timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

struct FileGate<'a>(&'a Path);

impl ScopeAuthorizer for FileGate<'_> {
    fn authorize(&mut self, requested: &Scope) -> Access {
        let arm = self.0.join("arm-old");
        match fs::remove_file(&arm) {
            Ok(()) => {
                ready(&self.0.join("old-arrived"), "parked");
                wait_file(&self.0.join("release-old"), DEADLINE);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => panic!("fixture gate: {err}"),
        }
        Policy.authorize(requested)
    }
}

fn ledger(root: &Path) -> PathBuf {
    root.join("controller").join("claims.db")
}

fn binding() -> EpochBinding {
    EpochBinding::new(
        scope().receiver().clone(),
        scope().origin().clone(),
        key("independent-origin"),
    )
}

fn cache(root: &Path) -> PathBuf {
    root.join("receiver.db")
}

#[test]
fn protected_process_worker() {
    let Some(root) = std::env::var_os("NESSA_SYNC_TEST_EPOCH_PROCESS_DIR").map(PathBuf::from)
    else {
        return; // In a normal suite run the driver starts each worker explicitly.
    };
    let role = std::env::var("NESSA_SYNC_TEST_EPOCH_PROCESS_ROLE").unwrap();
    if matches!(role.as_str(), "receiver-floor" | "receiver-complete") {
        let mut store = SqliteEpochReplicaStore::open(
            cache(&root),
            binding(),
            EpochSessionAdmission::new(Policy),
        )
        .unwrap();
        assert_eq!(store.observed_epoch(), Ok(OriginEpoch::new(2)));
        let history = store.history_progress(&scope()).unwrap().unwrap();
        let (generation, live, payloads) = if role == "receiver-floor" {
            (1, 3, vec![b"c".to_vec()])
        } else {
            (2, 4, vec![b"c".to_vec(), b"d".to_vec()])
        };
        assert_eq!(
            (history.generation, history.lower_bound, history.live_head),
            (generation, 3, live)
        );
        assert_eq!(
            store.checkpoint(&scope()),
            Ok(Some(Checkpoint::new(scope(), live)))
        );
        assert_eq!(
            store
                .read_after(&scope(), 2, 2, 2)
                .unwrap()
                .into_iter()
                .map(|r| r.payload)
                .collect::<Vec<_>>(),
            payloads
        );
        return;
    }
    let (leaf, previous) = match role.as_str() {
        "old" => ("server", None),
        "new" => ("server-sibling", Some(OriginEpoch::FIRST)),
        _ => panic!("unknown fixture role"),
    };
    // Both processes independently open their own restored-equivalent source
    // store and the same non-restorable controller ledger. Their ephemeral
    // TLS identities are preprovisioned in this fixture, not sent to the relay.
    let fixture = ManuallyDrop::new(Fixture(root.clone()));
    let mut producer = SqliteEpochRecordSource::claim(
        source(&root.join(&role).join("records.db")),
        SqliteOriginAuthority::open(ledger(&root)).unwrap(),
        key("independent-origin"),
        previous,
    )
    .unwrap();
    let listener = endpoint();
    ready(
        &root.join(format!("{role}-endpoint")),
        listener.local_addr().unwrap().to_string(),
    );
    let (socket, _) = listener.accept().unwrap();
    let mut stream = TlsSessionIo::accept(
        tcp(socket),
        fixture.identity(leaf),
        fixture.peer("client"),
        Instant::now() + DEADLINE,
    )
    .unwrap();
    let result = if role == "old" {
        serve_epoch_record_session(
            &mut stream,
            1,
            LIMITS,
            &scope(),
            &mut FileGate(&root),
            &mut producer,
        )
    } else {
        serve_epoch_record_session(&mut stream, 1, LIMITS, &scope(), &mut Policy, &mut producer)
    };
    end(result);
}

#[test]
fn protected_epoch_handoff_across_processes_and_opaque_relay() {
    let fixture = Fixture::new();
    let root = &fixture.0;
    for name in ["controller", "old", "new"] {
        fs::create_dir(root.join(name)).unwrap();
    }
    for role in ["old", "new"] {
        let mut records = source(&root.join(role).join("records.db"));
        for name in ["a", "b", "c", "d"]
            .into_iter()
            .take(if role == "old" { 3 } else { 4 })
        {
            records.append(&id(name), name.as_bytes()).unwrap();
        }
    }
    SqliteOriginAuthority::provision(ledger(root)).unwrap();
    let old_worker = Worker::spawn(root, "old");
    let old_addr = wait_file(&root.join("old-endpoint"), STARTUP)
        .parse()
        .unwrap();
    let k = key("independent-origin");
    let old = connect(
        &fixture,
        old_addr,
        approved(
            &fixture,
            &ledger(root),
            k.clone(),
            OriginEpoch::FIRST,
            "server",
        ),
    )
    .unwrap();
    let scope = scope();
    let old_binding = binding();
    let mut store = SqliteEpochReplicaStore::provision(
        cache(root),
        old_binding.clone(),
        EpochSessionAdmission::new(Policy),
    )
    .unwrap();
    assert_eq!(
        store.observe_head(old.head().unwrap()),
        Ok(OriginEpoch::FIRST)
    );
    let limits = Limits::new(1, 1, 1).unwrap();
    let tail = TailRequest {
        scope: scope.clone(),
        generation: 1,
        max_records: 1,
        max_payload_bytes: 1,
        max_record_bytes: 1,
    };
    // Seed durable state at 3, then hold all four predecessor results.
    let saved = store
        .install_tail(old.tail(&tail, limits).unwrap())
        .unwrap();
    assert_eq!(
        (saved.generation, saved.lower_bound, saved.live_head),
        (1, 3, 3)
    );
    source(&root.join("old").join("records.db"))
        .append(&id("d"), b"d")
        .unwrap();
    let c4 = Checkpoint::new(scope.clone(), 4);
    let c3 = Checkpoint::new(scope.clone(), 3);
    let replacement = TailRequest {
        generation: 2,
        ..tail
    };
    let older = OlderRequest {
        scope: scope.clone(),
        generation: 1,
        before: 3,
        max_records: 1,
        max_payload_bytes: 1,
        max_record_bytes: 1,
    };
    let page = PageRequest {
        scope: scope.clone(),
        after: 3,
        target: 4,
        max_records: 1,
        max_payload_bytes: 1,
        max_record_bytes: 1,
    };
    let held_head = old.head().unwrap();
    let held_page = old.page(&c3, &page, limits).unwrap();
    let held_tail = old.tail(&replacement, limits).unwrap();
    let held_older = old.older(&older, limits).unwrap();

    ready(&root.join("arm-old"), "arm");
    let old_attempt = thread::spawn(move || {
        let result = old.head();
        (old, result)
    });
    wait_file(&root.join("old-arrived"), DEADLINE);
    // The predecessor is still alive during the independent claimant's CAS.
    let new_worker = Worker::spawn(root, "new");
    let new_addr = wait_file(&root.join("new-endpoint"), STARTUP)
        .parse()
        .unwrap();
    ready(&root.join("release-old"), "release");
    let (old, refused) = old_attempt.join().unwrap();
    assert!(matches!(
        refused,
        Err(EpochSessionError::Record(RecordSessionError::Source(
            SourceError::IdentityChanged
        )))
    ));
    old.close().unwrap();
    old_worker.wait();

    let e2 = OriginEpoch::new(2).unwrap();
    let (relay_addr, relay_worker) = relay(new_addr);
    let new = connect(
        &fixture,
        relay_addr,
        approved(&fixture, &ledger(root), k, e2, "server-sibling"),
    )
    .unwrap();
    assert_eq!(store.observe_head(new.head().unwrap()), Ok(e2));
    drop(store);
    Worker::spawn(root, "receiver-floor").wait();
    let mut store =
        SqliteEpochReplicaStore::open(cache(root), old_binding, EpochSessionAdmission::new(Policy))
            .unwrap();
    assert_eq!(
        store.observe_head(held_head),
        Err(EpochReplicaError::Fenced)
    );
    assert_eq!(store.apply(held_page), Err(EpochReplicaError::Fenced));
    assert_eq!(
        store.install_tail(held_tail),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
    );
    assert_eq!(
        store.install_older(held_older),
        Err(EpochHistoryError::Epoch(EpochReplicaError::Fenced))
    );
    assert_eq!(store.history_progress(&scope), Ok(Some(saved)));
    assert_eq!(store.checkpoint(&scope), Ok(Some(c3.clone())));
    let rows = store.read_after(&scope, 2, 2, 2).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload, b"c");
    store.apply(new.page(&c3, &page, limits).unwrap()).unwrap();
    assert_eq!(store.checkpoint(&scope), Ok(Some(c4)));

    // Same authorized live window after handoff; the existing history owner
    // installs the replacement generation and expands its exact lower edge.
    store
        .install_tail(new.tail(&replacement, limits).unwrap())
        .unwrap();
    store
        .install_older(
            new.older(
                &OlderRequest {
                    generation: 2,
                    before: 4,
                    ..older
                },
                limits,
            )
            .unwrap(),
        )
        .unwrap();
    drop(store);
    new.close().unwrap();
    new_worker.wait();
    let (up, down) = relay_worker.join().unwrap();
    assert!(up > 0 && down > 0);
    Worker::spawn(root, "receiver-complete").wait();
}
