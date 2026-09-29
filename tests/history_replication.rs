#![cfg(feature = "sqlite")]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use nessa_sync::replication::application::{
    begin_pass, finish_pass, Access, RecordSource, SourceError, StoreError,
};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::history::{
    fetch_older, fetch_tail, validate_older, validate_tail, HistoryError, HistoryReadState,
    HistorySource, HistorySourceError, HistoryStore, HistoryStoreError, HydrationQueue,
    OlderRequest, TailRequest,
};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        loop {
            let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "nessa-sync-history-test-{}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("temporary directory: {error}"),
            }
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("only this test's new temporary directory is removed");
    }
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn scope() -> Scope {
    Scope::new(
        id("phone"),
        id("origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
        id("epoch-1"),
    )
}

fn source(path: PathBuf) -> SqliteReferenceSource {
    SqliteReferenceSource::open(
        path,
        id("origin"),
        id("transcript"),
        id("first"),
        id("text-v1"),
    )
    .unwrap()
}

fn limits() -> Limits {
    Limits::new(64, 128 * 1024, 128 * 1024).unwrap()
}

fn tail(scope: &Scope, generation: u64, count: usize) -> TailRequest {
    TailRequest {
        scope: scope.clone(),
        generation,
        max_records: count,
        max_payload_bytes: 128 * 1024,
        max_record_bytes: 128 * 1024,
    }
}

fn older(scope: &Scope, generation: u64, before: u64, count: usize) -> OlderRequest {
    OlderRequest {
        scope: scope.clone(),
        generation,
        before,
        max_records: count,
        max_payload_bytes: 128 * 1024,
        max_record_bytes: 128 * 1024,
    }
}

fn append(source: &mut SqliteReferenceSource, start: u64, end: u64) {
    for position in start..=end {
        assert_eq!(
            source
                .append(
                    &id(&format!("fact-{position}")),
                    format!("message-{position}").as_bytes()
                )
                .unwrap(),
            position
        );
    }
}

#[test]
fn tail_live_and_older_have_independent_durable_progress() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    append(&mut source, 1, 10);

    let plan = fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source).unwrap();
    let progress = receiver.install_tail(plan).unwrap();
    assert_eq!((progress.live_head, progress.lower_bound), (10, 8));
    assert_eq!(receiver.checkpoint(&scope).unwrap().unwrap().position(), 10);
    assert_eq!(receiver.read_after(&scope, 7, 4, 100).unwrap().len(), 3);

    let delayed = fetch_older(&older(&scope, 1, 8, 3), limits(), &mut auth, &mut source).unwrap();
    append(&mut source, 11, 11);
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut receiver).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut receiver).unwrap();
    assert_eq!(pass.position(), 11);
    let progress = receiver.install_older(delayed).unwrap();
    assert_eq!((progress.live_head, progress.lower_bound), (11, 5));

    drop(receiver);
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let saved = receiver.history_progress(&scope).unwrap().unwrap();
    assert_eq!((saved.live_head, saved.lower_bound), (11, 5));
    assert_eq!(receiver.read_after(&scope, 4, 10, 1000).unwrap().len(), 7);

    let mut views = HydrationQueue::new(50).unwrap();
    for index in 0..50 {
        views.request(if index % 2 == 0 { 1 } else { 2 }).unwrap();
    }
    while let Some(target) = views.next_target(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
    ) {
        let before = receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound;
        assert!(target < before);
        let page = fetch_older(
            &older(&scope, 1, before, 3),
            limits(),
            &mut auth,
            &mut source,
        )
        .unwrap();
        let progress = receiver.install_older(page).unwrap();
        views.resolve(progress.lower_bound);
    }
    let final_progress = receiver.history_progress(&scope).unwrap().unwrap();
    assert_eq!(
        (final_progress.live_head, final_progress.lower_bound),
        (11, 1)
    );
    assert_eq!(views.waiters(), 0);
    assert_eq!(source.older_reads(), 3); // one delayed page plus two coalesced pages
}

#[test]
fn stale_snapshot_older_reply_and_deletion_fence_cannot_resurrect_data() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    append(&mut source, 1, 8);
    let delayed_tail = fetch_tail(&tail(&scope, 1, 2), limits(), &mut auth, &mut source).unwrap();
    receiver
        .install_tail(fetch_tail(&tail(&scope, 2, 2), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    assert_eq!(
        receiver.install_tail(delayed_tail).unwrap_err(),
        HistoryStoreError::Stale
    );

    let delayed_older =
        fetch_older(&older(&scope, 2, 7, 2), limits(), &mut auth, &mut source).unwrap();
    append(&mut source, 9, 9);
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut receiver).unwrap();
    finish_pass(&mut pass, limits(), &mut auth, &mut source, &mut receiver).unwrap();
    receiver
        .install_tail(fetch_tail(&tail(&scope, 3, 3), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    assert_eq!(
        receiver.install_older(delayed_older).unwrap_err(),
        HistoryStoreError::Stale
    );
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .live_head,
        9
    );

    let delayed_after_delete =
        fetch_tail(&tail(&scope, 4, 3), limits(), &mut auth, &mut source).unwrap();
    receiver.fence_deletion(&scope).unwrap();
    assert!(receiver.history_progress(&scope).unwrap().unwrap().deleted);
    assert_eq!(receiver.count(&scope).unwrap(), 0);
    assert_eq!(receiver.checkpoint(&scope), Err(StoreError::Fenced));
    assert_eq!(
        receiver.install_tail(delayed_after_delete).unwrap_err(),
        HistoryStoreError::Fenced
    );
    assert_eq!(
        receiver.read_after(&scope, 0, 4, 100),
        Err(StoreError::Fenced)
    );
    let mut pass = begin_pass(&scope, &mut auth, &mut source, &mut receiver);
    assert!(matches!(
        pass,
        Err(nessa_sync::replication::application::SyncError::Store(
            StoreError::Fenced
        ))
    ));
    pass = begin_pass(&scope, &mut auth, &mut source, &mut receiver);
    assert!(pass.is_err());
}

#[test]
fn overlap_is_idempotent_but_conflict_and_gap_leave_progress_unchanged() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    append(&mut source, 1, 10);
    receiver
        .install_tail(fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    let page = source.older(&older(&scope, 1, 8, 3)).unwrap();
    receiver
        .install_older(validate_older(&page.request, page.clone(), limits()).unwrap())
        .unwrap();
    let progress = receiver
        .install_older(validate_older(&page.request, page.clone(), limits()).unwrap())
        .unwrap();
    assert_eq!(progress.lower_bound, 5);

    let mut conflicting = page.clone();
    conflicting.records[0].payload = b"different".to_vec();
    let conflict_plan =
        validate_older(&conflicting.request.clone(), conflicting, limits()).unwrap();
    assert_eq!(
        receiver.install_older(conflict_plan).unwrap_err(),
        HistoryStoreError::Conflict
    );
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
        5
    );

    let gap_page = source.older(&older(&scope, 1, 4, 2)).unwrap();
    let gap_plan = validate_older(&gap_page.request.clone(), gap_page, limits()).unwrap();
    assert_eq!(
        receiver.install_older(gap_plan).unwrap_err(),
        HistoryStoreError::Gap
    );
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
        5
    );
}

#[test]
fn pruning_and_incompatible_scope_return_typed_reset_requirement() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    append(&mut source, 1, 10);
    receiver
        .install_tail(fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    source.prune_through(7).unwrap();
    assert_eq!(
        source.append(&id("fact-1"), b"message-1").unwrap(),
        1,
        "pruning keeps immutable ID deduplication"
    );
    assert_eq!(
        source.older(&older(&scope, 1, 8, 3)),
        Err(HistorySourceError::ResetRequired)
    );
    assert_eq!(
        source.page(&nessa_sync::replication::domain::PageRequest {
            scope: scope.clone(),
            after: 0,
            target: 10,
            max_records: 2,
            max_payload_bytes: 100,
            max_record_bytes: 100,
        }),
        Err(SourceError::Pruned)
    );
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
        8
    );

    let different = Scope::new(
        scope.receiver().clone(),
        scope.origin().clone(),
        scope.stream().clone(),
        scope.incarnation().clone(),
        id("text-v2"),
        scope.access_epoch().clone(),
    );
    let fake = nessa_sync::replication::history::TailSnapshot {
        request: tail(&different, 2, 3),
        watermark: 0,
        first: 1,
        oldest_available: 1,
        records: Vec::new(),
    };
    let plan = validate_tail(&fake.request, fake.clone(), limits()).unwrap();
    assert_eq!(
        receiver.install_tail(plan).unwrap_err(),
        HistoryStoreError::ResetRequired
    );
}

#[test]
fn failed_older_commit_rolls_back_records_and_lower_boundary() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    append(&mut source, 1, 10);
    receiver
        .install_tail(fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    let page = source.older(&older(&scope, 1, 8, 3)).unwrap();
    let competing = rusqlite::Connection::open(directory.path("receiver.db")).unwrap();
    competing
        .execute_batch(
            "CREATE TRIGGER fail_history BEFORE INSERT ON replica_records
             WHEN NEW.position = 6 BEGIN SELECT RAISE(FAIL, 'injected'); END;",
        )
        .unwrap();
    let failed =
        receiver.install_older(validate_older(&page.request, page.clone(), limits()).unwrap());
    assert_eq!(failed, Err(HistoryStoreError::Conflict));
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
        8
    );
    assert_eq!(receiver.count(&scope).unwrap(), 3);
    competing
        .execute_batch("DROP TRIGGER fail_history")
        .unwrap();
    drop(receiver);
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    receiver
        .install_older(validate_older(&page.request.clone(), page, limits()).unwrap())
        .unwrap();
    assert_eq!(
        receiver
            .history_progress(&scope)
            .unwrap()
            .unwrap()
            .lower_bound,
        5
    );
    assert_eq!(receiver.count(&scope).unwrap(), 6);
}

#[test]
fn authorization_and_view_states_preserve_unknown_vs_empty() {
    let directory = TestDirectory::new();
    let mut source = source(directory.path("source.db"));
    let scope = scope();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    auth.decision = Access::Denied;
    assert!(matches!(
        fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source),
        Err(HistoryError::Denied)
    ));
    assert_eq!(source.tail_reads(), 0);
    auth.decision = Access::Unverifiable;
    assert!(matches!(
        fetch_older(&older(&scope, 1, 5, 3), limits(), &mut auth, &mut source),
        Err(HistoryError::Unverifiable)
    ));
    assert_eq!(source.older_reads(), 0);
    assert!(matches!(
        HistoryReadState::from_progress(None),
        HistoryReadState::Unloaded
    ));
    auth.decision = Access::Allowed(scope.clone());
    let mut receiver = SqliteReplicaStore::open(directory.path("receiver.db")).unwrap();
    let empty = receiver
        .install_tail(fetch_tail(&tail(&scope, 1, 3), limits(), &mut auth, &mut source).unwrap())
        .unwrap();
    assert!(matches!(
        HistoryReadState::from_progress(Some(empty.clone())),
        HistoryReadState::CompleteEmpty(_)
    ));
    assert!(matches!(
        HistoryReadState::Loading(Some(empty.clone())),
        HistoryReadState::Loading(_)
    ));
    assert!(matches!(
        HistoryReadState::Failed(
            Some(empty.clone()),
            HistoryError::Source(HistorySourceError::Unavailable)
        ),
        HistoryReadState::Failed(_, _)
    ));
    assert!(matches!(
        HistoryReadState::Stale(empty),
        HistoryReadState::Stale(_)
    ));
}
