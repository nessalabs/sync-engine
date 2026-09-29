#![cfg(feature = "sqlite")]

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use nessa_sync::replication::application::Access;
use nessa_sync::replication::catalogue::{
    apply_next_page, begin_or_resume, reset_catalogue, validate_manifest, CatalogueError,
    CataloguePagePlan, CataloguePass, CatalogueSource, CatalogueSourceError, CatalogueStore,
    CatalogueStoreError, CatalogueValidationError, ManifestPage, ManifestRequest, ResolvedEntry,
    MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES,
};
use nessa_sync::replication::domain::{Id, Scope};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        loop {
            let path = std::env::temp_dir().join(format!(
                "nessa-catalogue-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("temporary directory: {error}"),
            }
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope(epoch: &str) -> Scope {
    Scope::new(
        id("phone"),
        id("gateway"),
        id("catalogue"),
        id("first"),
        id("opaque-v1"),
        id(epoch),
    )
}
fn source(path: PathBuf) -> SqliteCatalogueSource {
    SqliteCatalogueSource::open(
        path,
        id("gateway"),
        id("catalogue"),
        id("first"),
        id("opaque-v1"),
    )
    .unwrap()
}
fn run_pass(
    source: &mut SqliteCatalogueSource,
    store: &mut SqliteCatalogueStore,
    scope: &Scope,
    size: usize,
) -> u64 {
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_or_resume(scope, &mut auth, source, store)
        .unwrap()
        .unwrap();
    let boundary = pass.boundary;
    loop {
        let next = apply_next_page(&pass, size, 1024 * 1024, &mut auth, source, store).unwrap();
        match next.active {
            Some(active) => pass = active,
            None => break,
        }
    }
    boundary
}

#[test]
fn public_catalogue_bounds_accept_the_boundary_and_reject_over_bound_requests() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let selected = scope("epoch-1");
    for number in 0..MAX_CATALOGUE_ENTRIES {
        src.upsert(&id(&format!("item-{number:03}")), b"x").unwrap();
    }
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let request = ManifestRequest {
        pass: pass.clone(),
        max_entries: MAX_CATALOGUE_ENTRIES,
    };
    let page = src.manifest(&request).unwrap();
    assert_eq!(page.entries.len(), MAX_CATALOGUE_ENTRIES);
    assert_eq!(validate_manifest(&request, &page, usize::MAX), Ok(()));
    let over = ManifestRequest {
        max_entries: MAX_CATALOGUE_ENTRIES + 1,
        ..request
    };
    let over_page = ManifestPage {
        request: over.clone(),
        ..page
    };
    assert_eq!(
        validate_manifest(&over, &over_page, usize::MAX),
        Err(CatalogueValidationError::InvalidRequest)
    );
    assert_eq!(
        apply_next_page(
            &pass,
            MAX_CATALOGUE_ENTRIES + 1,
            MAX_CATALOGUE_PAYLOAD_BYTES,
            &mut auth,
            &mut src,
            &mut dst,
        ),
        Err(CatalogueError::Validation(
            CatalogueValidationError::InvalidRequest
        ))
    );
    assert_eq!(
        dst.progress(&selected).unwrap().unwrap().active,
        Some(pass.clone())
    );
    let done = apply_next_page(
        &pass,
        MAX_CATALOGUE_ENTRIES,
        MAX_CATALOGUE_PAYLOAD_BYTES,
        &mut auth,
        &mut src,
        &mut dst,
    )
    .unwrap();
    assert!(done.active.is_none());
    assert_eq!(dst.count(&selected).unwrap(), MAX_CATALOGUE_ENTRIES as u64);

    let payload = vec![b'x'; MAX_CATALOGUE_PAYLOAD_BYTES];
    src.upsert(&id("large"), &payload).unwrap();
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    assert_eq!(
        apply_next_page(
            &pass,
            1,
            MAX_CATALOGUE_PAYLOAD_BYTES + 1,
            &mut auth,
            &mut src,
            &mut dst,
        ),
        Err(CatalogueError::Validation(
            CatalogueValidationError::InvalidRequest
        ))
    );
    assert_eq!(
        dst.progress(&selected).unwrap().unwrap().active,
        Some(pass.clone())
    );
    let next = apply_next_page(
        &pass,
        1,
        MAX_CATALOGUE_PAYLOAD_BYTES,
        &mut auth,
        &mut src,
        &mut dst,
    )
    .unwrap();
    assert!(next.active.is_none());
    assert_eq!(
        dst.cached_entry(&selected, &id("large"))
            .unwrap()
            .unwrap()
            .payload
            .len(),
        MAX_CATALOGUE_PAYLOAD_BYTES
    );
}

#[test]
fn fixed_pass_finishes_under_churn_then_catches_earlier_edits_and_new_entries() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let scope = scope("epoch-1");
    for number in 0..600 {
        src.upsert(&id(&format!("item-{number:03}")), b"old")
            .unwrap();
    }
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let mut pass = begin_or_resume(&scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    assert_eq!(pass.boundary, 600);
    let first = apply_next_page(&pass, 40, 4096, &mut auth, &mut src, &mut dst).unwrap();
    pass = first.active.unwrap();
    src.upsert(&id("item-000"), b"new").unwrap();
    src.upsert(&id("item-600"), b"born-later").unwrap();
    while let Some(next) = apply_next_page(&pass, 40, 4096, &mut auth, &mut src, &mut dst)
        .unwrap()
        .active
    {
        pass = next;
    }
    let finished = dst.progress(&scope).unwrap().unwrap();
    assert_eq!(finished.completed, 600);
    assert_eq!(dst.count(&scope).unwrap(), 600);
    assert_eq!(
        dst.cached_entry(&scope, &id("item-000"))
            .unwrap()
            .unwrap()
            .payload,
        b"old"
    );
    let next_boundary = run_pass(&mut src, &mut dst, &scope, 40);
    assert_eq!(next_boundary, 602);
    assert_eq!(
        dst.cached_entry(&scope, &id("item-000"))
            .unwrap()
            .unwrap()
            .payload,
        b"new"
    );
    assert_eq!(
        dst.cached_entry(&scope, &id("item-600"))
            .unwrap()
            .unwrap()
            .payload,
        b"born-later"
    );
    let before = src.payload_bytes();
    assert!(begin_or_resume(&scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .is_none());
    assert_eq!(src.payload_bytes(), before);
    assert!(src.manifest_bytes() < 600 * 64);
}

#[test]
fn a_pass_finishes_while_an_earlier_entry_changes_after_every_page() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let selected = scope("epoch-1");
    for number in 0..100 {
        src.upsert(&id(&format!("item-{number:03}")), b"old")
            .unwrap();
    }
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let mut pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let mut pages = 0;
    loop {
        let next = apply_next_page(&pass, 10, 1024, &mut auth, &mut src, &mut dst).unwrap();
        pages += 1;
        src.upsert(&id("item-000"), format!("edit-{pages}").as_bytes())
            .unwrap();
        match next.active {
            Some(active) => pass = active,
            None => break,
        }
    }
    assert_eq!(pages, 10);
    assert_eq!(dst.progress(&selected).unwrap().unwrap().completed, 100);
    assert_eq!(run_pass(&mut src, &mut dst, &selected, 10), 110);
    assert_eq!(
        dst.cached_entry(&selected, &id("item-000"))
            .unwrap()
            .unwrap()
            .payload,
        b"edit-10"
    );
}

#[test]
fn empty_catalogue_is_durably_confirmed_without_a_manifest_page() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let receiver_path = dir.path("receiver.db");
    let selected = scope("epoch-1");
    let mut dst = SqliteCatalogueStore::open(&receiver_path).unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    assert!(begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .is_none());
    let saved = dst.progress(&selected).unwrap().unwrap();
    assert_eq!(saved.completed, 0);
    assert!(saved.active.is_none());
    assert_eq!(src.manifest_reads(), 0);
    drop(dst);
    let mut restarted = SqliteCatalogueStore::open(receiver_path).unwrap();
    assert_eq!(restarted.progress(&selected).unwrap(), Some(saved));
}

#[test]
fn competing_source_handles_assign_distinct_revisions_and_identical_write_is_quiet() {
    let dir = Directory::new();
    let path = dir.path("source.db");
    let mut first = source(path.clone());
    let mut second = source(path);
    assert_eq!(first.upsert(&id("a"), b"same").unwrap(), 1);
    assert_eq!(second.upsert(&id("b"), b"other").unwrap(), 2);
    assert_eq!(first.upsert(&id("a"), b"same").unwrap(), 1);
    assert_eq!(second.head(&scope("epoch-1")).unwrap(), 2);
}

#[test]
fn malformed_manifest_and_unresolved_page_cannot_advance_progress() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let selected = scope("epoch-1");
    src.upsert(&id("a"), b"value").unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let request = ManifestRequest {
        pass: pass.clone(),
        max_entries: 1,
    };
    let valid = src.manifest(&request).unwrap();
    let mut wrong = valid.clone();
    wrong.entries[0].key.creation = pass.boundary + 1;
    assert_eq!(
        validate_manifest(&request, &wrong, 256),
        Err(CatalogueValidationError::InvalidOrder)
    );
    wrong = valid.clone();
    wrong.request.pass.generation += 1;
    assert_eq!(
        validate_manifest(&request, &wrong, 256),
        Err(CatalogueValidationError::WrongRequest)
    );
    let unresolved = CataloguePagePlan {
        pass: pass.clone(),
        manifest: valid.clone(),
        next_cursor: Some(valid.entries[0].key.clone()),
        final_page: true,
        entries: Vec::new(),
        unchanged: Vec::new(),
    };
    assert_eq!(
        dst.apply_page(unresolved),
        Err(CatalogueStoreError::Conflict)
    );
    assert_eq!(dst.progress(&selected).unwrap().unwrap().active, Some(pass));
    assert_eq!(dst.count(&selected).unwrap(), 0);
}

#[test]
fn changed_and_deleted_payloads_resolve_at_latest_revision_before_page_commit() {
    struct RacingSource(SqliteCatalogueSource, bool);
    impl CatalogueSource for RacingSource {
        fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError> {
            self.0.head(scope)
        }
        fn manifest(
            &mut self,
            request: &ManifestRequest,
        ) -> Result<ManifestPage, CatalogueSourceError> {
            let page = self.0.manifest(request)?;
            if !self.1 {
                self.0.upsert(&id("a"), b"latest").unwrap();
                self.0.delete(&id("b")).unwrap();
                self.1 = true;
            }
            Ok(page)
        }
        fn resolve(
            &mut self,
            pass: &CataloguePass,
            entry_id: &Id,
            limit: usize,
        ) -> Result<ResolvedEntry, CatalogueSourceError> {
            self.0.resolve(pass, entry_id, limit)
        }
    }
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let scope = scope("epoch-1");
    src.upsert(&id("a"), b"old").unwrap();
    src.upsert(&id("b"), b"old").unwrap();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    let pass = begin_or_resume(&scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let mut racing = RacingSource(src, false);
    let done = apply_next_page(&pass, 2, 1024, &mut auth, &mut racing, &mut dst).unwrap();
    assert!(done.active.is_none());
    assert_eq!(done.completed, 2);
    assert_eq!(
        dst.cached_entry(&scope, &id("a")).unwrap().unwrap().payload,
        b"latest"
    );
    let deleted = dst.cached_entry(&scope, &id("b")).unwrap().unwrap();
    assert!(deleted.manifest.deleted);
    assert!(deleted.payload.is_empty());
    assert_eq!(run_pass(&mut racing.0, &mut dst, &scope, 2), 4);
    assert_eq!(dst.count(&scope).unwrap(), 2);
}

#[test]
fn interrupted_page_restarts_at_saved_cursor_and_stale_reset_cannot_restore_content() {
    let dir = Directory::new();
    let source_path = dir.path("source.db");
    let receiver_path = dir.path("receiver.db");
    let mut src = source(source_path.clone());
    let mut dst = SqliteCatalogueStore::open(receiver_path.clone()).unwrap();
    let old_scope = scope("epoch-1");
    for number in 0..5 {
        src.upsert(&id(&format!("item-{number}")), b"content")
            .unwrap();
    }
    let mut auth = MemoryAuthorizer::allowed(old_scope.clone());
    let pass = begin_or_resume(&old_scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let first = apply_next_page(&pass, 2, 1024, &mut auth, &mut src, &mut dst).unwrap();
    assert_eq!(
        first.active.as_ref().unwrap().cursor.as_ref().unwrap().id,
        id("item-1")
    );
    drop(dst);
    let mut dst = SqliteCatalogueStore::open(receiver_path.clone()).unwrap();
    let resumed = begin_or_resume(&old_scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    assert_eq!(resumed, first.active.unwrap());
    let request = ManifestRequest {
        pass: resumed.clone(),
        max_entries: 2,
    };
    let page = src.manifest(&request).unwrap();
    let resolved: Vec<_> = page
        .entries
        .iter()
        .map(|entry| src.resolve(&resumed, &entry.key.id, 1024).unwrap())
        .collect();
    let stale = CataloguePagePlan {
        pass: resumed.clone(),
        manifest: page.clone(),
        next_cursor: page.entries.last().map(|entry| entry.key.clone()),
        final_page: !page.has_more,
        entries: resolved,
        unchanged: Vec::new(),
    };
    let new_scope = Scope::new(
        id("phone"),
        id("gateway"),
        id("catalogue"),
        id("first"),
        id("opaque-v1"),
        id("epoch-2"),
    );
    let mut new_auth = MemoryAuthorizer::allowed(new_scope.clone());
    reset_catalogue(
        &new_scope,
        dst.progress(&old_scope).unwrap().unwrap(),
        &mut new_auth,
        &mut dst,
    )
    .unwrap();
    assert_eq!(
        dst.apply_page(stale),
        Err(CatalogueStoreError::ResetRequired)
    );
    assert_eq!(dst.count(&new_scope).unwrap(), 0);
    assert_eq!(run_pass(&mut src, &mut dst, &new_scope, 2), 5);
    assert_eq!(dst.count(&new_scope).unwrap(), 5);
}

#[test]
fn denial_precedes_source_reads_and_deleted_marker_fences_stale_content() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let scope = scope("epoch-1");
    src.upsert(&id("x"), b"content").unwrap();
    let mut auth = MemoryAuthorizer::allowed(scope.clone());
    auth.decision = Access::Denied;
    assert_eq!(
        begin_or_resume(&scope, &mut auth, &mut src, &mut dst),
        Err(CatalogueError::Denied)
    );
    assert_eq!(src.head_reads(), 0);
    auth.decision = Access::Allowed(scope.clone());
    run_pass(&mut src, &mut dst, &scope, 1);
    src.upsert(&id("y"), b"new").unwrap();
    let in_flight = begin_or_resume(&scope, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let manifest_reads = src.manifest_reads();
    auth.decision = Access::Denied;
    assert_eq!(
        apply_next_page(&in_flight, 1, 1024, &mut auth, &mut src, &mut dst),
        Err(CatalogueError::Denied)
    );
    assert_eq!(src.manifest_reads(), manifest_reads);
    auth.decision = Access::Allowed(scope.clone());
    run_pass(&mut src, &mut dst, &scope, 1);
    src.delete(&id("x")).unwrap();
    run_pass(&mut src, &mut dst, &scope, 1);
    assert!(
        dst.cached_entry(&scope, &id("x"))
            .unwrap()
            .unwrap()
            .manifest
            .deleted
    );
    assert_eq!(
        src.upsert(&id("x"), b"resurrected"),
        Err(CatalogueStoreError::Fenced)
    );
    let new_scope = Scope::new(
        id("phone"),
        id("gateway"),
        id("catalogue"),
        id("first"),
        id("opaque-v1"),
        id("epoch-2"),
    );
    let mut new_auth = MemoryAuthorizer::allowed(new_scope.clone());
    reset_catalogue(
        &new_scope,
        dst.progress(&scope).unwrap().unwrap(),
        &mut new_auth,
        &mut dst,
    )
    .unwrap();
    let retained = dst.cached_entry(&new_scope, &id("x")).unwrap().unwrap();
    assert!(retained.manifest.deleted);
    assert!(retained.payload.is_empty());
}

#[test]
fn failed_page_transaction_rolls_back_values_and_cursor_before_restart() {
    let dir = Directory::new();
    let source_path = dir.path("source.db");
    let receiver_path = dir.path("receiver.db");
    let mut src = source(source_path);
    src.upsert(&id("a"), b"first").unwrap();
    src.upsert(&id("b"), b"second").unwrap();
    let selected = scope("epoch-1");
    let mut dst = SqliteCatalogueStore::open(receiver_path.clone()).unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let fault = rusqlite::Connection::open(&receiver_path).unwrap();
    fault.execute_batch("CREATE TRIGGER reject_b BEFORE INSERT ON catalogue_entries WHEN NEW.entry_id = 'b' BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert_eq!(
        apply_next_page(&pass, 2, 1024, &mut auth, &mut src, &mut dst),
        Err(CatalogueError::Store(CatalogueStoreError::Failed))
    );
    assert_eq!(dst.count(&selected).unwrap(), 0);
    assert_eq!(
        dst.progress(&selected).unwrap().unwrap().active,
        Some(pass.clone())
    );
    fault.execute_batch("DROP TRIGGER reject_b;").unwrap();
    drop(dst);
    let mut restarted = SqliteCatalogueStore::open(receiver_path).unwrap();
    assert_eq!(
        begin_or_resume(&selected, &mut auth, &mut src, &mut restarted).unwrap(),
        Some(pass.clone())
    );
    let done = apply_next_page(&pass, 2, 1024, &mut auth, &mut src, &mut restarted).unwrap();
    assert_eq!(done.completed, 2);
    assert_eq!(restarted.count(&selected).unwrap(), 2);
}
