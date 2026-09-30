#![cfg(feature = "sqlite")]

use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use nessa_sync::replication::application::{Access, ScopeAuthorizer};
use nessa_sync::replication::catalogue::{
    apply_next_page, begin_or_resume, reset_catalogue, validate_manifest, CatalogueError,
    CataloguePagePlan, CataloguePass, CatalogueSource, CatalogueSourceError, CatalogueStore,
    CatalogueStoreError, CatalogueValidationError, EntryKey, ManifestEntry, ManifestPage,
    ManifestRequest, ResolvedEntry, MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES,
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
fn empty_final_page_after_saved_cursor_completes_without_rewinding_it() {
    struct EmptyFinalSource;
    impl CatalogueSource for EmptyFinalSource {
        fn head(&mut self, _: &Scope) -> Result<u64, CatalogueSourceError> {
            panic!("continuing a saved pass does not recapture head")
        }
        fn manifest(
            &mut self,
            request: &ManifestRequest,
        ) -> Result<ManifestPage, CatalogueSourceError> {
            Ok(ManifestPage {
                request: request.clone(),
                entries: vec![],
                has_more: false,
            })
        }
        fn resolve(
            &mut self,
            _: &CataloguePass,
            _: &Id,
            _: usize,
        ) -> Result<ResolvedEntry, CatalogueSourceError> {
            panic!("an empty manifest resolves no payload")
        }
    }
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    src.upsert(&id("a"), b"a").unwrap();
    src.upsert(&id("b"), b"b").unwrap();
    let selected = scope("epoch-1");
    let path = dir.path("receiver.db");
    let mut dst = SqliteCatalogueStore::open(&path).unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let progress = apply_next_page(&pass, 1, 1024, &mut auth, &mut src, &mut dst).unwrap();
    let continued = progress.active.unwrap();
    assert_eq!(
        continued.cursor,
        Some(EntryKey {
            creation: 1,
            id: id("a")
        })
    );
    drop(dst);
    let mut reopened = SqliteCatalogueStore::open(&path).unwrap();
    let completed = apply_next_page(
        &continued,
        1,
        1024,
        &mut auth,
        &mut EmptyFinalSource,
        &mut reopened,
    )
    .unwrap();
    assert_eq!(completed.completed, continued.boundary);
    assert_eq!(completed.generation, continued.generation);
    assert_eq!(completed.active, None);
    assert_eq!(reopened.count(&selected).unwrap(), 1);
    assert_eq!(
        reopened
            .cached_entry(&selected, &id("a"))
            .unwrap()
            .unwrap()
            .payload,
        b"a"
    );
}

#[test]
fn contradictory_public_plan_leaves_real_cache_and_pass_unchanged() {
    let dir = Directory::new();
    let mut src = source(dir.path("source.db"));
    let mut dst = SqliteCatalogueStore::open(dir.path("receiver.db")).unwrap();
    let selected = scope("epoch-1");
    src.upsert(&id("a"), b"value").unwrap();
    let mut auth = MemoryAuthorizer::allowed(selected.clone());
    let pass = begin_or_resume(&selected, &mut auth, &mut src, &mut dst)
        .unwrap()
        .unwrap();
    let before = dst.progress(&selected).unwrap();
    let request = ManifestRequest {
        pass: pass.clone(),
        max_entries: 1,
    };
    let manifest = src.manifest(&request).unwrap();
    let entry = src.resolve(&pass, &id("a"), 1024).unwrap();
    let valid = CataloguePagePlan::new(manifest, vec![entry], vec![]).unwrap();
    let mut variants = vec![];
    let mut invalid = valid.clone();
    invalid.next_cursor = None;
    variants.push(invalid);
    invalid = valid.clone();
    invalid.final_page = false;
    variants.push(invalid);
    invalid = valid.clone();
    invalid.pass.generation += 1;
    variants.push(invalid);
    invalid = valid.clone();
    invalid.entries[0].manifest.key.id = id("foreign");
    variants.push(invalid);
    invalid = valid.clone();
    invalid.entries[0].manifest.deleted = true;
    variants.push(invalid);
    invalid = valid.clone();
    invalid.entries[0].manifest.deleted = true;
    invalid.entries[0].payload.clear();
    variants.push(invalid);
    invalid = valid.clone();
    invalid.manifest.entries[0].deleted = true;
    invalid.entries[0].manifest.revision += 1;
    variants.push(invalid);
    for invalid in variants {
        assert_eq!(dst.apply_page(invalid), Err(CatalogueStoreError::Conflict));
        assert_eq!(dst.progress(&selected).unwrap(), before);
        assert_eq!(dst.count(&selected).unwrap(), 0);
    }
    dst.apply_page(valid).unwrap();
    assert_eq!(dst.count(&selected).unwrap(), 1);
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

/// A current value can be newer than the pass boundary. The next pass compares
/// its coherent saved descriptor with incoming metadata from that same identity.
fn retained_cache(path: &PathBuf, deleted: bool) -> (SqliteCatalogueStore, Scope, ResolvedEntry) {
    let selected = scope("epoch-1");
    let mut store = SqliteCatalogueStore::open(path).unwrap();
    let pass = store.begin(&selected, None, 1).unwrap().active.unwrap();
    let manifest = ManifestEntry {
        key: EntryKey {
            creation: 1,
            id: id("retained"),
        },
        revision: 3,
        deleted,
    };
    let value = ResolvedEntry {
        manifest: manifest.clone(),
        payload: if deleted {
            vec![]
        } else {
            b"current-content".to_vec()
        },
    };
    let page = ManifestPage {
        request: ManifestRequest {
            pass,
            max_entries: 1,
        },
        entries: vec![manifest],
        has_more: false,
    };
    store
        .apply_page(CataloguePagePlan::new(page, vec![value.clone()], vec![]).unwrap())
        .unwrap();
    (store, selected, value)
}

#[test]
fn cached_descriptor_comparisons_preserve_revision_meaning() {
    for resolved_mode in [false, true] {
        for (name, cached_deleted, incoming_revision, incoming_deleted, expected) in [
            (
                "live-same-deleted",
                false,
                3,
                true,
                Err(CatalogueStoreError::Conflict),
            ),
            (
                "live-newer-deleted",
                false,
                2,
                true,
                Err(CatalogueStoreError::Fenced),
            ),
            (
                "deleted-same-live",
                true,
                3,
                false,
                Err(CatalogueStoreError::Conflict),
            ),
            ("deleted-newer-live", true, 2, false, Ok(())),
            ("live-newer-live", false, 2, false, Ok(())),
            ("deleted-newer-deleted", true, 2, true, Ok(())),
            ("live-same-live", false, 3, false, Ok(())),
            ("deleted-same-deleted", true, 3, true, Ok(())),
        ] {
            let dir = Directory::new();
            let path = dir.path("receiver.db");
            let (mut store, selected, retained) = retained_cache(&path, cached_deleted);
            let initial = store.progress(&selected).unwrap();
            let progress = store.begin(&selected, initial, 3).unwrap();
            let incoming = ManifestEntry {
                revision: incoming_revision,
                deleted: incoming_deleted,
                ..retained.manifest.clone()
            };
            let page = ManifestPage {
                request: ManifestRequest {
                    pass: progress.active.clone().unwrap(),
                    max_entries: 1,
                },
                entries: vec![incoming.clone()],
                has_more: false,
            };
            let plan = if resolved_mode {
                let value = ResolvedEntry {
                    manifest: incoming,
                    payload: if incoming_deleted {
                        vec![]
                    } else {
                        retained.payload.clone()
                    },
                };
                CataloguePagePlan::new(page, vec![value], vec![]).unwrap()
            } else {
                CataloguePagePlan::new(page, vec![], vec![incoming]).unwrap()
            };
            let actual = store.apply_page(plan);
            assert_eq!(
                actual.as_ref().map(|_| ()).map_err(Clone::clone),
                expected,
                "{name}, resolved={resolved_mode}"
            );
            drop(store);
            let mut reopened = SqliteCatalogueStore::open(&path).unwrap();
            assert_eq!(
                reopened.cached_entry(&selected, &id("retained")).unwrap(),
                Some(retained.clone())
            );
            if expected.is_err() {
                assert_eq!(
                    reopened.progress(&selected).unwrap(),
                    Some(progress.clone())
                );
                let valid_page = ManifestPage {
                    request: ManifestRequest {
                        pass: progress.active.unwrap(),
                        max_entries: 1,
                    },
                    entries: vec![retained.manifest.clone()],
                    has_more: false,
                };
                reopened
                    .apply_page(
                        CataloguePagePlan::new(valid_page, vec![], vec![retained.manifest])
                            .unwrap(),
                    )
                    .unwrap();
            }
            let final_progress = reopened.progress(&selected).unwrap().unwrap();
            assert_eq!(final_progress.completed, 3);
            assert!(final_progress.active.is_none());
        }
    }
}

#[test]
fn resolved_cache_orders_newer_values_and_rejects_foreign_creation_and_payload_conflicts() {
    for (name, saved_deleted, incoming_deleted, expected) in [
        ("new-live", false, false, Ok(())),
        ("new-deletion", false, true, Ok(())),
        ("new-tombstone", true, true, Ok(())),
        (
            "resurrection",
            true,
            false,
            Err(CatalogueStoreError::Fenced),
        ),
    ] {
        let dir = Directory::new();
        let path = dir.path("receiver.db");
        let (mut store, selected, retained) = retained_cache(&path, saved_deleted);
        let previous = store.progress(&selected).unwrap();
        let progress = store.begin(&selected, previous, 4).unwrap();
        let manifest = ManifestEntry {
            revision: 4,
            deleted: incoming_deleted,
            ..retained.manifest.clone()
        };
        let value = ResolvedEntry {
            manifest: manifest.clone(),
            payload: if incoming_deleted {
                vec![]
            } else {
                b"new-content".to_vec()
            },
        };
        let page = ManifestPage {
            request: ManifestRequest {
                pass: progress.active.clone().unwrap(),
                max_entries: 1,
            },
            entries: vec![manifest],
            has_more: false,
        };
        let actual =
            store.apply_page(CataloguePagePlan::new(page, vec![value.clone()], vec![]).unwrap());
        assert_eq!(
            actual.as_ref().map(|_| ()).map_err(Clone::clone),
            expected,
            "{name}"
        );
        drop(store);
        let mut reopened = SqliteCatalogueStore::open(path).unwrap();
        if expected.is_ok() {
            assert_eq!(
                reopened.cached_entry(&selected, &id("retained")).unwrap(),
                Some(value)
            );
            assert_eq!(reopened.progress(&selected).unwrap().unwrap().completed, 4);
        } else {
            assert_eq!(
                reopened.cached_entry(&selected, &id("retained")).unwrap(),
                Some(retained)
            );
            assert_eq!(reopened.progress(&selected).unwrap(), Some(progress));
        }
    }
    for resolved_mode in [false, true] {
        let dir = Directory::new();
        let path = dir.path("receiver.db");
        let (mut store, selected, retained) = retained_cache(&path, false);
        let previous = store.progress(&selected).unwrap();
        let progress = store.begin(&selected, previous, 4).unwrap();
        let foreign = ManifestEntry {
            key: EntryKey {
                creation: 2,
                ..retained.manifest.key.clone()
            },
            ..retained.manifest.clone()
        };
        let page = ManifestPage {
            request: ManifestRequest {
                pass: progress.active.clone().unwrap(),
                max_entries: 1,
            },
            entries: vec![foreign.clone()],
            has_more: false,
        };
        let plan = if resolved_mode {
            CataloguePagePlan::new(
                page,
                vec![ResolvedEntry {
                    manifest: foreign,
                    payload: retained.payload.clone(),
                }],
                vec![],
            )
            .unwrap()
        } else {
            CataloguePagePlan::new(page, vec![], vec![foreign]).unwrap()
        };
        assert_eq!(store.apply_page(plan), Err(CatalogueStoreError::Conflict));
        assert_eq!(store.progress(&selected).unwrap(), Some(progress));
        assert_eq!(
            store.cached_entry(&selected, &id("retained")).unwrap(),
            Some(retained)
        );
    }
    let dir = Directory::new();
    let path = dir.path("receiver.db");
    let (mut store, selected, retained) = retained_cache(&path, false);
    let previous = store.progress(&selected).unwrap();
    let progress = store.begin(&selected, previous, 4).unwrap();
    let page = ManifestPage {
        request: ManifestRequest {
            pass: progress.active.clone().unwrap(),
            max_entries: 1,
        },
        entries: vec![retained.manifest.clone()],
        has_more: false,
    };
    assert_eq!(
        store.apply_page(
            CataloguePagePlan::new(
                page,
                vec![ResolvedEntry {
                    payload: b"different-same-revision".to_vec(),
                    ..retained.clone()
                }],
                vec![]
            )
            .unwrap()
        ),
        Err(CatalogueStoreError::Conflict)
    );
    assert_eq!(store.progress(&selected).unwrap(), Some(progress));
    assert_eq!(
        store.cached_entry(&selected, &id("retained")).unwrap(),
        Some(retained)
    );
}

#[test]
fn sqlite_manifest_request_refusal_precedes_metadata_and_preserves_read_evidence() {
    let dir = Directory::new();
    let path = dir.path("request-source.db");
    let mut src = source(path.clone());
    src.upsert(&id("entry"), b"value").unwrap();
    let valid = ManifestRequest {
        pass: CataloguePass {
            scope: scope("epoch-1"),
            completed: 0,
            boundary: 1,
            cursor: None,
            generation: 1,
        },
        max_entries: MAX_CATALOGUE_ENTRIES,
    };
    let page = src.manifest(&valid).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(
        validate_manifest(&valid, &page, MAX_CATALOGUE_ENTRIES),
        Ok(())
    );
    let reads = src.manifest_reads();
    let bytes = src.manifest_bytes();
    let mut invalid = Vec::new();
    let mut zero = valid.clone();
    zero.max_entries = 0;
    invalid.push(zero);
    let mut over = valid.clone();
    over.max_entries += 1;
    invalid.push(over);
    let mut equal = valid.clone();
    equal.pass.completed = 1;
    invalid.push(equal);
    let mut backward = valid.clone();
    backward.pass.completed = 2;
    invalid.push(backward);
    let mut generation = valid.clone();
    generation.pass.generation = 0;
    invalid.push(generation);
    for creation in [0, 2] {
        let mut request = valid.clone();
        request.pass.cursor = Some(EntryKey {
            creation,
            id: id("cursor"),
        });
        invalid.push(request);
    }
    for request in &invalid {
        assert_eq!(
            src.manifest(request),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!((src.manifest_reads(), src.manifest_bytes()), (reads, bytes));
    }
    assert_eq!(src.manifest(&valid).unwrap(), page);
    let reads = src.manifest_reads();
    let bytes = src.manifest_bytes();
    // A real missing metadata table distinguishes early owner refusal from a
    // metadata read: an admissible request below must encounter Unavailable.
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE catalogue_source_meta")
        .unwrap();
    for request in &invalid {
        assert_eq!(
            src.manifest(request),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!((src.manifest_reads(), src.manifest_bytes()), (reads, bytes));
    }
    assert_eq!(src.manifest(&valid), Err(CatalogueSourceError::Unavailable));
    assert_eq!((src.manifest_reads(), src.manifest_bytes()), (reads, bytes));
}

#[test]
fn invalid_manifest_request_precedes_application_authorization_and_source() {
    struct CountingAuthority(usize);
    impl ScopeAuthorizer for CountingAuthority {
        fn authorize(&mut self, scope: &Scope) -> Access {
            self.0 += 1;
            Access::Allowed(scope.clone())
        }
    }
    struct NoRead;
    impl CatalogueSource for NoRead {
        fn head(&mut self, _: &Scope) -> Result<u64, CatalogueSourceError> {
            panic!("unexpected head");
        }
        fn manifest(&mut self, _: &ManifestRequest) -> Result<ManifestPage, CatalogueSourceError> {
            panic!("unexpected manifest");
        }
        fn resolve(
            &mut self,
            _: &CataloguePass,
            _: &Id,
            _: usize,
        ) -> Result<ResolvedEntry, CatalogueSourceError> {
            panic!("unexpected resolve");
        }
    }
    let dir = Directory::new();
    let mut store = SqliteCatalogueStore::open(dir.path("preflight-receiver.db")).unwrap();
    let mut authority = CountingAuthority(0);
    let valid = CataloguePass {
        scope: scope("epoch-1"),
        completed: 0,
        boundary: 1,
        cursor: None,
        generation: 1,
    };
    let mut generation = valid.clone();
    generation.generation = 0;
    let mut equal = valid.clone();
    equal.completed = 1;
    let mut backward = valid.clone();
    backward.completed = 2;
    for (pass, count) in [
        (&valid, 0),
        (&valid, MAX_CATALOGUE_ENTRIES + 1),
        (&generation, 1),
        (&equal, 1),
        (&backward, 1),
    ] {
        assert_eq!(
            apply_next_page(pass, count, 1024, &mut authority, &mut NoRead, &mut store),
            Err(CatalogueError::Validation(
                CatalogueValidationError::InvalidRequest
            ))
        );
    }
    assert_eq!(authority.0, 0);
    assert_eq!(store.progress(&valid.scope).unwrap(), None);
    let mut source = source(dir.path("preflight-source.db"));
    source.upsert(&id("entry"), b"value").unwrap();
    let mut pass_authority = MemoryAuthorizer::allowed(valid.scope.clone());
    let pass = begin_or_resume(&valid.scope, &mut pass_authority, &mut source, &mut store)
        .unwrap()
        .unwrap();
    let completed =
        apply_next_page(&pass, 1, 1024, &mut authority, &mut source, &mut store).unwrap();
    assert_eq!(completed.completed, 1);
    assert_eq!(authority.0, 3);
}

#[test]
fn invalid_resolve_pass_precedes_sqlite_metadata() {
    let dir = Directory::new();
    let path = dir.path("resolve-source.db");
    let mut src = source(path.clone());
    let entry = id("entry");
    src.upsert(&entry, b"value").unwrap();
    let valid = CataloguePass {
        scope: scope("epoch-1"),
        completed: 0,
        boundary: 1,
        cursor: None,
        generation: 1,
    };
    assert_eq!(src.resolve(&valid, &entry, 1024).unwrap().payload, b"value");
    let reads = src.resolve_reads();
    let bytes = src.payload_bytes();
    let mut generation = valid.clone();
    generation.generation = 0;
    let mut equal = valid.clone();
    equal.completed = 1;
    let mut backward = valid.clone();
    backward.completed = 2;
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE catalogue_source_entries")
        .unwrap();
    let mut zero_cursor = valid.clone();
    zero_cursor.cursor = Some(EntryKey {
        creation: 0,
        id: id("cursor"),
    });
    let mut beyond_cursor = valid.clone();
    beyond_cursor.cursor = Some(EntryKey {
        creation: 2,
        id: id("cursor"),
    });
    for pass in [generation, equal, backward, zero_cursor, beyond_cursor] {
        assert_eq!(
            src.resolve(&pass, &entry, 1024),
            Err(CatalogueSourceError::InvalidRequest)
        );
        assert_eq!((src.resolve_reads(), src.payload_bytes()), (reads, bytes));
    }
    assert_eq!(
        src.resolve(&valid, &entry, 1024),
        Err(CatalogueSourceError::Unavailable)
    );
}

#[test]
fn sqlite_retained_progress_is_validated_before_return() {
    for creation in [0, 6] {
        let dir = Directory::new();
        let path = dir.path("corrupt-progress.db");
        let mut store = SqliteCatalogueStore::open(&path).unwrap();
        let selected = scope("epoch-1");
        let original = store.begin(&selected, None, 5).unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE catalogue_progress SET cursor_creation=?1, cursor_id='cursor'",
                [creation],
            )
            .unwrap();
        assert_eq!(store.progress(&selected), Err(CatalogueStoreError::Failed));
        drop(store);
        let mut reopened = SqliteCatalogueStore::open(&path).unwrap();
        assert_eq!(
            reopened.progress(&selected),
            Err(CatalogueStoreError::Failed)
        );
        let saved: i64 = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT cursor_creation FROM catalogue_progress",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(saved, creation);
        Connection::open(&path)
            .unwrap()
            .execute_batch("UPDATE catalogue_progress SET cursor_creation=NULL, cursor_id=NULL")
            .unwrap();
        assert_eq!(reopened.progress(&selected).unwrap(), Some(original));
    }
}

#[test]
fn sqlite_receipts_match_planned_transition() {
    let dir = Directory::new();
    let mut store = SqliteCatalogueStore::open(dir.path("planned-progress.db")).unwrap();
    let selected = scope("epoch-1");
    let first = store.begin(&selected, None, 5).unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(first.active.as_ref().unwrap().boundary, 5);
    let pass = first.active.clone().unwrap();
    let page = ManifestPage {
        request: ManifestRequest {
            pass,
            max_entries: 1,
        },
        entries: vec![],
        has_more: false,
    };
    let completed = store
        .apply_page(CataloguePagePlan::new(page, vec![], vec![]).unwrap())
        .unwrap();
    assert_eq!((completed.completed, completed.generation), (5, 1));
    assert!(completed.active.is_none());
    let reset = store.reset(&selected, completed).unwrap();
    assert_eq!((reset.completed, reset.generation), (0, 2));
    assert!(reset.active.is_none());
    let next = store.begin(&selected, Some(reset), 9).unwrap();
    assert_eq!((next.completed, next.generation), (0, 3));
    assert_eq!(next.active.as_ref().unwrap().boundary, 9);
}
