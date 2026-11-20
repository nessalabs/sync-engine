use nessa_sync::replication::domain::{
    validate_capabilities, validate_head_transition, validate_writer_capabilities, CapabilityError,
    CapabilitySupport, HeadTransitionError, Id, Scope, SourceHead, SOURCE_HEAD_VERSION,
};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope(receiver: &str, incarnation: &str, schema: &str, epoch: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("origin"),
        id("stream"),
        id(incarnation),
        id(schema),
        id(epoch),
    )
}
#[test]
fn reader_and_writer_requirements_are_distinct_and_undefined_support_is_refused() {
    let s = scope("reader", "first", "schema", "epoch");
    let support = CapabilitySupport::current();
    for bit in [1, 1 << 31, 1 << 63, u64::MAX] {
        let writer = SourceHead::new(s.clone(), 1, 0, bit);
        assert_eq!(validate_capabilities(&writer, support), Ok(()));
        assert_eq!(
            validate_writer_capabilities(&writer, support),
            Err(CapabilityError::MissingWriter(bit))
        );
        let reader = SourceHead::new(s.clone(), 1, bit, 0);
        assert_eq!(
            validate_capabilities(&reader, support),
            Err(CapabilityError::MissingReader(bit))
        );
        assert!(CapabilitySupport::new(bit, 0).is_err());
        assert!(CapabilitySupport::new(0, bit).is_err());
    }
    assert_eq!(
        validate_capabilities(
            &SourceHead::advertised(s, SOURCE_HEAD_VERSION + 1, 1, 0, 0),
            support
        ),
        Err(CapabilityError::UnsupportedVersion(SOURCE_HEAD_VERSION + 1))
    );
}
#[test]
fn requirements_are_monotonic_by_source_incarnation_not_receiver_or_epoch() {
    let before = SourceHead::new(scope("reader", "first", "schema", "epoch"), 9, 1, 2);
    let extended = SourceHead::new(
        scope("another-reader", "first", "schema", "another-epoch"),
        10,
        3,
        6,
    );
    assert_eq!(validate_head_transition(&before, &extended), Ok(()));
    for (reader, writer) in [(0, 2), (1, 0), (0, 0)] {
        let cleared = SourceHead::new(
            scope("another-reader", "first", "schema", "another-epoch"),
            10,
            reader,
            writer,
        );
        assert!(matches!(
            validate_head_transition(&before, &cleared),
            Err(HeadTransitionError::RequirementsCleared { .. })
        ));
    }
    let reset = SourceHead::new(scope("reader", "second", "new-schema", "epoch"), 0, 0, 0);
    assert_eq!(validate_head_transition(&before, &reset), Ok(()));
    assert_eq!(before.position(), 9);
    assert_eq!(before.version(), SOURCE_HEAD_VERSION);
}
#[test]
fn publication_refuses_regression_and_inconsistent_identity_without_implicit_reset() {
    let s = scope("reader", "first", "schema", "epoch");
    let before = SourceHead::new(s.clone(), 9, 1, 2);
    assert_eq!(
        validate_head_transition(&before, &SourceHead::new(s.clone(), 8, 1, 2)),
        Err(HeadTransitionError::PositionRegressed)
    );
    assert_eq!(
        validate_head_transition(
            &before,
            &SourceHead::new(scope("reader", "first", "different", "epoch"), 9, 1, 2)
        ),
        Err(HeadTransitionError::SchemaChanged)
    );
    let other = Scope::new(
        id("reader"),
        id("different-origin"),
        id("stream"),
        id("first"),
        id("schema"),
        id("epoch"),
    );
    assert_eq!(
        validate_head_transition(&before, &SourceHead::new(other, 9, 1, 2)),
        Err(HeadTransitionError::DifferentSource)
    );
    assert_eq!(
        validate_head_transition(&before, &SourceHead::advertised(s, 2, 9, 1, 2)),
        Err(HeadTransitionError::UnsupportedVersion)
    );
}

#[cfg(feature = "sqlite")]
#[path = "../examples/source_head/fixture.rs"]
mod fixture;
#[cfg(feature = "sqlite")]
mod admission {
    use super::*;
    use crate::fixture::{scope as selected, Directory};
    use nessa_sync::replication::{
        application::{
            begin_pass_with_capabilities, finish_pass, Access, ReplicaStore, StoreError, SyncError,
        },
        catalogue::{
            apply_next_page, begin_or_resume_with_capabilities, CatalogueError, CatalogueStore,
            CatalogueStoreError, CatalogueValidationError,
        },
        domain::{Checkpoint, CommitPlan, Limits, ValidationError},
        infrastructure::{MemoryAuthorizer, SqliteCatalogueStore, SqliteReplicaStore},
    };
    #[test]
    fn records_refuse_reader_requirements_before_fetch_and_preserve_reopened_cache() {
        let f = Directory::new();
        let s = selected();
        let mut source = f.records();
        source.inner.append(&id("fact"), b"value").unwrap();
        let path = f.path("cache.db");
        let mut store = SqliteReplicaStore::open(&path).unwrap();
        let mut auth = MemoryAuthorizer::allowed(s.clone());
        let support = CapabilitySupport::current();
        source.meta.reader = 1;
        assert!(matches!(
            begin_pass_with_capabilities(&s, support, &mut auth, &mut source, &mut store),
            Err(SyncError::Incompatible(CapabilityError::MissingReader(1)))
        ));
        assert!(store.checkpoint(&s).unwrap().is_none());
        assert_eq!(source.inner.page_reads(), 0);
        source.meta.reader = 0;
        source.meta.writer = u64::MAX;
        let mut pass =
            begin_pass_with_capabilities(&s, support, &mut auth, &mut source, &mut store).unwrap();
        finish_pass(
            &mut pass,
            Limits::new(10, 100, 100).unwrap(),
            &mut auth,
            &mut source,
            &mut store,
        )
        .unwrap();
        let before = store.checkpoint(&s).unwrap();
        source.meta.reader = 1;
        assert!(matches!(
            begin_pass_with_capabilities(&s, support, &mut auth, &mut source, &mut store),
            Err(SyncError::Incompatible(_))
        ));
        assert_eq!(store.checkpoint(&s).unwrap(), before);
        assert_eq!(source.inner.page_reads(), 1);
        assert_eq!(source.inner.head_reads(), 3);
        drop(store);
        let mut reopened = SqliteReplicaStore::open(path).unwrap();
        assert_eq!(reopened.count(&s).unwrap(), 1);
        assert_eq!(
            reopened.read_after(&s, 0, 1, 100).unwrap()[0].payload,
            b"value"
        );
    }
    #[test]
    fn catalogue_refuses_before_begin_or_resume_and_keeps_active_cursor() {
        let f = Directory::new();
        let s = selected();
        let mut source = f.catalogue();
        for n in 1..=3 {
            source
                .inner
                .upsert(&id(&format!("item-{n}")), b"value")
                .unwrap();
        }
        let path = f.path("cache.db");
        let mut store = SqliteCatalogueStore::open(&path).unwrap();
        let mut auth = MemoryAuthorizer::allowed(s.clone());
        let support = CapabilitySupport::current();
        source.meta.reader = 1;
        assert!(matches!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut source, &mut store),
            Err(CatalogueError::Incompatible(_))
        ));
        assert!(store.progress(&s).unwrap().is_none());
        assert_eq!(source.inner.manifest_reads(), 0);
        source.meta.reader = 0;
        source.meta.writer = u64::MAX;
        let pass =
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut source, &mut store)
                .unwrap()
                .unwrap();
        let saved = apply_next_page(&pass, 1, 100, &mut auth, &mut source, &mut store).unwrap();
        source.meta.reader = 1;
        assert!(matches!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut source, &mut store),
            Err(CatalogueError::Incompatible(_))
        ));
        assert_eq!(store.progress(&s).unwrap(), Some(saved.clone()));
        assert_eq!(source.inner.manifest_reads(), 1);
        assert_eq!(source.inner.resolve_reads(), 1);
        source.meta.reader = 0;
        source.meta.position = Some(1);
        assert_eq!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut source, &mut store),
            Err(CatalogueError::Store(CatalogueStoreError::ResetRequired))
        );
        assert_eq!(store.progress(&s).unwrap(), Some(saved.clone()));
        source.meta.position = None;
        assert_eq!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut source, &mut store)
                .unwrap(),
            saved.active
        );
        drop(store);
        let mut reopened = SqliteCatalogueStore::open(path).unwrap();
        assert_eq!(reopened.count(&s).unwrap(), 1);
        assert_eq!(reopened.progress(&s).unwrap(), Some(saved));
    }
    #[test]
    fn wrong_scope_and_unknown_metadata_version_do_not_create_progress() {
        let f = Directory::new();
        let s = selected();
        let mut records = f.records();
        let mut catalogue = f.catalogue();
        let mut store = SqliteReplicaStore::open(f.path("records-cache.db")).unwrap();
        let mut cache = SqliteCatalogueStore::open(f.path("catalogue-cache.db")).unwrap();
        let mut auth = MemoryAuthorizer::allowed(s.clone());
        let support = CapabilitySupport::current();
        records.meta.version = 2;
        catalogue.meta.version = 2;
        assert_eq!(
            begin_pass_with_capabilities(&s, support, &mut auth, &mut records, &mut store),
            Err(SyncError::Incompatible(
                CapabilityError::UnsupportedVersion(2)
            ))
        );
        assert_eq!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut catalogue, &mut cache),
            Err(CatalogueError::Incompatible(
                CapabilityError::UnsupportedVersion(2)
            ))
        );
        records.meta.version = 1;
        catalogue.meta.version = 1;
        let wrong = scope("other", "first", "schema", "epoch");
        records.meta.echo = Some(wrong.clone());
        catalogue.meta.echo = Some(wrong);
        assert_eq!(
            begin_pass_with_capabilities(&s, support, &mut auth, &mut records, &mut store),
            Err(SyncError::Validation(ValidationError::WrongRequest))
        );
        assert_eq!(
            begin_or_resume_with_capabilities(&s, support, &mut auth, &mut catalogue, &mut cache),
            Err(CatalogueError::Validation(
                CatalogueValidationError::WrongRequest
            ))
        );
        assert!(store.checkpoint(&s).unwrap().is_none());
        assert!(cache.progress(&s).unwrap().is_none());
        assert_eq!(records.inner.page_reads(), 0);
        assert_eq!(catalogue.inner.manifest_reads(), 0);
    }
    struct ForbiddenStore;
    impl ReplicaStore for ForbiddenStore {
        fn load(&mut self, _: &Scope) -> Result<Option<Checkpoint>, StoreError> {
            panic!("authorization must precede saved scope load")
        }
        fn apply(&mut self, _: CommitPlan) -> Result<(), StoreError> {
            panic!("denial cannot apply")
        }
    }
    #[test]
    fn denial_precedes_head_and_local_progress_access() {
        let f = Directory::new();
        let s = selected();
        let mut source = f.records();
        let mut auth = MemoryAuthorizer::allowed(s.clone());
        auth.decision = Access::Denied;
        assert_eq!(
            begin_pass_with_capabilities(
                &s,
                CapabilitySupport::current(),
                &mut auth,
                &mut source,
                &mut ForbiddenStore
            ),
            Err(SyncError::Denied)
        );
        assert_eq!(source.inner.head_reads(), 0);
        assert_eq!(source.inner.page_reads(), 0);
    }
}
