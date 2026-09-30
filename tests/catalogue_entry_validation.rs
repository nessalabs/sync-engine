use nessa_sync::replication::{
    catalogue::{
        validate_manifest, validate_manifest_entry, CataloguePass, CatalogueValidationError,
        EntryKey, ManifestEntry, ManifestPage, ManifestRequest,
    },
    domain::{Id, Scope},
};

fn entry(creation: u64, revision: u64, deleted: bool) -> ManifestEntry {
    ManifestEntry {
        key: EntryKey {
            creation,
            id: Id::new("entry").unwrap(),
        },
        revision,
        deleted,
    }
}

#[test]
fn individual_entry_refuses_invalid_numeric_evidence() {
    for value in [
        entry(0, 0, false),
        entry(0, 1, true),
        entry(0, u64::MAX, false),
        entry(2, 1, false),
        entry(u64::MAX, u64::MAX - 1, true),
    ] {
        assert_eq!(
            validate_manifest_entry(&value),
            Err(CatalogueValidationError::InvalidOrder)
        );
    }
}

#[test]
fn individual_entry_accepts_full_numeric_range() {
    for creation in [1, 2, i64::MAX as u64 + 1, u64::MAX] {
        for revision in [creation, u64::MAX] {
            for deleted in [false, true] {
                let value = entry(creation, revision, deleted);
                let original = value.clone();
                assert_eq!(validate_manifest_entry(&value), Ok(()));
                assert_eq!(value, original);
            }
        }
    }
}

#[test]
fn manifest_consumes_individual_entry_admission() {
    let id = Id::new("opaque").unwrap();
    let request = ManifestRequest {
        pass: CataloguePass {
            scope: Scope::new(
                id.clone(),
                id.clone(),
                id.clone(),
                id.clone(),
                id.clone(),
                id,
            ),
            completed: 0,
            boundary: u64::MAX,
            cursor: None,
            generation: 1,
        },
        max_entries: 1,
    };
    for value in [
        entry(0, 1, false),
        entry(2, 1, false),
        entry(u64::MAX, u64::MAX - 1, true),
    ] {
        let page = ManifestPage {
            request: request.clone(),
            entries: vec![value],
            has_more: false,
        };
        assert_eq!(
            validate_manifest(&request, &page, 1),
            Err(CatalogueValidationError::InvalidOrder)
        );
    }
    let page = ManifestPage {
        request: request.clone(),
        entries: vec![entry(u64::MAX, u64::MAX, true)],
        has_more: false,
    };
    assert_eq!(validate_manifest(&request, &page, 1), Ok(()));
}
