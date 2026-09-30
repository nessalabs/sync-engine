//! Request-only admissibility before source reads; no fabricated response required.

use nessa_sync::replication::{
    catalogue::{
        validate_manifest, validate_manifest_request, CataloguePass, CatalogueValidationError,
        EntryKey, ManifestPage, ManifestRequest, MAX_CATALOGUE_ENTRIES,
    },
    domain::{Id, Scope},
};

fn request() -> ManifestRequest {
    let id = Id::new("opaque").unwrap();
    ManifestRequest {
        pass: CataloguePass {
            scope: Scope::new(
                id.clone(),
                id.clone(),
                id.clone(),
                id.clone(),
                id.clone(),
                id,
            ),
            completed: 4,
            boundary: 5,
            cursor: None,
            generation: 1,
        },
        max_entries: 1,
    }
}

#[test]
fn request_rules_refuse_directly_and_before_response_correlation() {
    let valid = request();
    let mut invalid = Vec::new();
    let mut zero_count = valid.clone();
    zero_count.max_entries = 0;
    invalid.push((zero_count, MAX_CATALOGUE_ENTRIES));
    let mut over_core = valid.clone();
    over_core.max_entries = MAX_CATALOGUE_ENTRIES + 1;
    invalid.push((over_core, usize::MAX));
    let mut over_caller = valid.clone();
    over_caller.max_entries = 2;
    invalid.push((over_caller, 1));
    invalid.push((valid.clone(), 0));
    let mut equal_boundary = valid.clone();
    equal_boundary.pass.boundary = equal_boundary.pass.completed;
    invalid.push((equal_boundary, MAX_CATALOGUE_ENTRIES));
    let mut backward_boundary = valid.clone();
    backward_boundary.pass.boundary = backward_boundary.pass.completed - 1;
    invalid.push((backward_boundary, MAX_CATALOGUE_ENTRIES));
    let mut zero_generation = valid.clone();
    zero_generation.pass.generation = 0;
    invalid.push((zero_generation, MAX_CATALOGUE_ENTRIES));

    for (request, cap) in invalid {
        assert_eq!(
            validate_manifest_request(&request, cap),
            Err(CatalogueValidationError::InvalidRequest)
        );
        let mut wrong_echo = valid.clone();
        wrong_echo.pass.generation = 9;
        let page = ManifestPage {
            request: wrong_echo,
            entries: vec![],
            has_more: false,
        };
        assert_eq!(
            validate_manifest(&request, &page, cap),
            Err(CatalogueValidationError::InvalidRequest)
        );
    }
}

#[test]
fn exact_limits_and_large_revisions_preserve_the_borrowed_request() {
    let mut request = request();
    request.pass.completed = u64::MAX - 1;
    request.pass.boundary = u64::MAX;
    request.pass.generation = u64::MAX;
    request.pass.cursor = Some(EntryKey {
        creation: 1,
        id: Id::new("saved-cursor").unwrap(),
    });
    for count in [1, MAX_CATALOGUE_ENTRIES] {
        request.max_entries = count;
        let saved = request.clone();
        assert_eq!(validate_manifest_request(&request, count), Ok(()));
        assert_eq!(validate_manifest_request(&request, usize::MAX), Ok(()));
        assert_eq!(request, saved);
        let page = ManifestPage {
            request: request.clone(),
            entries: vec![],
            has_more: false,
        };
        assert_eq!(validate_manifest(&request, &page, count), Ok(()));
    }
}
