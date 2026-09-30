//! Pure public page-plan relationships, independent of database/transport features.

use nessa_sync::replication::catalogue::{
    validate_catalogue_page_plan, validate_manifest, CataloguePagePlan, CataloguePass,
    CatalogueValidationError, EntryKey, ManifestEntry, ManifestPage, ManifestRequest,
    ResolvedEntry, MAX_CATALOGUE_PAYLOAD_BYTES,
};
use nessa_sync::replication::domain::{Id, Scope};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn entry(value: &str, creation: u64) -> ManifestEntry {
    ManifestEntry {
        key: EntryKey {
            creation,
            id: id(value),
        },
        revision: creation,
        deleted: false,
    }
}
fn page() -> ManifestPage {
    ManifestPage {
        request: ManifestRequest {
            pass: CataloguePass {
                scope: Scope::new(
                    id("receiver"),
                    id("origin"),
                    id("catalogue"),
                    id("incarnation"),
                    id("schema"),
                    id("epoch"),
                ),
                completed: 0,
                boundary: 3,
                cursor: None,
                generation: 1,
            },
            max_entries: 3,
        },
        entries: vec![entry("a", 1), entry("b", 2)],
        has_more: false,
    }
}
fn plan() -> CataloguePagePlan {
    let page = page();
    CataloguePagePlan::new(
        page.clone(),
        vec![ResolvedEntry {
            manifest: page.entries[0].clone(),
            payload: b"value".to_vec(),
        }],
        vec![page.entries[1].clone()],
    )
    .unwrap()
}

#[test]
fn valid_mixed_coverage_and_newer_resolved_revision_are_accepted() {
    let mut plan = plan();
    plan.entries[0].manifest.revision = 9;
    assert_eq!(validate_catalogue_page_plan(&plan), Ok(()));
    plan.entries[0].manifest.deleted = true;
    plan.entries[0].payload.clear();
    assert_eq!(validate_catalogue_page_plan(&plan), Ok(()));
}

#[test]
fn revision_and_deletion_meaning_cannot_contradict_manifest() {
    let valid = plan();
    let mut same_revision_changed_deletion = valid.clone();
    same_revision_changed_deletion.entries[0].manifest.deleted = true;
    same_revision_changed_deletion.entries[0].payload.clear();
    assert_eq!(
        validate_catalogue_page_plan(&same_revision_changed_deletion),
        Err(CatalogueValidationError::WrongPayload)
    );
    let mut deleted = valid.manifest.clone();
    deleted.entries[0].deleted = true;
    let mut resurrection = valid.entries.clone();
    resurrection[0].manifest.revision += 1;
    assert_eq!(
        CataloguePagePlan::new(
            deleted.clone(),
            resurrection.clone(),
            valid.unchanged.clone()
        ),
        Err(CatalogueValidationError::WrongPayload)
    );
    resurrection[0].manifest.deleted = true;
    resurrection[0].payload.clear();
    assert!(CataloguePagePlan::new(
        deleted.clone(),
        resurrection.clone(),
        valid.unchanged.clone()
    )
    .is_ok());
    resurrection[0].manifest.revision = deleted.entries[0].revision;
    assert!(CataloguePagePlan::new(deleted, resurrection, valid.unchanged).is_ok());
}

#[test]
fn pass_final_and_cursor_contradictions_are_refused() {
    let valid = plan();
    let mut invalid = valid.clone();
    invalid.pass.generation += 1;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongRequest)
    );
    invalid = valid.clone();
    invalid.final_page = false;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongRequest)
    );
    invalid = valid;
    invalid.next_cursor = None;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongRequest)
    );
}

#[test]
fn missing_duplicate_foreign_and_changed_unchanged_coverage_are_refused() {
    let valid = plan();
    let mut invalid = valid.clone();
    invalid.unchanged.clear();
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid.clone();
    invalid.unchanged[0] = invalid.manifest.entries[0].clone();
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid.clone();
    invalid.entries[0].manifest.key.id = id("foreign");
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid.clone();
    invalid.unchanged[0].revision += 1;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid;
    invalid.entries.push(invalid.entries[0].clone());
    invalid.unchanged.clear();
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = plan();
    invalid.entries.push(invalid.entries[0].clone());
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
}

#[test]
fn changed_resolved_key_backward_revision_and_live_tombstone_bytes_are_refused() {
    let valid = plan();
    let mut invalid = valid.clone();
    invalid.entries[0].manifest.key.creation += 1;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid.clone();
    invalid.entries[0].manifest.revision = 0;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
    invalid = valid;
    invalid.entries[0].manifest.deleted = true;
    assert_eq!(
        validate_catalogue_page_plan(&invalid),
        Err(CatalogueValidationError::WrongPayload)
    );
}

#[test]
fn individual_and_aggregate_payload_ceilings_are_owned_by_validator() {
    let mut plan = plan();
    plan.entries[0].payload = vec![0; MAX_CATALOGUE_PAYLOAD_BYTES + 1];
    assert_eq!(
        validate_catalogue_page_plan(&plan),
        Err(CatalogueValidationError::BoundsExceeded)
    );
    plan.entries[0]
        .payload
        .truncate(MAX_CATALOGUE_PAYLOAD_BYTES);
    assert_eq!(validate_catalogue_page_plan(&plan), Ok(()));
    plan.entries.push(ResolvedEntry {
        manifest: plan.unchanged.remove(0),
        payload: vec![1],
    });
    assert_eq!(
        validate_catalogue_page_plan(&plan),
        Err(CatalogueValidationError::BoundsExceeded)
    );
    plan.entries[0].payload.pop();
    assert_eq!(validate_catalogue_page_plan(&plan), Ok(()));
}

#[test]
fn empty_final_page_retains_prior_cursor_and_nonfinal_empty_page_refuses() {
    let mut page = page();
    let cursor = page.entries[0].key.clone();
    page.request.pass.cursor = Some(cursor.clone());
    page.entries.clear();
    let plan = CataloguePagePlan::new(page.clone(), vec![], vec![]).unwrap();
    assert_eq!(plan.next_cursor, Some(cursor));
    assert!(plan.final_page);
    page.has_more = true;
    assert_eq!(
        CataloguePagePlan::new(page, vec![], vec![]),
        Err(CatalogueValidationError::NoProgress)
    );
}

#[test]
fn ordered_manifest_cannot_repeat_an_identity_under_another_key() {
    let mut page = page();
    page.entries[1].key.id = page.entries[0].key.id.clone();
    assert_eq!(
        validate_manifest(&page.request, &page, 3),
        Err(CatalogueValidationError::InvalidOrder)
    );
    assert_eq!(
        CataloguePagePlan::new(page, vec![], vec![]),
        Err(CatalogueValidationError::InvalidOrder)
    );
}
