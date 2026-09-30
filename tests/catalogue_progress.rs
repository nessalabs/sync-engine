use nessa_sync::replication::{
    application::{Access, ScopeAuthorizer},
    catalogue::{
        apply_next_page, begin_or_resume, catalogue_progress_after_begin,
        catalogue_progress_after_page, catalogue_progress_after_reset, reset_catalogue,
        validate_catalogue_pass, validate_catalogue_progress, validate_manifest_request,
        CatalogueError, CataloguePagePlan, CataloguePass, CatalogueProgress,
        CatalogueProgressError, CatalogueSource, CatalogueSourceError, CatalogueStore,
        CatalogueStoreError, CatalogueValidationError, EntryKey, ManifestEntry, ManifestPage,
        ManifestRequest, ResolvedEntry,
    },
    domain::{Id, Scope},
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("index"),
        id("first"),
        id("schema"),
        id("epoch"),
    )
}
fn changed_scope(index: usize) -> Scope {
    let fields = ["receiver", "origin", "index", "first", "schema", "epoch"];
    let fields: [Id; 6] = std::array::from_fn(|part| {
        id(if part == index {
            "different"
        } else {
            fields[part]
        })
    });
    Scope::new(
        fields[0].clone(),
        fields[1].clone(),
        fields[2].clone(),
        fields[3].clone(),
        fields[4].clone(),
        fields[5].clone(),
    )
}
fn retained() -> CatalogueProgress {
    CatalogueProgress {
        scope: scope(),
        completed: 3,
        generation: 7,
        active: None,
    }
}
fn active() -> CatalogueProgress {
    catalogue_progress_after_begin(&scope(), Some(&retained()), 5).unwrap()
}

#[test]
fn progress_validates_enclosing_evidence() {
    for valid in [
        retained(),
        active(),
        CatalogueProgress {
            completed: 0,
            generation: 0,
            ..retained()
        },
    ] {
        let original = valid.clone();
        assert_eq!(validate_catalogue_progress(&valid), Ok(()));
        assert_eq!(valid, original);
    }
    let valid = active();
    let mut cases = Vec::new();
    let mut value = retained();
    value.generation = 0;
    cases.push(value);
    let mut value = valid.clone();
    value.generation = 0;
    cases.push(value);
    let mut value = valid.clone();
    value.completed += 1;
    cases.push(value);
    let mut value = valid.clone();
    value.generation += 1;
    cases.push(value);
    let mut value = valid.clone();
    value.active.as_mut().unwrap().scope = changed_scope(5);
    cases.push(value);
    let mut value = valid.clone();
    value.active.as_mut().unwrap().completed += 1;
    cases.push(value);
    let mut value = valid.clone();
    value.active.as_mut().unwrap().generation += 1;
    cases.push(value);
    for boundary in [0, 3] {
        let mut value = valid.clone();
        value.active.as_mut().unwrap().boundary = boundary;
        cases.push(value);
    }
    for creation in [0, 6] {
        let mut value = valid.clone();
        value.active.as_mut().unwrap().cursor = Some(EntryKey {
            creation,
            id: id("cursor"),
        });
        cases.push(value);
    }
    for value in cases {
        let original = value.clone();
        assert_eq!(
            validate_catalogue_progress(&value),
            Err(CatalogueProgressError::InvalidProgress)
        );
        assert_eq!(value, original);
    }
}

#[test]
fn progress_begin_preserves_empty_and_finite_passes() {
    let empty = catalogue_progress_after_begin(&scope(), None, 0).unwrap();
    assert_eq!(
        empty,
        CatalogueProgress {
            scope: scope(),
            completed: 0,
            generation: 1,
            active: None
        }
    );
    let first = catalogue_progress_after_begin(&scope(), None, u64::MAX).unwrap();
    assert_eq!(first.completed, 0);
    assert_eq!(first.generation, 1);
    assert_eq!(first.active.as_ref().unwrap().boundary, u64::MAX);
    assert!(first.active.as_ref().unwrap().cursor.is_none());
    let current = retained();
    let next = catalogue_progress_after_begin(&scope(), Some(&current), 5).unwrap();
    assert_eq!(next.completed, 3);
    assert_eq!(next.generation, 8);
    assert_eq!(next.active.as_ref().unwrap().completed, 3);
    assert_eq!(next.active.as_ref().unwrap().generation, 8);
    assert_eq!(current, retained());
}

#[test]
fn progress_begin_refuses_conflicting_state() {
    for boundary in [0, 2, 3] {
        assert_eq!(
            catalogue_progress_after_begin(&scope(), Some(&retained()), boundary),
            Err(CatalogueProgressError::Stale)
        );
    }
    assert_eq!(
        catalogue_progress_after_begin(&scope(), Some(&active()), 9),
        Err(CatalogueProgressError::Stale)
    );
    for index in 0..6 {
        assert_eq!(
            catalogue_progress_after_begin(&changed_scope(index), Some(&retained()), 5),
            Err(CatalogueProgressError::WrongScope)
        );
    }
    let exhausted = CatalogueProgress {
        generation: u64::MAX,
        ..retained()
    };
    assert_eq!(
        catalogue_progress_after_begin(&scope(), Some(&exhausted), 5),
        Err(CatalogueProgressError::GenerationExhausted)
    );
    let invalid = CatalogueProgress {
        generation: 0,
        ..retained()
    };
    assert_eq!(
        catalogue_progress_after_begin(&scope(), Some(&invalid), 5),
        Err(CatalogueProgressError::InvalidProgress)
    );
}

fn page_plan(more: bool, empty: bool) -> CataloguePagePlan {
    let pass = active().active.unwrap();
    let entry = ManifestEntry {
        key: EntryKey {
            creation: 2,
            id: id("entry"),
        },
        revision: 4,
        deleted: false,
    };
    CataloguePagePlan::new(
        ManifestPage {
            request: ManifestRequest {
                pass,
                max_entries: 1,
            },
            entries: if empty { vec![] } else { vec![entry.clone()] },
            has_more: more,
        },
        if empty {
            vec![]
        } else {
            vec![ResolvedEntry {
                manifest: entry,
                payload: vec![42],
            }]
        },
        vec![],
    )
    .unwrap()
}

#[test]
fn page_progress_matches_validated_plan() {
    for (more, empty) in [(true, false), (false, false), (false, true)] {
        let plan = page_plan(more, empty);
        let original = plan.clone();
        let next = catalogue_progress_after_page(&plan).unwrap();
        assert_eq!(next.scope, scope());
        assert_eq!(next.generation, 8);
        assert_eq!(next.completed, if more { 3 } else { 5 });
        assert_eq!(next.active.is_some(), more);
        if more {
            assert_eq!(
                next.active.as_ref().unwrap().cursor,
                Some(EntryKey {
                    creation: 2,
                    id: id("entry")
                })
            );
        }
        assert_eq!(validate_catalogue_progress(&next), Ok(()));
        assert_eq!(plan, original);
    }
}

#[test]
fn page_progress_refuses_edited_plan() {
    let original = page_plan(true, false);
    let mut cases = Vec::new();
    let mut plan = original.clone();
    plan.final_page = true;
    cases.push(plan);
    let mut plan = original.clone();
    plan.next_cursor = None;
    cases.push(plan);
    let mut plan = original.clone();
    plan.pass.generation += 1;
    cases.push(plan);
    for plan in cases {
        assert!(matches!(
            catalogue_progress_after_page(&plan),
            Err(CatalogueProgressError::InvalidPage(_))
        ));
    }
}

#[test]
fn progress_reset_preserves_target_and_generation() {
    let current = active();
    for index in 0..6 {
        let target = changed_scope(index);
        assert_eq!(scope().same_receiver_stream(&target), index >= 3);
        let result = catalogue_progress_after_reset(&target, &current);
        if index < 3 {
            assert_eq!(result, Err(CatalogueProgressError::WrongScope));
        } else {
            assert_eq!(
                result.unwrap(),
                CatalogueProgress {
                    scope: target,
                    completed: 0,
                    generation: 9,
                    active: None
                }
            );
        }
    }
    let same = catalogue_progress_after_reset(&scope(), &current).unwrap();
    assert_eq!(same.generation, 9);
    assert!(same.active.is_none());
    let exhausted = CatalogueProgress {
        generation: u64::MAX,
        ..retained()
    };
    assert_eq!(
        catalogue_progress_after_reset(&scope(), &exhausted),
        Err(CatalogueProgressError::GenerationExhausted)
    );
    let invalid = CatalogueProgress {
        generation: 0,
        ..retained()
    };
    assert_eq!(
        catalogue_progress_after_reset(&scope(), &invalid),
        Err(CatalogueProgressError::InvalidProgress)
    );
    assert_eq!(current, active());
}

struct Authority(usize);
impl ScopeAuthorizer for Authority {
    fn authorize(&mut self, scope: &Scope) -> Access {
        self.0 += 1;
        Access::Allowed(scope.clone())
    }
}
struct Source {
    reads: usize,
}
impl CatalogueSource for Source {
    fn head(&mut self, _: &Scope) -> Result<u64, CatalogueSourceError> {
        self.reads += 1;
        Ok(5)
    }
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestPage, CatalogueSourceError> {
        self.reads += 1;
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
        panic!("empty manifest must not resolve")
    }
}
struct Store {
    saved: Option<CatalogueProgress>,
    writes: usize,
    wrong_return: bool,
}
impl Store {
    fn returned(&self, mut planned: CatalogueProgress) -> CatalogueProgress {
        if self.wrong_return {
            planned.generation += 1;
            if let Some(pass) = &mut planned.active {
                pass.generation += 1;
            }
        }
        planned
    }
}
impl CatalogueStore for Store {
    fn progress(&mut self, _: &Scope) -> Result<Option<CatalogueProgress>, CatalogueStoreError> {
        Ok(self.saved.clone())
    }
    fn begin(
        &mut self,
        scope: &Scope,
        expected: Option<CatalogueProgress>,
        head: u64,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        self.writes += 1;
        Ok(self.returned(catalogue_progress_after_begin(
            scope,
            expected.as_ref(),
            head,
        )?))
    }
    fn cached_revision(&mut self, _: &Scope, _: &Id) -> Result<Option<u64>, CatalogueStoreError> {
        panic!("empty manifest has no cache lookup")
    }
    fn apply_page(
        &mut self,
        plan: CataloguePagePlan,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        self.writes += 1;
        Ok(self.returned(catalogue_progress_after_page(&plan)?))
    }
    fn reset(
        &mut self,
        scope: &Scope,
        expected: CatalogueProgress,
    ) -> Result<CatalogueProgress, CatalogueStoreError> {
        self.writes += 1;
        Ok(self.returned(catalogue_progress_after_reset(scope, &expected)?))
    }
}

#[test]
fn custom_progress_refusal_has_no_source_effects() {
    for field in 0..4 {
        let mut corrupt = active();
        match field {
            0 => corrupt.active.as_mut().unwrap().scope = changed_scope(5),
            1 => corrupt.active.as_mut().unwrap().completed += 1,
            2 => corrupt.active.as_mut().unwrap().generation += 1,
            _ => {
                corrupt.active.as_mut().unwrap().cursor = Some(EntryKey {
                    creation: 6,
                    id: id("cursor"),
                })
            }
        }
        let mut authority = Authority(0);
        let mut source = Source { reads: 0 };
        let mut store = Store {
            saved: Some(corrupt),
            writes: 0,
            wrong_return: false,
        };
        assert_eq!(
            begin_or_resume(&scope(), &mut authority, &mut source, &mut store),
            Err(CatalogueError::Store(CatalogueStoreError::Failed))
        );
        assert_eq!((authority.0, source.reads, store.writes), (1, 0, 0));
        store.saved = Some(active());
        assert_eq!(
            begin_or_resume(&scope(), &mut authority, &mut source, &mut store).unwrap(),
            active().active
        );
        assert_eq!((source.reads, store.writes), (0, 0));
    }
}

#[test]
fn store_return_is_correlated_with_each_planned_transition() {
    let refusal = CatalogueError::Validation(CatalogueValidationError::WrongRequest);
    for wrong_return in [true, false] {
        let mut authority = Authority(0);
        let mut source = Source { reads: 0 };
        let mut store = Store {
            saved: Some(retained()),
            writes: 0,
            wrong_return,
        };
        let begun = begin_or_resume(&scope(), &mut authority, &mut source, &mut store);
        if wrong_return {
            assert_eq!(begun.unwrap_err(), refusal);
        } else {
            assert_eq!(begun.unwrap(), active().active);
        }
        let pass = active().active.unwrap();
        let page = apply_next_page(&pass, 1, 1, &mut authority, &mut source, &mut store);
        let reset = reset_catalogue(&changed_scope(5), active(), &mut authority, &mut store);
        if wrong_return {
            assert_eq!(page.unwrap_err(), refusal);
            assert_eq!(reset.unwrap_err(), refusal);
        } else {
            assert_eq!(page.unwrap().completed, 5);
            assert_eq!(reset.unwrap().scope, changed_scope(5));
        }
        assert_eq!(store.writes, 3);
    }
}

#[test]
fn public_cursor_validation_precedes_final_page_and_source() {
    for creation in [0, 6, 1, 5] {
        let mut pass = active().active.unwrap();
        pass.cursor = Some(EntryKey {
            creation,
            id: id("cursor"),
        });
        let request = ManifestRequest {
            pass: pass.clone(),
            max_entries: 1,
        };
        let page = ManifestPage {
            request: request.clone(),
            entries: vec![],
            has_more: false,
        };
        let original = request.clone();
        let plan = CataloguePagePlan {
            pass: pass.clone(),
            manifest: page.clone(),
            entries: vec![],
            unchanged: vec![],
            next_cursor: pass.cursor.clone(),
            final_page: true,
        };
        let mut authority = Authority(0);
        let mut source = Source { reads: 0 };
        let mut store = Store {
            saved: Some(active()),
            writes: 0,
            wrong_return: false,
        };
        if creation == 0 || creation == 6 {
            let invalid = CatalogueValidationError::InvalidRequest;
            assert_eq!(validate_catalogue_pass(&pass), Err(invalid.clone()));
            assert_eq!(validate_manifest_request(&request, 1), Err(invalid.clone()));
            assert_eq!(
                CataloguePagePlan::new(page, vec![], vec![]),
                Err(invalid.clone())
            );
            assert_eq!(
                catalogue_progress_after_page(&plan),
                Err(CatalogueProgressError::InvalidPage(invalid.clone()))
            );
            assert_eq!(
                apply_next_page(&pass, 1, 1, &mut authority, &mut source, &mut store),
                Err(CatalogueError::Validation(invalid))
            );
            assert_eq!((authority.0, source.reads, store.writes), (0, 0, 0));
        } else {
            assert_eq!(validate_catalogue_pass(&pass), Ok(()));
            assert_eq!(validate_manifest_request(&request, 1), Ok(()));
            assert_eq!(CataloguePagePlan::new(page, vec![], vec![]).unwrap(), plan);
            let completed = catalogue_progress_after_page(&plan).unwrap();
            assert_eq!(completed.completed, 5);
            assert!(completed.active.is_none());
            assert_eq!(
                apply_next_page(&pass, 1, 1, &mut authority, &mut source, &mut store).unwrap(),
                completed
            );
            assert_eq!((authority.0, source.reads, store.writes), (2, 1, 1));
        }
        assert_eq!(request, original);
    }
}
