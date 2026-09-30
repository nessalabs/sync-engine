//! Pure retained-progress validation and catalogue progress replacements.
//!
//! Adapters read coherent progress and compare current/expected evidence in their
//! transaction. These helpers establish relationships and planned replacements;
//! they perform no I/O, authorization, payload decoding, mutation or durable CAS.
//! Inputs may be shared immutably across callers. Page planning consumes the
//! existing bounded page-plan owner and clones only scope/cursor metadata.

#![deny(missing_docs)]

use super::{
    validate_catalogue_page_plan, validate_catalogue_pass, CataloguePagePlan, CataloguePass,
    CatalogueProgress, CatalogueValidationError,
};
use crate::replication::domain::Scope;

/// Typed refusal of an enclosing progress value or requested replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueProgressError {
    /// Parent and active-pass facts do not describe one retained state.
    InvalidProgress,
    /// A public page plan failed its existing structural owner.
    InvalidPage(CatalogueValidationError),
    /// The requested scope does not match the operation's retained target.
    WrongScope,
    /// The requested begin does not advance inactive progress.
    Stale,
    /// Incrementing the retained generation would exceed its representation.
    GenerationExhausted,
}

/// Validates retained parent and active-pass evidence together.
///
/// Generation zero is permitted only for empty inactive initial progress. An
/// active pass must agree with its parent scope, completed revision and generation,
/// satisfy the existing pass owner and retain a positive cursor within its boundary.
/// This borrowed operation does not certify cache presence or durable truth.
///
/// # Errors
/// Returns [`CatalogueProgressError::InvalidProgress`] for contradictory evidence.
pub fn validate_catalogue_progress(
    progress: &CatalogueProgress,
) -> Result<(), CatalogueProgressError> {
    if progress.generation == 0 && (progress.completed != 0 || progress.active.is_some()) {
        return Err(CatalogueProgressError::InvalidProgress);
    }
    if let Some(pass) = &progress.active {
        if pass.scope != progress.scope
            || pass.completed != progress.completed
            || pass.generation != progress.generation
            || validate_catalogue_pass(pass).is_err()
        {
            return Err(CatalogueProgressError::InvalidProgress);
        }
    }
    Ok(())
}

/// Plans progress for a new finite pass or an initially confirmed empty source.
///
/// `scope` is the admitted exact source scope, `current` is coherent retained
/// evidence, and `boundary` is the captured source revision. The adapter must
/// compare actual current progress with its expected value before saving this
/// replacement. No source read or save occurs here.
///
/// # Example
/// ```
/// use nessa_sync::replication::{
///     catalogue::{catalogue_progress_after_begin, validate_catalogue_progress},
///     domain::{Id, Scope},
/// };
/// let id = Id::new("opaque").unwrap();
/// let scope = Scope::new(id.clone(), id.clone(), id.clone(), id.clone(), id.clone(), id);
/// let progress = catalogue_progress_after_begin(&scope, None, 5).unwrap();
/// assert_eq!(progress.active.as_ref().unwrap().boundary, 5);
/// assert_eq!(validate_catalogue_progress(&progress), Ok(()));
/// ```
///
/// # Errors
/// Refuses invalid retained evidence, changed exact scope, an active/nonadvancing
/// begin, or exhausted generation through [`CatalogueProgressError`].
pub fn catalogue_progress_after_begin(
    scope: &Scope,
    current: Option<&CatalogueProgress>,
    boundary: u64,
) -> Result<CatalogueProgress, CatalogueProgressError> {
    if let Some(current) = current {
        validate_catalogue_progress(current)?;
        if current.scope != *scope {
            return Err(CatalogueProgressError::WrongScope);
        }
    }
    let completed = current.map_or(0, |current| current.completed);
    let confirmed_empty = current.is_none() && boundary == 0;
    if (!confirmed_empty && boundary <= completed)
        || current.is_some_and(|current| current.active.is_some())
    {
        return Err(CatalogueProgressError::Stale);
    }
    let generation = current
        .map_or(Some(1), |current| current.generation.checked_add(1))
        .ok_or(CatalogueProgressError::GenerationExhausted)?;
    Ok(CatalogueProgress {
        scope: scope.clone(),
        completed,
        generation,
        active: (!confirmed_empty).then(|| CataloguePass {
            scope: scope.clone(),
            completed,
            boundary,
            cursor: None,
            generation,
        }),
    })
}

/// Plans the exact progress replacement described by a validated page plan.
///
/// A final page completes the captured boundary and clears the active pass; a
/// continuation retains completed progress and saves the validated next cursor.
/// The page generation is retained. This function does not compare an actual
/// store row, acquire payloads or perform effects; adapters retain that ownership.
///
/// # Errors
/// Returns [`CatalogueProgressError::InvalidPage`] with the page owner's typed
/// refusal when public plan fields disagree.
pub fn catalogue_progress_after_page(
    plan: &CataloguePagePlan,
) -> Result<CatalogueProgress, CatalogueProgressError> {
    validate_catalogue_page_plan(plan).map_err(CatalogueProgressError::InvalidPage)?;
    Ok(CatalogueProgress {
        scope: plan.pass.scope.clone(),
        completed: if plan.final_page {
            plan.pass.boundary
        } else {
            plan.pass.completed
        },
        generation: plan.pass.generation,
        active: (!plan.final_page).then(|| CataloguePass {
            cursor: plan.next_cursor.clone(),
            ..plan.pass.clone()
        }),
    })
}

/// Plans an explicit reset within the same receiver, origin and stream.
///
/// `scope` supplies the replacement incarnation/schema/epoch; `current` retains
/// the admitted prior evidence. Completed progress becomes zero, the active pass
/// is cleared and generation advances. Actual CAS, removal of live cache rows,
/// retained deletion markers, audit and commit remain with the adapter.
///
/// # Errors
/// Refuses contradictory progress, changed stable target or exhausted generation.
pub fn catalogue_progress_after_reset(
    scope: &Scope,
    current: &CatalogueProgress,
) -> Result<CatalogueProgress, CatalogueProgressError> {
    validate_catalogue_progress(current)?;
    if !current.scope.same_receiver_stream(scope) {
        return Err(CatalogueProgressError::WrongScope);
    }
    let generation = current
        .generation
        .checked_add(1)
        .ok_or(CatalogueProgressError::GenerationExhausted)?;
    Ok(CatalogueProgress {
        scope: scope.clone(),
        completed: 0,
        generation,
        active: None,
    })
}
