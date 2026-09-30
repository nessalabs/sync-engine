//! Current-value catalogue replication with finite, resumable passes.
//!
//! `validate_catalogue_pass` owns finite-pass admissibility;
//! `validate_manifest_request` consumes it before source I/O.
//! `validate_manifest_entry` owns individual numerical descriptor admission;
//! manifest validation and host restoration consume it without invented passes.
//! The domain validates responses and complete public page-plan relationships.
//! `CataloguePagePlan::new` derives continuation through that same owner;
//! stores call `validate_catalogue_page_plan` before their durable comparisons.
//! `validate_catalogue_revision_transition` owns descriptor compatibility across
//! source resolution and coherent unchanged/replaced cached evidence.
//! `validate_catalogue_progress` checks retained parent/active-pass agreement;
//! `catalogue_progress_after_begin`, `catalogue_progress_after_page` and
//! `catalogue_progress_after_reset` own planned progress replacements.
//! Adapters retain coherent reads, actual CAS, cache effects and commit ownership;
//! application callers correlate returned progress with the planned replacement.
//! Application ports coordinate authorization,
//! bounded source reads, and atomic local commits. Hosts supply scheduling and
//! interpret opaque entry payloads.
//! `MAX_CATALOGUE_ENTRIES` and `MAX_CATALOGUE_PAYLOAD_BYTES` are the core
//! request ceilings. A transport may impose smaller limits.

mod application;
mod domain;

pub use application::*;
pub use domain::*;
