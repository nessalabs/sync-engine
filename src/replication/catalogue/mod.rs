//! Current-value catalogue replication with finite, resumable passes.
//!
//! `validate_catalogue_pass` owns finite-pass admissibility;
//! `validate_manifest_request` consumes it before source I/O.
//! The domain validates responses and complete public page-plan relationships.
//! `CataloguePagePlan::new` derives continuation through that same owner;
//! stores call `validate_catalogue_page_plan` before their durable comparisons.
//! Application ports coordinate authorization,
//! bounded source reads, and atomic local commits. Hosts supply scheduling and
//! interpret opaque entry payloads.
//! `MAX_CATALOGUE_ENTRIES` and `MAX_CATALOGUE_PAYLOAD_BYTES` are the core
//! request ceilings. A transport may impose smaller limits.

mod application;
mod domain;

pub use application::*;
pub use domain::*;
