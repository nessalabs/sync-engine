//! Current-value catalogue replication with finite, resumable passes.
//!
//! The domain validates responses. Application ports coordinate authorization,
//! bounded source reads, and atomic local commits. Hosts supply scheduling and
//! interpret opaque entry payloads.
//! `MAX_CATALOGUE_ENTRIES` and `MAX_CATALOGUE_PAYLOAD_BYTES` are the core
//! request ceilings. A transport may impose smaller limits.

mod application;
mod domain;

pub use application::*;
pub use domain::*;
