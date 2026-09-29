//! Current-value catalogue replication with finite, resumable passes.
//!
//! The domain validates responses. Application ports coordinate authorization,
//! bounded source reads, and atomic local commits. Hosts supply scheduling and
//! interpret opaque entry payloads.

mod application;
mod domain;

pub use application::*;
pub use domain::*;
