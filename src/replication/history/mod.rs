//! A transcript can install a bounded recent tail and fill older content later.
//! Domain validation is pure; application ports own source and store effects.
//! `SqliteReferenceSource` and `SqliteReplicaStore` provide optional reference
//! adapters through the existing `sqlite` feature.

mod application;
mod domain;

pub use application::*;
pub use domain::*;
