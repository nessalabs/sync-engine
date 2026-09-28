//! Explicit finite-pass coordination through host-injected ports. A host owns
//! wake scheduling and calls this use case; no background work starts here.

mod follower;
pub use follower::*;
