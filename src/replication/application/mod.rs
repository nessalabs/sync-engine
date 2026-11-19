//! Explicit finite-pass coordination through host-injected ports. A host owns
//! wake scheduling and calls this use case; no background work starts here.
//! The optional open-target first-page start retains authorization before local
//! progress and before the bounded source read; stores still own atomic apply.

mod follower;
pub use follower::*;
