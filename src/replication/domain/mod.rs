//! Pure record identity, bounds, and commit-plan validation. This module owns
//! all invariants checked before a receiver store effect.
//! Hosts call `validate_page_request` before source I/O. `validate_page` consumes
//! that owner and additionally checks receiver checkpoint and response evidence.

mod records;
pub use records::*;
