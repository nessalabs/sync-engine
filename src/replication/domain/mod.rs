//! Pure record identity, bounds, and commit-plan validation. This module owns
//! all invariants checked before a receiver store effect.
//! Hosts call `validate_page_request` before source I/O. `validate_page` consumes
//! that owner and additionally checks receiver checkpoint and response evidence.
//! Open-target first pages validate their exact echo/captured target before
//! delegating nonempty records to the same canonical page validator.

mod records;
pub use records::*;

mod source_head;
pub use source_head::*;

mod retention;
pub use retention::*;
