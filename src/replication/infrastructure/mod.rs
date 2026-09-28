//! In-memory reference adapters for the first-slice lab. They implement the
//! application ports but provide no process-restart durability or wire security.

mod memory;
pub use memory::*;
