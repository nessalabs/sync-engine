//! Reference adapters for the application ports. SQLite adapters are optional;
//! the in-memory adapters remain available without any database dependency.
//! The loopback adapter implements bounded framed live and history reads only
//! when `transport` is enabled; the default core opens no sockets.

mod memory;
pub use memory::*;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

#[cfg(feature = "transport")]
mod loopback;
#[cfg(feature = "transport")]
pub use loopback::*;
