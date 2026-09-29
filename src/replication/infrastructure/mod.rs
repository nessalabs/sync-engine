//! Reference adapters for the application ports. SQLite adapters are optional;
//! the in-memory adapters remain available without any database dependency.
//! `transport` provides bounded framed clients and a generic read-only record
//! server. Enable `sqlite` as well for the SQLite-backed reference server.
//! The default core opens no sockets.

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
