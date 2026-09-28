//! Optional, file-backed reference adapters. A source file and each receiver
//! file have distinct schemas. They are process-restart durable on SQLite's
//! local filesystem contract; this slice does not claim power-loss durability,
//! backup consistency, or authenticated remote delivery.
//!
//! `source` implements bounded committed reads and append for the example.
//! `replica` implements atomic apply/checkpoint and bounded cached reads.
//! `connection` owns file identity, opening policy, and schema setup. Each
//! adapter owns its connection and closes it on drop; the domain and application
//! layers import only their ports and never import SQLite.

mod connection;
mod replica;
mod source;

pub use connection::SqliteOpenError;
use connection::{
    configure_connection, ensure_schema, open_connection, REPLICA_APPLICATION_ID,
    SOURCE_APPLICATION_ID,
};
pub use replica::SqliteReplicaStore;
pub use source::SqliteReferenceSource;
