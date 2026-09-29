//! Optional, file-backed reference adapters. A source file and each receiver
//! file have distinct schemas. They are process-restart durable on SQLite's
//! local filesystem contract; this slice does not claim power-loss durability,
//! backup consistency, or authenticated remote delivery.
//!
//! `source` implements bounded committed reads, recent tails, older pages and
//! append for the example. `replica` implements atomic live apply/checkpoint,
//! tail installation, older coverage and deletion fences, plus cached reads.
//! `connection` owns file identity, opening policy, and schema setup. Each
//! adapter owns its connection and closes it on drop; the domain and application
//! layers import only their ports and never import SQLite.
//! `catalogue` supplies separate latest-value source and receiver files for
//! finite list passes; it does not share transcript history tables.
//! `artifact` keeps current source bytes and a separate receiver staging cache.

mod artifact;
mod catalogue;
mod connection;
mod replica;
mod source;

pub use artifact::{SqliteArtifactCache, SqliteArtifactSource};
pub use catalogue::{SqliteCatalogueSource, SqliteCatalogueStore};
pub use connection::SqliteOpenError;
use connection::{
    configure_connection, ensure_schema, open_connection, ARTIFACT_CACHE_APPLICATION_ID,
    ARTIFACT_SOURCE_APPLICATION_ID, CATALOGUE_REPLICA_APPLICATION_ID,
    CATALOGUE_SOURCE_APPLICATION_ID, REPLICA_APPLICATION_ID, SOURCE_APPLICATION_ID,
};
pub use replica::SqliteReplicaStore;
pub use source::SqliteReferenceSource;
