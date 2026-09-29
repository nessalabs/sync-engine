//! Record replication. `domain` owns pure validation; `application` owns port
//! coordination; `infrastructure` provides an illustrative memory adapter.
//!
//! Imports point inward: application imports domain, infrastructure implements
//! application ports, and the host composes both. No module starts background work.

/// One-page and finite-pass use cases with injected ports.
pub mod application;
/// Artifact identity, manifests and local content availability contracts.
pub mod artifacts;
/// Finite current-value catalogue passes, independent of transcript history.
pub mod catalogue;
/// Pure identities, bounds, records, and commit-plan validation.
pub mod domain;
/// Bounded recent-tail and older-history contracts with separate progress.
pub mod history;
/// In-memory reference adapters for tests and the two-device lab.
pub mod infrastructure;
