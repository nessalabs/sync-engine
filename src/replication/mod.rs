//! Record replication. `domain` owns pure validation; `application` owns port
//! coordination; `infrastructure` provides an illustrative memory adapter.
//!
//! Imports point inward: application imports domain, infrastructure implements
//! application ports, and the host composes both. No module starts background work.

/// One-page and finite-pass use cases with injected ports.
pub mod application;
/// Pure identities, bounds, records, and commit-plan validation.
pub mod domain;
/// In-memory reference adapters for tests and the two-device lab.
pub mod infrastructure;
