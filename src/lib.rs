//! Bounded, host-agnostic replication of immutable ordered records, including
//! recent-tail bootstrap and separately tracked older-history coverage.
//!
//! A host supplies source, authorization, and atomic replica-store adapters. The
//! library neither schedules work nor interprets record payloads.
//!
//! A host validates one untrusted page before giving its plan to an atomic store:
//!
//! ```
//! use nessa_sync::replication::domain::{validate_page, Checkpoint, Id, Limits, Page, PageRequest, Record, Scope};
//! let id = |value| Id::new(value).unwrap();
//! let scope = Scope::new(id("receiver"), id("origin"), id("stream"), id("first"), id("schema"), id("epoch"));
//! let checkpoint = Checkpoint::new(scope.clone(), 0);
//! let limits = Limits::new(2, 16, 16).unwrap();
//! let request = PageRequest { scope: scope.clone(), after: 0, target: 1, max_records: 2, max_payload_bytes: 16, max_record_bytes: 16 };
//! let page = Page { request: request.clone(), records: vec![Record { position: 1, id: id("fact-1"), scope, payload: b"hello".to_vec() }] };
//! let plan = validate_page(&checkpoint, &request, page, limits).unwrap();
//! assert_eq!(plan.next().position(), 1);
//! ```
#![deny(missing_docs)]
#![forbid(unsafe_code)]

/// Record replication feature and its domain, application, and memory adapter.
pub mod replication;
