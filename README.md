# Sync engine

Reusable Rust replication for local-first applications, with independent releases
and CI. The core keeps app schemas, agent execution, permissions and UI frameworks
in host adapters.

**Current status: slice 1 implemented locally.** The default-feature core has
bounded record replication and an in-memory two-device lab. Persistence across
process restart, a wire protocol, and product integration are later slices.

## Start here

- [Vertical-slice implementation plan](docs/implementation-plan.md), starting with [slice 1 / issue #2](https://github.com/nessalabs/sync-engine/issues/2)
- [Component and sequence diagrams](docs/design/core-walkthrough.md)
- [Sync ADR](docs/adr/1-reusable-local-first-sync-engine.md)
- [Detailed target contract and validation plan](docs/design/sync-engine.md)
- [Contributing](CONTRIBUTING.md)

The design moved from [nessa-agent#247](https://github.com/nessalabs/nessa-agent/issues/247).
[Issue #1](https://github.com/nessalabs/sync-engine/issues/1) is the parent tracker here.
Nessa's broader runtime architecture remains separate.

## Delivery milestones

1. Two-device replication lab.
2. Durable storage and transcript restart.
3. Real transport, missed notifications and weak links.
4. Transcript and task-board example apps: **first milestone complete**.
5. Tail snapshots, historical backfill and bounded lazy loading.
6. Catalogue passes that finish despite continuous updates.

A cached view should not fetch unchanged records remotely. Notifications, access
checks, head checks and connection maintenance still consume protocol bytes; the
examples will measure those separately from record payloads.

## Slice 1 end-to-end verification

```sh
./scripts/verify-slice-1
```

The command asserts both devices have the exact five source records, separate
checkpoints, an offline catch-up, and zero new record payload bytes for an unchanged
head. It prints one JSON object with read, payload-byte, and apply counters. The
integration tests inject malformed pages, policy failures, competing plans, store
failures, uncertain commits, and concurrent source writes. CI runs the same script.
The in-memory store does not survive process restart; the source and authorizer are
illustrative injected adapters, not a production trust or transport boundary. A
transport adapter must enforce its wire frame limit before decode, and a product
adapter must interpret its declared payload schema before applying it. A saved
incarnation, schema, or access-epoch mismatch returns both scopes and requires an
explicit host reset; slice 1 never rewinds or silently replaces that checkpoint.

## Core checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.85.0 check --locked --all-targets
```

The minimum Rust toolchain may need `rustup toolchain install 1.85.0 --profile minimal`.
CI installs its toolchains and runs only this repository's checks. Organization
runner quotas can still be shared. Nessa will pin a reviewed core revision when its
integration starts; this setup adds no dependency or CI job to nessa-agent.

The crate currently defines no Cargo features. The lab and checks run without
accounts, credentials, SQLite, networking, or Nessa.
