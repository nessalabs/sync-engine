# Sync engine

Reusable Rust replication for local-first applications, with independent releases
and CI. The core keeps app schemas, agent execution, permissions and UI frameworks
in host adapters.

**Current status: planning and repository scaffold only. No sync behavior is
implemented yet.** The workspace compiles, but there are no behavioral tests yet.
A green scaffold build is not proof of replication or recovery.

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

## Current scaffold checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --no-default-features
cargo test --locked --all-features --all-targets
cargo test --locked --all-features --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo +1.85.0 check --locked --all-targets --all-features
```

The minimum Rust toolchain may need `rustup toolchain install 1.85.0 --profile minimal`.
CI installs its toolchains and runs only this repository's checks. Organization
runner quotas can still be shared. Nessa will pin a reviewed core revision when its
integration starts; this setup adds no dependency or CI job to nessa-agent.

Example commands and `scripts/verify-slice-N` runners are deliverables of the
implementation issues. They do not exist yet. The first example must be runnable
without accounts, credentials or Nessa.
