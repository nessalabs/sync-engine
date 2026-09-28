# Sync engine

Reusable Rust replication for local-first applications. This repository owns its own Cargo workspace, CI and releases, independently of Nessa.

The core is being implemented under [issue #1](https://github.com/nessalabs/sync-engine/issues/1), moved from [nessa-agent#247](https://github.com/nessalabs/nessa-agent/issues/247).

## Design

- [Sync ADR](docs/adr/1-reusable-local-first-sync-engine.md)
- [Detailed design and staged validation plan](docs/design/sync-engine.md)

These documents include later Nessa integration requirements. They describe a target, not completed features. The reusable core keeps product records, agent execution and permission policy in host adapters.
