# Sync engine

Reusable Rust replication for local-first applications, with independent releases
and CI. The core keeps app schemas, agent execution, permissions and UI frameworks
in host adapters.

**Current status: slices 1–3 implemented.** The default-feature core
has bounded record replication and an in-memory two-device lab. The optional
`sqlite` feature adds a restartable receiver store and a file-backed reference
source. The optional `transport` feature adds a loopback-only, development
server and network source adapter. Product integration remains a later slice.

## Start here

- [Vertical-slice implementation plan](docs/implementation-plan.md), including [slice 2 / issue #3](https://github.com/nessalabs/sync-engine/issues/3)
- [Component and sequence diagrams](docs/design/core-walkthrough.md)
- [Loopback transport and recovery sequence](docs/design/loopback-transport.md)
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
4. Transcript and task-board example apps, completing the first milestone.
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

## Slice 2 restartable transcript verification

```sh
python3 scripts/verify-slice-2.py
```

The runner builds the optional SQLite example, then starts fresh processes
against files it creates in a temporary directory. It verifies cached reads
while the source file is unavailable, a failure on the second insert of a page
rolling back the entire page and checkpoint, and a restart after a committed
reply is discarded. It prints one JSON result with the final checkpoint,
record count and recovery assertions. User-selected database paths are never
reset or removed by this runner.

Run the example manually from the repository root:

```sh
cargo run --locked --features sqlite --example transcript_sqlite -- append ./source.db fact-1 "hello"
cargo run --locked --features sqlite --example transcript_sqlite -- sync ./source.db ./device.db device-a
cargo run --locked --features sqlite --example transcript_sqlite -- show ./device.db device-a
```

`show` only opens the receiver file. The example creates the named database
files and never removes them. Its text decoder is a host example; core records
remain opaque.
`SqliteReferenceSource` is an indexed, local example source. Nessa can later
adapt committed `event-stream` reads to `RecordSource`; the sync core does not
depend on that crate. SQLite transactions protect process-restart recovery on
the locally tested filesystem; this slice makes no power-loss, backup, remote
authorization, or wire-security claim.

## Slice 3 loopback transport verification

```sh
python3 scripts/verify-slice-3.py
python3 scripts/verify-slice-3.py --profiles
```

The first command runs independent server and receiver processes against
temporary SQLite files. It asserts two receivers converge, an offline receiver
fetches only missing payload, every hint can be lost and a fallback check still
converges, truncated and wrong-identity pages do not advance a checkpoint,
authorization precedes source reads, and one held receiver does not block the
other or an append. It also sends an oversized frame header and checks rejection.
The JSON result separates payload and protocol bytes, duplicate bytes, head
checks, and applied lag. `--profiles` additionally sends actual transcript
payload at 32 and 64 kbit/s with 0.8 and 1.5 second emulated RTT, and verifies
an outage followed by checkpoint recovery. The elapsed times are local evidence,
not a service availability or phone latency guarantee. CI runs the fast command.

The example accepts only IPv4 loopback addresses. Its explicit read and write
tokens and fixed receiver allowlist are development credentials; they provide no
pairing, encryption or safe exposure outside this machine. The local host owns
the fallback interval. A subscription is a wake hint, and the receiver always
fetches and validates bounded pages from its durable checkpoint. See the
[transport sequence](docs/design/loopback-transport.md) for the race ordering.

## Core checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --no-default-features --all-targets
cargo test --locked --all-features --all-targets
cargo test --locked --all-features --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
cargo +1.85.0 check --locked --all-features --all-targets
```

The minimum Rust toolchain may need `rustup toolchain install 1.85.0 --profile minimal`.
CI installs its toolchains and runs only this repository's checks. Organization
runner quotas can still be shared. Nessa will pin a reviewed core revision when its
integration starts; this setup adds no dependency or CI job to nessa-agent.

The `sqlite` feature is optional; a default or `--no-default-features` build
does not link SQLite. Both labs run without accounts, networking, or Nessa.
