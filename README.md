# Sync engine

Reusable Rust replication for local-first applications, with independent releases
and CI. The core keeps app schemas, agent execution, permissions and UI frameworks
in host adapters.

**Current status: slices 1–5 implemented.** The default-feature core
has bounded record replication and an in-memory two-device lab. The optional
`sqlite` feature adds a restartable receiver store and a file-backed reference
source. The optional `transport` feature adds a loopback-only, development
server and network source adapter. Product integration remains a later slice.

## Start here

- [Vertical-slice implementation plan](docs/implementation-plan.md), including [slice 2 / issue #3](https://github.com/nessalabs/sync-engine/issues/3)
- [Component and sequence diagrams](docs/design/core-walkthrough.md)
- [Loopback transport and recovery sequence](docs/design/loopback-transport.md)
- [Transcript and task app sequence](docs/design/example-apps.md)
- [Tail and older-history state table](docs/design/tail-history.md)
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

## Slice 4 local app verification

```sh
python3 scripts/verify-slice-4.py
```

The same process-level suite runs against a transcript and a task board. Each
uses the same Rust replication core, loopback transport and durable receiver
store, with different payload encoders and projections in the example host. It
starts source and local browser-view processes for phone and laptop identities,
stops and restarts the source, and restarts a receiver view with the source off.
It asserts cached navigation makes zero remote record reads, an unchanged head
transfers zero record payload, and one update transfers only that event. The
task view folds create, title, completion and deletion events. An unsynced view
shows **Not loaded yet**; a confirmed empty source shows **Complete and empty**.

To explore one app manually, run the source in one terminal, then use other
terminals for mutations, sync and the local browser view:

```sh
cargo run --locked --features transport --example local_apps -- source transcript ./messages-source.db 4311 read-token write-token
cargo run --locked --features transport --example local_apps -- mutate transcript 127.0.0.1:4311 write-token fact-1 message "hello"
cargo run --locked --features transport --example local_apps -- sync transcript 127.0.0.1:4311 ./phone.db ./phone.status phone read-token
cargo run --locked --features transport --example local_apps -- view transcript ./phone.db ./phone.status phone 4312
```

Open `http://127.0.0.1:4312/`. Browser requests use the local receiver cache;
their bytes are separate from the remote sync counters. The source receives
mutations; receiver views are read-only. The explicit status path holds last
check and byte counters; the SQLite checkpoint remains authoritative for
applied position. If status is missing after a crash, the view labels existing
records partial instead of presenting a false empty collection. Paths supplied
by the user are never deleted by these examples. The web view is a small local
demonstration, not an authenticated public UI or offline command outbox.
The view shows the time since the last network check separately from the time
since the last applied change. An unchanged or failed check does not make old
content look newly applied.

## Slice 5 tail and older-history verification

```sh
python3 scripts/verify-slice-5.py
```

The runner starts an independent loopback source and receiver processes. A
40-record transcript first transfers only its most recent five records. Its
forward checkpoint remains separate from the lower boundary of saved older
history. Fifty overlapping view requests combine into three bounded older-page
reads; a fresh process resumes from the committed lower boundary. A delayed
older reply cannot rewind a newer live head. Delayed snapshot and history
replies are refused after a newer reset or deletion fence. A source pruning
floor produces typed `ResetRequired` and leaves the partial cache intact.
The JSON result reports payload and protocol bytes, request counts and both
positions. CI runs this same command.

The example source keeps physical fact rows after advancing its historical
read floor, preserving immutable ID deduplication. The reference receiver
saves tail records, two progress boundaries, reset generation and deletion
fence transactionally. The host-facing `HistoryReadState` distinguishes
unloaded, loading, partial, complete-empty, complete, failed, stale and
deleted states. Large-history UI rendering, mobile background scheduling and
production snapshot compatibility policy remain host work.

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

The `sqlite` and `transport` features are optional; a default or
`--no-default-features` build does not link SQLite or open sockets. The first
lab runs in memory. Later labs use local files and loopback networking without
accounts or Nessa.
