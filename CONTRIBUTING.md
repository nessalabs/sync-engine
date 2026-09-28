# Contributing

The [implementation plan](docs/implementation-plan.md) is the starting point. The
[sync ADR](docs/adr/1-reusable-local-first-sync-engine.md) records direction; the
[detailed contract](docs/design/sync-engine.md) owns the wider protocol requirements.
The [walkthrough](docs/design/core-walkthrough.md) illustrates them and maps the
first-slice orderings to verification. Keep current implementation status explicit.

## Structure and ownership

Organize by feature, then domain/application/infrastructure. Domain decisions are
pure. Application-owned ports isolate storage, policy, transport and clocks;
composition supplies adapters. Product schemas, UI state and command execution stay
in the host. Value objects are immutable and validated. Expected failures are typed.
Name one owner per decision; link to the canonical rule instead of copying it.
Do not introduce empty layers, global registries or a universal effect framework.

Module maps explain responsibilities, imports and lifecycle. Group Rust imports at
module scope. Document public APIs, errors, resource ownership and concurrency;
keep missing-doc and unsafe-code gates enabled. Effects and I/O belong outside the
domain. Diagnostics go to stderr and machine-readable example results to stdout.

## Verification and review

An issue exists before an implementation branch. Design retry, reset, cancellation
and crash orderings before code. Each slice includes a runnable example and a
repeatable end-to-end verification command. Test actual boundary failures, not
only a reimplementation of the happy path inside a fake.

Review identity, authority, commit state and progress together. Inject races at
controlled boundaries, use deterministic gates, and prove both invalid and valid
orderings. For each bug fix, demonstrate a regression test fails when that fix is
reverted and restore the final tree. Store adapters require transaction/restart and
competing-handle evidence. A Rust type check does not establish persistence atomicity.

Before review, run formatting, lint, relevant tests, Rustdoc and the slice's example
runner on the actual final tree. Give reviewers the checkout, base/head, dirty-file
scope, goals and known findings. Record inspected evidence and exclusions. Fix
findings without silently expanding scope. Stop at the agreed user review budget.

The library starts unpublished. Package publication, production remote exposure,
automatic failover and durability claims beyond tested boundaries need explicit
scope. Source data and user-selected directories are never disposable demo fixtures.

## Independent CI

Use this repository's workflow and lockfile. Add checks to the existing job when
practical. A new OS job needs behavior that differs by platform. Verify optional
infrastructure does not leak into the default core dependency graph. Keep the
published minimum Rust version tested when dependencies are introduced.
