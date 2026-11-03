---
id: "verification"
title: "Check atlas evidence"
kind: "guide"
status: "implemented"
summary: "Current local execution, independent review and finite model results are separate evidence. Reference labs do not qualify a production deployment."
parent: "sync"
sources: ["Cargo.toml", "README.md", "CONTRIBUTING.md"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Check tests and model limits

Source checkpoint: `f2a05ef24fcff66df9d55e508539cec718e2d805`. The local macOS arm64 batch used isolated Rust 1.99.1 and passed all 20 commands: default/sqlite/transport/all-feature all-target tests, all-feature doctests, seven slice labs, generic record-server lab, artifact contract/transfer, wire-contract export, CI classifier tests, formatting, Clippy with warnings denied and warnings-denied Rustdoc.

The four feature-matrix test commands passed 54 default, 101 SQLite, 58 transport and 115 all-feature cases; all-feature doctests passed four. These counts overlap across configurations; no failures or ignores were observed. See [exact commands and outcomes](../../verification/state-models/execution.json).

Selected finite models run through the canonical Nessa Docs pinned TLC checker. They execute mathematical transition rules, not Rust functions or SQLite. Independent adversarial review and a semantic negative control for each selected rule are required. Six baseline models passed all 12 selected invariants across 40 aggregate states (largest 14); all 12 controls broke their intended rule. See [canonical receipts](../../verification/state-models/results.json) and [control definitions](../../verification/state-models/controls.json). Independent reader/source/model review closed after correcting two diagram ambiguities: history deletion applies beyond partial loading, and lost artifact publication replies require reloading the actual state, including full-but-unverified bytes. See the [independent review](../../verification/state-models/review.md).

Atlas browser smoke passed all 15 pages and five diagrams with zero errors. Changed titles/history/transfer views were checked again; the final transfer diagram also passed separately. See the [full browser receipt](../../verification/state-models/browser-results.json) and [final selected receipt](../../verification/state-models/browser-selected-results.json).

```sh
NESSA_DOCS_ROOT=/absolute/path/to/nessa-docs node scripts/state-docs/verify.mjs --write
```

The runner validates the atlas and runs each unmodified baseline before mutations. A parser error, timeout or unrelated invariant failure cannot count as the expected control. Terminal deadlock checks are disabled for these finite safety scenarios; no eventual progress is proved.

No fresh weak-link profile benchmark, production remote exposure, mobile integration, physical device power loss, backup consistency or human browser usability study is claimed. Rust 1.85 MSRV and Linux execution are configured CI responsibilities; this local run used 1.99.1 on macOS. Browser atlas smoke, when recorded, tests these docs rather than the example host UI.
