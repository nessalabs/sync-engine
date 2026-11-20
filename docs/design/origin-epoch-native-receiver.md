# Opt-in native record origin-epoch receiver

This is one standalone reference adapter for [#50](https://github.com/nessalabs/sync-engine/issues/50).
It is separate from Nessa product integration, future Swift wire work, and
gateway restore. See the [local claim ledger](origin-epoch-authority.md) and
[finite receiver model](origin-epoch-receiver.md).

A host provisions a new SQLite file with an independently trusted
`(receiver, origin, stable authority key)` binding. An application ID
separates it from normal replica files: the ordinary replica opener refuses
this file, the protected opener refuses legacy files, and neither migration
nor a protected-to-legacy conversion is supplied. Reopening requires the exact
binding. No wrapper exposes the inner store, unqualified apply, reset, tail or
older-history mutation. Legacy stores elsewhere remain unprotected.

Composition installs one `NativeEpochAdmission` implementation. Its inputs
must be actual host-defined operation evidence, not bare epoch numbers or a
plan stamped from a separate response. That trusted port authenticates the
source/authority binding, checks exact current scope, and correlates the head
or entire validated record plan. The reference adapter does not implement
cryptographic identity, remote admission, or access revocation. The port
must enforce the host's current access/operation rules at its admission point.
The separate receipt types do not imply that a native or remote source already
implements this port.

| Event | Durable effect |
| --- | --- |
| Admitted head | Check binding, exact result scope and existing head compatibility. Persist only a higher observed writer epoch. Do not record the source position as receiver progress. |
| Admitted plan | In the existing SQLite mutation boundary, check the exact result scope and the durable floor **before** ordinary scope/CAS, replay or writes. Accept only the currently observed epoch; distinguish a stale one from a not-yet-observed one. |
| Reopen or uncertain result | Reopen the same protected store and reload its observed floor and scope-specific checkpoint. Do not accept a retry as evidence of an earlier commit or rewrite old progress. |
| Access change | A new exact authorized grant can be observed without erasing the per-origin authority floor. The existing record checkpoint still refuses implicit grant change or reset. |

Both record modes use one shared transaction body for checkpoint, record
identity, records, and idempotent replay. The protected store adds only an
immutable binding and monotone epoch floor alongside that owner. It does not
create a separate record store or a remote checkpoint owner.

Run the real local fixture:

```sh
cargo test --locked --features sqlite --test epoch_replication
```

Two source handles over the same record history share one durable claim ledger. The first captures a real
core-validated native plan and commits position 1. A second claim changes epoch
from 1 to 2 without altering saved progress. After the receiver observes 2, a
competing and then reopened receiver handle rejects the old result, even for
a duplicate. Position stays at 1 with the original row; a matching epoch-2
plan advances to 2. The fixture's injected source helper checks its local
claim before capturing results; no production source serving guard is added.
The tests also cover fresh files, wrong binding, current grant changes, and
no ordinary opener bypass through the public API.

There is no epoch wire encoding, protected history/catalogue transport,
cross-host authority, or cryptographic claim binding here. Existing source
writers and transports do not become fenced automatically. The host-owned
authority ledger is separate and must survive any source restore if used for
that purpose. The finite model and local file restart evidence do not
establish power-loss behavior or actual mobile deployment.
