# Native receiver origin-epoch safety model

[Issue #50](https://github.com/nessalabs/sync-engine/issues/50) has a
[local claim ledger](origin-epoch-authority.md). This finite model exercises
the corresponding receiver ownership, not a new runtime adapter. It assumes
exactly one independently trusted, stable authority binding and does not
infer or persist a host origin-to-authority mapping.

| Event | Owner and required effect |
| --- | --- |
| Host binding and scope | A trusted host authenticates the writer, chooses the stable authority key, checks the exact `Scope`, and correlates the request and response. An access-epoch transition does not rename the writer or clear its highest durable observation. |
| Admitted observation | The receiver store raises the per-authority writer floor on a trusted, exactly correlated epoch observation. This changes neither cached records nor the checkpoint. Untrusted, lower or missing metadata cannot lower that floor or establish authority. |
| Commit | The existing record writer transaction checks the exact queued scope and expected checkpoint, and reads the current durable floor in that same transaction. Only a stamped epoch equal to the floor may apply. A positive higher stamp first needs a trusted observation; a lower or missing stamp cannot be treated as idempotent success. |
| Reopen or uncertainty | Read the durable floor/checkpoint from the same receiver store, not a process-local default. An in-flight result from another handle still passes the same commit checks. No silent scope reset. |

One key, three source epochs, two access grants for a single stream, one
delayed result, one reopen and at most two atomic record/checkpoint advances are
bounded in [OriginEpoch.tla](../state/tla/OriginEpoch.tla). It checks equality to
the writer floor at apply, the exact selected grant, preservation of the floor
across reopen, and record/checkpoint atomicity. An unstamped result is represented
separately. Witness runs confirm that a matching result can commit after reopen
and a higher writer epoch can continue a matching checkpoint. Separate negative
controls remove each modeled check. See the exact
[model-check receipt](../verification/origin-epoch.json).

The model represents committed facts and authorization assumptions abstractly.
It does not qualify SQLite migrations/transactions, transport frame encoding,
storage durability on power loss, distributed source claiming, or replacement
pairing. It makes no eventual-delivery claim. A receiver only fences an older
writer after learning and durably committing a higher epoch.

## Runtime seams, deliberately not implemented here

- `src/replication/application/follower.rs` currently accepts a `CommitPlan`
  without authenticated writer evidence. The host binding must accompany all
  protected results, and legacy unbound apply must not remain usable for the
  same protected history. Do not reinterpret `SourceHead` v1.
- `src/replication/infrastructure/sqlite/replica.rs` owns the existing atomic
  record and checkpoint apply. Share its transaction with the epoch floor
  check and keep the floor independent of a full-scope progress row. Tail and
  older-history installs also mutate this store; they cannot silently bypass
  a claimed receiver policy. Catalogue ownership is separate.
- A replacement gateway may use a new `ReplicaId`. The identity association
  and revocation must come from trusted host pairing or registration, with a
  trusted authority that survives backup restoration. Inferring the mapping
  from received frames would not satisfy the model's assumption.

Run with the trusted official
[TLA+ v1.7.4 JAR](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)
(pinned checksum checked; the runner makes no downloads):

```sh
python3 scripts/verify-origin-epoch-model.py --jar /path/to/tla2tools.jar
```
