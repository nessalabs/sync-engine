# Native v2 reset composition (#49)

Explicit `start_catalogue_recovery_with_horizon` and
`step_catalogue_recovery_with_horizon` compose v2 metadata with the existing native
same-scope journal. The same Enumerate/Reconcile/Finish transition engine owns
candidate floors, complete-page staging, generation comparisons and progress.
There is no second checkpoint, timer, reset scheduler or metadata registry.

Incremental admission still compares durable C. A full reset intentionally keeps
C=0 while rebuilding, so its source reads instead compare captured boundary H
against the horizon, with equality accepted. The source checks that request kind,
zero C, reader support and H coverage in its coherent snapshot before acquiring
descriptors/payload. A reset whose H is overtaken refuses; it does not reinterpret
H as a durable completed receipt. One pure comparison-point helper selects C or H
for both source admission and validation of metadata-bearing refusals.

Every v2 response/refusal validates exact scope, version, support, request/resource
and revision coverage. A valid higher horizon is authorized and observed durably
before returning expiry, including a refused manifest or value. A delayed lower
publication is rejected after reopen. An expiry that compares a reset's C rather
than H is an invalid response, not permission to discard its journal. The adapter
extracts the already validated nested SourceHead only to reuse the shared phase
engine; it never makes a legacy request or synthesizes zero metadata.

| Transition | Existing native owner and atomic checks |
|---|---|
| Fresh Start (`None`) | CAS actual parent absence; require no orphan cache/candidates; insert first parent and generation-bound journal in one transaction; never first commit an ordinary zero checkpoint |
| Existing/superseding Start (`Some(expected)`) | Full exact progress CAS; preserve pending candidates and permanent fences, merge prior live creation/revision floors, then erase live payloads and advance generation atomically |
| Enumerate | Complete owned current-value page; candidate pruning, cache/fences and cursor remain one commit |
| Reconcile | Only owned value/deletion evidence classifies candidates; missing/unavailable is not absence proof; a bounded batch stages completely before effects |
| Finish, unpublished | Empty worklist required; publish captured H under the existing recovery fence |
| Finish, published | Fresh v2 head and current authorization after H is durable; only then release the fence and return the full v2 publication |

Every recovery effect rechecks the durable observed horizon inside its SQLite
transaction. A rebuilding H below that observation returns ResetRequired without
changing journal, cache or cursor. Published Finish compares actual durable C and
returns typed expiry before release. A competing metadata observation can therefore
refuse already staged work. Fresh start also checks H before creating a parent.

If the final fresh head overtakes H with a higher horizon, the accepted metadata
observation persists and Finish stays fenced. The host explicitly reloads and may
start a new generation from the current source head; pending floors and fences
survive that supersession. No automatic replay/reset is added. Uncertain commits
return immediately; explicit reload discovers either unchanged or committed state.
A transient classification failure retains pending work, without promising that
old live payloads remain available after reset Start erased them.

Authority remains full-scope and current before every source operation, after
metadata/value acquisition and before durable effects. Even an empty candidate
read is followed by a current authorization check before its phase advance. Binding, metadata and a
previous successful call grant no later read. New or foreign scopes still require
the existing explicit scope/reset contract; this API does not infer generic absence
or take over another incarnation. Unknown writer-only requirements allow read-only
recovery; unsupported readers or versions stop before effects.

## Compatibility and activation boundary

No file schema changes are needed: source v4, receiver v3 and the existing reset
candidate journal already own every durable field used here. Existing zero-floor
legacy reset APIs retain their behavior. Positive native source horizons continue
to refuse legacy head/manifest/value routes before data; v2 never falls back.
Hosts must explicitly select the v2 recovery API. This native composition adds no wire operation by itself; the separate
[v2 development route](catalogue-horizon-wire.md) now qualifies network reset.
Old binary/host upgrade fencing remains necessary.

The [native collection boundary](catalogue-collection.md) now atomically advances
coverage and hides strict-below retired markers while retaining permanent source
identity and receiver fences. Its real-activation process lab and collection-aware
finite model extend this earlier fixture qualification. Catalogue pins (#48) and
the #54 secure production catalogue dialect remain separate. The explicit
development v2 route is qualified independently. Issue #49 remains open.

## Reproduction and measured costs

```sh
cargo test --locked --all-features --test catalogue_horizon_recovery
python3 scripts/verify-catalogue-horizon-reset.py --repetitions 3
python3 scripts/verify-catalogue-horizon-reset.py --profile legacy-zero --repetitions 3
python3 scripts/verify-catalogue-horizon-reset.py --profile legacy-positive --repetitions 3
```

The process lab starts from 620 cached live IDs, deletes 160 and captures H=780.
A positive fixture hides 159 markers with revision below 780 and retains the equal
marker. Every start/step is a fresh process; all 620 reopened current rows, 160
fences and exact completion must match. The zero-floor comparison serves all
markers and is matched before/after on the same workload. Before v2 composition,
legacy reset refuses the positive fixture and preserves its cache/progress.

Host-only counters distinguish source reads, candidate reads, journal effects and
passive observations. These are port calls, not network exchanges or disk fsyncs.
Work timing excludes open; process timing includes open/effect/report. Preparation
and subsequent inspection are excluded. Timings are noisy development evidence,
not CI thresholds or performance improvement claims. Existing direct/opaque-relay
loopback measurements separately qualify unchanged zero-floor wire behavior.
The existing finite reset model is rerun for shared journal/fence safety; it does
not claim to model the new horizon publication/collection policy.
