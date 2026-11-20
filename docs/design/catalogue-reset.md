# Native same-scope catalogue reset recovery (#49)

This opt-in path recovers the existing native SQLite catalogue under the exact
same scope/incarnation/schema/epoch. It does not add a transport dialect, horizon
policy, collection API or foreign/unknown absence semantics by itself; the
subsequent [native collection boundary](catalogue-collection.md) composes with
explicit v2 recovery. The source still
retains permanent identity/deletion rows. Missing rows and unavailable ownership
are errors, never deletion proof. The host explicitly authorizes each bounded
step and every payload read; the application checks authority again before
publishing acquired values.

One existing catalogue progress row owns scope, generation, captured boundary,
phase and completion. A temporary candidate worklist holds only prior live IDs,
creation keys and revision floors, with a foreign key to that progress owner.
There is no positive seen-ID store, timer, retry loop or second checkpoint owner.

| Durable phase | Atomic operation | Completion |
|---|---|---|
| Start | Compare expected exact progress; increment generation; retain pending candidates from a superseded same-scope reset and merge prior live floors; erase live payloads; retain permanent fences; capture H | 0 |
| Enumerate | Admit coherent source metadata and stage a complete bounded manifest/value page; validate candidate floors and cache revisions; commit values, removal of manifest-confirmed candidates and cursor together | 0, including final manifest page |
| Reconcile | Resolve remaining prior IDs using current owned live/deletion evidence; stage a whole bounded batch; commit values/fences and candidate removal together | 0 |
| Finish, unpublished | Require empty candidates; publish captured H and retain the recovery fence | H |
| Finish, published | Fresh authorization/head/capability check after the durable H commit; compare exact state and release recovery | H |

An empty catalogue can enter Reconcile directly. The worklist is finite; ordinary
source churn does not restart the pass. Values can be newer than H, but do not
raise completion beyond H. The final fresh head is returned to the host: if it is
higher, a normal next pass is needed. It is not a promise to remain synchronized
with future source mutations. The normal begin/progress/page and same-scope reset paths refuse active recovery
so they cannot accidentally treat a final manifest as completion. The existing
currently authorized scope-changing reset can cancel recovery: full old-progress
CAS and generation advance, live-value erasure, candidate/phase cleanup and
retained deletion fences commit together. It performs no absence classification
under the old grant, and late old-scope receipts are stale.
Local cached reads can display the progressively rebuilt cache.

Pure recovery planners compute exact receipts; the application correlates returned
receipts, while the native store performs coherent reads/CAS and persistence.
Recovery page plans require resolved values, not unchanged shortcuts. All values
use the existing cache revision/deletion-fence writer. A stale generation, phase,
cursor or revision floor refuses the whole transaction. A superseding reset keeps
unclassified IDs and captures newly rebuilt live rows before clearing payloads.
No old reply can erase pending work in a newer generation.

A definitely failed write changes neither cache nor worklist/progress. Uncertain
results return immediately; the host must explicitly reload durable recovery.
Reopen finds either the old state or the committed effects and remaining work.
No automatic replay, new retry scheduler or synthesized successful timestamp is
introduced. There is no transaction across source/network round trips.

## Storage and upgrade boundary

The native catalogue receiver alone migrates v1 to v2 in an immediate transaction,
adding phase/boundary fields and the temporary candidate table under existing
progress. Fence/progress and phase/progress reloads each use one short coherent
read transaction. Deterministic inter-query fixtures commit a competing WAL reset
to verify snapshot consistency. Populated cache/progress are preserved; malformed/future/partial layouts
are refused and failed migration rolls back. New columns do not change normal
progress DTOs or the generic record/receiver formats. Source files stay v3 and
SourceHead/wire versions and optional masks remain unchanged. Hosts must stop old
binaries before upgrade; already-open binaries are not fenced by a file version.

## Reproduction and evidence limits

`python3 scripts/verify-catalogue-reset.py --repetitions 2` runs the actual native
application/source/store path. An owned fixture first caches 620 live entries,
then deletes 160 and hides those serving markers through fixture-only SQL. That
is not an exposed collection procedure. Every start/step is a new process;
exact final rows include 160 permanent deletion fences, completed H=780, no active
pass/recovery or candidates. The script records native port calls, compact
manifest bytes and opaque payload bytes, not RPCs or wire bytes. Process time
includes open/configure; separately reported work time excludes them. Preparation
is excluded. Timings have no CI threshold or production latency claim.

A frozen local all-candidate implementation supplies the measured baseline.
Reproduce the unpruned baseline with the same harness using `--root CHECKOUT`
and `--expected-resolve-reads 1080`; final runs gate the expected 620 resolutions.
The final implementation removes a candidate only in the same transaction as
its confirmed manifest value and cursor, reducing repeated payload resolution
without another durable owner. No unseen candidate is inferred absent.

The finite `CatalogueReset.tla` model uses two IDs, three generation advances, one queued
reply, one crash, deletion/marker omission, authority epoch change/authorized cancellation and atomic
page/batch commits. It checks typed states, stale-result rejection, no early
completion, delivered deletion proof, no resurrection, deletion coverage by H
post-commit fresh checking and cleared inactive journals. Both all-candidate/pruned models run with pinned
official TLC; semantic controls remove generation checking, complete on the final
manifest, treat an unavailable response as deletion, or retain an old-scope candidate
journal after cancellation. Run:

```
python3 scripts/verify-catalogue-reset-model.py --jar /path/to/tla2tools.jar
```

The runner verifies the SHA-256 of the official v1.7.4 release before execution;
CI fetches that vendor artifact. The model abstracts payload revisions/order,
actual SQLite atomicity, grant-owner implementation and arbitrary power loss.
Those are separate named test/host contracts. It proves no horizon policy,
collection eligibility, generic absence or transport behavior. Wider #49 horizon
admission and delayed-head protections and #54 production transport remain open.

The [reviewed receipt](../verification/catalogue-reset.json) contains source-bound
matched recovery measurements, exact repository commands, finite-model outputs,
ordinary reversion evidence and independent review. Actual measured costs and
limits are also included directly in the pull request description.
