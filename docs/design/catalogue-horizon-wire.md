# Explicit catalogue coverage operations (#49)

Implementation footprint: one new loopback codec/adapter module, three dispatch/exports,
shared v1 metadata helper visibility only, one owned process client example,
ordinary codec/TCP contracts and a matched process/relay measurement runner.
The native coverage/reset/store ports, SQLite publication/collection transaction,
source metadata owner and v1 successful bytes remain unchanged. Catalogue pins
can use the collection seam independently; no wire or source mutation is needed
in that lane. #54 secure production catalogue session work stays separate.

## Request and response ownership

Three new operation kinds 20/21/22 select head, manifest and current-value reads.
The request prefix is explicit u16 catalogue version 2 and u64 reader support;
SourceHead remains its own nested v1 metadata. Head carries exact Scope. Manifest
and value carry a read-kind byte (0 incremental,1 explicit full reset), canonical
pass (including exact scope, C, H, cursor, generation), then the existing bounded
entry count or ID/byte budget. Unknown versions/kinds never select legacy behavior.

Every correlated response echoes operation/version/support and the full resource
request, including read kind. Success carries the coherent nested SourceHead and
horizon before descriptors/payload. Client validation checks exact echoes,
versions, support, h<=head, head>=H and C>=h (or reset H>=h with C=0) before decoding
data. Current values may be newer than H but must be covered by their own returned
head; owned deletion proof has empty payload. No metadata implies a grant or apply.

Status 11 applies only to manifest/value calls (head has no coverage point).
It is typed HorizonExpired: the complete request echo plus owned v2 head,
no descriptors/payload. The echoed C and actual read-kind comparison point must
prove expiry. Status 10 carries an unsupported catalogue contract version;
status 8 carries unsupported SourceHead version or nonzero missing-reader bits.
Normal known refusals retain existing meaning with full request correlation;
Unavailable remains retryable. Unknown statuses and malformed/trailing details
are fatal InvalidRequest. Existing pre-source access refusals may
remain bare denial/identity codes and reveal no coverage details. No reply can
cause automatic reset, replay, downgrade or legacy probe.

The server delegates to native owned horizon snapshots after current existing
route authorization and checks route access again before write. Transactions and
source handles end before network I/O. The same 1 MiB frame,128-entry manifest and
512 KiB value budget apply. No scheduler, queue, timer, grant cache, checkpoint,
new connection mode or public network exposure is introduced.

## Receiver and qualification

The opt-in HorizonCatalogueClient implements only CatalogueHorizonSource. Hosts
use existing fresh admission on cold/resumed passes, whole-page staging and the
same SQLite observation/progress owner. C==h is accepted; C<h refuses before page
or apply. Mid-pass expiry can persist a validated passive higher observation but
must preserve cached values,cursor,boundary,completion and permanent fences.
Delayed lower observations remain rejected through reopen. Reset is an explicit
currently authorized host action through the existing generation-bound journal,
including candidate floors, owned per-ID reconciliation and fresh Finish recheck.

The lab uses real process TCP direct and opaque relay paths, 620 records
and exact reopened comparisons. It compares zero-floor v1/v2 exchanges and bytes,
partial/restarted resume, churn/rechecks, actual native activation, equality and
expired active C, authorized reset after actual collection and no resurrection.
It exercises old client/new server refusal and new client/old server failure without
fallback; old servers may simply close unknown-operation connections. Record
successful and refused exchanges, descriptor/opaque/protocol bytes, repeated timing
ranges and real limits. Fixture plaintext tokens are not production authority,
encryption, production relay or secure-session qualification.

Issue #49 remains open until the remaining acceptance is independently verified.

Run `python3 scripts/verify-catalogue-horizon-wire.py`. It builds an actual pinned
old repository peer from ancestor `8596fd3` (fetch that object first, as CI does),
or accepts `--baseline-root` for a clean checkout at that exact SHA. The default
mode builds the old peer in a temporary owned archive; no downloaded test server
or simulated old protocol stands in for it. Direct and opaque-relay runs compare
identical zero-floor v1/v2 fixtures, interruption and explicit retry, native API
activation, warm/cold authorized reset and a genuine delayed head snapshot while
another process persists a newer observation. All normal reset source semantics
remain owned by the existing native implementation.

A warm cache retains all 620 known identity rows and 160 fences after reset. A fresh
cache after collection obtains 460 live rows plus the one equality marker; it does
not invent 159 unknown fences from omission. Source permanent IDs remain 620, and
an ordinary later upsert of a retired ID remains Fenced. MemoryAuthorizer is an always-allowed owned host fixture; token tests establish
server-route refusal rather than product local-cache authorization. The plaintext
fixture route has immutable process configuration; it does not qualify dynamic product
revocation or #54 secure catalogue transport.

The [measurement receipt](catalogue-horizon-wire-measurement.md) contains actual
paired v1/v2 request counts, complete/unanswered replies, framed bytes and repeated
direct/opaque-relay timings, plus exact reopened correctness outcomes. Existing
v1 frames remain stable; explicit v2 metadata adds bounded bytes without reducing
RPC count. Native/source-schema and pin mutation ownership remain unchanged.

## Native v5 retention composition

The current source file is v5 from the separately merged pin owner. Baseline
producer/server remain at their actual v4 ancestor in a different owned file;
original ID/revision/payload rows are compared before protocol measurements.
No file-version field is reinterpreted as the catalogue wire version.

The coverage fixture registers an actual active C620/H780 receiver through the
existing host-authorized native pin API. Unforced preferred780 collection stays
at620 while that pin is live; direct compare_collect refuses the registered
ledger. The controlled advance uses collect_with_retention at exact expiry10,
then proves the manual path still refuses after expiry. It never deletes pin
rows, rewinds source time or adds a network mutation/registration endpoint.
Native pinned selection/publication remain one existing writer transaction.
