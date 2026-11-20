# Native catalogue coverage admission (#49 prerequisite)

The explicit `CatalogueHorizonHead` v2 envelope contains a coherent SourceHead v1
and a deletion-coverage horizon. It is a new native port contract, not a
reinterpretation of existing SourceHead or catalogue wire v1. Unknown versions,
reader requirements and exact-scope mismatches refuse without fallback. Unknown
writer-only requirements do not block read-only admission.

Incremental admission compares durable completed **C** with the horizon: C equal
to the horizon is valid; C below it expires. An active pass's boundary H or cursor
cannot substitute for C. Initial and resumed starts always obtain fresh v2
metadata. Every manifest and current-value read validates coverage and reader
support inside the same short source snapshot **before** descriptor/payload
acquisition. Values may be newer than H but must be covered by their own head.
Owned results release the transaction before any host or network work.

An expiry refusal includes its coherent v2 publication and requested C. The
application validates version, scope, capabilities, C and boundary, checks current
authority, and commits the passive horizon observation before returning expiry.
Thus manifest/value refusal cannot forget a learned higher horizon. Successful
responses check their exact request/resource and revision coverage before
observation; the existing shared page loop stages the entire page, reauthorizes,
and commits through the existing progress owner. A final page is not a head
recheck: the host must call fresh admission after each finite pass, as the lab does.

Receiver observations are keyed by all six Scope components, including epoch and
incarnation. They persist independently of the existing progress row, never create
one and never advance completion. A lower delayed horizon is a typed refusal,
including after reopen. Equal observations are allowed. SQLite checks observed
coverage again inside `begin` and `apply_page` transactions, using actual C before
cache/cursor/completion effects. A competing handle's accepted higher observation
can therefore refuse a previously staged page. `CatalogueHorizonStore` requires
that atomic check from every implementation, not merely SQLite.

“Refuse before effects” here means before projection, cursor, boundary or completed
progress changes. A validated higher passive observation deliberately survives a
refusal. It grants no access and is not progress. Authority checks remain before
source reads, after acquired metadata and before the existing whole-page commit.
A refused or uncertain operation does not replay itself; hosts explicitly reload.

## Storage and disabled activation

The source's existing singleton metadata row owns horizon alongside head/masks;
source file v4 adds one integer with `0 <= horizon <= head`. Head and horizon are
read in one snapshot. Existing writers preserve it. Receiver file v3 adds one
exact-scope passive observation table under `SqliteCatalogueStore`; it is not a
second checkpoint, seen-ID registry or scheduler. Both migrations are atomic and
retain populated identity, values, permanent deletion fences, cursors and reset
journals. Recognized old files initialize horizon to zero; malformed/future rows
are not converted to zero. Stop old binaries before upgrading: reopening refuses
unknown file versions, but that does not fence a previously open old writer.

**Positive publication and collection remain disabled.** There is no production
horizon setter and no collector. Owned test/lab fixtures alone inject positive
floors. This prerequisite does not promise native recovery after positive
activation: the existing reset application still uses v1 ports, which correctly
fail closed at a positive floor. The explicit FullReset source read kind requires
C=0, but is only a read primitive; it does not authorize or complete a reset.
Before exposing advancement/collection, the existing generation-bound
Enumerate/Reconcile/Finish journal must consume v2 reset reads, including its
fresh finish check. The source owner must then atomically select a monotone safe
floor and marker removals. No parallel marker/identity owner is introduced here.

Legacy native head, manifest and current-value operations fail closed for positive
fixtures, even if their C happens to be high enough: their request contract cannot
prove v2 coverage. Existing loopback dialects map this to their established
terminal invalid-request refusal and never downgrade or emit descriptors/payload.
At horizon zero the existing wire layout and exchange counts stay unchanged.
There is no successful v2 network operation in this slice.

## Reproduction and remaining work

```sh
cargo test --locked --all-features --test catalogue_horizon
python3 scripts/verify-catalogue-horizon.py --repetitions 3
python3 scripts/measure-catalogue-publication.py --repetitions 3 --native-repetitions 3
python3 scripts/measure-catalogue-publication.py --client-dialect capabilities --repetitions 3 --native-repetitions 3
```

The native lab starts a fresh process for each operation. A 620-entry bootstrap,
160 deletions, interrupted 64-entry page, resumed equality/expiry and reopened
cache are checked against exact IDs, payloads and durable progress. Native source
calls/logical bytes are distinct from framed loopback exchanges/total wire bytes.
Debug timings have no CI threshold. Wire measurement uses the existing development
plaintext loopback route, direct plus counted hop or with an additional opaque
relay, with no session/secure transport/weak-link claim.

Remaining #49 acceptance includes compatible reset composition, monotone source
publication/atomic marker collection, versioned transport coverage and typed expiry,
full cross-boundary delayed-result/restart proof and collection measurements. The
existing finite reset model remains a reset-only proof; it makes no horizon or
collection claim. #48 pin integration, #51 encoding and #54 production transport
remain separate work. No savings or full-issue completion is claimed.
