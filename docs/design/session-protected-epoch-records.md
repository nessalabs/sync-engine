# Protected epoch record sessions: Head, Page, Tail and Older

This standalone slice uses the [verified TLS channel](session-secure-tls.md)
and the [native claim-qualified reader](origin-epoch-native-forward.md) for
[#50](https://github.com/nessalabs/sync-engine/issues/50). It does not change
ordinary TLS, legacy records, SourceHead v1, Swift, or the existing epoch
receiver file.

## Host authority and wire mode

The trusted standalone host maintains the claim ledger outside source stores
and their backups. Its independently provisioned deployment configuration
associates one exact source scope and stable authority key with a confirmed
ledger epoch and an exact writer TLS leaf. A locator, peer message, ambient
TLS identity, or restored source is never such a binding. During handoff,
deploy the distinct verified leaf with the newer confirmed claim. Mismatched
or unavailable binding/ledger state refuses; a newly larger value from the
network does not replace the host configuration. Existing writer issuance
and storage remain host responsibilities, not a new credential authority
inside this adapter. The fixture has one trusted local controller; this is
not a remote claim service or product pairing.

The connector constructs the same TLS 1.3 verification/pinning with the
binding's own leaf, then requires one protected application mode:
`NSEPOCH1` → `NSEACK01`. This occurs after TLS admission and before the existing
serialized Hello/Bind controls; there is no retry or fallback. Protected
Bind uses the guarded native source, but its head hint remains advisory and
cannot observe a writer epoch. Only an exact stamped protected Head can
enter the receiver's observed floor.

The mode reuses existing Head/Page/Tail/Older capability operation kinds, fragment
bounds, canonical request/scope codecs and serialized correlation. Each
successful protected result is prefixed by its captured authority ID and
eight-byte writer epoch. The server checks current scope policy before the
native capture and again before transmitting. The existing short authority
guard spans a bounded local source read, never the transport send. A request
already arrived at the old server is refused if the writer was replaced
before its capture. A completed old capture may still arrive later.

The client compares the exact host scope/claim only after authenticated TLS
and the required mode. It checks its independent ledger before use and after
a correlated response, verifies the full scope/operation/limits and claim
echo, then calls the existing canonical page or history validator. Returned proof is
opaque; decoding bytes is not native-capture evidence. The host's
`EpochSessionAdmission` rechecks current scope immediately before the existing
SQLite observation/apply. Any old page delayed between verified acquisition
and apply is still checked against the durable observed epoch inside the
record transaction. A stale page cannot become current just because the
transport closed or the receiver reopened.

Tail and Older are separate typed reads and cannot advance the observed writer
epoch. Requests must match the exact echoed scope, history generation, Older
boundary, and payload limits. The client checks caller limits before sending
either operation, in addition to negotiated physical bounds and qualified
response validation. The receiver uses its existing protected
history admission and SQLite installation transaction. An unobserved newer
claim fails before install; a delayed older claim fails before either
installation or any idempotent path. History reads count payload without
advancing the forward duplicate-counter high water. A build that predates
protected history may not implement these operations: the session fails
rather than retrying an ordinary path.

## Qualification boundary

```sh
cargo test --locked --features tls,sqlite --test session_tls epochs -- --nocapture
```

One real direct TCP/TLS fixture uses two independently pinned writer leaves,
two guarded source handles, the same reference source history, one
outside-source claim ledger, and one protected SQLite receiver. The old
connection completed seven exchanges, including a typed refusal after a
deterministically parked request, with 1,170 application protocol bytes and
6 payload bytes. The new connection completed five exchanges with 718
protocol bytes and 3 payload bytes. These are different fixture phases,
not a matched performance comparison; TLS bytes and elapsed time are not
included. The staged epoch-1 page leaves the reopened checkpoint at 1;
the approved epoch-2 page advances it to 2 with exact payloads. Independent
negative fixtures cover wrong TLS leaf, a valid TLS peer without the protected
mode, a valid protected peer with mismatched stable key, and local ledger
disagreement.

A second direct TCP/TLS + SQLite fixture installs an epoch-1 Tail, holds
verified Tail and Older results, and parks an ordinary Older request across
the claim change. That request is refused by the old source; applying the
already verified results after epoch-2 observation and reopen leaves the
history generation, coverage, checkpoint and payload unchanged. An epoch-2
Tail and Older apply through the same owner and reopen with coverage
`[3,4]` at live position 4. Acquiring epoch-2 Tail before observing epoch-2
Head does not authorize its installation.

No raw or generic protected transport constructor, legacy fallback,
catalogue/artifact traffic, secure relay
qualification for this mode, source writes, separate-host restore, Nessa
or Swift integration is introduced.
