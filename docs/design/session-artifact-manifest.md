# Artifact manifest over the serialized secure session

Issue #54: one opt-in, read-only manifest operation. The
[artifact contract](artifact-contract.md) continues to own current revisions,
content identities, deletion and local availability; the
[transfer contract](artifact-transfer.md) continues to own durable staging
and publication. This route implements neither content transfer nor cache apply.

The client explicitly obtains a manifest port from an existing
`RecordSession<TlsSessionIo>`. The server explicitly enables it on
`TlsSessionIo::accept`. Hosts must know the peer supports this operation. An
endpoint without the route does not answer the new operation, and there is no
legacy or plaintext fallback. Open, Head, Page, control frames, Swift, scope
pairs and Follow do not change. The new private request kind and version use
the original one-scope binding, serialized driver and interruption owner.

## Authorization and absence

1. Check the exact current receiver/source scope, matching the retained binding.
2. Check the exact artifact key and independently authorize it using the
   verified TLS client leaf's DER SHA-256. This fingerprint comes from the
   completed accept-side handshake, never from request metadata.
3. Acquire the existing manifest source, recheck both grants before using it,
   and copy out one validated DTO. No source handle survives into reply I/O.
4. Recheck both grants before emitting the sole response frame, including a
   response of authorized absence. A success echoes the exact canonical key;
   the transport correlates kind, session, request, binding handle and generation.

A denial has no artifact payload; the lookup is not consulted for either known
or unknown denied keys. `Missing` is an authorized lookup result without a
revision, distinct from an authorized retained deletion. No enumeration, content
read, path resolution or implicit grant comes from a certificate fingerprint,
artifact ID, digest or record permission. The host owns the grants and cache
policy. Checks are not an atomic transaction with external host policy or
storage; there is no timing-obliviousness or rollback for an already completed
reply. Interrupting an in-flight RPC can leave no completed response, but
never changes any artifact stage because this adapter does not own one.

## Evidence

Run `cargo test --locked --all-features --test session_artifact_manifest -- --nocapture`
(with OpenSSL available for ephemeral test credentials). The test uses the same real
SQLite scope and four lookup keys on every path: nonempty live, empty live,
retained deletion and authorized missing. It compares one persistent mTLS TCP
session with a fresh mTLS TCP session per lookup, measured both directly and
through a byte-opaque local relay. Application exchange and byte counts include
Open/bind. The fixture observes no artifact content bytes. The relay counts
actual forwarded TLS bytes, including handshakes; certificates are regenerated
and raw byte counts can vary slightly.

| Matching four-read workload | TLS connections | Complete application exchanges | Application protocol bytes |
| --- | ---: | ---: | ---: |
| Persistent, direct or relay | 1 | 5 | 1,012 |
| Fresh per lookup, direct or relay | 4 | 8 | 1,561 |

The fixture also checks known/unknown denial without a source read, wrong
scope, a peer-bound current-policy change during source open and after live or
missing reads, terminal refusal on the disabled endpoint, ordinary Head/Page
on the same session, and a gated in-flight interrupt followed by a fresh
pinned TLS bind and successful lookup against the reopened SQLite source.
There is no proved chunk-resume, latency, wide-area, deployed relay, physical
network or product authorization claim. Issue #54 remains open.
