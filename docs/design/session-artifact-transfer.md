# Bounded artifact ranges over the serialized secure session

Issue #54: explicit bounded transfer on the existing one-scope TLS record
session. The [artifact contract](artifact-contract.md) owns exact immutable
content identity and [the transfer application](artifact-transfer.md) owns
durable staging and publication. This transport adds no cache owner, install
path, retry loop, scheduler or flow control.

Hosts separately opt into the transfer endpoint and port. The older
[manifest-only operation](session-artifact-manifest.md) does not confer content
permission and the manifest-only client port does not implement `ChunkSource`.
The host authorizes `Manifest` and `Chunk` separately against the verified
client leaf fingerprint from the actual TLS accept operation and exact request
identity. No version probing, plaintext/legacy fallback, dynamic negotiation,
or change to ordinary control, record, Follow, pair or Swift frames.

## One bounded request

1. Bind and validate the existing correlated session and exact access scope.
   The new request kind names the artifact key, immutable revision, length and
   SHA-256, exact offset and nonzero maximum up to 64 KiB.
2. Check current scope and independent content permission before opening the
   artifact source and again before reading. The existing SQLite source reads
   the exact current version and range in one snapshot. Copy the bounded result
   and release source storage before network I/O.
3. Recheck both grants before each fragment or refusal. A policy denial is
   header-only and does not distinguish a present key from an absent one.
   An authorized stale revision or retained tombstone returns the corresponding
   typed result without revealing a newer revision or digest. Unavailable or
   missing bytes are not deletion.
4. The sole reader either reassembles one complete correlated response or
   returns a refusal. A successful body must echo the full exact request and
   byte count. On disconnect or revocation no partial chunk DTO reaches the
   application, so no partial append can move the offset. Already emitted
   bytes cannot be recalled; no atomicity with external policy is implied.
5. The host can explicitly reconnect and reload the persisted offset.
   `transfer_one` can append a fully validated chunk; `publish_if_current`
   refreshes the exact manifest and the existing cache verifies full length
   and hash before exposing bytes.

The host may give other work a turn between chunks. No operation overtakes an
in-flight chunk on this serialized session. An explicitly denied grant is not
a reason to resume automatically; cache invalidation remains host policy.

## Evidence

Run `cargo test --locked --all-features --test session_artifact_transfer -- --nocapture`
with OpenSSL CLI for disposable lab certificates. Both pinned mutual TLS
paths use real localhost TCP and SQLite: direct and through a byte-opaque
local relay. The test commits the first 8,192 B of an identical 16,475 B file,
gates the second read after its first frame flushed, interrupts, and reopens
the cache. Offset remains 8,192 with no visible bytes. A new TLS session
re-requests that exact range, commits a final 91 B, refreshes its manifest for
publication and reopens the verified artifact. Both resumed paths observe
four complete logical exchanges, 1,265 application protocol bytes and 8,283
complete content bytes, including the final short range and excluding
handshake bytes. In the relay path, raw ciphertext forwarded before cut is
measured independently of the partial, unclassified application wire bytes.
The range is requested twice; only one complete response advances the durable
offset. This adapter intentionally does not keep network replay history across
sessions.

Other fixtures verify metadata-only grant followed by identical known/absent
content denial without opening a range source, revision and deletion refusals
without stage changes, and revocation between two frames without a partial
commit. No physical LAN, deployed relay, throughput, urgent-work concurrency,
product authorization, or encrypted Follow claim. Issue #54 remains open.
