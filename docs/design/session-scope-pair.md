# Two bounded scope admissions on a serialized session (#54)

This first multi-scope reference admits two distinct receiver/stream scopes
during setup and reads ordinary authorized heads on the same persistent channel.
It does not add batched heads, mux scheduling, transport-level authentication,
record/history reads on the pair endpoint, or multi-scope hint semantics.

`ScopePairHello` is explicit NSC1 control kind 8, with the existing limits
fields and required binding count two. The caller already knows the service
supports it. Neither the client nor server silently uses single-scope mode.
The current Head v1 request and reply encodings and statuses do not change.
Existing single-scope and follow endpoints do not admit this opening.

| State or event | Decision |
| --- | --- |
| Explicit pair offer | Select binding count two; no scope grant. |
| Bind 1 succeeds | Retain slot (handle 1, generation 1) for exact primary scope. |
| Bind 1 refused | No session returned. |
| Bind 2 succeeds | Retain slot (2, 2) for exact distinct secondary scope. |
| Bind 2 receives correlated typed refusal | No secondary slot; keep primary. No retry on this channel. |
| Fresh Head v1 on either bound scope | Check current policy before opening its source, reading, and replying. Return metadata correlated to that binding and request. |
| Temporary scope refusal | Return the typed result; the other binding remains usable. |
| Peer reports identity changed for one bound scope | The client tombstones only that identity. Subsequent reads of it fail locally; other binding remains usable. |
| Wrong scope, handle, generation or request | No default routing. Local scope miss fails before wire access; malformed wire correlation closes the channel. |
| Explicit close / terminal framing or I/O failure | Fence both scopes and interrupt the channel. If the interrupt fails, return its error so the host can retry close or dispose. No durable state was altered. |

There is one shared serialized channel and `SessionLifecycle`. Both binds
finish before the first data RPC, keeping the lifecycle and wire request
counters consecutive without a second allocator. Passive Bound metadata is
not authority and is never the result of a head read. Bound/Refused control
and data response checks reuse the existing codec. Each native read view is
opened by exact selected scope, and is released before response I/O.

The host authenticates the supplied stream, supplies current policy for exact
scopes, and enforces finite deadlines. This does not qualify production secure
channels or relay identity. The persistent Follow dispatcher still owns only
one hint-binding and one wake slot. Extending it requires a separate explicit
scope-aware send admission and notification design; do not route secondary
scopes through that primary-only gate.

Run `cargo test --locked --all-features --test session_scope_pair` for real
direct loopback TCP, two native SQLite source files, distinct heads on one
connection, independent refusal and identity fencing, old-endpoint refusal,
the exact source-factory effects, and peer half-close ending both the live TCP
connection and the local scopes. No latency or scalability claim.
