# Host-composed TLS for serialized record sessions (#54)

The optional `tls` feature layers Rustls TLS 1.3 over the existing connected
`TcpSessionIo`, without a second reader or a new application-session owner.
Use `TlsSessionIo::connect` or `accept`, then pass the returned stream to the
existing serialized record client/server. It has no split/Follow implementation
and changes no Head, Page, or Swift wire framing.

The host chooses the socket route, local identity, trust roots, exact peer leaf,
and expected server DNS/IP name. Rustls verifies certification chains and the
handshake proof of possession. Both endpoints require peer certificates.
The client adds its exact server-leaf restriction to the built-in server
certificate verification stage, before Rustls sends the client identity.
The server independently checks the authorized client leaf before dispatch.
Both check the fixed session ALPN before exposing an application stream. The expected endpoint is unchanged through a
relay that only copies bytes. This is not a peer discovery or enrollment
mechanism. Do not authorize a peer based on its relaying host or a scope name.

TLS 1.3 only; no plaintext fallback, opt-out verifier, resumption, or early
application data. No endpoint hands off to its application before finishing its own admission.
The client completing locally does not certify the server has admitted it or
granted a scope. Server handshake refusal cannot dispatch a policy query or
acquire a record source. The authenticated host still supplies the current scope
authorizer for this endpoint; a TLS certificate alone grants no record access.
The host manages key storage/rotation, trust/revocation decisions, and network
deployment. The appendix tests use ephemeral fixture identities, never user keys.

The host sets finite socket read and write timeouts before handing over the
connected socket; do not mutate those options through another socket handle.
An absolute handshake deadline caps each handshake socket operation, and the
original post-admission options are restored. This is not an infinite blocking
handshake, an application RPC deadline, or a bound on operating system
scheduling. The same existing TCP interrupt permanently fences TLS reads and
writes, including buffered reads, and shuts down the physical socket without
taking the TLS/RPC locks. Use the existing explicit record-session close and
retain the TCP interrupt externally if setup itself must be cancelled. On
admission failure the adapter requests physical interruption and returns an
error; there is no fallback.

## Runnable qualification

```sh
cargo test --locked --features sqlite,tls --test session_tls -- --nocapture
```

Requires the OpenSSL CLI only to create temporary fixture certificates. Runtime
TLS uses Rustls/WebPKI, not that CLI or custom cryptography. The matched direct
and opaque-relay tests exercise the same existing Head/Page application work
and verify SQLite after reopening. Application exchange counters omit TLS
handshake and record overhead; the opaque relay can count raw transported TLS
bytes separately. No deployed network, production authorization/identity
lifecycle, shaped-link latency, graceful close-notify, relayed availability,
or HTTP/TLS endpoint behavior is qualified. Issue #54 remains open.
