# Dedicated pinned-TLS advisory Follow (#54)

This new standalone implementation is independent work on main `574b481`; it is
not a recovery of any earlier unpublished implementation. It supplies one opt-in,
one-scope hint-only connection and leaves #54 open. The host explicitly opens a
second, ordinary authenticated record session for reads and current authorization.
There is no generic duplex TLS splitter, new handshake, implicit fallback,
reconnection loop, record port on the hint connection, product rollout or key
management. The existing pinned mTLS 1.3 channel supplies authentication and
confidentiality; the host supplies exact pins, principal-bound current policy,
finite socket/callback deadlines and the socket route.

The hint connection sends existing FollowOpen/Welcome/Bound and NSR1 hint frames.
The client retains the selected scope, session, first request, handle and generation;
it rejects a malformed, unknown, wrong-correlated, duplicate or regressing hint,
partial frame and EOF, then terminally closes. A valid hint is advisory only: it
neither loads the store nor grants permission nor advances a checkpoint. A separate
existing record owner reauthorizes, reloads durable progress and validates fetched
records before atomic apply. Reconnection creates new owners and reads the actual
saved checkpoint. Hints may be lost; the host must schedule authorized fallback.

The server captures the existing bounded signal version before binding, preventing
an intervening commit from disappearing between the bound head and subscription.
For every wake it reuses the existing Follow egress owner: current policy before
source access, bounded head capture and source release, then current policy again
before writing a complete frame. A changed grant emits a metadata-free terminal
refusal. Stop and output admission share a short lock only for the decision: the
server checks the stop fence after the final policy callback and before admitting
a complete frame. The lock is never held over host callbacks or network I/O. A
frame already admitted before stop may complete, fail or be interrupted; stop does
not recall bytes already sent. Policy and source callbacks also check the stop
fence before admission; a callback already admitted may finish.

After confirming that the supplied stop token belongs to this exact socket, the
serving scope interrupts that socket on every return, including setup refusal,
malformed setup, lease expiry and terminal hint refusal. A mismatched token is
never interrupted. The primary serving error is preserved if interruption also
fails; the host retains the token and must retry cleanup. On a clean return an
interruption failure is reported. Unwinding requests best-effort interruption.

The host selects a nonzero finite `wait_lease`; it starts after binding and does
not reset on hints. Expiry ends the signal wait and closes this TLS channel, so a
silent peer cannot retain an idle server beyond that lease plus any in-flight
bounded host callback or socket I/O. This is not peer-EOF detection: the host must
still enforce finite handshake, socket and callback deadlines, stop on shutdown,
join its worker, and explicitly reconnect and fetch. The client has no internal
task and must retain the channel interrupt handle before connecting if another
task needs to interrupt a blocked `next_hint`. TLS idle first-byte timeouts retry;
partial-frame timeouts are terminal. The host chooses a fresh nonzero session ID
for each connection; this adapter checks nonzero, not global uniqueness. The
existing full duplex Follow endpoint and this hint-only endpoint require explicit
host routing. The source and receiver never treat a certificate or hint as a grant.

Run `python3 scripts/verify-secure-follow.py`. Its owned fixture uses ephemeral
pinned certificates, actual mTLS on direct loopback and through an opaque TCP relay,
separate SQLite record reads and reopened cache. Each of three matched single-record
fetches consumed 5 exchanges, 873 application protocol bytes and 3 payload bytes;
each dedicated Follow connection plus one hint consumed 1 control exchange, 239
application protocol bytes and no record payload. Direct and relayed counts matched.
The second connection incurs its own TLS handshake and ongoing socket resources;
those bytes, TCP/IP overhead and latency were not measured. These are fixture counts,
not a speedup or weak-link, production availability or security deployment claim.
[The measurement receipt](../verification/session-secure-follow-e2e.json) records
commands and source fingerprints. The normal tests also exercise pinned-peer refusal,
policy withdrawal after capture, wrong context, duplicate frames, EOF, retained-token
terminal cleanup, finite lease expiry, stop during final policy, and stop-handle identity.

[Controlled regression evidence](../verification/session-secure-follow-regressions.json)
records isolated removals of hint correlation, stale-head rejection, current-policy
egress recheck, exact stop-handle identity, stop wake and pre-binding signal
snapshot, and durable checkpoint loading. Every removal failed its targeted test;
removing stop wake left the server wait blocked until the test was stopped. The
original sources were restored and the final fixture passed afterward.

An additional binding-order regression stops the connection inside current-policy
authorization. Before the stop-fence fix, the source factory ran once afterward;
with the fix, no later source acquisition is admitted. Removing either the policy
or factory stop guard separately fails its corresponding callback-count assertion.

Terminal and admission qualification uses both direct and opaque-relay TLS peers.
The frame-admission unit test uses a deliberately no-op interrupt so physical socket
shutdown cannot mask a missing admission guard; it checks Bound and Hint separately.
A failed interrupt preserves the primary outcome and the retained token can retry.
