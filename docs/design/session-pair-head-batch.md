# Batched heads for the fixed scope pair (#54)

An explicitly matched host and peer can select `ScopePairBatchHello` (NSC1
kind 9) and request two heads at once on the existing serialized pair channel.
There are still exactly two setup-time bindings, one mutex and one request
allocator. This mode also supports ordinary Head v1 unchanged. Neither side
probes, falls back, creates a new channel, or accepts an arbitrary batch.

## One bounded request and one bounded reply

A batch (NSR1 kind 10) has the existing outer session/request correlation
anchored to binding (1, 1), followed by the capability metadata version and
reader support. The two ordered slots each carry binding handle, generation,
last applied checkpoint position, and all six exact scope identifiers.
There is no implied/default scope. Both slots must match active, authorized
setup bindings; otherwise no batch is sent by the client. The server validates
both slots before source acquisition. A secondary refused at setup leaves only
the ordinary primary head available; this batch cannot repurpose an unbound slot.

The reply carries two exact slot echoes and each own status. Success includes
the ordinary Head v1 position/version and requirements fields. An incompatible
or denied scope contributes only its typed refusal, not metadata. Both exact
results validate before either is returned. A well-formed denial cannot conceal
the neighboring successful result. Source views are independent and have no
payload; each is dropped before the reply. Current policy is independently
checked before each view is acquired and again before publishing both outcomes
in one bounded frame. This is not an atomic snapshot across scopes or an
instantaneous revocation claim. The maximum exact-ID request and reply fit
one minimum 2 KiB body. Outer malformed replies discard both outcomes and
fence the channel; a typed per-scope identity change fences only that local slot.

The checkpoint is supplied by the host and never loaded, advanced or reset here.
`at_checkpoint` means equality of *positions after successful current admission*.
The wire never omits head metadata: requirements can change even if position
does not. A position lower than the caller's checkpoint can be observed but is
not authority to rewind durable progress.

## Verification

`cargo test --locked --all-features --test session_scope_pair -- --nocapture`
compares two independent ordinary Head v1 requests to one batch, on matched
direct TCP connections and two distinct real SQLite sources. Both cases start
from the same pair-batch setup. It measures again after explicit reconnect,
retaining the caller's checkpoints and appending one record to the right source.
The exact raw counters for this fixture:

| Post-open mode | Exchanges | Protocol bytes | Payload bytes |
| --- | ---: | ---: | ---: |
| Ordinary sequential Head v1 | 2 | 343 | 0 |
| Head batch | 1 | 398 | 0 |

Each opening adds 3 exchanges and 354 bytes. A batch saves one serialized RPC
while spending 55 extra bytes in exact correlated checkpoints and echoes. No
latency, shaped-link, relay, mux, credit, or production security qualification.
The interruption, denial, stale-binding and old-endpoint proofs remain separate
from this narrow comparison. Issue #54 remains open.
