# Claim-qualified native older pages

This is the protected older-record coverage slice of
[#50](https://github.com/nessalabs/sync-engine/issues/50). It composes the
[same protected receiver](origin-epoch-native-receiver.md) with existing
[qualified tail installation](origin-epoch-native-tail.md). No new file or
state mode, progress owner, transport operation or application dependency
is introduced.

The confirmed source claim covers one bounded `older_with_capabilities`
read using the existing local authority guard. Check the older request's
generation, exclusive boundary and limits before opening the guard. The
native SQLite read checks fresh reader compatibility and captures the
source head, history floor and records in the same snapshot. Release the
ledger lock before handoff. The opaque page retains the original claim;
canonical validation compares the independently expected exact request
with the response echo, demand limits, range, and matching metadata scope
and source boundary. This does not elevate the receiver's observed epoch.

The same trusted host admission object can opt into older operations.
The protected receiver checks the current durable writer floor **before**
the existing canonical older-page mutation, including overlap or repeat
handling. Both modes share the same transaction body for the saved
generation, live-head checks, exact overlap, payload insertion and lower
coverage. An older page never advances live progress or replaces a tail.
No ordinary `HistoryStore`, deletion, general reset or less-trusted
alternate operation is exposed through the protected store.

```sh
cargo test --locked --features sqlite --test epoch_tail
```

The fixture starts at generation 1 with live head 4 and only row `d`
covered. Old delayed, future-unobserved and wrong-request results keep
that state unchanged. After epoch 2 is observed and the receiver reopens,
authorized older pages extend lower coverage 4→3→2 while live head stays 4.
An already-covered current repeat succeeds under ordinary overlap rules;
the equivalent old-epoch repeat is fenced. Selector, generation and
fresh reader-compatibility refusals remain typed. The final reopen reads
exact rows `b,c,d`.

## Protected wire boundary, not implemented

The native head, forward, tail and older operations each now have an
explicit qualified path. A later authenticated session adapter must
negotiate one required protected envelope/version for these operations.
The existing SourceHead v1 and legacy endpoints cannot be substituted for
protected ones. The transport must authenticate the session peer, and an
independent trusted host authority must attest the writer claim's stable
origin binding and epoch. A raw numeric value inside a TLS connection does
not by itself establish that binding. The verified response must carry
the actual session/operation correlation, exact scope and request, and the
claim from source capture to the existing native admission decision.
Only a trusted matched head may observe a newer epoch; subsequent payload
decisions still compare the durable floor before committing. A protected
session must refuse an unqualified or incompatible response, not fall back.

There is no wire, catalogue/artifact, ordinary source writer, separate-host
lease, cryptographic binding, physical crash or Nessa/Swift claim here.
