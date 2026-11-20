# Timed-outage qualification for a serialized record session

This is a matched reference lab for issue #54, not a protocol or retry change.
The same real source and receiver SQLite workload runs through the existing
per-operation loopback adapter and the existing interruptible serialized record
session. Both paths have the same application-frame shaper; the opaque route
adds an actual byte-forwarding TCP hop behind it.

## Controlled ordering

1. Seed records 1–7 and confirm the same durable replica. Retain that session
   for the warm attempt where the adapter supports a persistent connection.
2. Append records 8–14. Arm only the exact next warm Page operation. Forward
   512 bytes from that reply's wire frame, then shut the actual receiver-route
   TCP sockets. This is a prefix accepted by the forwarder, not a first-page
   or payload receipt.
3. Reopen SQLite. All seven original records and the checkpoint are unchanged;
   neither implementation accepted a complete new page. Make a real fresh
   TCP attempt during the requested three-second gate. The gate admits no
   upstream connection and forwards no bytes from that attempt.
4. Append records 15–21 to the live source during the route outage. Release
   the gate no sooner than three monotonic seconds after cut. A fresh authorized
   connection resumes from the real SQLite checkpoint. A separate process
   reopens the replica and checks all 21 exact ordered identities and bytes.

The host holds the outage and explicitly drives both probe and reopen. The
fixture does not add an implicit product retry, keepalive, or timing threshold.
Configured application profiles are unshaped, 32 kbit/s with 0.8 s round-trip
delay, and 64 kbit/s with 1.5 s delay. The model applies a half-delay per
application frame per direction and paces 256-byte writes, not TCP/IP packets.
The raw counters are actual socket-forwarded application bytes, separately
from the adapter's completed-exchange and accepted-payload counts. In particular
a dropped prefix is not counted as a duplicate committed record, and a client
can report attempted bytes that the closed gate forwards to no upstream.

## Reproduce

Run the fast correctness matrix:

    python3 scripts/verify-session-record-outage.py --output outage.json

Run both shaped profiles as well:

    python3 scripts/verify-session-record-outage.py --profiles --output outage-profiles.json

Use optional --jobs 2 through --jobs 4 for faster isolated fixture runs; use
serial execution for any timing comparison. Each raw report includes measured
outage/probe time, configured shaping, source fingerprints, per-phase adapter
and route counters, and fresh-process cache checks. No throughput, physical
LAN, NAT, TCP congestion, random packet loss, cellular power, TLS or deployed
relay qualification is inferred. This retires only the controlled timed-outage
evidence gap for the matched single-receiver record workload. Issue #54 remains
open.

## Wire accounting

The reference matched paths seed to position 7, remain at 7 through the cut
and blocked probe, and resume to 21 with 8,252 newly received record bytes.
Across the unshaped and both shaped direct and opaque-relay runs, the
persistent variant used 2 admitted upstream connections versus the
baseline's 19 across the exercise;
both had an additional blocked attempt. Both completed 7 exchanges during
resume, so this does not save resume exchanges. Resume forwarded 10,689
application bytes on the persistent path versus 10,301 baseline. Socket reuse
did not mean less wire traffic in this fixture.
