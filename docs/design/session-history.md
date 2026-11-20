# Bounded history on one follow connection (#54)

Hosts that know both channel endpoints support this mode may choose
`serve_history_follow_record_session` and `FollowSession::history_parts`.
It adds two reads on the existing single-scope follow connection. There is no
probe, legacy retry, second reader, storage owner, or automatic reconnect. The
capability record and advisory wakes remain available on the same binding.

The host still authenticates the channel and owns finite I/O deadlines and
current policy. Recheck policy before acquiring the scoped source, before the
history call, and before transmitting each frame. The source implements
[the coherent history capability port](history-capability-view.md), which
admits reader compatibility and acquires rows in the same read view. Validate
the owned result, drop all source handles, then encode and send bounded bytes.
No earlier binding, policy reply, or source metadata is reusable authority.

## Wire and state boundary

Use existing NSR1 session, request, scope handle and binding generation
correlations. Kinds 8 (Tail) and 9 (Older) use the existing reply flag and
status vocabulary. Requests carry the metadata version, offered reader bits,
history generation and record/count/byte ceilings, plus exclusive older
boundary for Older. The retained binding supplies the exact request scope.
This history generation is not the session binding generation.

Success echoes the exact scope and all numeric request fields, followed by
the coherent capability metadata, tail first position (Tail only), oldest
available position and bounded records. Tail watermark is exactly the metadata
head. The receiver checks echoes and metadata before decoding records and runs
the canonical history validator. Fragment correlation and ordering, reply byte
ceilings and teardown on malformed, interrupted or incomplete replies are
owned by the existing session. A denial between fragments discards previously
received fragments. Neither interruption nor a refusal produces an installable
plan; nothing can recall an already returned authorized DTO or committed data.

The application still owns `fetch_tail`, `fetch_older` and atomic installation.
Older pages do not change the live checkpoint. History payload is included in
wire byte accounting without treating older pages as duplicate forward traffic
or altering the forward duplicate counter's high-water mark.

## Verification

```sh
cargo test --locked --all-features --test session_history
python3 scripts/verify-session-history.py
python3 scripts/verify-session-history.py --profiles --repetitions 2
```

The tests exercise real IPv4 TCP with SQLite, fragmentation, policy revocation
after the first fragment, connection interruption and a reopened cache. The
matched runner compares the same SQLite workload through the legacy Loopback
history adapter and this one scoped connection. Its weak-link frame profiles
are controlled application-frame delay and bandwidth, not packet loss, TLS,
device identity, production relay security, multiplexing, or urgent-under-bulk
qualification. This milestone does not close #54.
