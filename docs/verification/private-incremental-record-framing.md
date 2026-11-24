# Private incremental record framing

This independently implemented #54 slice changes only acquisition of serialized
record packet frames. It is not a restoration of the earlier backup. The
four-byte network-order length is read incrementally and checked against the
negotiated and hard limits before acquiring the body. Body reads retain only
bytes received so far and stop at the packet boundary. Packet field validation,
wire encoding, protocol counters and caller-owned deadlines remain in their
existing owners. The retained body length is bounded by the accepted length;
the vector's reserved capacity may exceed the bytes received or the negotiated
limit. The separate persistent Follow physical reader still acquires a complete
bounded frame before passing it to this packet parser; this slice does not make
that physical ingress incremental.

A clean EOF before any prefix byte means the connection ended; EOF after any
prefix byte is truncation. Interruptions are retried; idle timeout/would-block
is retried only before the first prefix byte. Once acquisition has begun, a
timeout is returned to the caller. There is no partial-packet recovery across a
returned error.

The codec unit tests cover fragmented and adjacent frames, every truncation
point of a minimal frame, length bounds before body reads, the maximum accepted
body, adjacent frames across the 4 KiB read boundary, interruption and
would-block after partial reads, idle and mid-frame timeout behavior, and a
fixed wire vector read one byte at a time. Existing session tests and `python3 scripts/verify-session-e2e.py`
exercise the surrounding transport. This byte-preserving change makes no
performance claim and does not complete #54.

## Local verification

On the independent working tree based on `d1c31660005c304fb0d1ecafec775fa2b7714609`:

- `cargo fmt --all -- --check` and all-feature, all-target Clippy passed.
- Before the final review's test-only additions, locked all-target tests passed
  for default-disabled core (79 tests), SQLite (234), transport (188) and all
  features (477). Eleven doctests passed. The focused record-session suite was
  rerun after the review additions.
- Rustdoc with warnings denied and an all-feature, all-target check using the
  declared Rust 1.85.0 minimum passed.
- All seven `verify-slice` runners, wire contract, record server, session
  control, lifecycle, authorization, record E2E, follow, secure follow and
  session history runners passed. CI path-classification tests passed.

These are local reference checks, not production transport qualification.
