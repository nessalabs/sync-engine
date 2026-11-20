# Warm catalogue manifest baseline (#51)

Run `python3 scripts/measure-catalogue-warm.py --output warm.json` for the full
matrix or add `--sizes 500` for a smaller run. All measurements are unshaped
development loopback TCP using the actual source, catalogue ports and durable
SQLite cache. No alternate encoding is implemented or projected.

The lab seeds 500, 5,000 or 50,000 stable creation keys and populates a complete
receiver with the native pass. The 1% workload updates every 100th key; the 20%
workload updates every fifth key. Every fourth block of 100 keys receives
deletions rather than replacement values. Both wire dialects start from copies
of the exact same completed cache and read the same fixed post-churn source.
Source creation, mutation and receiver backup are outside pass timings. The lab
compares all reopened receiver rows and the durable completed revision to the
source, observes actual manifest descriptors and frames, and crosses the
wire-byte accounting against the existing slice 7 counters.

| Rows | Changed | Actual manifest descriptor wire B | Slice 7 manifest B | Legacy total wire B | Capability total wire B | Observed bytes eligible for unchanged-only pages |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 500 | 5 | 140 | 135 | 1,816 | 2,137 | 0 |
| 500 | 100 | 2,800 | 2,700 | 32,491 | 37,371 | 0 |
| 5,000 | 50 | 1,400 | 1,350 | 15,785 | 18,268 | 0 |
| 5,000 | 1,000 | 28,000 | 27,000 | 338,955 | 387,169 | 0 |
| 50,000 | 500 | 14,400 | 13,900 | 171,661 | 195,811 | 0 |
| 50,000 | 10,000 | 288,000 | 278,000 | 3,458,403 | 3,940,192 | 0 |

An exact match needs the same ID, creation revision, current revision and
deletion state in the pre-pass cache. Eligibility here counts the physically
observed descriptor bytes in nonempty pages where **all** descriptors match.
It does not account for any proposed envelope, encoding overhead, negotiation,
or validation cost. The physical descriptor includes the one-byte ID prefix;
the slice 7 manifest counter leaves that byte in its protocol bucket. Frame
headers, scopes, echoes and value metadata are not manifest descriptors.

In the matched fixed workloads, the source filters on revision greater than
completed. All currently unchanged rows are omitted, not repeated records
eligible for compression. There were 80–99% omitted keys but no emitted
exact-cache-match descriptors. The distinction matters: multiplying the entire
catalogue by descriptor size would invent savings on bytes that never crossed
the wire.

A positive cached-ahead control opens a full pass at revision 500, commits the
first page, updates a later key, and finishes the old boundary. The cache now
contains that key at 501 despite completed still being 500. The following warm
pass really emits one already cached descriptor (28 physical bytes; 27 slice 7
counter bytes) with no value resolution or opaque payload. The entire eligible
page costs 375 total wire bytes in legacy and 461 in capability mode. This
establishes a real case for eligibility, not its prevalence in production.

Do not add a wire variant based on the fixed matrix. Measure cached-ahead
traffic frequency and clustering before deciding whether that case warrants
added negotiation. The full-reset seen-set question is distinct and remains
behind the tombstone-horizon reset contract. If the receiver already persists
only pre-reset live candidates, compare subtracting each admitted page's
exact IDs from those candidates in the same transaction before building a
second seen-ID store. Preserve scope/generation fencing and classify residual
candidates only after full completion. Creation revision alone is not an ID.
