# One opt-in PAGE and Cancel on the serialized Follow owner

Issue #54: a host-configured, one-page lifetime channel demonstrates real
correlated cancellation without a second reader, writer, connection, or
general dispatcher. Ordinary Follow, advisory hints, and fragmented pages
remain unchanged. This mode has no advisory hints, second PAGE, fallback,
credit, priority, general multiplexing or secure-duplex claim.

The opt-in endpoint uses a distinct eight-byte version preface, then the
existing FollowOpen/Welcome/Bound identity and frame codec. One exact PAGE
can follow. Its requested worst-case response must fit one negotiated frame;
the strict client and server both check before that page is requested/read.
That deliberate bound lets the sole existing reader route one subsequent
control receipt even while the only complete PAGE frame is parked.
It does not negotiate or prove fragmented-response cancellation.

After the client completely flushes that exact PAGE request, it hands its
sole writer to the consumed optional Cancel handle. The unchanged physical
read owner services both PAGE and the exact Cancel receipt. Cancel must match
the already bound session, request, handle and generation. No cancel-current
selector, additional queues, or independently reusable request slots.
The server ingress is the sole reader and starts one worker for that PAGE;
the worker still uses the existing current policy and bounded source
validator. Source storage is released before any response I/O.

| Outcome | Exact meaning |
| --- | --- |
| StoppedBeforeResponse | The server owned the existing frame lock, won before the complete response flushed, fenced future PAGE emission and sent a typed stop acknowledgement. No page DTO escapes the client. |
| TooLate | A complete response already flushed before Cancel acquired that lock. This is a wire receipt only; a complete validated page can be read, but no cache or transaction result is implied. |
| Uncertain | No complete exact timely acknowledgement was captured, including EOF, transport error or deadline. Do not infer that the peer stopped or that applied state changed. |

The one data frame and one control reply commit their local outcome under
the same existing complete-frame writer lock. A partial physical write or
flush failure fences that channel rather than producing a prefix receipt.
Stopped means no PAGE bytes follow the acknowledgement; it does not mean an
arbitrary host callback was interrupted. Teardown still depends on host socket
and callback deadlines. A complete locally captured acknowledgement does not
turn into a rollback if later I/O fails. Receiver validation, apply and
[local pre-apply cancellation](local-page-cancellation.md) are separate.
This transport never owns or rolls back receiver state.

## Real owned-channel fixture

Run:

    cargo test --locked --features transport,sqlite --lib cancel_page::tests -- --nocapture

Both direct localhost TCP and a byte-opaque local relay use actual SQLite
page materialization and the real native client endpoint. A page held by its
host callback admits a Stop acknowledgement before the response is emitted.
The retained ingress is also checked independently for EOF/no subsequent
data after the held source is released. A completed PAGE sits parked in the
existing reader slot while the same connection delivers TooLate, and the
caller then reads the unchanged complete page. For uncertainty, the fixture
cuts the real channel after the ACK is selected, while its physical send is
still gated; no known acknowledgement or page is returned.

| Matched direct and opaque relay case | PAGE/setup logical completions | Application protocol bytes | Complete page payload bytes |
| --- | ---: | ---: | ---: |
| No Cancel | 2 | 500 | 1,000 |
| Stop before response | 2 | 392 | 0 |
| TooLate | 2 | 597 | 1,000 |
| Lost acknowledgement | 1 | 343 | 0 |

Accounting includes the preface, actual control bytes and existing setup.
The complete late round trip adds 48 Cancel plus 49 acknowledgement bytes.
A stop counts one terminal response for the PAGE instead of a data result;
the separately observed late control acknowledgement is not a second PAGE
completion. These counts are not wall-latency, head-of-line, TLS, physical LAN,
deployed relay, or durable cache evidence. Issue #54 remains open.
