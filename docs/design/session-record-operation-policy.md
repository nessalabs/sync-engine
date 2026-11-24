# Serialized capability record operation policy

The host may opt into `serve_operation_capability_record_session` to add current
operation decisions to the existing current scope policy. The host authenticates
the channel and binds the supplied policy to that verified peer; packet fields,
scope names and binding metadata are not credentials. This endpoint supplies no
identity lifecycle, listener, transport security, durable progress or reconnect.

| Request | Required operation decisions |
| --- | --- |
| Bind or Open binding | Bind and Head: Bound exposes a head |
| AUTHORIZE | Authorize |
| HEAD_CAP | Head |
| PAGE_CAP and OPEN_CAP | Head and Page: both retrieve and return head metadata |
| Any other selector | Terminate with protocol error before source acquisition |

The same immutable operation set applies to each check for a request. Scope and
each operation must allow the exact scope before acquisition and every response
fragment, including refusal metadata. Combined operations conservatively require
both grants throughout, including an empty OPEN_CAP. An operation revocation may
end a response after earlier fragments were already sent; receivers must discard
incomplete replies and only commit validated complete pages. A check is not an
atomic guarantee against revocation during a subsequent write.

No existing endpoint, peer negotiation, wire format or default behavior changes.
Follow, historical reads, scope pairs, artifacts, protected origin epochs and
catalogue are not covered by this endpoint. The host must separately authorize
them and must not route them through this hook. Capability PAGE's head dependency
means a host that grants Page without Head cannot use that operation here.

Run `python3 scripts/verify-record-operation-policy.py`. Its tests use synthetic
local policy and a loopback fixture, not production enrollment or TLS authority.
