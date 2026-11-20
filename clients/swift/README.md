# NessaSync Swift package

Build and test from the Sync Engine repository root with Swift 6:

`swift test`

The package is independent of Nessa iOS, SwiftUI, app storage, network APIs,
and external dependencies. The root manifest permits a normal Git URL SwiftPM
dependency once a reviewed revision is published. This first slice implements
the version-1 reader metadata portion of the existing NSR1 record capability
operations, as specified by [SourceHead wire](../../docs/design/source-head-wire.md).
The test vectors are shared with the Rust record codec.

This is not a production network client. The API requires already-correlated
metadata bytes from a host adapter, will not guess a legacy fallback, and never
grants access or advances a checkpoint. Before app adoption, the remaining #54
transport and Nessa #1378 work must supply authenticated LAN/relay selection,
full request/scope correlation, bounded record decoding, durable apply and
reconnect. Use the existing iOS retained-session port as the composition seam;
do not replace its cache or authorization owner with this metadata primitive.
