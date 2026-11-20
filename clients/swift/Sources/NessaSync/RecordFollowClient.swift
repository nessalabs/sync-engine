/// Experimental single-scope capability Follow on a trusted injected channel.
/// Calls are synchronous and must never overlap. There is one demand-driven
/// read pump, no background listener, automatic reconnect or transport fallback.
/// The host supplies an absolute deadline per complete Open, wait and RPC;
/// a timeout retires the channel because this port cannot prove zero bytes read.
public final class RecordFollowClient<Channel: RecordHeadChannel> {
    private let bound: BoundRecordClient<Channel>

    private init(_ bound: BoundRecordClient<Channel>) { self.bound = bound }

    /// Select FollowOpen v1 only on an already authenticated channel whose
    /// peer explicitly supports the capability Follow endpoint.
    public static func open(
        channel: Channel, scopeFields: [String], frameBodyLimit: Int = 2048
    ) throws(RecordSessionOpenError) -> RecordFollowClient<Channel> {
        let bound = try BoundRecordClient.establish(channel: channel, scopeFields: scopeFields,
            frameBodyLimit: frameBodyLimit, following: true)
        return RecordFollowClient(bound)
    }

    public var boundScope: RecordScopeIdentity { bound.boundScope }

    /// Consume the maximum advisory head seen during a previous RPC, or wait
    /// for the next frame. Hints are wakeups, never durable progress or access.
    public func waitForHint() throws(RecordHeadReadError) -> UInt64 {
        try bound.takeOrReadHint()
    }

    public func readHead() throws(RecordHeadReadError) -> RecordSourceMetadata { try bound.readHead() }

    public func readPage(_ page: RecordPageRequest) throws(RecordHeadReadError) -> RecordPageReceipt {
        try bound.readPage(page)
    }

    public func close() { bound.close() }
}
