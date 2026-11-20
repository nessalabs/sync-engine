/// Read the retained records immediately below an installed history boundary.
/// The caller owns the history generation; it is not the socket binding generation.
public struct RecordOlderRequest: Equatable, Sendable {
    public let scope: RecordScopeIdentity
    public let historyGeneration: UInt64
    public let before: UInt64
    public let maxRecords: Int
    public let maxPayloadBytes: Int
    public let maxRecordBytes: Int

    public init(scope: RecordScopeIdentity, historyGeneration: UInt64, before: UInt64,
        maxRecords: Int, maxPayloadBytes: Int, maxRecordBytes: Int)
    {
        self.scope = scope
        self.historyGeneration = historyGeneration
        self.before = before
        self.maxRecords = maxRecords
        self.maxPayloadBytes = maxPayloadBytes
        self.maxRecordBytes = maxRecordBytes
    }
}

/// One exact nonempty historical page, not authorization or live checkpoint
/// progress. Before atomically prepending, compare the receipt's scope,
/// history generation and exclusive before against the installed coverage;
/// reject overlap or repeated IDs against the store. Keep the live cursor unchanged.
public struct RecordOlderReceipt: Equatable, Sendable {
    public let request: RecordOlderRequest
    public let firstPosition: UInt64
    public let oldestAvailable: UInt64
    public let records: [SyncRecord]
}

extension RecordFollowClient {
    /// Read Older on a known history-capable Follow peer, without fallback.
    public func readOlder(_ older: RecordOlderRequest) throws(RecordHeadReadError) -> RecordOlderReceipt {
        try bound.readOlder(older)
    }
}

extension BoundRecordClient {
    func readOlder(_ older: RecordOlderRequest) throws(RecordHeadReadError) -> RecordOlderReceipt {
        guard older.before > 1, let common = historyRequestFields(scope: older.scope,
            generation: older.historyGeneration, records: older.maxRecords,
            payloadBytes: older.maxPayloadBytes, recordBytes: older.maxRecordBytes)
        else { throw .invalidOlderRequest }
        var fields = common
        fields.append(contentsOf: bytes(older.before))
        let maximumReply = historyReplyLimit(records: older.maxRecords, payloadBytes: older.maxPayloadBytes)
        let reply = try exchange(kind: 9, payload: RecordCapabilityV1.requestPrefix + fields,
            maximumReply: maximumReply)
        do throws(RecordHeadReadError) {
            return try validateOlder(reply, request: older, fields: fields)
        } catch {
            if error == .malformedReply { close() }
            throw error
        }
    }

    private func validateOlder(_ reply: [UInt8], request: RecordOlderRequest, fields: [UInt8])
        throws(RecordHeadReadError) -> RecordOlderReceipt
    {
        var cursor = RecordReplyCursor(reply[...])
        let metadata = try historyMetadata(&cursor, fields: fields)
        guard metadata.position >= request.before - 1 else { throw .malformedReply }
        let oldest = try cursor.integer(8)
        let count = Int(try cursor.integer(1))
        guard (1...request.maxRecords).contains(count), UInt64(count) < request.before
        else { throw .malformedReply }
        let first = request.before - UInt64(count)
        guard oldest > 0, oldest <= first else { throw .malformedReply }
        let records = try historyRows(&cursor, first: first, count: count,
            payloadBytes: request.maxPayloadBytes, recordBytes: request.maxRecordBytes)
        return RecordOlderReceipt(request: request, firstPosition: first,
            oldestAvailable: oldest, records: records)
    }
}
