/// Request for one bounded snapshot of the current source suffix on a peer
/// explicitly configured to serve history on its existing Follow connection.
public struct RecordTailRequest: Equatable, Sendable {
    public let scope: RecordScopeIdentity
    public let historyGeneration: UInt64
    public let maxRecords: Int
    public let maxPayloadBytes: Int
    public let maxRecordBytes: Int

    public init(scope: RecordScopeIdentity, historyGeneration: UInt64, maxRecords: Int,
        maxPayloadBytes: Int, maxRecordBytes: Int)
    {
        self.scope = scope
        self.historyGeneration = historyGeneration
        self.maxRecords = maxRecords
        self.maxPayloadBytes = maxPayloadBytes
        self.maxRecordBytes = maxRecordBytes
    }
}

/// Exact bounded suffix, not authorization or an installed checkpoint.
/// A store must compare the owning scope and expected local history generation
/// before atomically installing records and coverage. The history generation
/// is caller-owned and independent of the current session binding.
public struct RecordTailReceipt: Equatable, Sendable {
    public let request: RecordTailRequest
    public let metadata: RecordSourceMetadata
    public let firstPosition: UInt64
    public let oldestAvailable: UInt64
    public let records: [SyncRecord]
}

extension RecordFollowClient {
    /// Read a complete Tail from a known history-capable Follow peer.
    /// A peer refusal is returned as-is; the client never probes another dialect.
    public func readTail(_ tail: RecordTailRequest) throws(RecordHeadReadError) -> RecordTailReceipt {
        try bound.readTail(tail)
    }
}

extension BoundRecordClient {
    func readTail(_ tail: RecordTailRequest) throws(RecordHeadReadError) -> RecordTailReceipt {
        guard let fields = historyRequestFields(scope: tail.scope, generation: tail.historyGeneration,
            records: tail.maxRecords, payloadBytes: tail.maxPayloadBytes, recordBytes: tail.maxRecordBytes)
        else { throw .invalidTailRequest }
        let maximumReply = historyReplyLimit(records: tail.maxRecords, payloadBytes: tail.maxPayloadBytes)
        let reply = try exchange(kind: 8, payload: RecordCapabilityV1.requestPrefix + fields,
            maximumReply: maximumReply)
        do throws(RecordHeadReadError) {
            return try validateTail(reply, request: tail, fields: fields)
        } catch {
            if error == .malformedReply { close() }
            throw error
        }
    }

    private func validateTail(_ reply: [UInt8], request: RecordTailRequest, fields: [UInt8])
        throws(RecordHeadReadError) -> RecordTailReceipt
    {
        var cursor = RecordReplyCursor(reply[...])
        let metadata = try historyMetadata(&cursor, fields: fields)
        let first = try cursor.integer(8)
        let oldest = try cursor.integer(8)
        let count = Int(try cursor.integer(1))
        if metadata.position == 0 {
            guard first == 1, oldest == 1, count == 0 else { throw .malformedReply }
        } else {
            guard (1...request.maxRecords).contains(count),
                oldest > 0, oldest <= first, first <= metadata.position,
                metadata.position < UInt64.max,
                metadata.position - first == UInt64(count - 1)
            else { throw .malformedReply }
        }

        let records = try historyRows(&cursor, first: first, count: count,
            payloadBytes: request.maxPayloadBytes, recordBytes: request.maxRecordBytes)
        return RecordTailReceipt(request: request, metadata: metadata, firstPosition: first,
            oldestAvailable: oldest, records: records)
    }

    func historyRequestFields(scope: RecordScopeIdentity, generation: UInt64, records: Int,
        payloadBytes: Int, recordBytes: Int) -> [UInt8]?
    {
        guard let recordByteLimit, scope == boundScope, generation != 0,
            (1...64).contains(records), (1...524_288).contains(payloadBytes),
            (1...recordByteLimit).contains(recordBytes)
        else { return nil }
        var fields = bytes(generation)
        fields.append(contentsOf: bytes(UInt64(records)))
        fields.append(contentsOf: bytes(UInt64(payloadBytes)))
        fields.append(contentsOf: bytes(UInt64(recordBytes)))
        return fields
    }

    func historyReplyLimit(records: Int, payloadBytes: Int) -> Int {
        // Older echoes eight extra request bytes instead of Tail's first position.
        min(1_048_576, 75 + scope.count + records * (141 + scope.count) + payloadBytes)
    }

    func historyMetadata(_ cursor: inout RecordReplyCursor, fields: [UInt8])
        throws(RecordHeadReadError) -> RecordSourceMetadata
    {
        guard try cursor.take(scope.count).elementsEqual(scope),
            try cursor.take(fields.count).elementsEqual(fields)
        else { throw .malformedReply }
        return try admitMetadata(cursor.take(RecordCapabilityV1.metadataByteCount))
    }

    func historyRows(_ cursor: inout RecordReplyCursor, first: UInt64, count: Int,
        payloadBytes: Int, recordBytes: Int) throws(RecordHeadReadError) -> [SyncRecord]
    {
        var result: [SyncRecord] = []
        var ids = Set<[UInt8]>()
        var remaining = payloadBytes
        for i in 0..<count {
            let record = try cursor.record(scope: scope, maxBytes: min(remaining, recordBytes))
            guard record.position == first + UInt64(i), ids.insert(record.idBytes).inserted
            else { throw .malformedReply }
            remaining -= record.payload.count
            result.append(record)
        }
        guard cursor.remaining.isEmpty else { throw .malformedReply }
        return result
    }
}
