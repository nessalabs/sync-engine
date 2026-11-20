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
        guard let recordByteLimit, tail.scope == boundScope, tail.historyGeneration != 0,
            (1...64).contains(tail.maxRecords), (1...524_288).contains(tail.maxPayloadBytes),
            (1...recordByteLimit).contains(tail.maxRecordBytes)
        else { throw .invalidTailRequest }

        var fields = bytes(tail.historyGeneration)
        fields.append(contentsOf: bytes(UInt64(tail.maxRecords)))
        fields.append(contentsOf: bytes(UInt64(tail.maxPayloadBytes)))
        fields.append(contentsOf: bytes(UInt64(tail.maxRecordBytes)))
        let envelope = 75 + scope.count
        let records = tail.maxRecords * (141 + scope.count)
        let maximumReply = min(1_048_576, envelope + records + tail.maxPayloadBytes)
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
        guard try cursor.take(scope.count).elementsEqual(scope),
            try cursor.take(fields.count).elementsEqual(fields)
        else { throw .malformedReply }
        let metadata = try admitMetadata(cursor.take(RecordCapabilityV1.metadataByteCount))
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

        var result: [SyncRecord] = []
        var ids = Set<[UInt8]>()
        var remaining = request.maxPayloadBytes
        for i in 0..<count {
            let record = try cursor.record(scope: scope, maxBytes: min(remaining, request.maxRecordBytes))
            guard record.position == first + UInt64(i), ids.insert(record.idBytes).inserted
            else { throw .malformedReply }
            remaining -= record.payload.count
            result.append(record)
        }
        guard cursor.remaining.isEmpty else { throw .malformedReply }
        return RecordTailReceipt(request: request, metadata: metadata, firstPosition: first,
            oldestAvailable: oldest, records: result)
    }
}
