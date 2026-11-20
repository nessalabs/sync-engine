/// The caller's durable cursor, finite target, and requested work budget.
public struct RecordPageRequest: Equatable, Sendable {
    public let after: UInt64
    public let target: UInt64
    public let maxRecords: Int
    public let maxPayloadBytes: Int
    public let maxRecordBytes: Int

    public init(after: UInt64, target: UInt64, maxRecords: Int, maxPayloadBytes: Int, maxRecordBytes: Int) {
        self.after = after
        self.target = target
        self.maxRecords = maxRecords
        self.maxPayloadBytes = maxPayloadBytes
        self.maxRecordBytes = maxRecordBytes
    }
}

/// One opaque record in a fully correlated, bounded page.
public struct SyncRecord: Equatable, Sendable {
    public let position: UInt64
    /// UTF-8 wire bytes; identity equality is byte-exact, not normalized String equality.
    public let idBytes: [UInt8]
    public let payload: [UInt8]
}

/// A fully validated read receipt, not a committed checkpoint or access grant.
/// The host must atomically compare this receipt's exact logical scope and
/// previous durable checkpoint before applying the complete receipt. Duplicate IDs
/// from earlier committed pages must also be rejected by that store.
public struct RecordPageReceipt: Equatable, Sendable {
    public let scope: RecordScopeIdentity
    public let request: RecordPageRequest
    public let records: [SyncRecord]
    public let nextPosition: UInt64
}

extension BoundRecordClient {
    /// Read one fixed-target v1 page. An incomplete page is never returned.
    /// Set recordByteLimit from the validated Welcome when constructing this
    /// client. Unsupported capabilities do not cause a legacy fallback.
    public func readPage(_ page: RecordPageRequest) throws(RecordHeadReadError) -> RecordPageReceipt {
        guard let recordByteLimit, page.target > page.after, (1...64).contains(page.maxRecords),
            (1...524_288).contains(page.maxPayloadBytes), (1...recordByteLimit).contains(page.maxRecordBytes)
        else { throw .invalidPageRequest }
        var fields: [UInt8] = bytes(page.after)
        fields.append(contentsOf: bytes(page.target))
        fields.append(contentsOf: bytes(UInt64(page.maxRecords)))
        fields.append(contentsOf: bytes(UInt64(page.maxPayloadBytes)))
        fields.append(contentsOf: bytes(UInt64(page.maxRecordBytes)))
        let limit = 68 + scope.count + page.maxRecords * (141 + scope.count) + page.maxPayloadBytes
        let bytes = try exchange(kind: 7, payload: RecordCapabilityV1.requestPrefix + fields,
            maximumReply: limit)
        do throws(RecordHeadReadError) {
            var cursor = RecordReplyCursor(bytes[...])
            let metadata = try cursor.take(26)
            guard try cursor.integer(1) == 0,
                try cursor.take(scope.count).elementsEqual(scope),
                try cursor.take(fields.count).elementsEqual(fields)
            else { throw .malformedReply }
            var advertised = RecordReplyCursor(metadata.dropFirst(2))
            guard try advertised.integer(8) >= page.target else { throw .malformedReply }
            _ = try admitMetadata(metadata)
            let count = Int(try cursor.integer(1))
            guard (1...page.maxRecords).contains(count) else { throw .malformedReply }

            var records: [SyncRecord] = []
            var ids = Set<[UInt8]>()
            var previous = page.after
            var remainingPayload = page.maxPayloadBytes
            for _ in 0..<count {
                let record = try cursor.record(scope: scope, maxBytes: min(page.maxRecordBytes, remainingPayload))
                guard previous < page.target, record.position == previous + 1,
                    ids.insert(record.idBytes).inserted
                else { throw .malformedReply }
                remainingPayload -= record.payload.count
                previous = record.position
                records.append(record)
            }
            guard cursor.remaining.isEmpty else { throw .malformedReply }
            return RecordPageReceipt(scope: boundScope, request: page, records: records, nextPosition: previous)
        } catch {
            if error == .malformedReply { close() }
            throw error
        }
    }
}

struct RecordReplyCursor {
    var remaining: ArraySlice<UInt8>
    init(_ bytes: ArraySlice<UInt8>) { remaining = bytes }

    mutating func take(_ count: Int) throws(RecordHeadReadError) -> ArraySlice<UInt8> {
        guard count <= remaining.count else { throw .malformedReply }
        let result = remaining.prefix(count)
        remaining = remaining.dropFirst(count)
        return result
    }

    mutating func record(scope: [UInt8], maxBytes: Int) throws(RecordHeadReadError) -> SyncRecord {
        let position = try integer(8)
        let idLength = Int(try integer(1))
        guard (1...128).contains(idLength) else { throw .malformedReply }
        let id = Array(try take(idLength))
        guard let text = String(validating: id, as: UTF8.self),
            !text.unicodeScalars.allSatisfy({ $0.properties.isWhitespace }),
            try take(scope.count).elementsEqual(scope)
        else { throw .malformedReply }
        let length = Int(try integer(4))
        guard length <= maxBytes else { throw .malformedReply }
        return SyncRecord(position: position, idBytes: id, payload: Array(try take(length)))
    }

    mutating func integer(_ count: Int) throws(RecordHeadReadError) -> UInt64 {
        try take(count).reduce(0) { ($0 << 8) | UInt64($1) }
    }
}
