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

/// A durable cursor and budgets for capturing a new finite source target.
public struct RecordOpenPageRequest: Equatable, Sendable {
    public let scope: RecordScopeIdentity
    public let after: UInt64
    public let maxRecords: Int
    public let maxPayloadBytes: Int
    public let maxRecordBytes: Int

    public init(scope: RecordScopeIdentity, after: UInt64, maxRecords: Int, maxPayloadBytes: Int, maxRecordBytes: Int) {
        self.scope = scope
        self.after = after
        self.maxRecords = maxRecords
        self.maxPayloadBytes = maxPayloadBytes
        self.maxRecordBytes = maxRecordBytes
    }
}

/// Only `page` includes records to apply. Empty outcomes never authorize an
/// apply or a rewind. A page's target is a finish line, not a durable cursor.
public enum RecordOpenPageOutcome: Equatable, Sendable {
    case page(RecordPageReceipt)
    case alreadyCurrent
    case sourceBehind(RecordSourceMetadata)
}

extension BoundRecordClient {
    /// Read one fixed-target v1 page. An incomplete page is never returned.
    /// Set recordByteLimit from the validated Welcome when constructing this
    /// client. Unsupported capabilities do not cause a legacy fallback.
    public func readPage(_ page: RecordPageRequest) throws(RecordHeadReadError) -> RecordPageReceipt {
        guard page.target > page.after else { throw .invalidPageRequest }
        let request = RecordOpenPageRequest(scope: boundScope, after: page.after, maxRecords: page.maxRecords,
            maxPayloadBytes: page.maxPayloadBytes, maxRecordBytes: page.maxRecordBytes)
        guard case .page(let receipt) = try readForwardPage(request, fixedTarget: page.target)
        else { close(); throw .malformedReply }
        return receipt
    }

    /// Capture a source target and the bounded first page using one explicit
    /// capability operation, on a known peer that implements kind 6.
    public func readOpenPage(_ request: RecordOpenPageRequest) throws(RecordHeadReadError) -> RecordOpenPageOutcome {
        try readForwardPage(request, fixedTarget: nil)
    }

    private func readForwardPage(_ request: RecordOpenPageRequest, fixedTarget: UInt64?)
        throws(RecordHeadReadError) -> RecordOpenPageOutcome
    {
        guard let recordByteLimit, request.scope == boundScope, (1...64).contains(request.maxRecords),
            (1...524_288).contains(request.maxPayloadBytes), (1...recordByteLimit).contains(request.maxRecordBytes)
        else { throw fixedTarget == nil ? .invalidOpenPageRequest : .invalidPageRequest }

        var budgets = bytes(UInt64(request.maxRecords))
        budgets.append(contentsOf: bytes(UInt64(request.maxPayloadBytes)))
        budgets.append(contentsOf: bytes(UInt64(request.maxRecordBytes)))
        var body = RecordCapabilityV1.requestPrefix
        body.append(contentsOf: bytes(request.after))
        if let fixedTarget { body.append(contentsOf: bytes(fixedTarget)) }
        body.append(contentsOf: budgets)
        let maximumReply = min(1_048_576,
            68 + scope.count + request.maxRecords * (141 + scope.count) + request.maxPayloadBytes)
        let reply = try exchange(kind: fixedTarget == nil ? 6 : 7, payload: body, maximumReply: maximumReply)
        do throws(RecordHeadReadError) {
            return try validateForwardPage(reply, request: request, fixedTarget: fixedTarget, budgets: budgets)
        } catch {
            if error == .malformedReply { close() }
            throw error
        }
    }

    private func validateForwardPage(_ reply: [UInt8], request: RecordOpenPageRequest, fixedTarget: UInt64?,
        budgets: [UInt8]) throws(RecordHeadReadError) -> RecordOpenPageOutcome
    {
        var cursor = RecordReplyCursor(reply[...])
        let metadataBytes = try cursor.take(RecordCapabilityV1.metadataByteCount)
        guard try cursor.integer(1) == 0, try cursor.take(scope.count).elementsEqual(scope),
            try cursor.integer(8) == request.after
        else { throw .malformedReply }
        let target = try cursor.integer(8)
        guard try cursor.take(budgets.count).elementsEqual(budgets) else { throw .malformedReply }
        var advertised = RecordReplyCursor(metadataBytes.dropFirst(2))
        let head = try advertised.integer(8)
        guard target == (fixedTarget ?? head), head >= target
        else { throw .malformedReply }
        let metadata = try admitMetadata(metadataBytes)
        let count = Int(try cursor.integer(1))
        if target <= request.after {
            guard fixedTarget == nil, count == 0, cursor.remaining.isEmpty else { throw .malformedReply }
            return target == request.after ? .alreadyCurrent : .sourceBehind(metadata)
        }
        guard (1...request.maxRecords).contains(count) else { throw .malformedReply }

        var records: [SyncRecord] = []
        var ids = Set<[UInt8]>()
        var previous = request.after
        var remaining = request.maxPayloadBytes
        for _ in 0..<count {
            let record = try cursor.record(scope: scope, maxBytes: min(request.maxRecordBytes, remaining))
            guard previous < target, record.position == previous + 1, ids.insert(record.idBytes).inserted
            else { throw .malformedReply }
            remaining -= record.payload.count
            previous = record.position
            records.append(record)
        }
        guard cursor.remaining.isEmpty else { throw .malformedReply }
        let page = RecordPageRequest(after: request.after, target: target, maxRecords: request.maxRecords,
            maxPayloadBytes: request.maxPayloadBytes, maxRecordBytes: request.maxRecordBytes)
        return .page(RecordPageReceipt(scope: boundScope, request: page, records: records, nextPosition: previous))
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
