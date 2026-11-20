/// Blocking, host-owned byte channel for an already authenticated and bound
/// experimental NSR1 record endpoint. Implementations must enforce an absolute
/// I/O deadline, must not call back into the client, and must never consume
/// more than the requested count in readExactly.
public protocol RecordHeadChannel {
    mutating func writeAll(_ bytes: [UInt8]) throws
    mutating func readExactly(_ count: Int) throws -> [UInt8]
    mutating func close()
}

/// A canonical metadata-free refusal from the current source policy or reader.
public enum RecordHeadRefusal: UInt8, Error, Equatable, Sendable {
    case denied = 1
    case unavailable = 2
    case pruned = 3
    case invalidRequest = 4
    case identityChanged = 5
    case oversizedRecord = 6
}

public enum RecordHeadReadError: Error, Equatable, Sendable {
    case invalidBinding
    case unavailableChannel
    case malformedReply
    case invalidPageRequest
    case source(RecordHeadRefusal)
    case incompatible(RecordCapabilityError)
}

/// Logical identity of a Rust record scope, independent of the socket session,
/// request number, handle and generation.
public struct RecordScopeIdentity: Hashable, Sendable {
    /// Exact length-prefixed UTF-8 fields in receiver, origin, stream,
    /// incarnation, schema, access-epoch order. Suitable for equality and
    /// persistence as an opaque key; do not normalize or reorder the bytes.
    public let canonicalBytes: [UInt8]

    init(validatingFields fields: [String]) throws(RecordHeadReadError) {
        guard fields.count == 6 else { throw .invalidBinding }
        var encoded: [UInt8] = []
        for field in fields {
            let utf8 = field.utf8
            guard utf8.count <= 128, !field.unicodeScalars.allSatisfy({ $0.properties.isWhitespace }) else {
                throw .invalidBinding
            }
            encoded.append(UInt8(utf8.count))
            encoded.append(contentsOf: utf8)
        }
        canonicalBytes = encoded
    }
}

/// Owns request numbering on one serialized, already-bound NSR1 v1 channel.
/// The host must verify the explicit peer dialect, authentication, Open/Bound
/// exchange and exact scope before transferring exclusive channel ownership.
/// Head and page receipts are never authorization grants or local checkpoints.
/// This owner does not dispatch hints, perform retries, or restore a session.
public final class BoundRecordClient<Channel: RecordHeadChannel> {
    private static var headerBytes: Int { 44 }
    private static var preamble: [UInt8] { [78, 83, 82, 49, 0, 1] } // NSR1 v1

    private var channel: Channel?
    private var request: UInt64?
    private let session: UInt64
    private let handle: UInt32
    private let generation: UInt64
    private let limit: Int
    public let boundScope: RecordScopeIdentity
    var scope: [UInt8] { boundScope.canonicalBytes }
    let recordByteLimit: Int?

    public init(
        channel: Channel, session: UInt64, handle: UInt32, generation: UInt64,
        lastRequest: UInt64, frameBodyLimit: Int, scopeFields: [String], recordByteLimit: Int? = nil
    ) throws(RecordHeadReadError) {
        guard session != 0, handle != 0, generation != 0, lastRequest != 0,
            lastRequest != UInt64.max, (2_048...8_192).contains(frameBodyLimit),
            recordByteLimit.map({ (1...524_288).contains($0) }) ?? true
        else { throw .invalidBinding }

        self.boundScope = try RecordScopeIdentity(validatingFields: scopeFields)
        self.channel = channel
        self.session = session
        self.handle = handle
        self.generation = generation
        self.limit = frameBodyLimit
        self.request = lastRequest + 1
        self.recordByteLimit = recordByteLimit
    }

    deinit { channel?.close() }

    /// Explicitly retire the owner; no wire-level rollback is implied.
    public func close() {
        var owned = channel
        channel = nil
        owned?.close()
    }

    /// Read current v1 capability metadata without changing durable progress.
    public func readHead() throws(RecordHeadReadError) -> RecordSourceMetadata {
        let reply = try exchange(kind: 5, payload: RecordCapabilityV1.requestPrefix,
            maximumReply: scope.count + RecordCapabilityV1.metadataByteCount)
        guard reply.count == scope.count + RecordCapabilityV1.metadataByteCount,
            reply.prefix(scope.count).elementsEqual(scope)
        else { close(); throw .malformedReply }
        return try admitMetadata(reply.dropFirst(scope.count))
    }

    func admitMetadata(_ payload: ArraySlice<UInt8>) throws(RecordHeadReadError) -> RecordSourceMetadata {
        do { return try RecordCapabilityV1.admitMetadata(payload) }
        catch {
            if error == .malformedPayload { close(); throw .malformedReply }
            throw .incompatible(error)
        }
    }

    func exchange(kind: UInt8, payload: [UInt8], maximumReply: Int) throws(RecordHeadReadError) -> [UInt8] {
        guard var owned = channel else { throw .unavailableChannel }
        channel = nil
        var retain = false
        defer {
            if retain { channel = owned } else { owned.close() }
        }
        guard let next = request else { throw .unavailableChannel }
        request = next == .max ? nil : next + 1

        var echo: [UInt8] = bytes(session)
        echo.append(contentsOf: bytes(next))
        echo.append(contentsOf: bytes(handle))
        echo.append(contentsOf: bytes(generation))

        var body: [UInt8] = Self.preamble
        body.append(contentsOf: [kind, 0])
        body.append(contentsOf: echo)
        body.append(contentsOf: bytes(UInt32(payload.count)))
        body.append(contentsOf: bytes(UInt32(0)))
        body.append(contentsOf: payload)
        do { try owned.writeAll(bytes(UInt32(body.count)) + body) }
        catch { throw .unavailableChannel }

        var complete: [UInt8] = []
        var expectedTotal: Int?
        while true {
            let length = Int(wireWord(try read(&owned, 4)[...]))
            let ceiling = Self.headerBytes + max(maximumReply, 9)
            guard (Self.headerBytes...min(limit, ceiling)).contains(length) else { throw .malformedReply }
            let reply = try read(&owned, length)
            guard reply[0..<6].elementsEqual(Self.preamble), reply[6] == (kind | 0x80),
                reply[8..<36].elementsEqual(echo)
            else { throw .malformedReply }

            let total = Int(wireWord(reply[36..<40]))
            let offset = Int(wireWord(reply[40..<44]))
            let fragment = reply[Self.headerBytes...]
            switch reply[7] {
            case 0:
                guard total > 0, total <= maximumReply, (expectedTotal ?? total) == total,
                    offset == complete.count, !fragment.isEmpty, fragment.count <= total - offset
                else { throw .malformedReply }
                if expectedTotal == nil { complete.reserveCapacity(total); expectedTotal = total }
                complete.append(contentsOf: fragment)
                if complete.count == total { retain = true; return complete }
                guard length == limit else { throw .malformedReply }
            case 8:
                guard complete.isEmpty, offset == 0, total == fragment.count else { throw .malformedReply }
                let refusal: RecordCapabilityError
                do { refusal = try RecordCapabilityV1.decodeRefusal(fragment) }
                catch { throw .malformedReply }
                retain = true
                throw .incompatible(refusal)
            default:
                guard total == 0, offset == 0, fragment.isEmpty,
                    let refusal = RecordHeadRefusal(rawValue: reply[7])
                else { throw .malformedReply }
                retain = refusal != .identityChanged
                throw .source(refusal)
            }
        }
    }

    private func read(_ channel: inout Channel, _ count: Int) throws(RecordHeadReadError) -> [UInt8] {
        do {
            let value = try channel.readExactly(count)
            guard value.count == count else { throw RecordHeadReadError.unavailableChannel }
            return value
        } catch { throw .unavailableChannel }
    }

    func bytes<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
        withUnsafeBytes(of: value.bigEndian) { Array($0) }
    }

    private func wireWord(_ bytes: ArraySlice<UInt8>) -> UInt64 {
        bytes.reduce(0) { ($0 << 8) | UInt64($1) }
    }
}

/// Compatible spelling for the original head-only package user.
public typealias BoundRecordHeadClient<Channel: RecordHeadChannel> = BoundRecordClient<Channel>
