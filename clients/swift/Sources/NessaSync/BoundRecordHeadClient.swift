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
    case source(RecordHeadRefusal)
    case incompatible(RecordCapabilityError)
}

/// Owns request numbering on one serialized, already-bound NSR1 v1 channel.
/// The host must verify the explicit peer dialect, authentication, Open/Bound
/// exchange and exact scope before transferring exclusive channel ownership.
/// Head is source metadata, never an authorization grant or local checkpoint.
/// This owner does not dispatch hints, perform retries, or restore a session.
public final class BoundRecordHeadClient<Channel: RecordHeadChannel> {
    private static var headerBytes: Int { 44 }
    private static var preamble: [UInt8] { [78, 83, 82, 49, 0, 1] } // NSR1 v1

    private var channel: Channel?
    private var request: UInt64?
    private let session: UInt64
    private let handle: UInt32
    private let generation: UInt64
    private let limit: Int
    private let scope: [UInt8]

    public init(
        channel: Channel, session: UInt64, handle: UInt32, generation: UInt64,
        lastRequest: UInt64, frameBodyLimit: Int, scopeFields: [String]
    ) throws(RecordHeadReadError) {
        guard session != 0, handle != 0, generation != 0, lastRequest != 0,
            lastRequest != UInt64.max, (2_048...8_192).contains(frameBodyLimit),
            scopeFields.count == 6
        else { throw .invalidBinding }

        var encoded: [UInt8] = []
        for field in scopeFields {
            let utf8 = field.utf8
            guard utf8.count <= 128, !field.unicodeScalars.allSatisfy({ $0.properties.isWhitespace }) else {
                throw .invalidBinding
            }
            encoded.append(UInt8(utf8.count))
            encoded.append(contentsOf: utf8)
        }
        self.scope = encoded
        self.channel = channel
        self.session = session
        self.handle = handle
        self.generation = generation
        self.limit = frameBodyLimit
        self.request = lastRequest + 1
    }

    deinit { channel?.close() }

    /// Explicitly retire the owner; no wire-level rollback is implied.
    public func close() {
        var owned = channel
        channel = nil
        owned?.close()
    }

    /// Read one v1 capability head. Healthy, correlated source refusals may be
    /// followed by another explicit operation; malformed/I/O/identity failures
    /// retire this owner and never trigger a fallback.
    public func readHead() throws(RecordHeadReadError) -> RecordSourceMetadata {
        guard var owned = channel else { throw .unavailableChannel }
        channel = nil
        var retain = false
        defer {
            if retain { channel = owned } else { owned.close() }
        }
        guard let next = request else { throw .unavailableChannel }
        request = next == .max ? nil : next + 1

        let echo = bytes(session) + bytes(next) + bytes(handle) + bytes(generation)
        let prefix = RecordCapabilityV1.requestPrefix
        let body = Self.preamble + [5, 0] + echo + bytes(UInt32(prefix.count)) + bytes(UInt32(0)) + prefix
        let lengthBytes: [UInt8]
        do {
            try owned.writeAll(bytes(UInt32(body.count)) + body)
            lengthBytes = try owned.readExactly(4)
        } catch { throw .unavailableChannel }
        guard lengthBytes.count == 4 else { throw .unavailableChannel }
        let length = Int(wireWord(lengthBytes[...]))
        // A Head cannot fragment: six length-prefixed IDs plus metadata fit the
        // minimum negotiated frame. Never read a body larger than that bound.
        let maximum = Self.headerBytes + scope.count + RecordCapabilityV1.metadataByteCount
        guard (Self.headerBytes...min(limit, maximum)).contains(length) else { throw .malformedReply }
        let reply: [UInt8]
        do { reply = try owned.readExactly(length) } catch { throw .unavailableChannel }
        guard reply.count == length else { throw .unavailableChannel }
        guard reply[0..<6].elementsEqual(Self.preamble), reply[6] == 0x85,
            reply[8..<36].elementsEqual(echo), wireWord(reply[40..<44]) == 0,
            wireWord(reply[36..<40]) == UInt64(length - Self.headerBytes)
        else { throw .malformedReply }

        let payload = reply[Self.headerBytes...]
        switch reply[7] {
        case 0:
            guard payload.count == scope.count + RecordCapabilityV1.metadataByteCount,
                payload.prefix(scope.count).elementsEqual(scope)
            else { throw .malformedReply }
            do {
                let metadata = try RecordCapabilityV1.admitMetadata(payload.dropFirst(scope.count))
                retain = true
                return metadata
            } catch {
                retain = error != .malformedPayload
                if !retain { throw .malformedReply }
                throw .incompatible(error)
            }
        case 8:
            let refusal: RecordCapabilityError
            do { refusal = try RecordCapabilityV1.decodeRefusal(payload) }
            catch { throw .malformedReply }
            retain = true
            throw .incompatible(refusal)
        default:
            guard payload.isEmpty, let refusal = RecordHeadRefusal(rawValue: reply[7]) else {
                throw .malformedReply
            }
            retain = refusal != .identityChanged
            throw .source(refusal)
        }
    }

    private func bytes<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
        withUnsafeBytes(of: value.bigEndian) { Array($0) }
    }

    private func wireWord(_ bytes: ArraySlice<UInt8>) -> UInt64 {
        bytes.reduce(0) { ($0 << 8) | UInt64($1) }
    }
}
