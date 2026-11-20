/// Failure of the existing single-scope experimental Open exchange.
/// Failure always closes the transferred channel; there is no fallback.
public enum RecordSessionOpenError: Error, Equatable, Sendable {
    case invalidOffer
    case unavailableChannel
    case incompatibleControl
    case malformedControl
    case refused(RecordHeadRefusal)
}

extension BoundRecordClient {
    /// Select the explicit serialized v1 record endpoint, using a channel the
    /// trusted host has already authenticated. The caller owns dialing, channel
    /// trust and absolute I/O deadlines. This does not discover peer support.
    ///
    /// Transfer exclusive ownership of channel, even on failure. Only a
    /// correlated Welcome followed by Bound installs a data owner. The binding
    /// and advisory head are not authorization or a durable checkpoint.
    public static func open(
        channel: Channel, scopeFields: [String], frameBodyLimit: Int = 2048
    ) throws(RecordSessionOpenError) -> BoundRecordClient<Channel> {
        var owned = channel
        var transferred = false
        defer { if !transferred { owned.close() } }
        guard (2048...8192).contains(frameBodyLimit) else { throw .invalidOffer }

        let scope: RecordScopeIdentity
        do { scope = try RecordScopeIdentity(validatingFields: scopeFields) }
        catch { throw .invalidOffer }

        var offer: [UInt8] = Control.preamble
        offer.append(contentsOf: [6, 1]) // Open, explicit per-operation mode
        offer.append(contentsOf: Control.bytes(UInt32(frameBodyLimit)))
        offer.append(contentsOf: Control.bytes(UInt16(1))) // one scope
        offer.append(contentsOf: Control.bytes(UInt64(1))) // first request
        offer.append(contentsOf: scope.canonicalBytes)
        do { try owned.writeAll(Control.bytes(UInt32(offer.count)) + offer) }
        catch { throw .unavailableChannel }

        let welcome = try Control.read(&owned, limit: frameBodyLimit)
        guard welcome[6] == 2 else { throw .incompatibleControl }
        guard welcome.count == 26 else { throw .malformedControl }
        let session = Control.number(welcome[7..<15])
        guard welcome[15] == 1 else { throw .incompatibleControl }
        let chosen = Int(Control.number(welcome[16..<20]))
        let bindingCount = Control.number(welcome[20..<22])
        let recordLimit = Int(Control.number(welcome[22..<26]))
        guard session != 0, (2048...frameBodyLimit).contains(chosen), bindingCount == 1,
            (1...524_288).contains(recordLimit)
        else { throw .malformedControl }

        let reply = try Control.read(&owned, limit: chosen)
        switch reply[6] {
        case 4:
            let scopeBytes = scope.canonicalBytes
            guard reply.count == 43 + scopeBytes.count,
                Control.number(reply[7..<15]) == session, Control.number(reply[15..<23]) == 1,
                reply[35..<(35 + scopeBytes.count)].elementsEqual(scopeBytes)
            else { throw .malformedControl }
            let handle = UInt32(Control.number(reply[23..<27]))
            let generation = Control.number(reply[27..<35])
            let client: BoundRecordClient<Channel>
            do {
                client = try BoundRecordClient(channel: owned, session: session, handle: handle,
                    generation: generation, lastRequest: 1, frameBodyLimit: chosen, scope: scope,
                    recordByteLimit: recordLimit)
            } catch { throw .malformedControl }
            transferred = true
            return client
        case 5:
            guard reply.count == 24, Control.number(reply[7..<15]) == session,
                Control.number(reply[15..<23]) == 1
            else { throw .malformedControl }
            guard let reason = RecordHeadRefusal(rawValue: reply[23]) else { throw .incompatibleControl }
            throw .refused(reason)
        default:
            throw .incompatibleControl
        }
    }
}

private enum Control {
    static let preamble: [UInt8] = [78, 83, 67, 49, 0, 1] // NSC1 v1

    static func read<C: RecordHeadChannel>(_ channel: inout C, limit: Int) throws(RecordSessionOpenError)
        -> [UInt8]
    {
        let length = Int(number(try exact(&channel, 4)[...]))
        guard (7...limit).contains(length) else { throw .malformedControl }
        let body = try exact(&channel, length)
        guard body.prefix(6).elementsEqual(preamble) else { throw .incompatibleControl }
        return body
    }

    private static func exact<C: RecordHeadChannel>(_ channel: inout C, _ count: Int)
        throws(RecordSessionOpenError) -> [UInt8]
    {
        let result: [UInt8]
        do { result = try channel.readExactly(count) }
        catch { throw .unavailableChannel }
        guard result.count == count else { throw .unavailableChannel }
        return result
    }

    static func bytes<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
        withUnsafeBytes(of: value.bigEndian) { Array($0) }
    }

    static func number(_ bytes: ArraySlice<UInt8>) -> UInt64 {
        bytes.reduce(0) { ($0 << 8) | UInt64($1) }
    }
}
