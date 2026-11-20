import Testing
import NessaSync

private final class FixtureChannel: RecordHeadChannel {
    var input: [UInt8] = []
    var output: [[UInt8]] = []
    var reads: [Int] = []
    var offset = 0
    var closed = 0
    func writeAll(_ bytes: [UInt8]) { output.append(bytes) }
    func readExactly(_ count: Int) -> [UInt8] {
        reads.append(count)
        let end = min(offset + count, input.count)
        defer { offset = end }
        return Array(input[offset..<end])
    }
    func close() { closed += 1 }
}
private func be<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
    withUnsafeBytes(of: value.bigEndian) { Array($0) }
}
private let fixtureScope = ["device", "origin", "transcript", "first", "text-v1", "epoch"]
private func reply(
    scope: [String] = fixtureScope, session: UInt64 = 1, request: UInt64 = 2,
    status: UInt8 = 0, position: UInt64 = 7, supplied: [UInt8]? = nil
) -> [UInt8] {
    let identity = scope.flatMap { [UInt8($0.utf8.count)] + Array($0.utf8) }
    var metadata: [UInt8] = be(UInt16(1))
    metadata.append(contentsOf: be(position))
    metadata.append(contentsOf: be(UInt64(0)))
    metadata.append(contentsOf: be(UInt64.max))
    let payload = supplied ?? (identity + metadata)

    var body: [UInt8] = [78, 83, 82, 49, 0, 1, 0x85, status]
    body.append(contentsOf: be(session))
    body.append(contentsOf: be(request))
    body.append(contentsOf: be(UInt32(1)))
    body.append(contentsOf: be(UInt64(1)))
    body.append(contentsOf: be(UInt32(payload.count)))
    body.append(contentsOf: be(UInt32(0)))
    body.append(contentsOf: payload)
    return be(UInt32(body.count)) + body
}
private func client(
    _ channel: FixtureChannel, _ session: UInt64 = 1, _ scope: [String] = fixtureScope
) throws -> BoundRecordHeadClient<FixtureChannel> {
    try .init(channel: channel, session: session, handle: 1, generation: 1,
        lastRequest: 1, frameBodyLimit: 2048, scopeFields: scope)
}

struct BoundRecordHeadTests {
    @Test func deterministicStressAndExactRequest() throws {
        var seed: UInt64 = 0x4caa_7701
        for session in 1...16 as ClosedRange<UInt64> {
            let channel = FixtureChannel()
            let scope = fixtureScope.dropLast() + ["e\u{301}🌍"]
            let endpoint = try client(channel, session, Array(scope))
            for request in 2...21 as ClosedRange<UInt64> {
                seed = seed &* 6_364_136_223_846_793_005 &+ 1
                channel.input += reply(scope: Array(scope), session: session, request: request, position: seed)
                let metadata = try endpoint.readHead()
                #expect(metadata.position == seed)
                #expect(metadata.writerRequirements == .max)
                let message = try #require(channel.output.last)
                #expect(message.count == 58)
                #expect(Array(message[0..<12]) == [0, 0, 0, 54, 78, 83, 82, 49, 0, 1, 5, 0])
                #expect(Array(message[12..<20]) == be(session))
                #expect(Array(message[20..<28]) == be(request))
                #expect(Array(message[28..<40]) == be(UInt32(1)) + be(UInt64(1)))
                #expect(Array(message[40..<48]) == [0, 0, 0, 10, 0, 0, 0, 0])
                #expect(Array(message.suffix(10)) == RecordCapabilityV1.requestPrefix)
            }
            endpoint.close()
            endpoint.close()
            #expect(channel.closed == 1)
        }
    }

    @Test func allTruncatedPrefixesCloseAndOversizeDoesNotReadBody() throws {
        let valid = reply()
        for count in 0..<valid.count {
            let channel = FixtureChannel()
            channel.input = Array(valid.prefix(count))
            let endpoint = try client(channel)
            #expect(throws: RecordHeadReadError.unavailableChannel) { try endpoint.readHead() }
            #expect(throws: RecordHeadReadError.unavailableChannel) { try endpoint.readHead() }
            #expect(channel.closed == 1)
            #expect(channel.output.count == 1)
        }
        let channel = FixtureChannel()
        channel.input = be(UInt32.max)
        let endpoint = try client(channel)
        #expect(throws: RecordHeadReadError.malformedReply) { try endpoint.readHead() }
        #expect(channel.reads == [4])
        #expect(channel.closed == 1)
    }

    @Test func scopeIsByteExactAndAllCorrelationFieldsMatter() throws {
        let fields = Array(fixtureScope.dropLast()) + ["e\u{301}"]
        let other = Array(fixtureScope.dropLast()) + ["é"]
        var bad: [[UInt8]] = [reply(scope: other)]
        for index in [4, 9, 10, 19, 27, 31, 39, 43, 47] {
            var data = reply(scope: fields)
            data[index] ^= 1
            bad.append(data)
        }
        for frame in bad {
            let channel = FixtureChannel()
            channel.input = frame
            let endpoint = try client(channel, 1, fields)
            #expect(throws: RecordHeadReadError.malformedReply) { try endpoint.readHead() }
            #expect(channel.closed == 1)
        }
    }

    @Test func canonicalRefusalAndChannelRetirement() throws {
        for status in [1, 2, 3, 4, 5, 6] as [UInt8] {
            let channel = FixtureChannel()
            channel.input = reply(status: status, supplied: []) + reply(request: 3)
            let endpoint = try client(channel)
            let reason = try #require(RecordHeadRefusal(rawValue: status))
            #expect(throws: RecordHeadReadError.source(reason)) { try endpoint.readHead() }
            if reason == .identityChanged {
                #expect(throws: RecordHeadReadError.unavailableChannel) { try endpoint.readHead() }
                #expect(channel.closed == 1)
            } else {
                #expect(try endpoint.readHead().position == 7)
            }
        }
        let channel = FixtureChannel()
        channel.input = reply(status: 8, supplied: [2] + be(UInt64(1))) + reply(request: 3)
        let endpoint = try client(channel)
        #expect(throws: RecordHeadReadError.incompatible(.missingReaderCapabilities(1))) { try endpoint.readHead() }
        #expect(try endpoint.readHead().position == 7)
    }
}
