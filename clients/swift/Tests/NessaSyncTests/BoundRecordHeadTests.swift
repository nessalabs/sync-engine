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

// One canonical page body in the existing Rust layout. Live parity is checked
// separately by the TCP fixture; these pure tests exercise receiver boundaries.
private let pageRequest = RecordPageRequest(after: 20, target: 23, maxRecords: 3,
    maxPayloadBytes: 5000, maxRecordBytes: 4096)
private let pageScopeBytes = fixtureScope.flatMap { [UInt8($0.utf8.count)] + Array($0.utf8) }

private func pageBody(
    ids: [String], payload: [UInt8], target: UInt64 = 23, scope: [UInt8] = pageScopeBytes
) -> [UInt8] {
    var data = be(UInt16(1))
    data.append(contentsOf: be(UInt64(23)))
    data.append(contentsOf: be(UInt64(0)))
    data.append(contentsOf: be(UInt64(0)))
    data.append(0)
    data.append(contentsOf: scope)
    for field in [UInt64(20), target, 3, 5000, 4096] { data.append(contentsOf: be(field)) }
    data.append(UInt8(ids.count))
    for (idx, id) in ids.enumerated() {
        data.append(contentsOf: be(UInt64(21 + idx)))
        data.append(UInt8(id.utf8.count))
        data.append(contentsOf: id.utf8)
        data.append(contentsOf: scope)
        let record = idx == 0 ? payload : []
        data.append(contentsOf: be(UInt32(record.count)))
        data.append(contentsOf: record)
    }
    return data
}

private func pageFrame(_ payload: ArraySlice<UInt8>, total: Int, offset: Int, status: UInt8 = 0) -> [UInt8] {
    var body: [UInt8] = [78, 83, 82, 49, 0, 1, 0x87, status]
    body.append(contentsOf: be(UInt64(1)))
    body.append(contentsOf: be(UInt64(2)))
    body.append(contentsOf: be(UInt32(1)))
    body.append(contentsOf: be(UInt64(1)))
    body.append(contentsOf: be(UInt32(total)))
    body.append(contentsOf: be(UInt32(offset)))
    body.append(contentsOf: payload)
    return be(UInt32(body.count)) + body
}

private func pageFrames(_ bytes: [UInt8]) -> [UInt8] {
    var result: [UInt8] = []
    for start in stride(from: 0, to: bytes.count, by: 2004) {
        result.append(contentsOf: pageFrame(bytes[start..<min(start + 2004, bytes.count)],
            total: bytes.count, offset: start))
    }
    return result
}

private func pageClient(
    _ channel: FixtureChannel, fields: [String] = fixtureScope
) throws -> BoundRecordClient<FixtureChannel> {
    try .init(channel: channel, session: 1, handle: 1, generation: 1, lastRequest: 1,
        frameBodyLimit: 2048, scopeFields: fields, recordByteLimit: 524_288)
}

struct BoundRecordPageTests {
    @Test func sameCursorCannotConfuseDistinctLogicalScopes() throws {
        let a = FixtureChannel()
        let b = FixtureChannel()
        let left = try pageClient(a, fields: Array(fixtureScope.dropLast()) + ["e\u{301}"])
        let right = try pageClient(b, fields: Array(fixtureScope.dropLast()) + ["é"])
        let leftBytes = left.boundScope.canonicalBytes
        let rightBytes = right.boundScope.canonicalBytes
        a.input = pageFrames(pageBody(ids: ["id"], payload: [], scope: leftBytes))
        b.input = pageFrames(pageBody(ids: ["id"], payload: [], scope: rightBytes))
        let receiptA = try left.readPage(pageRequest)
        let receiptB = try right.readPage(pageRequest)
        let sameLeftScope = try pageClient(FixtureChannel(), fields: Array(fixtureScope.dropLast()) + ["e\u{301}"])
        let owningScope = sameLeftScope.boundScope
        let checkpoint = pageRequest.after
        #expect(receiptA.request == receiptB.request)
        #expect(receiptA.nextPosition == receiptB.nextPosition)
        #expect(receiptA.scope == owningScope && receiptA.request.after == checkpoint)
        #expect(!(receiptB.scope == owningScope && receiptB.request.after == checkpoint))
    }

    @Test func reconstructsFragmentedPageAndRetainsExactIDs() throws {
        let channel = FixtureChannel()
        let bulk = [UInt8](repeating: 112, count: 4096)
        channel.input = pageFrames(pageBody(ids: ["e\u{301}", "é"], payload: bulk))
        let endpoint = try pageClient(channel)
        let receipt = try endpoint.readPage(pageRequest)
        #expect(receipt.request == pageRequest)
        #expect(receipt.nextPosition == 22)
        #expect(receipt.records.map(\.position) == [21, 22])
        #expect(receipt.records[0].payload == bulk)
        #expect(receipt.records[1].payload.isEmpty)
        #expect(receipt.records[0].idBytes != receipt.records[1].idBytes)
        #expect(channel.reads.filter { $0 == 2048 }.count == 2)
        #expect(channel.output.first?[10] == 7)
    }

    @Test func duplicatesRangesAndBudgetsCannotBecomeReceipts() throws {
        let bad = [
            pageBody(ids: ["same", "same"], payload: []),
            pageBody(ids: ["valid"], payload: [], target: 22),
            pageBody(ids: ["valid"], payload: [UInt8](repeating: 0, count: 4097)),
            pageBody(ids: [], payload: []),
            pageBody(ids: ["valid"], payload: []) + [0],
        ]
        for body in bad {
            let channel = FixtureChannel()
            channel.input = pageFrames(body)
            let endpoint = try pageClient(channel)
            #expect(throws: RecordHeadReadError.malformedReply) { try endpoint.readPage(pageRequest) }
            #expect(channel.closed == 1)
        }
        let channel = FixtureChannel()
        let endpoint = try pageClient(channel)
        let invalid = RecordPageRequest(after: 23, target: 23, maxRecords: 1,
            maxPayloadBytes: 1024, maxRecordBytes: 1024)
        #expect(throws: RecordHeadReadError.invalidPageRequest) { try endpoint.readPage(invalid) }
        #expect(channel.output.isEmpty)
    }

    @Test func partialPageInterruptionAndRefusalYieldNoReceipt() throws {
        let body = pageBody(ids: ["bulk"], payload: [UInt8](repeating: 0, count: 4096))
        let first = pageFrame(body.prefix(2004), total: body.count, offset: 0)
        let channel = FixtureChannel()
        channel.input = first
        let endpoint = try pageClient(channel)
        #expect(throws: RecordHeadReadError.unavailableChannel) { try endpoint.readPage(pageRequest) }
        #expect(channel.closed == 1)
        let refused = FixtureChannel()
        refused.input = first + pageFrame([][...], total: 0, offset: 0, status: 1)
        let later = try pageClient(refused)
        #expect(throws: RecordHeadReadError.source(.denied)) { try later.readPage(pageRequest) }
    }
}

private func controlFrame(_ kind: UInt8, _ fields: [UInt8]) -> [UInt8] {
    var body: [UInt8] = [78, 83, 67, 49, 0, 1, kind]
    body.append(contentsOf: fields)
    return be(UInt32(body.count)) + body
}

private func welcome(
    session: UInt64 = 1, mode: UInt8 = 1, limit: UInt32 = 2048,
    slots: UInt16 = 1, maximumRecord: UInt32 = 524_288
) -> [UInt8] {
    var fields: [UInt8] = be(session)
    fields.append(mode)
    fields.append(contentsOf: be(limit))
    fields.append(contentsOf: be(slots))
    fields.append(contentsOf: be(maximumRecord))
    return controlFrame(2, fields)
}

private func bound(
    session: UInt64 = 1, request: UInt64 = 1, handle: UInt32 = 1,
    generation: UInt64 = 1, scope: [UInt8] = pageScopeBytes
) -> [UInt8] {
    var fields: [UInt8] = be(session)
    fields.append(contentsOf: be(request))
    fields.append(contentsOf: be(handle))
    fields.append(contentsOf: be(generation))
    fields.append(contentsOf: scope)
    fields.append(contentsOf: be(UInt64.max)) // metadata only
    return controlFrame(4, fields)
}

struct RecordSessionOpenTests {
    @Test func openHandsOneNegotiatedChannelToDataOwner() throws {
        let channel = FixtureChannel()
        channel.input = welcome(maximumRecord: 4096) + bound() + reply()
        let owner = try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope, frameBodyLimit: 8192)
        let offer = try #require(channel.output.first)
        #expect(Array(offer[4..<13]) == [78, 83, 67, 49, 0, 1, 6, 1, 0])
        #expect(Array(offer[12..<16]) == be(UInt32(8192)))
        #expect(Array(offer[16..<18]) == be(UInt16(1)))
        #expect(Array(offer[18..<26]) == be(UInt64(1)))
        #expect(Array(offer[26...]) == pageScopeBytes)
        #expect(owner.boundScope.canonicalBytes == pageScopeBytes)
        let outsideLimit = RecordPageRequest(after: 1, target: 2, maxRecords: 1,
            maxPayloadBytes: 4096, maxRecordBytes: 4097)
        #expect(throws: RecordHeadReadError.invalidPageRequest) { try owner.readPage(outsideLimit) }
        #expect(channel.output.count == 1)
        #expect(try owner.readHead().position == 7) // Bound advisory head ignored.
        #expect(channel.output.count == 2)
        owner.close()
        #expect(channel.closed == 1)
    }

    @Test func malformedBindingOrOfferClosesWithoutDowngrade() throws {
        let different = Array(fixtureScope.dropLast()) + ["other"]
        let differentScope = different.flatMap { [UInt8($0.utf8.count)] + Array($0.utf8) }
        let bad: [[UInt8]] = [
            welcome(session: 0), welcome(limit: 4096), welcome(slots: 2),
            welcome(maximumRecord: 0), welcome(maximumRecord: 524_289),
            welcome() + bound(session: 2), welcome() + bound(request: 2),
            welcome() + bound(handle: 0), welcome() + bound(generation: 0),
            welcome() + bound(scope: differentScope),
        ]
        for input in bad {
            let channel = FixtureChannel()
            channel.input = input
            #expect(throws: RecordSessionOpenError.malformedControl) {
                try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
            }
            #expect(channel.output.count == 1)
            #expect(channel.closed == 1)
        }

        let channel = FixtureChannel()
        #expect(throws: RecordSessionOpenError.invalidOffer) {
            try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope, frameBodyLimit: 8193)
        }
        #expect(channel.output.isEmpty)
        #expect(channel.closed == 1)
    }

    @Test func oversizeControlStopsBeforeBodyRead() {
        let channel = FixtureChannel()
        channel.input = be(UInt32(2049))
        #expect(throws: RecordSessionOpenError.malformedControl) {
            try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
        }
        #expect(channel.reads == [4])
        #expect(channel.closed == 1)
    }

    @Test func wrongDialectOrderAndModeNeverInstallDataOwner() throws {
        var wrongVersion = welcome()
        wrongVersion[9] = 2
        for input in [wrongVersion, bound(), welcome(mode: 2), welcome() + welcome()] {
            let channel = FixtureChannel()
            channel.input = input
            #expect(throws: RecordSessionOpenError.incompatibleControl) {
                try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
            }
            #expect(channel.output.count == 1)
            #expect(channel.closed == 1)
        }
    }

    @Test func truncatedFramesAndCorrelatedRefusalsCloseExactlyOnce() throws {
        let complete = welcome() + bound()
        for count in 0..<complete.count {
            let channel = FixtureChannel()
            channel.input = Array(complete.prefix(count))
            #expect(throws: RecordSessionOpenError.unavailableChannel) {
                try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
            }
            #expect(channel.closed == 1)
        }
        for status in [1, 2, 3, 4, 5, 6] as [UInt8] {
            let channel = FixtureChannel()
            var fields = be(UInt64(1))
            fields.append(contentsOf: be(UInt64(1)))
            fields.append(status)
            channel.input = welcome() + controlFrame(5, fields)
            let reason = try #require(RecordHeadRefusal(rawValue: status))
            #expect(throws: RecordSessionOpenError.refused(reason)) {
                try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
            }
            #expect(channel.output.count == 1)
            #expect(channel.closed == 1)
        }
    }
}

private func recordHint(_ head: UInt64, session: UInt64 = 1, request: UInt64 = 1,
    status: UInt8 = 0, handle: UInt32 = 1, generation: UInt64 = 1
) -> [UInt8] {
    let payload = status == 0 ? be(head) : []
    var frame: [UInt8] = [78, 83, 82, 49, 0, 1, 64, status]
    frame.append(contentsOf: be(session))
    frame.append(contentsOf: be(request))
    frame.append(contentsOf: be(handle))
    frame.append(contentsOf: be(generation))
    frame.append(contentsOf: be(UInt32(payload.count)))
    frame.append(contentsOf: be(UInt32(0)))
    frame.append(contentsOf: payload)
    return be(UInt32(frame.count)) + frame
}

struct RecordFollowTests {
    @Test func serializedOpenStillRefusesFollowSignals() throws {
        let channel = FixtureChannel()
        channel.input = welcome() + bound() + recordHint(3)
        let owner = try BoundRecordClient.open(channel: channel, scopeFields: fixtureScope)
        #expect(channel.output.first?[10] == 6)
        #expect(throws: RecordHeadReadError.malformedReply) { try owner.readHead() }
        #expect(channel.closed == 1)
    }


    @Test func followUsesOneOwnerAndCoalescesHintsBetweenPageFragments() throws {
        let channel = FixtureChannel()
        let payload = [UInt8](repeating: 112, count: 4096)
        let data = pageBody(ids: ["bulk"], payload: payload)
        channel.input = welcome() + bound()
        channel.input.append(contentsOf: pageFrame(data.prefix(2004), total: data.count, offset: 0))
        channel.input.append(contentsOf: recordHint(24))
        channel.input.append(contentsOf: recordHint(23))
        channel.input.append(contentsOf: pageFrame(data[2004..<4008], total: data.count, offset: 2004))
        channel.input.append(contentsOf: pageFrame(data[4008...], total: data.count, offset: 4008))
        let owner = try RecordFollowClient.open(channel: channel, scopeFields: fixtureScope)
        #expect(channel.output.first?[10] == 7)
        let page = try owner.readPage(pageRequest)
        #expect(page.nextPosition == 21)
        #expect(page.records.first?.payload == payload)
        let readsBefore = channel.reads.count
        #expect(try owner.waitForHint() == 24)
        #expect(channel.reads.count == readsBefore)
        #expect(channel.output.count == 2)
    }

    @Test func malformedOrWrongBoundHintsAndPartialReadAreTerminal() throws {
        let frames = [recordHint(1, session: 2), recordHint(1, request: 2),
                      recordHint(1, handle: 2), recordHint(1, generation: 2),
                      recordHint(0, status: 8)]
        for frame in frames {
            let channel = FixtureChannel()
            channel.input = welcome() + bound() + frame
            let owner = try RecordFollowClient.open(channel: channel, scopeFields: fixtureScope)
            #expect(throws: RecordHeadReadError.malformedReply) { try owner.waitForHint() }
            #expect(throws: RecordHeadReadError.unavailableChannel) { try owner.readHead() }
            #expect(channel.closed == 1)
        }
        let truncated = FixtureChannel()
        truncated.input = welcome() + bound() + Array(recordHint(3).dropLast())
        let owner = try RecordFollowClient.open(channel: truncated, scopeFields: fixtureScope)
        #expect(throws: RecordHeadReadError.unavailableChannel) { try owner.waitForHint() }
        #expect(throws: RecordHeadReadError.unavailableChannel) { try owner.readHead() }
        #expect(truncated.closed == 1)
    }

    @Test func terminalHintWinsBeforeIncompletePageAndPreBindingHintRejectsOpen() throws {
        let channel = FixtureChannel()
        let data = pageBody(ids: ["bulk"], payload: [UInt8](repeating: 112, count: 4096))
        channel.input = welcome() + bound()
        channel.input.append(contentsOf: pageFrame(data.prefix(2004), total: data.count, offset: 0))
        channel.input.append(contentsOf: recordHint(0, status: 1))
        let owner = try RecordFollowClient.open(channel: channel, scopeFields: fixtureScope)
        #expect(throws: RecordHeadReadError.source(.denied)) { try owner.readPage(pageRequest) }
        #expect(throws: RecordHeadReadError.source(.denied)) { try owner.waitForHint() }
        #expect(channel.closed == 1)

        let setup = FixtureChannel()
        setup.input = welcome() + recordHint(1) + bound()
        #expect(throws: RecordSessionOpenError.incompatibleControl) {
            try RecordFollowClient.open(channel: setup, scopeFields: fixtureScope)
        }
        #expect(setup.closed == 1)
        #expect(setup.output.count == 1)
    }
}
