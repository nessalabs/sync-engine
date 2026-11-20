import Foundation
import NessaSync
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

private struct FixtureStdio: RecordHeadChannel {
    var failAfterFirstFrame = false
    var writes = 0
    var reads = 0
    mutating func writeAll(_ bytes: [UInt8]) throws {
        writes += 1
        reads = 0
        try FileHandle.standardOutput.write(contentsOf: Data(bytes))
    }
    mutating func readExactly(_ count: Int) throws -> [UInt8] {
        if failAfterFirstFrame && writes == 2 && reads == 2 {
            #if canImport(Darwin) || canImport(Glibc)
            _ = shutdown(FileHandle.standardInput.fileDescriptor, Int32(SHUT_RDWR))
            #endif
            throw CocoaError(.fileReadUnknown)
        }
        reads += 1
        var result: [UInt8] = []
        while result.count < count {
            guard let part = try FileHandle.standardInput.read(upToCount: count - result.count),
                !part.isEmpty
            else { throw CocoaError(.fileReadUnknown) }
            result.append(contentsOf: part)
        }
        return result
    }
    // Python owns the fixture descriptors and waits for the process to exit.
    func close() {}
}

private func read() throws {
    let arguments = CommandLine.arguments
    guard (8...10).contains(arguments.count) else { throw RecordSessionOpenError.invalidOffer }
    let scope = Array(arguments[1..<7])
    let mode = arguments[7]
    if mode == "follow" {
        guard arguments.count == 10, let release = Int32(arguments[8])
        else { throw RecordSessionOpenError.invalidOffer }
        try followFixture(scope, release: release, path: URL(fileURLWithPath: arguments[9]))
        return
    }
    if ["tail", "tail-interrupt"].contains(mode), arguments.count == 9 {
        try tailFixture(scope, interrupted: mode == "tail-interrupt",
            path: URL(fileURLWithPath: arguments[8]))
        return
    }
    if ["tail-older", "older-interrupt", "older-restart"].contains(mode), arguments.count == 9 {
        try olderFixture(scope, mode: mode, path: URL(fileURLWithPath: arguments[8]))
        return
    }
    guard arguments.count <= 9 else { throw RecordSessionOpenError.invalidOffer }
    if mode == "deny-open" {
        try verifyDeniedOpen(scope: scope)
        return
    }
    let client = try openFixture(scope: scope, interrupt: mode == "interrupt")
    if mode == "overlap", arguments.count == 9, let release = Int32(arguments[8]) {
        try overlap(client, release: release)
        return
    }
    if arguments.count == 9 {
        try page(client, mode: mode, path: URL(fileURLWithPath: arguments[8]))
        return
    }
    guard mode == "head" else { throw RecordSessionOpenError.malformedControl }
    var report = Report()
    for _ in 0..<2 {
        do {
            let value = try client.readHead()
            print("head:\(value.position):\(value.writerRequirements)", to: &report)
        } catch .incompatible(.missingReaderCapabilities(let mask)) {
            print("missing-reader:\(mask)", to: &report)
        } catch .source(.identityChanged) {
            print("identity-changed", to: &report)
        }
    }
}
// Keep typed Open at a small boundary. Older Swift compilers can fail while
// lowering an outer do/catch that also conditionally initializes the owner.
private func openFixture(scope: [String], interrupt: Bool) throws -> BoundRecordClient<FixtureStdio> {
    try BoundRecordClient.open(
        channel: FixtureStdio(failAfterFirstFrame: interrupt), scopeFields: scope
    )
}

private func verifyDeniedOpen(scope: [String]) throws {
    do {
        _ = try openFixture(scope: scope, interrupt: false)
    } catch let error as RecordSessionOpenError {
        guard error == .refused(.denied) else { throw error }
        var report = Report()
        print("open-denied", to: &report)
        return
    }
    throw RecordSessionOpenError.malformedControl
}

private func overlap(_ client: BoundRecordClient<FixtureStdio>, release: Int32) throws {
    let gate = FileHandle(fileDescriptor: release, closeOnDealloc: true)
    var report = Report()
    print("bound", to: &report)
    guard try gate.read(upToCount: 1) == Data([1])
    else { throw CocoaError(.fileReadUnknown) }
    let metadata = try client.readHead()
    guard metadata.position == 7, metadata.writerRequirements == 0
    else { throw RecordHeadReadError.malformedReply }
    let first = try client.readPage(RecordPageRequest(after: 0, target: metadata.position,
        maxRecords: 2, maxPayloadBytes: 8192, maxRecordBytes: 8192))
    let last = try client.readPage(RecordPageRequest(after: first.nextPosition, target: metadata.position,
        maxRecords: 64, maxPayloadBytes: 8192, maxRecordBytes: 8192))
    let rows = first.records + last.records
    let positions: [UInt64] = [1, 2, 3, 4, 5, 6, 7]
    let ids = ["fact-1", "fact-2", "fact-3", "fact-4", "fact-5", "fact-6", "bulk-fact"]
    var payloads = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"].map { Array($0.utf8) }
    payloads.append([UInt8](repeating: 112, count: 4096))
    let expectedIDs = ids.map { Array($0.utf8) }
    let correctScopes = first.scope == client.boundScope && last.scope == client.boundScope
    let correctPositions = rows.map(\.position) == positions
    let correctContent = rows.map(\.idBytes) == expectedIDs && rows.map(\.payload) == payloads
    guard correctScopes, first.nextPosition == 2, last.nextPosition == 7,
        correctPositions, correctContent
    else { throw RecordHeadReadError.malformedReply }
    print("overlap:7", to: &report)
}

private func followFixture(_ scope: [String], release: Int32, path: URL) throws {
    let client = try RecordFollowClient.open(channel: FixtureStdio(), scopeFields: scope)
    defer { client.close() }
    var report = Report()
    print("bound", to: &report)
    let hint = try client.waitForHint()
    guard hint == 1 else { throw RecordHeadReadError.malformedReply }
    print("hint:1", to: &report)
    let gate = FileHandle(fileDescriptor: release, closeOnDealloc: true)
    guard try gate.read(upToCount: 1) == Data([1]) else { throw CocoaError(.fileReadUnknown) }

    let head = try client.readHead()
    guard head.position == 1 else { throw RecordHeadReadError.malformedReply }
    let request = RecordPageRequest(after: 0, target: head.position, maxRecords: 1,
        maxPayloadBytes: 8192, maxRecordBytes: 8192)
    let page = try client.readPage(request)
    guard page.scope == client.boundScope, page.nextPosition == 1, page.records.count == 1,
        let row = page.records.first, row.position == 1, row.idBytes == Array("fact-1".utf8),
        row.payload == Array("alpha".utf8)
    else { throw RecordHeadReadError.malformedReply }

    let saved = StoredRecord(position: row.position, id: "fact-1", payload: Data(row.payload))
    let updated = StoredPageState(scope: Data(page.scope.canonicalBytes), rows: [saved])
    try JSONEncoder().encode(updated).write(to: path, options: .atomic)
    print("follow:1:5", to: &report)
}

private func tailFixture(_ scope: [String], interrupted: Bool, path: URL) throws {
    let client = try RecordFollowClient.open(
        channel: FixtureStdio(failAfterFirstFrame: interrupted), scopeFields: scope)
    defer { client.close() }
    let request = RecordTailRequest(scope: client.boundScope, historyGeneration: 1, maxRecords: 2,
        maxPayloadBytes: 4000, maxRecordBytes: 1500)
    if interrupted {
        try verifyInterruptedTail(client, request: request)
        return
    }
    _ = try installFixtureTail(client, request: request, path: path)
    var report = Report()
    print("tail:4:3:1:3000", to: &report)
}

private func installFixtureTail(_ client: RecordFollowClient<FixtureStdio>, request: RecordTailRequest,
    path: URL) throws -> StoredTailState
{
    let tail = try client.readTail(request)
    guard tail.request == request, tail.metadata.position == 4, tail.firstPosition == 3,
        tail.oldestAvailable == 1, tail.records.count == 2
    else { throw RecordHeadReadError.malformedReply }
    var exact: [StoredRecord] = []
    for (offset, row) in tail.records.enumerated() {
        let position = offset + 3
        let id = "event-\(position)"
        guard row.position == UInt64(position), row.idBytes == Array(id.utf8),
            row.payload == [UInt8](repeating: UInt8(position), count: 1500)
        else { throw RecordHeadReadError.malformedReply }
        exact.append(StoredRecord(position: row.position, id: id, payload: Data(row.payload)))
    }
    let current = try Data(contentsOf: path)
    guard current == Data("uninstalled".utf8) else { throw RecordHeadReadError.invalidTailRequest }
    let state = StoredTailState(scope: Data(request.scope.canonicalBytes),
        historyGeneration: request.historyGeneration, watermark: tail.metadata.position,
        first: tail.firstPosition, floor: tail.oldestAvailable, rows: exact)
    try JSONEncoder().encode(state).write(to: path, options: .atomic)
    return state
}

private func olderFixture(_ scope: [String], mode: String, path: URL) throws {
    let client = try RecordFollowClient.open(
        channel: FixtureStdio(failAfterFirstFrame: mode == "older-interrupt"), scopeFields: scope)
    defer { client.close() }

    if mode == "tail-older" {
        let tail = RecordTailRequest(scope: client.boundScope, historyGeneration: 1, maxRecords: 2,
            maxPayloadBytes: 4000, maxRecordBytes: 1500)
        _ = try installFixtureTail(client, request: tail, path: path)
    }
    let checkpoint = try Data(contentsOf: path)
    let saved = try JSONDecoder().decode(StoredTailState.self, from: checkpoint)
    guard saved.scope == Data(client.boundScope.canonicalBytes), saved.historyGeneration == 1,
        saved.watermark == 4, saved.first == 3, saved.floor == 1, saved.rows.count == 2
    else { throw RecordHeadReadError.invalidOlderRequest }
    let request = RecordOlderRequest(scope: client.boundScope, historyGeneration: saved.historyGeneration,
        before: saved.first, maxRecords: 2, maxPayloadBytes: 4000, maxRecordBytes: 1500)
    if mode == "older-interrupt" {
        try verifyInterruptedOlder(client, request: request)
        return
    }
    let page = try client.readOlder(request)
    guard page.request == request, page.firstPosition == 1, page.oldestAvailable == 1,
        page.records.count == 2
    else { throw RecordHeadReadError.malformedReply }
    var prepend: [StoredRecord] = []
    var ids = Set(saved.rows.map { Array($0.id.utf8) })
    for (offset, row) in page.records.enumerated() {
        let position = offset + 1
        let id = "event-\(position)"
        guard row.position == UInt64(position), row.idBytes == Array(id.utf8),
            row.payload == [UInt8](repeating: UInt8(position), count: 1500),
            ids.insert(row.idBytes).inserted
        else { throw RecordHeadReadError.malformedReply }
        prepend.append(StoredRecord(position: row.position, id: id, payload: Data(row.payload)))
    }
    // Single-owner fixture: compare stored scope/generation/boundary and replace
    // rows and history coverage as one file. Never advance the saved live watermark.
    guard try Data(contentsOf: path) == checkpoint else { throw RecordHeadReadError.invalidOlderRequest }
    let next = StoredTailState(scope: saved.scope, historyGeneration: saved.historyGeneration,
        watermark: saved.watermark, first: page.firstPosition, floor: max(saved.floor, page.oldestAvailable),
        rows: prepend + saved.rows)
    try JSONEncoder().encode(next).write(to: path, options: .atomic)
    var report = Report()
    print("older:4:1:1:3000", to: &report)
}

private func verifyInterruptedOlder(_ client: RecordFollowClient<FixtureStdio>, request: RecordOlderRequest)
    throws
{
    do {
        _ = try client.readOlder(request)
    } catch .unavailableChannel {
        do {
            _ = try client.readOlder(request)
        } catch .unavailableChannel {
            var report = Report()
            print("older-interrupted", to: &report)
            return
        }
    }
    throw RecordHeadReadError.malformedReply
}

private func verifyInterruptedTail(_ client: RecordFollowClient<FixtureStdio>, request: RecordTailRequest)
    throws
{
    do {
        _ = try client.readTail(request)
    } catch .unavailableChannel {
        do {
            _ = try client.readTail(request)
        } catch .unavailableChannel {
            var report = Report()
            print("tail-interrupted", to: &report)
            return
        }
    }
    throw RecordHeadReadError.malformedReply
}

private struct StoredTailState: Codable {
    let scope: Data
    let historyGeneration: UInt64
    let watermark: UInt64
    let first: UInt64
    let floor: UInt64
    let rows: [StoredRecord]
}

private struct StoredRecord: Codable {
    let position: UInt64
    let id: String
    let payload: Data
}

private struct StoredPageState: Codable {
    let scope: Data?
    let rows: [StoredRecord]
}

private func page(_ client: BoundRecordClient<FixtureStdio>, mode: String, path: URL) throws {
    let previous = try JSONDecoder().decode(StoredPageState.self, from: Data(contentsOf: path))
    let established = Data(client.boundScope.canonicalBytes)
    guard previous.scope == established || (previous.scope == nil && previous.rows.isEmpty)
    else { throw RecordHeadReadError.invalidPageRequest }
    let after = previous.rows.last?.position ?? 0
    var report = Report()
    let request = RecordPageRequest(after: mode == "refuse" ? 6 : after, target: 7,
        maxRecords: mode == "first" ? 2 : 64, maxPayloadBytes: 8192,
        maxRecordBytes: mode == "refuse" ? 1024 : 8192)
    do {
        let receipt = try client.readPage(request)
        guard mode == "first" || mode == "resume" else { throw RecordHeadReadError.malformedReply }
        guard Data(receipt.scope.canonicalBytes) == established, receipt.request.after == after
        else { throw RecordHeadReadError.malformedReply }
        var next = previous.rows
        var ids = Set(previous.rows.map { Array($0.id.utf8) })
        for record in receipt.records {
            guard ids.insert(record.idBytes).inserted,
                let id = String(validating: record.idBytes, as: UTF8.self)
            else { throw RecordHeadReadError.malformedReply }
            next.append(StoredRecord(position: record.position, id: id, payload: Data(record.payload)))
        }
        guard next.last?.position == receipt.nextPosition else { throw RecordHeadReadError.malformedReply }
        // This is single-owner fixture storage, not a power-loss durability claim.
        // Position and all received records are replaced as one JSON file.
        try JSONEncoder().encode(StoredPageState(scope: established, rows: next)).write(to: path, options: .atomic)
        print("page:\(receipt.nextPosition):\(receipt.records.count)", to: &report)
    } catch RecordHeadReadError.unavailableChannel where mode == "interrupt" {
        print("interrupted", to: &report)
    } catch RecordHeadReadError.source(.oversizedRecord) where mode == "refuse" {
        print("oversized", to: &report)
    }
}

private struct Report: TextOutputStream {
    mutating func write(_ string: String) {
        FileHandle.standardError.write(Data(string.utf8))
    }
}
try read()
