import Foundation
import NessaSync
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

private struct FixtureStdio: RecordHeadChannel {
    var failAfterFirstFrame = false
    var reads = 0
    func writeAll(_ bytes: [UInt8]) throws {
        try FileHandle.standardOutput.write(contentsOf: Data(bytes))
    }
    mutating func readExactly(_ count: Int) throws -> [UInt8] {
        if failAfterFirstFrame && reads == 2 {
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
    guard (arguments.count == 11 || arguments.count == 14), let session = UInt64(arguments[1]), let handle = UInt32(arguments[2]),
        let generation = UInt64(arguments[3]), let limit = Int(arguments[4])
    else { throw RecordHeadReadError.invalidBinding }
    if arguments.count == 14 {
        try page(arguments, session, handle, generation, limit)
        return
    }
    let client = try BoundRecordHeadClient(
        channel: FixtureStdio(), session: session, handle: handle, generation: generation,
        lastRequest: 1, frameBodyLimit: limit, scopeFields: Array(arguments[5...])
    )
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
private struct StoredRecord: Codable {
    let position: UInt64
    let id: String
    let payload: Data
}

private struct StoredPageState: Codable {
    let scope: Data?
    let rows: [StoredRecord]
}

private func page(_ args: [String], _ session: UInt64, _ handle: UInt32, _ generation: UInt64, _ limit: Int) throws {
    guard let recordLimit = Int(args[13]) else { throw RecordHeadReadError.invalidBinding }
    let mode = args[11]
    let path = URL(fileURLWithPath: args[12])
    let channel = FixtureStdio(failAfterFirstFrame: mode == "interrupt")
    let client = try BoundRecordClient(channel: channel, session: session, handle: handle,
        generation: generation, lastRequest: 1, frameBodyLimit: limit, scopeFields: Array(args[5..<11]),
        recordByteLimit: recordLimit)
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
