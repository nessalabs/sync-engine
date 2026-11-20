import Foundation
import NessaSync

private struct FixtureStdio: RecordHeadChannel {
    func writeAll(_ bytes: [UInt8]) throws {
        try FileHandle.standardOutput.write(contentsOf: Data(bytes))
    }
    func readExactly(_ count: Int) throws -> [UInt8] {
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
    guard arguments.count == 11, let session = UInt64(arguments[1]), let handle = UInt32(arguments[2]),
        let generation = UInt64(arguments[3]), let limit = Int(arguments[4])
    else { throw RecordHeadReadError.invalidBinding }
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
private struct Report: TextOutputStream {
    mutating func write(_ string: String) {
        FileHandle.standardError.write(Data(string.utf8))
    }
}
try read()
