import Foundation
import Testing
@testable import NessaSync

struct RecordSourceMetadataTests {
    @Test func rustWireVectors() throws {
        let bytes = try vectors()
        #expect(RecordCapabilityV1.requestPrefix == bytes["request"])
        let empty = try RecordCapabilityV1.admitMetadata(try #require(bytes["empty-head"])[...])
        #expect(empty.position == 0)
        #expect(empty.writerRequirements == 0)
        let writerOnly = try RecordCapabilityV1.admitMetadata(try #require(bytes["writer-only"])[...])
        #expect(writerOnly.position == 0xfedc_ba98_7654_3210)
        #expect(writerOnly.writerRequirements == UInt64.max)

        let required = try #require(bytes["reader-required"])
        #expect(throws: RecordCapabilityError.missingReaderCapabilities(0x8000_0000_0000_0001)) {
            try RecordCapabilityV1.admitMetadata(required[...])
        }
        let nextVersion = try #require(bytes["next-version"])
        #expect(throws: RecordCapabilityError.unsupportedMetadataVersion(2)) {
            try RecordCapabilityV1.admitMetadata(nextVersion[...])
        }

        let versionRefusal = try #require(bytes["version-refusal"])
        #expect(try RecordCapabilityV1.decodeRefusal(versionRefusal[...]) == .unsupportedMetadataVersion(2))
        let readerRefusal = try #require(bytes["reader-refusal"])
        #expect(try RecordCapabilityV1.decodeRefusal(readerRefusal[...]) == .missingReaderCapabilities(0x8000_0000_0000_0001))
    }

    @Test func exactBoundsAndNonzeroSliceOrigin() throws {
        let metadata = try #require(try vectors()["empty-head"])
        let prefixed: [UInt8] = [12, 13] + metadata + [14]
        #expect(try RecordCapabilityV1.admitMetadata(prefixed[2..<28]).position == 0)
        for count in 0..<metadata.count {
            #expect(throws: RecordCapabilityError.malformedPayload) {
                try RecordCapabilityV1.admitMetadata(metadata.prefix(count))
            }
        }
        #expect(throws: RecordCapabilityError.malformedPayload) {
            try RecordCapabilityV1.admitMetadata(prefixed[2...])
        }
    }

    @Test func invalidRefusalsDoNotBecomeCompatibilityAdvice() {
        let invalid: [[UInt8]] = [[], [0], [1, 0], [1, 0, 1], [1, 0, 2, 0], [255, 0, 2],
                                   [2, 0, 0, 0, 0, 0, 0, 0], [2, 0, 0, 0, 0, 0, 0, 0, 0]]
        for value in invalid {
            #expect(throws: RecordCapabilityError.malformedPayload) {
                try RecordCapabilityV1.decodeRefusal(value[...])
            }
        }
    }

    private func vectors() throws -> [String: [UInt8]] {
        let url = try #require(Bundle.module.url(forResource: "capability-v1", withExtension: "tsv", subdirectory: "Fixtures"))
        var output: [String: [UInt8]] = [:]
        for line in try String(contentsOf: url, encoding: .utf8).split(separator: "\n") where !line.hasPrefix("#") {
            let fields = line.split(separator: "\t")
            try #require(fields.count == 2)
            let hex = Array(fields[1])
            try #require(hex.count.isMultiple(of: 2))
            var bytes: [UInt8] = []
            for index in stride(from: 0, to: hex.count, by: 2) {
                bytes.append(try #require(UInt8(String(hex[index...index + 1]), radix: 16)))
            }
            let name = String(fields[0])
            try #require(output.updateValue(bytes, forKey: name) == nil)
        }
        return output
    }
}
