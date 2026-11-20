/// Committed source position advertised by the versioned record endpoint.
///
/// The position is advisory input for a finite pass, not a checkpoint or proof of
/// authorization. Writer requirements are retained for inspection, but they do
/// not prevent a read-only client from using a supported reader format.
public struct RecordSourceMetadata: Equatable, Sendable {
    public let position: UInt64
    public let writerRequirements: UInt64

    fileprivate init(position: UInt64, writerRequirements: UInt64) {
        self.position = position
        self.writerRequirements = writerRequirements
    }
}

/// Compatibility information from metadata or a correlated peer refusal.
public enum RecordCapabilityError: Error, Equatable, Sendable {
    case malformedPayload
    case unsupportedMetadataVersion(UInt16)
    case missingReaderCapabilities(UInt64)
}

/// The SourceHead v1 portion of the record operations defined in source-head-wire.md.
///
/// A host still owns endpoint selection, authenticated transport, frame bounds,
/// exact request/scope comparison, current authorization and durable commit.
/// These helpers act only on fields the host has correlated to its request.
/// They neither probe nor fall back to the legacy record endpoint.
public enum RecordCapabilityV1 {
    public static let version: UInt16 = 1
    public static let metadataByteCount = 26

    /// Version and implemented reader mask for kinds 5–7.
    ///
    /// No optional reader feature is assigned by the current Sync Engine contract.
    public static var requestPrefix: [UInt8] {
        [UInt8(version >> 8), UInt8(truncatingIfNeeded: version)] + [UInt8](repeating: 0, count: 8)
    }

    /// Decode and admit exactly the 26 already-correlated metadata bytes.
    ///
    /// A head response places these bytes after the exact scope echo. A page
    /// places them before the canonical page echo and record bytes. Do not pass
    /// an entire frame or use the result to bypass page and commit validation.
    public static func admitMetadata(_ bytes: ArraySlice<UInt8>) throws(RecordCapabilityError)
        -> RecordSourceMetadata
    {
        guard bytes.count == metadataByteCount else { throw .malformedPayload }
        let start = bytes.startIndex
        let observedVersion = word(bytes[start..<(start + 2)])
        guard observedVersion == version else { throw .unsupportedMetadataVersion(observedVersion) }
        let readerBits = wideWord(bytes[(start + 10)..<(start + 18)])
        guard readerBits == 0 else { throw .missingReaderCapabilities(readerBits) }
        return .init(
            position: wideWord(bytes[(start + 2)..<(start + 10)]),
            writerRequirements: wideWord(bytes[(start + 18)..<(start + 26)])
        )
    }

    /// Decode the exact detail of a complete, correlated status-8 refusal.
    ///
    /// The host must reject a refusal on legacy operations or after a partial
    /// success. A malformed detail is not a retryable transport failure.
    public static func decodeRefusal(_ bytes: ArraySlice<UInt8>) throws(RecordCapabilityError)
        -> RecordCapabilityError
    {
        switch (bytes.first, bytes.count) {
        case (1, 3):
            let refusedVersion = word(bytes.dropFirst())
            guard refusedVersion != version else { throw .malformedPayload }
            return .unsupportedMetadataVersion(refusedVersion)
        case (2, 9):
            let missingBits = wideWord(bytes.dropFirst())
            guard missingBits != 0 else { throw .malformedPayload }
            return .missingReaderCapabilities(missingBits)
        default:
            throw .malformedPayload
        }
    }

    private static func word(_ bytes: ArraySlice<UInt8>) -> UInt16 {
        bytes.reduce(0) { ($0 << 8) | UInt16($1) }
    }

    private static func wideWord(_ bytes: ArraySlice<UInt8>) -> UInt64 {
        bytes.reduce(0) { ($0 << 8) | UInt64($1) }
    }
}
