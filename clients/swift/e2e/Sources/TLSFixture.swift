#if os(macOS)
import CryptoKit
import Foundation
import Network
import NessaSync
import Security

private enum TLSFixtureError: Error {
    case invalidFixture
    case missingIdentity
    case unavailableCertificatePublicKey
    case unavailablePrivatePublicKey
    case peerRejected
    case connection
    case deadline
    case unexpectedResult
}

private enum FixtureTrust: Equatable {
    case exactLeaf
    case wrongLeaf
    case failedTrust
}

private final class Callback<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private let ready = DispatchSemaphore(value: 0)
    private var value: Value?

    func finish(_ result: Value) {
        lock.lock()
        guard value == nil else { lock.unlock(); return }
        value = result
        lock.unlock()
        ready.signal()
    }

    func stored() -> Value? {
        lock.lock()
        defer { lock.unlock() }
        return value
    }

    func wait(until deadline: DispatchTime) throws -> Value {
        guard ready.wait(timeout: deadline) == .success, let value = stored()
        else { throw TLSFixtureError.deadline }
        return value
    }
}

private struct FixtureTLSChannel: RecordHeadChannel {
    let connection: NWConnection
    let deadline: DispatchTime

    static func connect(port: NWEndpoint.Port, fixture: URL) throws -> Self {
        let anchors = try Data(contentsOf: fixture.appendingPathComponent("ca.der"))
        let expected = try Data(contentsOf: fixture.appendingPathComponent("server.der"))
        let local = try localIdentity(fixture)

        let queue = DispatchQueue(label: "nessa.swift.tls.fixture")
        let tls = NWProtocolTLS.Options()
        let options = tls.securityProtocolOptions
        let name = "server.fixture.test"
        let alpn = "nessa-sync-session/1"
        sec_protocol_options_set_min_tls_protocol_version(options, .TLSv13)
        sec_protocol_options_set_max_tls_protocol_version(options, .TLSv13)
        sec_protocol_options_set_tls_resumption_enabled(options, false)
        sec_protocol_options_set_tls_tickets_enabled(options, false)
        sec_protocol_options_set_tls_server_name(options, name)
        sec_protocol_options_add_tls_application_protocol(options, alpn)
        sec_protocol_options_set_local_identity(options, local)

        let verified = Callback<FixtureTrust>()
        sec_protocol_options_set_verify_block(options, { _, secTrust, complete in
            let trust = sec_trust_copy_ref(secTrust).takeRetainedValue()
            let policy = SecPolicyCreateSSL(true, name as CFString)
            guard let ca = SecCertificateCreateWithData(nil, anchors as CFData),
                SecTrustSetPolicies(trust, policy) == errSecSuccess,
                SecTrustSetAnchorCertificates(trust, [ca] as CFArray) == errSecSuccess,
                SecTrustSetAnchorCertificatesOnly(trust, true) == errSecSuccess,
                SecTrustSetNetworkFetchAllowed(trust, false) == errSecSuccess,
                SecTrustEvaluateWithError(trust, nil),
                let chain = SecTrustCopyCertificateChain(trust) as? [SecCertificate],
                let leaf = chain.first
            else {
                verified.finish(.failedTrust)
                complete(false)
                return
            }
            let exact = SecCertificateCopyData(leaf) as Data == expected
            verified.finish(exact ? .exactLeaf : .wrongLeaf)
            complete(exact)
        }, queue)

        let endpoint = NWEndpoint.hostPort(host: "127.0.0.1", port: port)
        let parameters = NWParameters(tls: tls, tcp: NWProtocolTCP.Options())
        let connection = NWConnection(to: endpoint, using: parameters)
        let ready = Callback<Bool>()
        connection.stateUpdateHandler = { state in
            switch state {
            case .ready: ready.finish(true)
            case .failed, .cancelled, .waiting: ready.finish(false)
            default: break
            }
        }
        let deadline = DispatchTime.now() + .seconds(4)
        var transferred = false
        defer { if !transferred { connection.cancel() } }
        connection.start(queue: queue)
        guard try ready.wait(until: deadline) else {
            if verified.stored() == .wrongLeaf { throw TLSFixtureError.peerRejected }
            throw TLSFixtureError.connection
        }
        guard verified.stored() == .exactLeaf,
            let metadata = connection.metadata(definition: NWProtocolTLS.definition) as? NWProtocolTLS.Metadata
        else { throw TLSFixtureError.connection }
        let negotiated = metadata.securityProtocolMetadata
        guard sec_protocol_metadata_get_negotiated_tls_protocol_version(negotiated) == .TLSv13,
            !sec_protocol_metadata_get_early_data_accepted(negotiated),
            let protocolName = sec_protocol_metadata_get_negotiated_protocol(negotiated),
            String(cString: protocolName) == alpn
        else { throw TLSFixtureError.connection }
        transferred = true
        return Self(connection: connection, deadline: deadline)
    }

    private static func localIdentity(_ fixture: URL) throws -> sec_identity_t {
        let bytes = try Data(contentsOf: fixture.appendingPathComponent("client.der"))
        let pem = try String(contentsOf: fixture.appendingPathComponent("client.key"), encoding: .utf8)
        let privateKey = try P256.Signing.PrivateKey(pemRepresentation: pem)
        let attributes: [String: Any] = [
            kSecAttrKeyType as String: kSecAttrKeyTypeECSECPrimeRandom,
            kSecAttrKeyClass as String: kSecAttrKeyClassPrivate,
            kSecAttrIsPermanent as String: false,
        ]
        // Construct process-owned objects directly. No import, keychain items,
        // system trust roots or persistent credential store are involved.
        guard let cert = SecCertificateCreateWithData(nil, bytes as CFData),
            let key = SecKeyCreateWithData(privateKey.x963Representation as CFData, attributes as CFDictionary, nil)
        else { throw TLSFixtureError.missingIdentity }
        guard SecCertificateCopyKey(cert) != nil else { throw TLSFixtureError.unavailableCertificatePublicKey }
        guard SecKeyCopyPublicKey(key) != nil else { throw TLSFixtureError.unavailablePrivatePublicKey }
        guard let identity = SecIdentityCreate(nil, cert, key), let local = sec_identity_create(identity)
        else { throw TLSFixtureError.missingIdentity }
        return local
    }

    func writeAll(_ bytes: [UInt8]) throws {
        let sent = Callback<Bool>()
        connection.send(content: Data(bytes), completion: .contentProcessed { error in
            sent.finish(error == nil)
        })
        guard try sent.wait(until: deadline) else { throw TLSFixtureError.connection }
    }

    func readExactly(_ count: Int) throws -> [UInt8] {
        var result: [UInt8] = []
        while result.count < count {
            let received = Callback<Data>()
            connection.receive(minimumIncompleteLength: 1, maximumLength: count - result.count) { data, _, _, error in
                received.finish(error == nil ? data ?? Data() : Data())
            }
            let data = try received.wait(until: deadline)
            guard !data.isEmpty else { throw TLSFixtureError.connection }
            result.append(contentsOf: data)
        }
        return result
    }

    func close() { connection.cancel() }
}

// Separate ordinary throwing layers so Swift 6.1 does not lower a typed Open
// result and conditionally initialized owner in one fixture catch scope.
private func openTLS(_ channel: FixtureTLSChannel, receiver: String) throws -> BoundRecordClient<FixtureTLSChannel> {
    let scope = [receiver, "origin", "transcript", "first", "text", "epoch"]
    return try BoundRecordClient.open(channel: channel, scopeFields: scope)
}

private func expectDenied(_ channel: FixtureTLSChannel) throws {
    do { _ = try openTLS(channel, receiver: "fixture-denied") }
    catch let error as RecordSessionOpenError {
        guard error == .refused(.denied) else { throw error }
        return
    }
    throw TLSFixtureError.unexpectedResult
}

private func expectPeerMismatch(port: NWEndpoint.Port, fixture: URL) throws {
    do {
        let connection = try FixtureTLSChannel.connect(port: port, fixture: fixture)
        connection.close()
    } catch TLSFixtureError.peerRejected { return }
    throw TLSFixtureError.unexpectedResult
}

func runTLSFixture(_ arguments: [String]) throws {
    guard arguments.count == 3, let portNumber = UInt16(arguments[0]),
        let port = NWEndpoint.Port(rawValue: portNumber)
    else { throw TLSFixtureError.invalidFixture }
    let fixture = URL(fileURLWithPath: arguments[1], isDirectory: true)
    let mode = arguments[2]
    if mode == "peer-mismatch" {
        try expectPeerMismatch(port: port, fixture: fixture)
        print("peer-mismatch-before-record-dispatch")
        return
    }
    let channel = try FixtureTLSChannel.connect(port: port, fixture: fixture)
    if mode == "denied" {
        try expectDenied(channel)
        print("typed-scope-denied")
        return
    }
    guard mode == "healthy" else { channel.close(); throw TLSFixtureError.invalidFixture }
    let client = try openTLS(channel, receiver: "peer")
    defer { client.close() }
    let head = try client.readHead()
    guard head.position == 3, head.writerRequirements == 0 else { throw TLSFixtureError.unexpectedResult }
    let request = RecordPageRequest(after: 0, target: head.position, maxRecords: 3,
        maxPayloadBytes: 3000, maxRecordBytes: 1000)
    let page = try client.readPage(request)
    guard page.scope == client.boundScope, page.nextPosition == 3, page.records.count == 3
    else { throw TLSFixtureError.unexpectedResult }
    for (index, record) in page.records.enumerated() {
        let number = index + 1
        guard record.position == UInt64(number), record.idBytes == Array("record-\(number)".utf8),
            record.payload == [UInt8](repeating: UInt8(number), count: 1000)
        else { throw TLSFixtureError.unexpectedResult }
    }
    print("tls13-open-head-page:3:3000")
}
#endif
