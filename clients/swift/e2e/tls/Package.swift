// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "SwiftTLSFixture",
    platforms: [.macOS("26.0")],
    dependencies: [.package(name: "SyncEngine", path: "../../../..")],
    targets: [
        .executableTarget(name: "SwiftTLSFixture",
            dependencies: [.product(name: "NessaSync", package: "SyncEngine")]),
    ]
)
