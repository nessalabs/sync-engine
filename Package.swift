// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "NessaSync",
    platforms: [.iOS(.v18), .macOS(.v15)],
    products: [.library(name: "NessaSync", targets: ["NessaSync"])],
    targets: [
        .target(name: "NessaSync", path: "clients/swift/Sources/NessaSync"),
        .testTarget(
            name: "NessaSyncTests",
            dependencies: ["NessaSync"],
            path: "clients/swift/Tests/NessaSyncTests",
            resources: [.copy("Fixtures")]
        ),
    ]
)
