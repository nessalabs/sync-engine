#!/bin/sh
set -eu
cd "$(dirname "$0")/../../.."

if [ "$(uname -s)" != Darwin ]; then
    echo "Swift TLS fixture requires macOS 26 or later" >&2
    exit 2
fi

os_version=$(sw_vers -productVersion)
sdk_version=$(xcrun --sdk macosx --show-sdk-version)
if [ "${os_version%%.*}" -lt 26 ] || [ "${sdk_version%%.*}" -lt 26 ]; then
    echo "Swift TLS fixture requires macOS 26 and the Xcode 26 SDK or newer" >&2
    exit 2
fi

# macOS's bundled LibreSSL emits fixture certificates for which the observed
# Apple Security stack could not materialize the EC public key. Require the
# already-installed OpenSSL 3 fixture tool instead of altering trust policy.
if ! openssl version | grep -q '^OpenSSL 3\.'; then
    if ! command -v brew >/dev/null 2>&1; then
        echo "Swift TLS fixture requires OpenSSL 3 on PATH" >&2
        exit 2
    fi
    prefix=$(brew --prefix openssl@3)
    PATH="$prefix/bin:$PATH"
    export PATH
fi
if ! openssl version | grep -q '^OpenSSL 3\.'; then
    echo "Swift TLS fixture requires OpenSSL 3 on PATH" >&2
    exit 2
fi

package=clients/swift/e2e/tls
scratch=.build/swift-tls-fixture
swift build --package-path "$package" --scratch-path "$scratch" --product SwiftTLSFixture
bin_dir=$(swift build --package-path "$package" --scratch-path "$scratch" --show-bin-path)
NESSA_SWIFT_TLS_FIXTURE="$bin_dir/SwiftTLSFixture" \
  cargo test --locked --features tls,sqlite --test session_tls swift_tls_open_head_page_against_rust -- --ignored --nocapture
