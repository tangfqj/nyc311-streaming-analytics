#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Optional pinned macOS ARM runtime. Linux/Windows users install a supported Java17.
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo 'Install Java 17 for your OS and set JAVA_HOME; this helper is macOS ARM only.'
  exit 1
fi
runtime_url='https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jre_aarch64_mac_hotspot_17.0.13_11.tar.gz'
archive="$(mktemp -t nyc311-java17).tar.gz"
trap 'rm -f "$archive" "$archive.sha256.txt"' EXIT
curl -fL --retry 3 "$runtime_url" -o "$archive"
curl -fL --retry 3 "$runtime_url.sha256.txt" -o "$archive.sha256.txt"
expected_sha="$(awk '{print $1}' "$archive.sha256.txt")"
actual_sha="$(shasum -a 256 "$archive" | awk '{print $1}')"
[ "$expected_sha" = "$actual_sha" ] || { echo 'Java checksum mismatch'; exit 1; }
mkdir -p data/runtime/java17
tar -xzf "$archive" -C data/runtime/java17 --strip-components=1
source scripts/env.sh
"$JAVA_HOME/bin/java" -version
