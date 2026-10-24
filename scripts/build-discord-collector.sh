#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

version="${1:-0.1.1}"
arch="${2:-$(dpkg --print-architecture)}"
case "$arch" in amd64|arm64) ;; *) echo "unsupported architecture: $arch" >&2; exit 1 ;; esac
printf '%s\n' "$version" | LC_ALL=C grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || {
    echo 'version must have the form 1.2.3' >&2; exit 1;
}

mkdir -p build/discord
sed "s/^  version: .*/  version: $version/" distributions/discord/otelcol-builder.yaml > build/discord-builder.yaml
# Generate with the host toolchain; cross-compile only the resulting collector.
go run go.opentelemetry.io/collector/cmd/builder@v0.163.0 \
    --config=build/discord-builder.yaml --skip-compilation
(
    cd build/discord
    GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o discord-otelcol .
)
