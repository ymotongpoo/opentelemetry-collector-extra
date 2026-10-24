#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
export GOTOOLCHAIN=go1.27.3
version="${1:-0.2.1}"
arch="${2:-$(dpkg --print-architecture)}"
case "$arch" in amd64|arm64) ;; *) echo 'unsupported architecture' >&2; exit 1 ;; esac
printf '%s\n' "$version" | LC_ALL=C grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || exit 1
# One manifest defines both development and packaged components.
mkdir -p build
sed "s/^  version: .*/  version: $version/" otelcol-builder.yaml > build/otelcol-builder.yaml
go run go.opentelemetry.io/collector/cmd/builder@v0.163.0 --config=build/otelcol-builder.yaml --skip-compilation
mkdir -p build/extra
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o build/extra/otelcol-extra .
