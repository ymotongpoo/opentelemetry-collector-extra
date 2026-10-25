#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# mdatagen's published module contains local replaces. Building it as a dependency
# ignores those replaces and resolves the published versions instead.
tool_dir=$(mktemp -d)
trap 'rm -rf "$tool_dir"' EXIT HUP INT TERM
(
  cd "$tool_dir"
  go mod init local/metadata-tools
  go get go.opentelemetry.io/collector/cmd/mdatagen@v0.163.0
  go build -o "$tool_dir/mdatagen" go.opentelemetry.io/collector/cmd/mdatagen
)
if [ "$#" -eq 0 ]; then
    set -- receiver/discordreceiver receiver/slackreceiver receiver/switchbotreceiver receiver/smartheimreceiver exporter/discordexporter exporter/slackexporter
fi
for component in "$@"; do
  "$tool_dir/mdatagen" "$component/metadata.yaml"
done
