#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-}" = vet ]; then
  printf 'Checking distribution\n'
  go vet ./...
fi
for module in receiver/discordreceiver receiver/slackreceiver exporter/discordexporter exporter/slackexporter exporter/s3tablesexporter; do
  printf 'Checking %s\n' "$module"
  (cd "$module" && go "$@" ./...)
done
