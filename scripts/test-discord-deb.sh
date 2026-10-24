#!/bin/sh
# Install, reinstall, remove, and purge in a disposable Ubuntu container.
set -eu
package="$(realpath "${1:?usage: test-discord-deb.sh PACKAGE.deb}")"
test "$(dpkg-deb -f "$package" Package)" = discord-otelcol
test "$(dpkg-deb -f "$package" Architecture)" = amd64
docker run --rm -i -v "$package:/tmp/discord-otelcol.deb:ro" ubuntu:24.04 sh -s <<'EOF'
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates systemd >/dev/null
dpkg -i /tmp/discord-otelcol.deb
discord-otelcol --version
DISCORD_BOT_TOKEN=test discord-otelcol validate --config=/etc/discord-otelcol/config.yaml
discord-otelcol components > /tmp/components.txt
grep -q 'name: discord' /tmp/components.txt
grep -q 'name: otlp_http' /tmp/components.txt
systemd-analyze verify --man=no /usr/lib/systemd/system/discord-otelcol.service
test "$(stat -c '%a:%U:%G' /etc/discord-otelcol/environment)" = 640:root:root
test ! -e /etc/systemd/system/multi-user.target.wants/discord-otelcol.service
# A reinstall must preserve local configuration, including the token file.
printf '\n# local configuration\n' >> /etc/discord-otelcol/config.yaml
printf '\n# local environment\n' >> /etc/discord-otelcol/environment
cp /etc/discord-otelcol/config.yaml /tmp/config-before
cp /etc/discord-otelcol/environment /tmp/env-before
dpkg -i /tmp/discord-otelcol.deb
cmp /tmp/config-before /etc/discord-otelcol/config.yaml
cmp /tmp/env-before /etc/discord-otelcol/environment
dpkg -r discord-otelcol
test -f /etc/discord-otelcol/config.yaml
test ! -e /usr/bin/discord-otelcol
dpkg --purge discord-otelcol
test ! -e /etc/discord-otelcol/config.yaml
test ! -e /etc/discord-otelcol/environment
echo 'Package lifecycle checks passed'
EOF
