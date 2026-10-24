#!/bin/sh
# Test encryption and package migration in a disposable Ubuntu container.
set -eu
package="$(realpath "${1:?usage: test-discord-deb.sh PACKAGE.deb}")"
legacy="${2:-}"
if [ -z "$legacy" ]; then
    mkdir -p build
    legacy="$PWD/build/discord-otelcol_0.1.0_amd64.deb"
    curl -fsSL --retry 3 \
        https://github.com/ymotongpoo/opentelemetry-collector-extra/releases/download/discord-otelcol%2Fv0.1.0/discord-otelcol_0.1.0_amd64.deb \
        -o "$legacy"
fi
legacy="$(realpath "$legacy")"
printf '%s  %s\n' cc6aa73a63545b1aa7c4efc317ea6345c7fc865271e1a14447debe80a2759982 "$legacy" | sha256sum --check
test "$(dpkg-deb -f "$package" Package)" = discord-otelcol
test "$(dpkg-deb -f "$package" Architecture)" = amd64
docker run --rm -i -v "$package:/tmp/discord-otelcol.deb:ro" \
    -v "$legacy:/tmp/legacy.deb:ro" ubuntu:24.04 sh -s <<'EOF'
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates systemd python3 >/dev/null
dpkg -i /tmp/discord-otelcol.deb
discord-otelcol --version
test ! -e /etc/discord-otelcol/environment
test ! -e /etc/credstore.encrypted/discord-otelcol.token
if discord-otelcol-set-token </dev/null; then exit 1; fi
printf '%s' 'encryption-test-fixture' | discord-otelcol-set-token --stdin --with-key=host
test "$(stat -c '%a:%U:%G' /etc/credstore.encrypted/discord-otelcol.token)" = 600:root:root
before="$(sha256sum /etc/credstore.encrypted/discord-otelcol.token)"
if printf '%s' 'bad whitespace fixture' | discord-otelcol-set-token --stdin --with-key=host; then exit 1; fi
if printf '%s' 'another-test-fixture' | discord-otelcol-set-token --stdin --with-key=tpm2; then exit 1; fi
test "$before" = "$(sha256sum /etc/credstore.encrypted/discord-otelcol.token)"
mkdir -m 0700 /run/discord-test-credentials
systemd-creds decrypt --name=discord_bot_token /etc/credstore.encrypted/discord-otelcol.token /run/discord-test-credentials/discord_bot_token
printf '%s' 'encryption-test-fixture' | cmp - /run/discord-test-credentials/discord_bot_token
if grep -aFq 'encryption-test-fixture' /etc/credstore.encrypted/discord-otelcol.token; then exit 1; fi
CREDENTIALS_DIRECTORY=/run/discord-test-credentials discord-otelcol validate --config=/etc/discord-otelcol/config.yaml
discord-otelcol components > /tmp/components.txt
grep -q 'name: discord' /tmp/components.txt
grep -q 'name: otlp_http' /tmp/components.txt
systemd-analyze verify --man=no /usr/lib/systemd/system/discord-otelcol.service
test ! -e /etc/systemd/system/multi-user.target.wants/discord-otelcol.service
# A reinstall must preserve local configuration and encrypted credentials.
printf '\n# local configuration\n' >> /etc/discord-otelcol/config.yaml
cp /etc/discord-otelcol/config.yaml /tmp/config-before
dpkg -i /tmp/discord-otelcol.deb
cmp /tmp/config-before /etc/discord-otelcol/config.yaml
test "$before" = "$(sha256sum /etc/credstore.encrypted/discord-otelcol.token)"
dpkg --purge discord-otelcol
test ! -e /etc/credstore.encrypted/discord-otelcol.token

# Upgrade an actual 0.1.0 package, retaining a modified config and extra settings.
dpkg -i /tmp/legacy.deb
printf 'DISCORD_BOT_TOKEN="migration-test-fixture"\nEXTRA_SETTING=keep\n' > /etc/discord-otelcol/environment
printf '\n# preserve this configuration\n' >> /etc/discord-otelcol/config.yaml
dpkg --force-confold --unpack /tmp/discord-otelcol.deb
# Failure must leave the old token and config intact so migration can be retried.
if discord-otelcol-set-token --migrate --with-key=tpm2; then exit 1; fi
grep -q 'DISCORD_BOT_TOKEN=' /etc/discord-otelcol/environment
grep -Fq '${env:DISCORD_BOT_TOKEN}' /etc/discord-otelcol/config.yaml
test ! -e /etc/credstore.encrypted/discord-otelcol.token
dpkg --force-confold --configure discord-otelcol
grep -q 'EXTRA_SETTING=keep' /etc/discord-otelcol/environment
if grep -q 'DISCORD_BOT_TOKEN\|migration-test-fixture' /etc/discord-otelcol/environment; then exit 1; fi
grep -q 'preserve this configuration' /etc/discord-otelcol/config.yaml
grep -Fq '${file:${env:CREDENTIALS_DIRECTORY}/discord_bot_token}' /etc/discord-otelcol/config.yaml
rm -f /run/discord-test-credentials/discord_bot_token
systemd-creds decrypt --name=discord_bot_token /etc/credstore.encrypted/discord-otelcol.token /run/discord-test-credentials/discord_bot_token
printf '%s' 'migration-test-fixture' | cmp - /run/discord-test-credentials/discord_bot_token
CREDENTIALS_DIRECTORY=/run/discord-test-credentials discord-otelcol validate --config=/etc/discord-otelcol/config.yaml
# A previously registered encrypted token takes precedence over stale plaintext.
before="$(sha256sum /etc/credstore.encrypted/discord-otelcol.token)"
printf 'DISCORD_BOT_TOKEN=stale-test-fixture\n' > /etc/discord-otelcol/environment
discord-otelcol-set-token --migrate
test ! -e /etc/discord-otelcol/environment
test "$before" = "$(sha256sum /etc/credstore.encrypted/discord-otelcol.token)"
dpkg -r discord-otelcol
test -f /etc/discord-otelcol/config.yaml
test -f /etc/credstore.encrypted/discord-otelcol.token
test ! -e /usr/bin/discord-otelcol
dpkg --purge discord-otelcol
test ! -e /etc/discord-otelcol/config.yaml
test ! -e /etc/discord-otelcol/environment
test ! -e /etc/credstore.encrypted/discord-otelcol.token
echo 'Encryption, migration, and package lifecycle checks passed'
EOF
