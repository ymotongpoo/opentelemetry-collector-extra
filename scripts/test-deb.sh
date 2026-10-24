#!/bin/sh
# Exercise real credential encryption and both published legacy upgrade paths
# inside a disposable container. Never run package migrations on the host.
set -eu
cd "$(dirname "$0")/.."
package="$(realpath "${1:?usage: test-deb.sh otelcol-extra.deb}")"
version="$(dpkg-deb -f "$package" Version)"
mkdir -p build/fixtures
for old in 0.1.0 0.1.1; do
    file="build/fixtures/discord-otelcol_${old}_amd64.deb"
    if [ ! -f "$file" ]; then
        curl -fsSL --retry 3 "https://github.com/ymotongpoo/opentelemetry-collector-extra/releases/download/discord-otelcol%2Fv${old}/discord-otelcol_${old}_amd64.deb" -o "$file"
    fi
done
# This archived package is a fixture for installations that already migrated.
# Current builds and releases must never produce another package under this name.
transition=build/fixtures/discord-otelcol_0.2.1_all.deb
if [ ! -f "$transition" ]; then
    curl -fsSL --retry 3 'https://github.com/ymotongpoo/opentelemetry-collector-extra/releases/download/otelcol-extra%2Fv0.2.1/discord-otelcol_0.2.1_all.deb' -o "$transition"
fi
transition="$(realpath "$transition")"
printf '%s\n' \
    'cc6aa73a63545b1aa7c4efc317ea6345c7fc865271e1a14447debe80a2759982  build/fixtures/discord-otelcol_0.1.0_amd64.deb' \
    '0f78f13b4cc242c9b9676394463876b4c672a04cbfc6187eccbd23f01babcfaf  build/fixtures/discord-otelcol_0.1.1_amd64.deb' \
    'bb8d6e284236b2e8b962f1058d336a75b62b0df52f06902c49914bd5c2fafb40  build/fixtures/discord-otelcol_0.2.1_all.deb' | sha256sum --check
test "$(dpkg-deb -f "$package" Package)" = otelcol-extra
docker run --rm -i -v "$package:/tmp/extra.deb:ro" -v "$transition:/tmp/transition.deb:ro" \
    -v "$PWD/build/fixtures:/fixtures:ro" ubuntu:24.04 sh -s <<'EOF'
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq ca-certificates systemd python3 apt-utils >/dev/null
dpkg -i /tmp/extra.deb
otelcol-extra --version
otelcol-extra validate --config=/etc/otelcol-extra/config.yaml
otelcol-extra components > /tmp/components.txt
for component in discord slack switchbot s3tables otlp prometheus batch debug otlp_http; do
    grep -q "name: $component" /tmp/components.txt
done
test ! -e /etc/systemd/system/multi-user.target.wants/otelcol-extra.service
if otelcol-extra-set-credentials discord </dev/null; then exit 1; fi
printf '%s\n' 'encryption-fixture' | otelcol-extra-set-credentials discord --stdin --with-key=host
printf '%s\n' 'switchbot-token-fixture' 'switchbot-secret-fixture' | otelcol-extra-set-credentials switchbot --stdin --with-key=host
printf '%s\n' 'xoxb-slack-fixture' 'xapp-slack-fixture' | otelcol-extra-set-credentials slack --stdin --with-key=host
test "$(stat -c '%a:%U:%G' /etc/credstore.encrypted/otelcol-extra.discord)" = 600:root:root
test "$(stat -c '%a:%U:%G' /etc/credstore.encrypted/otelcol-extra.switchbot)" = 600:root:root
before="$(sha256sum /etc/credstore.encrypted/otelcol-extra.discord)"
if printf '%s' 'bad whitespace fixture' | otelcol-extra-set-credentials discord --stdin --with-key=host > /tmp/input-error 2>&1; then exit 1; fi
grep -q 'spaces or line breaks are present' /tmp/input-error
if grep -Eq 'TPM|bad whitespace fixture' /tmp/input-error; then exit 1; fi
if printf '%s' 'new-fixture' | otelcol-extra-set-credentials discord --stdin --with-key=tpm2; then exit 1; fi
test "$before" = "$(sha256sum /etc/credstore.encrypted/otelcol-extra.discord)"
mkdir -m 0700 /run/extra-credentials
systemd-creds decrypt --name=discord_bot_token /etc/credstore.encrypted/otelcol-extra.discord /run/extra-credentials/discord_bot_token
systemd-creds decrypt --name=switchbot_cloud /etc/credstore.encrypted/otelcol-extra.switchbot /run/extra-credentials/switchbot_cloud
systemd-creds decrypt --name=slack_receiver /etc/credstore.encrypted/otelcol-extra.slack /run/extra-credentials/slack_receiver
printf '%s' 'encryption-fixture' | cmp - /run/extra-credentials/discord_bot_token
CREDENTIALS_DIRECTORY=/run/extra-credentials otelcol-extra validate --config=/etc/otelcol-extra/config.yaml \
    --config=/usr/share/otelcol-extra/examples/discord.yaml --config=/usr/share/otelcol-extra/examples/switchbot.yaml --config=/usr/share/otelcol-extra/examples/slack.yaml
if grep -aFq 'switchbot-secret-fixture' /etc/credstore.encrypted/otelcol-extra.switchbot; then exit 1; fi
printf '\n# preserve new local config\n' >> /etc/otelcol-extra/config.yaml
cp /etc/otelcol-extra/config.yaml /tmp/config-before
dpkg -i /tmp/extra.deb
cmp /tmp/config-before /etc/otelcol-extra/config.yaml
test "$before" = "$(sha256sum /etc/credstore.encrypted/otelcol-extra.discord)"
systemd-analyze verify --man=no /usr/lib/systemd/system/otelcol-extra.service
dpkg --purge otelcol-extra
test ! -e /etc/credstore.encrypted/otelcol-extra.discord
test ! -e /etc/credstore.encrypted/otelcol-extra.switchbot
test ! -e /etc/credstore.encrypted/otelcol-extra.slack

# Model PID 1 state without starting Discord or contacting its Gateway.
mkdir -p /run/systemd/system
cat > /usr/bin/systemctl <<'SYSTEMCTL'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> /tmp/systemctl-events
case "$1" in
  is-active)
    case "$*" in *discord-otelcol*) test -e /tmp/old-active ;; *) test -e /tmp/new-active ;; esac ;;
  is-enabled)
    if [ -e /tmp/old-masked-runtime ]; then echo masked-runtime; exit 1; fi
    if [ -e /tmp/old-enabled-runtime ]; then echo enabled-runtime; exit 0; fi
    if [ -e /tmp/old-masked ]; then echo masked; exit 1; fi
    if [ -e /tmp/old-enabled ]; then echo enabled; else echo disabled; exit 1; fi ;;
  stop)
    case "$2" in discord-otelcol*) rm -f /tmp/old-active ;; *) rm -f /tmp/new-active ;; esac ;;
  start|try-restart)
    case "$2" in otelcol-extra*) touch /tmp/new-active ;; esac ;;
  enable) if [ "$2" = --runtime ]; then touch /tmp/new-enabled-runtime; else touch /tmp/new-enabled; fi ;;
  disable)
    case "$2" in discord-otelcol*) rm -f /tmp/old-enabled ;; *) rm -f /tmp/new-enabled ;; esac ;;
  mask) if [ "$2" = --runtime ]; then touch /tmp/new-masked-runtime; else touch /tmp/new-masked; fi ;;
esac
exit 0
SYSTEMCTL
chmod +x /usr/bin/systemctl
for scenario in plaintext active stopped active-disabled masked enabled-runtime masked-runtime; do
    rm -f /tmp/old-active /tmp/old-enabled /tmp/old-masked /tmp/new-active /tmp/new-enabled /tmp/new-masked
    rm -f /tmp/old-enabled-runtime /tmp/old-masked-runtime /tmp/new-enabled-runtime /tmp/new-masked-runtime
    old=0.1.1
    if [ "$scenario" = plaintext ]; then old=0.1.0; fi
    dpkg -i "/fixtures/discord-otelcol_${old}_amd64.deb"
    if [ "$old" = 0.1.0 ]; then
        printf 'DISCORD_BOT_TOKEN="migration-fixture"\nEXTRA_SETTING=keep\n' > /etc/discord-otelcol/environment
    else
        printf '%s' 'migration-fixture' | discord-otelcol-set-token --stdin --with-key=host
        cp /etc/credstore.encrypted/discord-otelcol.token /tmp/old-cipher
    fi
    printf '\n# keep old custom configuration\n' >> /etc/discord-otelcol/config.yaml
    mkdir -p /var/lib/discord-otelcol
    printf '%s' 'old-state' > /var/lib/discord-otelcol/fixture
    case "$scenario" in active) touch /tmp/old-active /tmp/old-enabled ;; stopped) touch /tmp/old-enabled ;; active-disabled) touch /tmp/old-active ;; masked) touch /tmp/old-masked ;; enabled-runtime) touch /tmp/old-enabled-runtime ;; masked-runtime) touch /tmp/old-masked-runtime ;; esac
    dpkg --auto-deconfigure --unpack /tmp/extra.deb
    test ! -e /tmp/old-active
    # Removal retains legacy configuration and credentials for the new postinst.
    dpkg --remove discord-otelcol
    if [ "$scenario" = plaintext ]; then
        if otelcol-extra-set-credentials --migrate --with-key=tpm2; then exit 1; fi
        grep -q 'DISCORD_BOT_TOKEN=' /etc/discord-otelcol/environment
        if [ -f /etc/otelcol-extra/config.yaml ]; then cmp /etc/otelcol-extra/config.yaml /usr/share/otelcol-extra/config.default.yaml; fi
        grep -q 'keep old custom configuration' /etc/discord-otelcol/config.yaml
        # Fail the actual package postinst, then retry configuration successfully.
        cat > /usr/local/bin/systemd-creds <<'CREDS'
#!/bin/sh
if [ -e /tmp/fail-encryption ]; then exit 1; fi
exec /usr/bin/systemd-creds "$@"
CREDS
        chmod +x /usr/local/bin/systemd-creds
        touch /tmp/fail-encryption
        if dpkg --configure otelcol-extra; then exit 1; fi
        grep -q 'DISCORD_BOT_TOKEN=' /etc/discord-otelcol/environment
        cmp /etc/otelcol-extra/config.yaml /usr/share/otelcol-extra/config.default.yaml
        rm /tmp/fail-encryption
    fi
    dpkg --force-confold --configure otelcol-extra
    grep -q 'keep old custom configuration' /etc/otelcol-extra/config.yaml
    grep -Fq '${file:${env:CREDENTIALS_DIRECTORY}/discord_bot_token}' /etc/otelcol-extra/config.yaml
    test "$(cat /var/lib/otelcol-extra/fixture)" = old-state
    rm -f /run/extra-credentials/discord_bot_token
    systemd-creds decrypt --name=discord_bot_token /etc/credstore.encrypted/otelcol-extra.discord /run/extra-credentials/discord_bot_token
    printf '%s' 'migration-fixture' | cmp - /run/extra-credentials/discord_bot_token
    if [ "$old" = 0.1.1 ]; then cmp /tmp/old-cipher /etc/credstore.encrypted/otelcol-extra.discord; fi
    if [ "$scenario" = plaintext ]; then
        grep -q 'EXTRA_SETTING=keep' /etc/otelcol-extra/environment
        if grep -q 'DISCORD_BOT_TOKEN\|migration-fixture' /etc/otelcol-extra/environment; then exit 1; fi
    fi
    CREDENTIALS_DIRECTORY=/run/extra-credentials otelcol-extra validate --config=/etc/otelcol-extra/config.yaml
    case "$scenario" in
        active) test -e /tmp/new-active; test -e /tmp/new-enabled ;;
        stopped) test ! -e /tmp/new-active; test -e /tmp/new-enabled ;;
        active-disabled) test -e /tmp/new-active; test ! -e /tmp/new-enabled ;;
        masked) test -e /tmp/new-masked; test ! -e /tmp/new-active ;;
        enabled-runtime) test -e /tmp/new-enabled-runtime; test ! -e /tmp/new-enabled; test ! -e /tmp/new-active ;;
        masked-runtime) test -e /tmp/new-masked-runtime; test ! -e /tmp/new-masked; test ! -e /tmp/new-active ;;
        plaintext) test ! -e /tmp/new-active; test ! -e /tmp/new-enabled ;;
    esac
    # The old package is no longer installed; purging its conffiles is safe.
    dpkg --purge discord-otelcol
    test -f /etc/credstore.encrypted/otelcol-extra.discord
    dpkg -r otelcol-extra
    test -f /etc/otelcol-extra/config.yaml
    test -f /etc/credstore.encrypted/otelcol-extra.discord
    dpkg --purge otelcol-extra
    test ! -e /etc/otelcol-extra/config.yaml
    test ! -e /etc/credstore.encrypted/otelcol-extra.discord
    rm -rf /etc/discord-otelcol /var/lib/discord-otelcol /var/lib/otelcol-extra
    rm -f /etc/credstore.encrypted/discord-otelcol.token
done
# Reproduce an already migrated installation using the last archived transition.
# Its removal through APT must leave credentials, configuration and service state.
rm -f /tmp/old-active /tmp/old-enabled /tmp/old-masked /tmp/old-enabled-runtime /tmp/old-masked-runtime
rm -f /tmp/new-active /tmp/new-enabled /tmp/new-masked /tmp/new-enabled-runtime /tmp/new-masked-runtime
mkdir -p /tmp/extra-apt
cp /tmp/extra.deb /tmp/extra-apt/
(cd /tmp/extra-apt && apt-ftparchive packages . > Packages)
if grep -q '^Package: discord-otelcol$' /tmp/extra-apt/Packages; then exit 1; fi
printf '%s\n' 'deb [trusted=yes] file:/tmp/extra-apt ./' > /etc/apt/sources.list.d/extra-test.list
apt-get update -qq
dpkg -i /fixtures/discord-otelcol_0.1.1_amd64.deb
printf '%s' 'apt-upgrade-fixture' | discord-otelcol-set-token --stdin --with-key=host
touch /tmp/old-active /tmp/old-enabled
dpkg --auto-deconfigure --unpack /tmp/transition.deb /tmp/extra.deb
dpkg --force-confold --configure otelcol-extra discord-otelcol
printf '\n# keep migrated config\n' >> /etc/otelcol-extra/config.yaml
cp /etc/otelcol-extra/config.yaml /tmp/retirement-config
cp /etc/credstore.encrypted/otelcol-extra.discord /tmp/retirement-credential
printf '%s\n' 'keep-switchbot-token' 'keep-switchbot-secret' | otelcol-extra-set-credentials switchbot --stdin --with-key=host
cp /etc/credstore.encrypted/otelcol-extra.switchbot /tmp/retirement-switchbot
events="$(wc -l < /tmp/systemctl-events)"
apt-get install -y -qq otelcol-extra discord-otelcol-
test "$(dpkg-query -W -f='${db:Status-Status}' discord-otelcol)" != installed
apt-mark showmanual | grep -qx otelcol-extra
cmp /tmp/retirement-config /etc/otelcol-extra/config.yaml
cmp /tmp/retirement-credential /etc/credstore.encrypted/otelcol-extra.discord
cmp /tmp/retirement-switchbot /etc/credstore.encrypted/otelcol-extra.switchbot
test "$events" = "$(wc -l < /tmp/systemctl-events)"
test -e /tmp/new-active
test -e /tmp/new-enabled
test ! -e /tmp/old-active
rm -f /run/extra-credentials/discord_bot_token
systemd-creds decrypt --name=discord_bot_token /etc/credstore.encrypted/otelcol-extra.discord /run/extra-credentials/discord_bot_token
printf '%s' 'apt-upgrade-fixture' | cmp - /run/extra-credentials/discord_bot_token
dpkg --purge discord-otelcol otelcol-extra
echo 'Fresh install, credentials, legacy data migration, failure recovery, service state, APT retirement, and purge checks passed'
EOF
