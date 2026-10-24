#!/bin/sh
# This installs packages: only run on a disposable systemd CI runner.
set -eu
test "${OTELCOL_EXTRA_SYSTEMD_TEST:-}" = 1
package="$(realpath "${1:?usage: test-systemd.sh PACKAGE.deb}")"
cleanup() {
    sudo systemctl stop otelcol-extra-credential-check.service || true
    sudo rm -f /run/systemd/system/otelcol-extra-credential-check.service
    sudo systemctl daemon-reload
    sudo dpkg --purge otelcol-extra
}
trap cleanup EXIT
sudo apt-get install -y "$package"
printf '%s\n' 'systemd-discord-fixture' | sudo otelcol-extra-set-credentials discord --stdin --with-key=host
printf '%s\n' 'systemd-token-fixture' 'systemd-secret-fixture' | sudo otelcol-extra-set-credentials switchbot --stdin --with-key=host
sudo tee /run/systemd/system/otelcol-extra-credential-check.service >/dev/null <<'UNIT'
[Service]
Type=oneshot
DynamicUser=yes
ProtectSystem=strict
ProtectHome=yes
LimitCORE=0
LoadCredentialEncrypted=discord_bot_token:/etc/credstore.encrypted/otelcol-extra.discord
LoadCredentialEncrypted=switchbot_cloud:/etc/credstore.encrypted/otelcol-extra.switchbot
ExecStart=/usr/bin/otelcol-extra validate --config=/etc/otelcol-extra/config.yaml --config=/usr/share/otelcol-extra/examples/discord.yaml --config=/usr/share/otelcol-extra/examples/switchbot.yaml
UNIT
sudo systemctl daemon-reload
sudo systemctl start otelcol-extra-credential-check.service
test "$(sudo systemctl show --property=Result --value otelcol-extra-credential-check.service)" = success
test "$(sudo systemctl show --property=ExecMainStatus --value otelcol-extra-credential-check.service)" = 0
