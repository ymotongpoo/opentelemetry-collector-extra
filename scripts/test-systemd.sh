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
sed \
    -e 's/^Type=simple$/Type=oneshot/' \
    -e '/^ExecStart=/d' \
    -e '/^Restart=/d' \
    -e '/^RestartSec=/d' \
    /usr/lib/systemd/system/otelcol-extra.service \
    | sudo tee /run/systemd/system/otelcol-extra-credential-check.service >/dev/null
# Keep the packaged service's full sandbox. NETLINK_ROUTE is required by Go's
# net.Interfaces during automatic LAN discovery, even without network-admin caps.
sudo tee -a /run/systemd/system/otelcol-extra-credential-check.service >/dev/null <<'UNIT'
[Service]
LoadCredentialEncrypted=discord_bot_token:/etc/credstore.encrypted/otelcol-extra.discord
LoadCredentialEncrypted=switchbot_cloud:/etc/credstore.encrypted/otelcol-extra.switchbot
ExecStartPre=/usr/bin/python3 -c 'import socket; s = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE); s.close()'
ExecStart=/usr/bin/otelcol-extra validate --config=/etc/otelcol-extra/config.yaml --config=/usr/share/otelcol-extra/examples/discord.yaml --config=/usr/share/otelcol-extra/examples/switchbot.yaml --config=/usr/share/otelcol-extra/examples/smartheim.yaml --config=/usr/share/otelcol-extra/examples/openmeteo.yaml --config=/usr/share/otelcol-extra/examples/jma.yaml
UNIT
sudo systemctl daemon-reload
sudo systemctl start otelcol-extra-credential-check.service
test "$(sudo systemctl show --property=Result --value otelcol-extra-credential-check.service)" = success
test "$(sudo systemctl show --property=ExecMainStatus --value otelcol-extra-credential-check.service)" = 0
