#!/bin/sh
# Run on an isolated CI runner with systemd and passwordless sudo.
set -eu
package="$(realpath "${1:?usage: test-discord-systemd.sh PACKAGE.deb}")"
sudo apt-get install -y "$package"
trap 'sudo dpkg --purge discord-otelcol' EXIT
printf '%s' 'systemd-integration-test-fixture' | sudo discord-otelcol-set-token --stdin --with-key=host
# PID 1 must decrypt the credential and make it readable to a dynamic user.
sudo systemd-run --quiet --unit=discord-otelcol-credential-check --wait --pipe \
    --property=DynamicUser=yes \
    --property=ProtectSystem=strict \
    --property=ProtectHome=yes \
    --property=LimitCORE=0 \
    --property=LoadCredentialEncrypted=discord_bot_token:/etc/credstore.encrypted/discord-otelcol.token \
    /usr/bin/discord-otelcol validate --config=/etc/discord-otelcol/config.yaml
