# otelcol-extra distribution

`otelcol-extra` includes all custom components in this repository: Discord, Slack, and SwitchBot receivers; Discord, Slack, and S3 Tables exporters.
It also includes OTLP and Prometheus receivers, batch processing, and debug and OTLP HTTP exporters.
Only configured components run. The S3 Tables exporter remains experimental.

Discord and SwitchBot run in the same Collector process. An optional Matter Server runs separately; it is not a systemd prerequisite for Discord.
The default configuration accepts OTLP HTTP on `127.0.0.1:4319` and forwards to Alloy on `127.0.0.1:4318`.
Collector self-metrics use `127.0.0.1:8889`. A fresh installation does not enable or start the service.

## Installation and credentials

After the renamed release and shared APT repository have been published, install with:

```sh
sudo apt update
sudo apt install otelcol-extra
```

The APT repository setup is documented in [activitylog](https://github.com/ymotongpoo/activitylog#readme).
For local testing, build with `make deb` and install the resulting package on a disposable system.
This source change does not publish a release or change a running installation.

Register Discord and SwitchBot credentials in an interactive terminal. Values are entered without echo and are never passed as command arguments:

```sh
sudo otelcol-extra-set-credentials discord
sudo otelcol-extra-set-credentials switchbot
sudo cp /usr/share/otelcol-extra/examples/discord.yaml /etc/otelcol-extra/conf.d/
sudo cp /usr/share/otelcol-extra/examples/switchbot.yaml /etc/otelcol-extra/conf.d/
sudo systemctl daemon-reload
sudo systemctl enable --now otelcol-extra.service
```

Enable only the snippets you need. Registering SwitchBot asks for both Token and Secret; it saves one encrypted JSON credential atomically.
For Slack, run `sudo otelcol-extra-set-credentials slack` and copy `slack.yaml` instead.
Unused components need no credentials. Additional receiver/exporter settings can be added to these snippets.
The launcher merges the main configuration and all `conf.d/*.yaml` files.

`systemd-creds` encrypts and verifies credentials before replacing an existing file. Files are root-owned mode 0600 in `/etc/credstore.encrypted/otelcol-extra.*`.
The helper creates a separate `LoadCredentialEncrypted` service drop-in for each registered component.
Discord retains the credential identity `discord_bot_token`; SwitchBot uses `switchbot_cloud`, and Slack uses `slack_receiver`.
The service runs with `DynamicUser=yes` and reads decrypted credentials only from its private credentials directory.
No persistent plaintext credential file is created. `--stdin` accepts one value per line for a trusted password-manager pipe; never put values in shell history.
Use `--with-key=host` if the default systemd key selection is unsuitable for the host. TPM-bound credentials require the same host/TPM conditions to decrypt.

For rotation, rerun the helper, then reload and restart the service. Remove a component's snippet and credential drop-in before disabling its credentials.
See the [receiver documentation](../../receiver/switchbotreceiver/README.md) for polling intervals, supported measurements, cloud quotas, and Matter bindings.

## Upgrade from discord-otelcol

The 0.2.0 `discord-otelcol` package is a transitional dependency on `otelcol-extra`.
After both packages are present in APT, a normal upgrade installs the combined distribution:

```sh
sudo apt update
sudo apt install discord-otelcol
```

The migration stops the old service before starting the new one, preserving whether it was enabled, active, stopped, or masked.
The old main configuration is copied when the new main configuration is still its packaged default. A customized new configuration takes precedence.
Discord token file references are rewritten to the preserved `discord_bot_token` identity, and the old state directory is copied if the new one is empty.
Other custom files and custom service drop-ins should be reviewed when migrating paths from `/etc/discord-otelcol` to `/etc/otelcol-extra`.

Encrypted credentials from version 0.1.1 are verified and copied without re-encryption or identity changes.
For version 0.1.0, the plaintext `DISCORD_BOT_TOKEN` assignment is encrypted before it is removed. Other environment assignments are retained.
If verification fails, configuration migration stops and source data remains available for recovery. Fix the host key or permissions, then run `sudo dpkg --configure -a`.
The old service cannot always be restarted automatically after its package files have been replaced; retain backups until the new service is verified.
No SwitchBot or Slack credentials are required for a Discord-only upgrade.

Inspect `/etc/otelcol-extra/config.yaml`, credential drop-ins, and `systemctl status otelcol-extra` after upgrading.
The legacy encrypted blob and configuration are retained for recovery; the transitional package may then be removed.
Removing `otelcol-extra` stops its service but retains configuration and encrypted credentials. Purging it deletes its own registered credential files and drop-ins.
Legacy backups and user-created snippets are not deleted automatically.

## Build and verification

Use Go 1.27.3. Build scripts select it with `GOTOOLCHAIN` without changing the host's default Go.
Collector modules remain at v0.163.0 / stable v1.69.0.

```sh
make build validate
make test vet
python3 scripts/test-credentials.py
python3 scripts/test-collector-integration.py
make deb VERSION=0.2.0 ARCH=amd64
./scripts/test-deb.sh dist/otelcol-extra_0.2.0_amd64.deb
make deb VERSION=0.2.0 ARCH=arm64
```

`test-deb.sh` uses a disposable Docker container and pinned published 0.1.0/0.1.1 packages.
It checks fresh installs, encryption, failed rotation, legacy migrations, failed migration recovery, service state, APT upgrades, remove, and purge.
`test-systemd.sh` installs a package on its host and requires `OTELCOL_EXTRA_SYSTEMD_TEST=1`; run it only on a disposable systemd CI runner.
The Collector integration test uses local fake services and verifies that cloud authentication and Matter connection failures leave an unrelated metrics pipeline running.

Release CI uses tags `otelcol-extra/vVERSION` and produces amd64 and arm64 packages, one architecture-independent transitional package, and `SHA256SUMS`.
The shared APT importer accepts both old and new release names and validates checksums and Debian metadata before importing each release.
Its index tests verify that the architecture-independent transitional package appears in both architecture indexes and that repository signatures verify.
Publish the Collector packages before advertising the renamed APT install command.
