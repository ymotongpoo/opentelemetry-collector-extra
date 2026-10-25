# otelcol-extra distribution

`otelcol-extra` includes all custom components in this repository: Discord, Slack, SwitchBot, Smart Heim Navi, Open-Meteo, and JMA receivers; Discord, Slack, and S3 Tables exporters.
It also includes OTLP and Prometheus receivers, batch processing, and debug and OTLP HTTP exporters.
Only configured components run. The S3 Tables exporter remains experimental.

Discord and SwitchBot run in the same Collector process. An optional Matter Server runs separately; it is not a systemd prerequisite for Discord.
The default configuration accepts OTLP HTTP on `127.0.0.1:4319` and forwards to Alloy on `127.0.0.1:4318`.
Collector self-metrics use `127.0.0.1:8889`. A fresh installation does not enable or start the service.

## Installation and credentials

Install the combined Collector from the shared APT repository:

```sh
sudo apt update
sudo apt install otelcol-extra
```

The APT repository setup is documented in [activitylog](https://github.com/ymotongpoo/activitylog#readme).
For local testing, build with `make deb` and install the resulting package on a disposable system.

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
Paste each value at its hidden prompt, then press Enter. Empty values, whitespace, and non-ASCII characters produce an input error and up to three attempts per value; the value is never printed or silently trimmed.
Input failures do not invoke encryption or change an existing credential. Errors from terminal access, input validation, and encryption are reported separately.
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

## Smart Heim Navi

The local [Smart Heim Navi receiver](../../receiver/smartheimreceiver) needs no cloud credentials.
Copy `/usr/share/otelcol-extra/examples/smartheim.yaml` into `/etc/otelcol-extra/conf.d/` and restart `otelcol-extra.service`.
The receiver discovers the unit on the LAN and polls every 30 seconds.
Set `device_id` to the unit's device number if multiple units share the network.
For containers or larger subnets, set `discovery.networks` to the device's private IPv4 `/24` network.
An explicit `endpoint` disables discovery.
The service allows `AF_NETLINK` for automatic LAN interface enumeration without network-administration capabilities.

## Upgrade from discord-otelcol

`otelcol-extra` is the only Collector package published to APT. The temporary `discord-otelcol` packages from releases 0.2.0 and 0.2.1 are retired; current builds do not generate them, and the APT index excludes the old package name. Historical GitHub release assets remain available for recovery.

On an installation that already runs `otelcol-extra`, remove the transitional package through APT:

```sh
sudo apt update
sudo apt install otelcol-extra discord-otelcol-
```

The trailing `-` requests removal of the old package. Explicitly naming `otelcol-extra` also marks it as manually installed, protecting it from automatic removal. This operation retains the combined Collector's configuration, encrypted credentials, and service. Old conffiles remain available for recovery.

For a remaining 0.1.x dedicated installation, back up its configuration and credentials and record whether its service is active, enabled, or masked before installing the new package. APT may remove the old service before the new package's pre-install script can inspect it. After validating the new configuration, restore the intended service state on `otelcol-extra`; never run both Collectors together.

Legacy data migration remains supported.
The old main configuration is copied when the new main configuration is still its packaged default. A customized new configuration takes precedence.
Discord token file references are rewritten to the preserved `discord_bot_token` identity, and the old state directory is copied if the new one is empty.
Other custom files and custom service drop-ins should be reviewed when migrating paths from `/etc/discord-otelcol` to `/etc/otelcol-extra`.

Encrypted credentials from version 0.1.1 are verified and copied without re-encryption or identity changes.
For version 0.1.0, the plaintext `DISCORD_BOT_TOKEN` assignment is encrypted before it is removed. Other environment assignments are retained.
If verification fails, configuration migration stops and source data remains available for recovery. Fix the host key or permissions, then run `sudo dpkg --configure -a`.
The old service cannot always be restarted automatically after its package files have been replaced; retain backups until the new service is verified.
No SwitchBot or Slack credentials are required for a Discord-only upgrade.

Inspect `/etc/otelcol-extra/config.yaml`, credential drop-ins, and `systemctl status otelcol-extra` after upgrading.
The legacy encrypted blob and configuration are retained for recovery.
Removing `otelcol-extra` stops its service but retains configuration and encrypted credentials. Purging it deletes its own registered credential files and drop-ins.
Legacy backups and user-created snippets are not deleted automatically.

## Build and verification

The optional `openmeteo.yaml` and `jma.yaml` snippets collect weather history
as logs for solar comparisons. They require version 0.4.0 or later.
Set the location and station before
enabling the snippets. State is stored under `/var/lib/otelcol-extra/weather`.
See the [weather guide](../../examples/weather-README.md) for delivery,
correction handling, source attribution, and Cloud retention requirements.

Use Go 1.27.3. Build scripts select it with `GOTOOLCHAIN` without changing the host's default Go.
Collector modules remain at v0.163.0 / stable v1.69.0.

```sh
make build validate
make test vet
python3 scripts/test-credentials.py
python3 scripts/test-collector-integration.py
python3 scripts/test-weather-integration.py
make deb VERSION=0.4.0 ARCH=amd64
./scripts/test-deb.sh dist/otelcol-extra_0.4.0_amd64.deb
make deb VERSION=0.4.0 ARCH=arm64
```

`test-deb.sh` uses a disposable Docker container and pinned published 0.1.0/0.1.1 packages.
It checks fresh installs, encryption, failed rotation, legacy data migrations, failed migration recovery, service state, APT removal of the last archived transitional package, remove, and purge.
`test-systemd.sh` installs a package on its host and requires `OTELCOL_EXTRA_SYSTEMD_TEST=1`; run it only on a disposable systemd CI runner.
The Collector integration test uses local fake services and verifies that cloud authentication and Matter connection failures leave an unrelated metrics pipeline running.
The systemd test inherits the packaged service's full sandbox and checks the route-query socket required by Smart Heim discovery.

Release CI uses tags `otelcol-extra/vVERSION` and produces only `otelcol-extra_VERSION_amd64.deb`, `otelcol-extra_VERSION_arm64.deb`, and `SHA256SUMS`. An explicit file list prevents old artifacts from entering uploads or releases.
The shared APT importer accepts only `otelcol-extra` packages, verifies checksums and Debian metadata, and ignores retired artifacts listed in historical manifests. It also removes old package files from a reused import pool before indexing.
