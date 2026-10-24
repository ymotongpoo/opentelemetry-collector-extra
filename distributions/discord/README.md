# Discord Collector for Grafana Alloy

`discord-otelcol` contains the Discord receiver and the OTLP HTTP exporter.
It forwards cumulative message metrics to Alloy at `http://127.0.0.1:4318`.
Alloy handles the connection and credentials for the telemetry backend.

## Install from apt

The package shares the existing [activitylog apt repository](https://ymotongpoo.github.io/activitylog/).
If that repository is already configured, run:

```sh
sudo apt update
sudo apt install discord-otelcol
```

On a new machine, add the repository first:

```sh
sudo install -d /usr/share/keyrings
curl -fsSL https://ymotongpoo.github.io/activitylog/apt/activitylog.gpg \
  | sudo tee /usr/share/keyrings/activitylog.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/activitylog.gpg] https://ymotongpoo.github.io/activitylog/apt stable main' \
  | sudo tee /etc/apt/sources.list.d/activitylog.list
sudo apt update
sudo apt install discord-otelcol
```

The package supports Debian and Ubuntu on amd64 and arm64 with systemd.
It does not start or enable the service on its first installation.

## Configure and start

Create a bot as described in the [receiver documentation](../../receiver/discordreceiver/README.md).
Edit `/etc/discord-otelcol/environment` with an editor and set `DISCORD_BOT_TOKEN`.
This root-owned file has mode `0640`. Keep the token out of shell history and Git.
Edit `/etc/discord-otelcol/config.yaml` to select channels or change the OTLP endpoint.

```sh
sudoedit /etc/discord-otelcol/environment
sudoedit /etc/discord-otelcol/config.yaml
sudo systemctl enable --now discord-otelcol
sudo systemctl status discord-otelcol
sudo journalctl -u discord-otelcol -f
```

Run one collector instance for each set of monitored channels to avoid duplicate counts.
The receiver requires an outbound connection to the Discord Gateway.
Its own Prometheus metrics listen on `127.0.0.1:8889`.
The service uses a dynamic user, and systemd reads the environment file before changing users.
Package upgrades preserve both configuration files and restart an active service.
Upgrades do not start a stopped service or change its enablement state.

Alloy must accept OTLP metrics and forward them to your backend.
When converting metrics to Prometheus, copy `discord.guild.id` from resource attributes
to datapoint attributes if you want a label on each metric.
The receiver resets its cumulative counters after a restart and does not backfill message history.
The sending queue is held in memory; it does not survive a collector restart.

## Build and release

Use Go 1.26 or later on Linux, plus `dpkg-deb`.
The build uses OCB `v0.163.0` and compiles only the dedicated distribution.

```sh
make discord-deb VERSION=0.1.0 ARCH=amd64
make discord-deb VERSION=0.1.0 ARCH=arm64
./scripts/test-discord-deb.sh dist/discord-otelcol_0.1.0_amd64.deb
```

The last command uses Docker to test installation, configuration preservation, removal, and purge.
Packages appear in `dist/`; generated Collector sources appear in `build/discord/`.

Push a tag such as `discord-otelcol/v0.1.0` to build and publish both Debian packages
and their SHA-256 checksums in a GitHub release.
The `apt` workflow in `ymotongpoo/activitylog` downloads those release assets and signs the apt metadata.
Run that workflow after a release to publish immediately; a daily schedule also imports releases.
The existing apt URL and signing key stay the same.
