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

The package supports Debian and Ubuntu on amd64 and arm64 with systemd 250 or later.
It does not start or enable the service on its first installation.

## Configure and start

Create a bot as described in the [receiver documentation](../../receiver/discordreceiver/README.md).
Run `sudo discord-otelcol-set-token` and paste the bot token at its hidden prompt.
Press Enter to finish. The command does not display the token or create a plaintext file.
It verifies decryption before atomically storing an encrypted credential at
`/etc/credstore.encrypted/discord-otelcol.token`, with owner `root` and mode `0600`.
Edit `/etc/discord-otelcol/config.yaml` to select channels or change the OTLP endpoint.

```sh
sudo discord-otelcol-set-token
sudoedit /etc/discord-otelcol/config.yaml
sudo systemctl enable --now discord-otelcol
sudo systemctl status discord-otelcol
sudo journalctl -u discord-otelcol -f
```

Run one collector instance for each set of monitored channels to avoid duplicate counts.
The receiver requires an outbound connection to the Discord Gateway.
Its own Prometheus metrics listen on `127.0.0.1:8889`.
The service uses a dynamic user. At startup, systemd decrypts the credential with
`LoadCredentialEncrypted=` and makes it available through a restricted credential file.
The Collector reads that file with its `file` configuration provider.
The token is not passed in an environment variable.
Package upgrades preserve local configuration and encrypted credentials, and restart an active service.
Upgrades do not start a stopped service or change its enablement state.

## Encryption and token rotation

The registration command defaults to `--with-key=auto`: systemd uses available TPM2
and host key protection, and falls back to the host key when TPM2 is unavailable.
The host key resides at `/var/lib/systemd/credential.secret` and is readable only by root.
Host key encryption protects a copied credential file; a full disk copy containing
both the credential and host key can still be decrypted.
Use `sudo discord-otelcol-set-token --with-key=host+tpm2` to require TPM2 protection.
This mode fails if the TPM is unavailable, preserving the previous credential.
TPM2 support depends on the host's hardware, libraries, and configuration.

To rotate the token, run the registration command again, then restart the service:

```sh
sudo discord-otelcol-set-token
sudo systemctl restart discord-otelcol
```

No Bitwarden session or interactive unlock is needed when the service restarts.
Losing the required host key or TPM may require issuing a new token in Discord.
Root and the running service can access the decrypted token.
Removing the package preserves its encrypted credential; purging the package deletes it.

On upgrade from 0.1.0, the package encrypts the legacy `DISCORD_BOT_TOKEN` value before
removing its plaintext assignment from `/etc/discord-otelcol/environment`.
It updates the legacy token reference in the Collector configuration while preserving other settings.
If dpkg asks about a modified `config.yaml`, keep the local version to retain your settings.
An existing encrypted credential takes precedence over a legacy environment value.
Other environment settings are retained; a file containing only comments is removed.
If encryption fails, configuration stops and the plaintext remains available for a retry.
An upgrade cannot undo earlier exposure through backups or filesystem snapshots.

Alloy must accept OTLP metrics and forward them to your backend.
When converting metrics to Prometheus, copy `discord.guild.id` from resource attributes
to datapoint attributes if you want a label on each metric.
The receiver resets its cumulative counters after a restart and does not backfill message history.
The sending queue is held in memory; it does not survive a collector restart.

## Build and release

Use Go 1.26 or later on Linux, plus `dpkg-deb`.
The build uses OCB `v0.163.0` and compiles only the dedicated distribution.

```sh
make discord-deb VERSION=0.1.1 ARCH=amd64
make discord-deb VERSION=0.1.1 ARCH=arm64
./scripts/test-discord-deb.sh dist/discord-otelcol_0.1.1_amd64.deb
```

The last command uses Docker to test encryption, failure recovery, installation,
configuration preservation, migration from the published 0.1.0 package, removal, and purge.
CI also verifies credential loading by systemd into a process with a dynamic user.
Packages appear in `dist/`; generated Collector sources appear in `build/discord/`.

Push a tag such as `discord-otelcol/v0.1.1` to build and publish both Debian packages
and their SHA-256 checksums in a GitHub release.
The `apt` workflow in `ymotongpoo/activitylog` downloads those release assets and signs the apt metadata.
Run that workflow after a release to publish immediately; a daily schedule also imports releases.
The existing apt URL and signing key stay the same.
