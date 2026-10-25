# OpenTelemetry Collector Extra

Experimental OpenTelemetry Collector components for community projects and personal use.

## Components

- [Discord receiver](receiver/discordreceiver): cumulative message counts and character totals from Discord Gateway events.
- [Slack receiver](receiver/slackreceiver): cumulative message counts and character totals from Slack Socket Mode events.
- [Discord exporter](exporter/discordexporter): posts OTLP metrics JSON to a Discord channel.
- [Slack exporter](exporter/slackexporter): posts OTLP metrics JSON to a Slack channel.
- [SwitchBot receiver](receiver/switchbotreceiver): environmental measurements and battery levels through the cloud API and optional Matter Server.
- [Smart Heim Navi receiver](receiver/smartheimreceiver): local solar generation, household consumption, grid flow, and circuit power, with LAN discovery.
- [S3 Tables exporter](exporter/s3tablesexporter): an unfinished experiment, included in the custom distribution.

The Discord and Slack components support **metrics**. Receivers observe new messages while running; they do not fetch message history.
Exporters suit small volumes of metrics that you want to inspect in chat.

The `otelcol-extra` distribution includes every component in this repository. Only components selected in the configuration run.
The [Debian package](distributions/extra/README.md) can run Discord and SwitchBot in one process and forward to an existing Alloy installation.
Slack and S3 Tables credentials are unnecessary when those components are disabled.

## Build and test

Use Go **1.27.3**. `make` selects this toolchain without changing the global Go installation. The distribution uses Collector `v0.163.0` and its stable API modules at `v1.69.0`.

```sh
make build
make test
make vet
```

`make build` runs the pinned Collector Builder and writes `otelcol-extra` in this directory.
`make test` runs race tests in each component module, including S3 Tables.
The receiver tests use local HTTP/WebSocket servers and fake clients. SwitchBot tests include anonymized device responses, cloud request limits, Matter reconnects, and source fallback. They require no service credentials.

To build the checked-in generated sources without regenerating them:

```sh
GOTOOLCHAIN=go1.27.3 go build -o otelcol-extra .
```

`make generate` regenerates the custom components' metadata with the pinned generator.


## Receive message statistics

Create the Discord and Slack apps using the component READMEs. Provide their tokens through environment variables:

- `DISCORD_BOT_TOKEN`
- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`

Run the example pipeline:

```sh
./otelcol-extra validate --config=otel-config.yaml
./otelcol-extra --config=otel-config.yaml
```

The example writes metrics through the debug exporter. Each receiver README includes a configuration for using that service alone.

## Post metrics to chat

[examples/export-metrics.yaml](examples/export-metrics.yaml) accepts OTLP metrics on localhost and posts them to both services.
Set `DISCORD_CHANNEL_ID` and `SLACK_CHANNEL_ID`, along with the bot tokens, before using it.
Remove either exporter from the configuration and pipeline if you need only one service.

Receivers exclude bot messages by default. Keep `include_bots: false` when an exporter posts to a monitored channel.

## Maintenance

These components are voluntary experiments without a production support guarantee.
For maintained Collector distributions, see the [Collector repository](https://github.com/open-telemetry/opentelemetry-collector)
and [Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib).
