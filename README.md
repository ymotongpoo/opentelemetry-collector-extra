# OpenTelemetry Collector Extra

Experimental OpenTelemetry Collector components for community projects and personal use.

## Components

- [Discord receiver](receiver/discordreceiver): cumulative message counts and character totals from Discord Gateway events.
- [Slack receiver](receiver/slackreceiver): cumulative message counts and character totals from Slack Socket Mode events.
- [Discord exporter](exporter/discordexporter): posts OTLP metrics JSON to a Discord channel.
- [Slack exporter](exporter/slackexporter): posts OTLP metrics JSON to a Slack channel.
- [S3 Tables exporter](exporter/s3tablesexporter): an unfinished experiment, included in the custom distribution.

The Discord and Slack components support **metrics**. Receivers observe new messages while running; they do not fetch message history.
Exporters suit small volumes of metrics that you want to inspect in chat.

For a Discord-only service that forwards to an existing Alloy installation, use the
[dedicated Debian package](distributions/discord/README.md).

## Build and test

Use Go 1.26 or later. The distribution uses Collector `v0.163.0` and its stable API modules at `v1.69.0`.

```sh
make build
make test
make vet
```

`make build` runs the pinned Collector Builder and writes `otelcol-custom` in this directory.
`make test` runs race tests in each component module, including S3 Tables.
The chat tests use local HTTP/WebSocket servers and fake clients. They require no service credentials.

To build the checked-in generated sources without regenerating them:

```sh
go build -o otelcol-custom .
```

`make generate` regenerates the four chat components' metadata with the pinned generator.
The implementation plan and verification results are in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Receive message statistics

Create the Discord and Slack apps using the component READMEs. Provide their tokens through environment variables:

- `DISCORD_BOT_TOKEN`
- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`

Run the example pipeline:

```sh
./otelcol-custom validate --config=otel-config.yaml
./otelcol-custom --config=otel-config.yaml
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
