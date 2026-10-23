# Discord and Slack components

## Scope

Keep the receivers' existing metrics interface: message counts and character totals.
Keep the exporters' metrics interface and add the missing Slack exporter.
Use channel IDs in every component. Exclude bot messages by default to prevent feedback loops.
S3 Tables remains experimental; change its dependencies only as needed to build the distribution.

## Findings

- Both receivers block in `Start`, and their background work does not stop cleanly.
- Each message produces a separate cumulative point with value `1`, rather than a running total.
- Message length counts UTF-8 bytes despite the `{characters}` unit.
- Discord does not request the message content intent.
- Slack discards errors from Socket Mode and does not cancel its event loop.
- Discord sends arbitrary OTLP JSON as one message, beyond Discord's 2,000-character limit.
- Slack has no exporter. The build workflow lives outside `.github/workflows` and uses obsolete Go versions.

## Implementation

1. Align all modules and the Collector Builder manifest with Collector `v0.163.0` / stable API `v1.69.0`.
   Use DiscordGo `v0.29.0` and slack-go `v0.30.1`. Regenerate component metadata when required.
2. Make receiver startup return after initialization. Own background contexts and wait for shutdown.
   Keep one cumulative series per server/workspace and channel. Flush at `buffer_interval` and shutdown.
   Count Unicode code points. Validate positive intervals and channel filters.
   Request Discord message intents and use Slack Socket Mode with acknowledgements and bounded event deduplication.
3. Implement both metrics exporters with OTLP JSON messages, Unicode-safe splitting, and explicit request contexts.
   Disable mentions and link previews. Retry transient errors and rate limits at the failed message.
   Do not replay earlier messages when a later part fails. Document partial delivery and timeout limits.
4. Register Slack in the distribution. Add usable configurations, setup instructions, a module test command, and CI.
5. Verify configuration decoding, aggregation, lifecycle, Socket Mode, and real HTTP request serialization with local servers.
   Run race tests, vet, the distribution build, and Collector configuration validation.

## Validation boundary

Automated tests use local HTTP/WebSocket servers and fake clients. They do not post messages to real services.
Live verification requires the user's Discord application and Slack app with the documented permissions.

## Primary sources

- [Collector v0.163.0 release](https://github.com/open-telemetry/opentelemetry-collector/releases/tag/v0.163.0)
- [Discord API authentication and User-Agent](https://docs.discord.com/developers/reference#authentication)
- [Discord message API](https://docs.discord.com/developers/resources/message#create-message)
- [Discord Gateway intents](https://docs.discord.com/developers/events/gateway#message-content-intent)
- [Slack Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode/)
- [Slack Web API JSON requests](https://docs.slack.dev/apis/web-api/#json-encoded-bodies)
- [Slack chat.postMessage](https://docs.slack.dev/reference/methods/chat.postMessage/)

## Completion

Implemented all four metrics components and updated the distribution, documentation, examples, and CI.
Collector is pinned to `v0.163.0` / stable API `v1.69.0`, with OpenTelemetry Go `v1.47.0`.
DiscordGo is `v0.29.0`; slack-go is `v0.30.1`. All modules require Go 1.26.

Verification on 2026-10-23 with Go 1.26.8:

- `make test`: passed race tests in both receivers, both chat exporters, and S3 Tables.
- `make build validate`: built the Linux Collector and validated both example configurations.
- Local Gateway and Socket Mode servers verified connection setup, message reception, acknowledgements, cancellation, and shutdown.
- Local HTTP servers verified authentication headers, Unicode-safe splitting, rate limits, partial failures, and request cancellation.
- A Collector process accepted OTLP metrics, exported them through debug, and exited successfully on SIGTERM.
- Regeneration reproduced all 30 checked metadata and distribution files byte for byte.
- `make vet`: passed in the distribution and all five component modules.
- `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build`: passed; the output is a Mach-O ARM64 executable.

The S3 Tables implementation is unchanged; only its Go/OpenTelemetry dependencies were updated.
Live Discord and Slack app permissions and delivery remain unverified without real service credentials.
