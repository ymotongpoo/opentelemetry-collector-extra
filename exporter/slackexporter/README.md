# Slack exporter

Posts OTLP metrics JSON to one Slack channel for inspection.

## Application setup

Install a Slack app with the `chat:write` bot scope and invite it to the destination channel.
Use the installed bot token (`xoxb-...`). Socket Mode and an app-level token are unnecessary for exporting.
The exporter uses [chat.postMessage](https://docs.slack.dev/reference/methods/chat.postMessage/).

## Configuration

```yaml
exporters:
  slack:
    bot_token: ${env:SLACK_BOT_TOKEN}
    channel: "C0123456789"
    timeout: 30s
    max_retries: 3
```

- `bot_token`: required bot token.
- `channel`: required destination channel ID.
- `timeout`: positive duration for an entire metrics batch, including retries and waits; default `30s`.
- `max_retries`: retries per message after the first attempt; default `3`, allowed range `0` to `10`.

Register the exporter in a metrics pipeline. See [the complete example](../../examples/export-metrics.yaml).
Logs and traces are unsupported.

## Delivery behavior

The exporter serializes each incoming batch as OTLP JSON and splits long payloads into Unicode-safe parts.
Each message starts with `[OTLP metrics i/n]` and remains below 4,000 UTF-16 code units, including its header.
To reconstruct the JSON, remove each header and join the parts in order.
Mentions and link previews are disabled. Empty metric batches produce no messages.

The exporter sends at most one message per second per instance and honors Slack rate-limit responses.
HTTP 429 responses honor `Retry-After`; Discord also supplies a JSON `retry_after` value.
Transient HTTP failures use exponential backoff. Authentication and permission errors fail without retries.
Request contexts interrupt HTTP requests, rate-limit waits, and waits behind another batch.

Retries operate on the failed part. A failure after partial delivery returns a permanent Collector error with the delivered part count.
This prevents automatic replay of the delivered prefix. It also means that the remaining parts of that batch are lost.
Network failures can leave delivery ambiguous, so duplicate messages remain possible.

There is no persistent sending queue. Keep batches small enough to send within `timeout`, especially when posting to Slack.
When a receiver watches the destination channel, keep its default `include_bots: false` to prevent feedback loops.
