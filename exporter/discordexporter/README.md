# Discord exporter

Posts OTLP metrics JSON to one Discord channel for inspection.

## Application setup

Create a Discord bot and install it in the destination server.
Give it **View Channel** and **Send Messages** permissions. Threads also require **Send Messages in Threads**.
The exporter uses the [Create Message REST API](https://docs.discord.com/developers/resources/message#create-message) without opening a Gateway connection.
Message Content Intent is unnecessary for exporting.

## Configuration

```yaml
exporters:
  discord:
    token: ${env:DISCORD_BOT_TOKEN}
    channel: "123456789012345678"
    timeout: 30s
    max_retries: 3
```

- `token`: required bot token.
- `channel`: required destination channel ID.
- `timeout`: positive duration for an entire metrics batch, including retries and waits; default `30s`.
- `max_retries`: retries per message after the first attempt; default `3`, allowed range `0` to `10`.

Register the exporter in a metrics pipeline. See [the complete example](../../examples/export-metrics.yaml).
Logs and traces are unsupported.

## Delivery behavior

The exporter serializes each incoming batch as OTLP JSON and splits long payloads into Unicode-safe parts.
Each message starts with `[OTLP metrics i/n]` and remains below 2,000 UTF-16 code units, including its header.
To reconstruct the JSON, remove each header and join the parts in order.
Mentions and link previews are disabled. Empty metric batches produce no messages.

Discord rate-limit headers control waits between requests.
HTTP 429 responses honor `Retry-After`; Discord also supplies a JSON `retry_after` value.
Transient HTTP failures use exponential backoff. Authentication and permission errors fail without retries.
Request contexts interrupt HTTP requests, rate-limit waits, and waits behind another batch.

Retries operate on the failed part. A failure after partial delivery returns a permanent Collector error with the delivered part count.
This prevents automatic replay of the delivered prefix. It also means that the remaining parts of that batch are lost.
Network failures can leave delivery ambiguous, so duplicate messages remain possible.

There is no persistent sending queue. Keep batches small enough to send within `timeout`, especially when posting to Slack.
When a receiver watches the destination channel, keep its default `include_bots: false` to prevent feedback loops.
