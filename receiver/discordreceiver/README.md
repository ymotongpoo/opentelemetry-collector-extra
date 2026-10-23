# Discord receiver

Collects metrics from new messages in guild channels visible to a Discord bot.

## Discord application setup

1. Create an application and bot in the [Discord Developer Portal](https://discord.com/developers/applications).
2. Enable **Message Content Intent** under the bot's privileged Gateway intents.
3. Install the bot in your server with the `bot` scope and **View Channel** permission for the monitored channels.
4. Copy the bot token into `DISCORD_BOT_TOKEN`.
5. Enable Developer Mode in Discord and use **Copy Channel ID** for each channel filter.

The receiver requests `GUILD_MESSAGES` and `MESSAGE_CONTENT`.
Discord requires the [message content intent](https://docs.discord.com/developers/events/gateway#message-content-intent) to expose ordinary message content.
Verified applications may require Discord's approval for that intent.

## Configuration

```yaml
receivers:
  discord:
    token: ${env:DISCORD_BOT_TOKEN}
    buffer_interval: 30s
    channels: ["123456789012345678"]
    include_bots: false

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [discord]
      exporters: [debug]
```

- `token`: required bot token without the `Bot ` prefix.
- `buffer_interval`: positive duration between metric emissions; default `30s`.
- `channels`: channel IDs, including thread IDs if needed. An empty list includes all visible guild channels.
- `server_wide`: default `false`. Set `true` to aggregate channels within each guild, using `@all@` as the channel ID.
- `include_bots`: default `false`. Set `true` to include bot and webhook messages.
- `metrics`: optional per-metric configuration from [documentation.md](documentation.md).

`server_wide: true` and a nonempty `channels` list are mutually exclusive.
Channel filters use IDs, not channel names. A parent channel filter does not include its threads automatically.

## Metric semantics

`discord.messages.count` counts observed messages. `discord.messages.length` counts Unicode code points in their content.
Both metrics are monotonic cumulative sums that reset when the receiver restarts.
Each emission contains one point per channel, including previously observed channels with no new messages.
The resource attribute `discord.guild.id` separates servers; the point attribute `discord.channel.id` separates channels.

The receiver includes ordinary messages and replies. It ignores direct messages and system events such as joins and pins.
It suppresses duplicate message IDs within a window of the latest 8,192 accepted messages.
It counts textual Discord content, including markup, rather than rendered glyphs or attachment contents.

The receiver reconnects with cancellable backoff and attempts to resume the DiscordGo session.
Gateway discovery and each connection handshake have a 15-second timeout.
There is no persistent checkpoint or history backfill.
Shutdown stops reception, waits for periodic emission to finish, and emits the final totals.
Downstream failures appear in Collector receiver telemetry and logs; the next emission still contains the cumulative totals.
