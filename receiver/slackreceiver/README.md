# Slack receiver

Collects message counts and character totals from Slack Events API messages through Socket Mode.

## Slack application setup

1. Create an app in [Slack's app settings](https://api.slack.com/apps) and enable **Socket Mode**.
2. Generate an app-level token with `connections:write`. Set it as `SLACK_APP_TOKEN` (`xapp-...`).
3. Add the bot scopes for the conversations you want to observe: `channels:history`, `groups:history`, `im:history`, or `mpim:history`.
4. Enable event subscriptions and subscribe to the matching bot events: `message.channels`, `message.groups`, `message.im`, or `message.mpim`.
5. Install or reinstall the app in your workspace. Set its bot token as `SLACK_BOT_TOKEN` (`xoxb-...`).
6. Invite the bot to each monitored public or private channel.

For public channels only, use `channels:history` and `message.channels`.
[Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode/) uses an outbound WebSocket connection and needs no public HTTP endpoint.

## Configuration

```yaml
receivers:
  slack:
    bot_token: ${env:SLACK_BOT_TOKEN}
    app_token: ${env:SLACK_APP_TOKEN}
    buffer_interval: 30s
    channels: ["C0123456789"]
    include_bots: false

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [slack]
      exporters: [debug]
```

- `bot_token` and `app_token`: required tokens for the installed bot and Socket Mode.
- `buffer_interval`: positive duration between metric emissions; default `30s`.
- `channels`: conversation IDs. An empty list includes all subscribed conversations that the app can access.
- `server_wide`: default `false`. Set `true` to aggregate conversations within each workspace, using `@all@` as the channel ID.
- `include_bots`: default `false`. Set `true` to count bot messages.
- `metrics`: optional per-metric configuration from [documentation.md](documentation.md).

`server_wide: true` and a nonempty `channels` list are mutually exclusive.
Filters use conversation IDs, such as `C0123456789`, rather than channel names.

## Metric semantics

`slack.messages.count` counts observed messages. `slack.messages.length` counts Unicode code points in the message text.
Both metrics are monotonic cumulative sums that reset when the receiver restarts.
Each emission contains one point per conversation, including previously observed conversations with no new messages.
The resource attribute `slack.team.id` separates workspaces; the point attribute `slack.channel.id` separates conversations.

The receiver includes ordinary messages, replies, file shares, action messages, and thread broadcasts.
It ignores edits, deletions, and channel administration events.
A shared window of the latest 8,192 accepted messages suppresses retries and duplicate thread broadcasts.
Each deduplication key includes the workspace, conversation, and message timestamp. Character totals count Slack's text representation, including markup.

The receiver acknowledges events after updating in-memory totals, independently of downstream metric delivery.
An abrupt process exit can lose acknowledged events. There is no durable checkpoint or history backfill.
Slack's client handles reconnects; unrecoverable Socket Mode failures report a fatal Collector component status.
Shutdown cancels the connection and emits final totals. Downstream failures appear in receiver telemetry and logs.
