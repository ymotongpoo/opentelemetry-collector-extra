# Slack Receiver

The Slack receiver is designed to retrieve Slack message statistical data from a single Slack workspace: it generates metrics from the channel messages in the workspace.

## Prerequisites

This collector receiver works as a Slack application, so you need to prepare the token in advance. Especially, because this only works in [the socket mode](https://api.slack.com/apis/connections/socket), you need [both a bot token and an app token](https://api.slack.com/authentication/token-types).

## Configuration

The following settings are required:

* `bot_token`: Slack Bot token for a workspace.
* `app_token`: Slack App token for a workspace.
* `server_wide` (default = `false`): If it is `true`, the receiver collects statistics from all channels in the server. If it is `false`, the receiver collects statistics per channel.
* `buffer_interval` (default = `"30s"`): Interval period to buffer Slack messages to collect metrics. Valid units are the ones supported by [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration).

The following settings are optional:

* `channels` (dafault = `""`): The list of channels to collect statistics from. If it is blank, all channels are collected. This setting is ignored if `server_wide` is `true`.

Example:

```yaml
receivers:
  slack:
    bot_token: <YOUR_BOT_TOKEN>
    app_token: <YOUR_APP_TOKEN>
    server_wide: false
    buffer_interval: "1m"
    channels:
      - "general"
      - "random"
```
