# Discord Receiver

The Discord receiver is designed to retrieve Discord statistical data from a single Discord server: it generates metrics from the channel messages in the server.

## Prerequisites

This collector receiver works as a Discord application, so you need to prepare the token in advance.

## Configuration

The following settings are required:

* `token`: Discord API token for a server.
* `server_wide` (default = `false`): If it is `true`, the receiver collects statistics from all channels in the server. If it is `false`, the receiver collects statistics per channel.

The following settings are optional:

* `channels` (dafault = `""`): The list of channels to collect statistics from. If it is blank, all channels are collected. This setting is ignored if `server_wide` is `true`.

Example:

```yaml
receivers:
  discord:
    token: <YOUR_APPLICATION_TOKEN>
    server_wide: false
    channels:
      - "general"
      - "random"
```
