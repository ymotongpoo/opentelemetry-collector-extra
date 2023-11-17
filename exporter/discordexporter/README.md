# Discord exporter

The Discord exporter is designed to export metrics data to a single Discord channel.

## Prerequisites

This collector exporter works as a Discord application, so you need to prepare the token in advance.

## Configuration

The following settings are required:

* `token` (string): Discord API token for a server.
* `cahnnel` (string): Discord channel to post metrics to.

Example:

```yaml
exporters:
  discord:
    token: <YOUR_APPLICATION_TOKEN>
    channel: "123456789012345"
```
