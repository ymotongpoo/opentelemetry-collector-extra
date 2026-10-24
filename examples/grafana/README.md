# SwitchBot Grafana dashboard

[switchbot-dashboard.json](switchbot-dashboard.json) is an importable Grafana dashboard for the [SwitchBot receiver](../../receiver/switchbotreceiver). It has 14 panels with Japanese labels: collection summaries, temperature, humidity, CO₂, Hub brightness, battery levels and history, device measurement status, and observation ages.

## Import

1. Send the receiver's metrics to a Prometheus-compatible data source, such as Grafana Cloud through Alloy.
2. In Grafana, open **Dashboards → New → Import** and upload `switchbot-dashboard.json`.
3. Choose the dashboard title and destination folder, then import.
4. Select the metrics data source using the **Prometheus** dropdown. Use **機種** to filter by device type and **デバイス** to select devices.

The device dropdown displays names and IDs and filters by stable device ID. Duplicate device names remain separate. The example has no fixed dashboard UID, data source UID, account URL, device IDs, or credentials. Importing it creates a separate dashboard.

Queries use the Prometheus data source's default variable interpolation with `=~`, including escaping device types with parentheses. See [Grafana's Prometheus variable documentation](https://grafana.com/docs/grafana/latest/datasources/prometheus/template-variables/).

The default range is six hours with a one-minute refresh. A new installation has no history before collection started; narrow the time range to inspect its first samples. The explanatory text and receipt-age thresholds assume the receiver's default intervals. Adjust them if you customize collection or export intervals.

## Reading the panels

- **観測値あり** counts devices with at least one unexpired measurement. **観測値なし** also includes unsupported status fields, failed reads, and expired measurements. These panels describe measurement validity.
- **最新受信からの経過** measures the age of the newest received sample. The bottom panels show time since actual device observations; cached values can continue arriving every minute between observations.
- The battery bar and device table use instant queries. Historical plots retain periods when measurements existed and leave gaps when they are missing.
- A reported battery level of zero remains zero. Unsupported, missing, and expired battery values stay absent. The low-battery count shows zero only when battery data exists but none of those values is at or below 20%.
- Battery values can be coarse stages. Battery-only devices normally refresh every three hours; battery values returned with environmental reads refresh more often.
- Hub brightness uses the device's 1–20 scale. Its unit is a device-specific level.
- Environmental observation age is the oldest temperature, humidity, CO₂, or brightness observation for each device. Ages can remain visible after the corresponding measurement expires.

The dashboard shows physical devices reported by the receiver. IR remotes have no observed appliance state, and press-mode Bot names do not establish a shutter or lock's physical state.

## Metric names

These Prometheus names were verified with the receiver → OTLP → Alloy → Grafana Cloud ingestion path. Other ingestion settings can translate names differently; update the queries to match your backend if needed.

| Prometheus metric | Unit |
| --- | --- |
| `switchbot_temperature_celsius` | °C |
| `switchbot_humidity_percent` | % |
| `switchbot_co2_ppm_` | ppm |
| `switchbot_light_level` | 1–20 level |
| `switchbot_battery_percent` | % |
| `switchbot_device_available_ratio` | 0 or 1 |
| `switchbot_observation_age_seconds` | seconds |

The trailing underscore in `switchbot_co2_ppm_` is intentional. Queries use the `switchbot_device_id`, `switchbot_device_name`, and `switchbot_device_type` labels. Observation age also uses `switchbot_metric`.
