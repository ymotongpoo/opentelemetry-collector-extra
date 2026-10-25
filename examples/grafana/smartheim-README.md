# Smart Heim Navi dashboard

[smartheim-dashboard.json](smartheim-dashboard.json) shows solar generation, household consumption, grid power, and circuit power with Japanese labels.
It contains ten panels and refreshes every 30 seconds. The default range is three hours.

## Import and ingestion

1. Send the [receiver's metrics](../../receiver/smartheimreceiver) to a Prometheus-compatible backend.
2. Preserve `smartheim.device.id` as the Prometheus label `smartheim_device_id`.
3. In Grafana, open **Dashboards → New → Import** and upload `smartheim-dashboard.json`.
4. Select the Prometheus data source and device number with the dropdowns.

The dashboard has no fixed UID, account URL, or device ID. Importing it creates a separate dashboard.

When Alloy forwards OTLP metrics through `otelcol.exporter.prometheus`, enable resource conversion on that exporter:

```alloy
otelcol.exporter.prometheus "metrics" {
  forward_to = [prometheus.remote_write.metrics.receiver]
  resource_to_telemetry_conversion = true
}
```

Use your existing component labels and remote-write destination.
This option copies all resource attributes to metric labels; review the attributes that your existing pipeline carries.
For direct OTLP ingestion, configure your backend to promote `smartheim.device.id` to a label instead.
Without that label, the device dropdown has no values.
See [Alloy's exporter documentation](https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.exporter.prometheus/) for resource conversion.

## Metric translation

Queries assume standard Prometheus normalization with unit suffixes:

The names and labels below passed a local test with the real device, Collector v0.163.0, Alloy v1.21.0, and a remote-write capture endpoint.
This test did not publish a dashboard or send measurements to Grafana Cloud.

| OTLP metric | Prometheus metric |
| --- | --- |
| `smartheim.solar.power` | `smartheim_solar_power_watts` |
| `smartheim.consumption.power` | `smartheim_consumption_power_watts` |
| `smartheim.grid.import.power` | `smartheim_grid_import_power_watts` |
| `smartheim.grid.export.power` | `smartheim_grid_export_power_watts` |
| `smartheim.branch.power` | `smartheim_branch_power_watts` |

Circuit labels become `smartheim_branch_id` and `smartheim_branch_name`.
Adjust queries if your backend uses a different translation strategy.

The stat panels and circuit bars omit samples older than 90 seconds.
The receipt-age panel shows the age of the latest consumption sample while the backend retains it in its instant-query lookback.
After that lookback expires, the panel shows missing data.
Historical plots show received samples. Backend lookback and query steps can bridge short gaps between samples.
The receiver emits no cached values or artificial zeros during communication failures.

The 90-second threshold assumes the default collection interval and prompt ingestion.
Adjust every freshness expression and the receipt-age thresholds if you use longer intervals or delayed forwarding.
Receipt time does not establish the age of a measurement inside the device.
Circuit totals can differ from total consumption because the pages represent separate snapshots and some circuits lack measurements.
This dashboard shows instantaneous watts. It does not estimate cumulative energy or electricity charges.
