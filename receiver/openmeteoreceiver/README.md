# Open-Meteo receiver

This pull receiver retrieves hourly weather model data and daily solar times
from the [Open-Meteo Forecast API](https://open-meteo.com/en/docs). It emits
**logs**, with a new event when a value or its quality changes. Model data is
identified separately from observations from a weather station.

```yaml
receivers:
  openmeteo:
    latitude: 35.6667
    longitude: 138.55
    location_id: kofu
    timezone: Asia/Tokyo
    collection_interval: 1h
    state_file: /var/lib/otelcol-extra/weather/openmeteo.json
```

Select the receiver in a `logs` pipeline. See the
[complete example](../../examples/weather.yaml) and
[weather history guide](../../examples/weather-README.md) for delivery,
revision selection, and retention.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `latitude`, `longitude` | Required | WGS84 coordinates, including valid zero coordinates. |
| `location_id` | Required | A stable name for this location. |
| `state_file` | Required | Durable outbox and deduplication state; unique per instance. |
| `endpoint` | `https://api.open-meteo.com/v1/forecast` | API endpoint, without a query string or credentials. |
| `timezone` | `Asia/Tokyo` | IANA timezone used for daily boundaries. |
| `model` | `best_match` | One model name, or automatic model selection. |
| `past_days` | `2` | Recheck this many past days; range 0–92. |
| `forecast_days` | `2` | Include today and this forecast horizon; range 1–16. |
| `collection_interval` | `1h` | Delay between completed collection passes. |
| `timeout` | `30s` | Timeout per HTTP request. |
| `state_ttl` | `2880h` | Keep deduplication entries for 120 days after their last change. This does not configure Cloud retention. |

The receiver requests `shortwave_radiation`, `sunshine_duration`,
`precipitation`, `temperature_2m`, `cloud_cover`, and `weather_code` hourly.
It requests `sunrise`, `sunset`, `daylight_duration`, and `sunshine_duration`
daily. Every value includes its unit and time semantics.

Hourly radiation, sunshine, and precipitation refer to the **preceding hour**.
Temperature, cloud cover, and weather code are instantaneous model values.
Daily boundaries use the selected timezone, including daylight-saving changes.
Sunrise and sunset values are Unix seconds within the record's local-date
interval, with `aggregation: event_time`. They are astronomical calculations;
the returned times do not describe shading from a nearby mountain or building.

The API returns the selected grid coordinates. The receiver preserves them in
`details`, together with the requested model selection. `best_match` can combine
models; it is not the identity of a single underlying model run. The Forecast
API does not return a run initialisation time here, so the receiver does not
invent one. `retrieved_at` identifies when this version was retrieved. For
evaluation of a particular forecast run, use a model-specific archive rather
than interpreting retrieval time as model initialisation time.

Null values produce `quality: missing`, not zero. Missing fields, unexpected
units, and inconsistent time arrays fail the collection without replacing the
last accepted data. HTTP errors are retried at the next collection pass.

Changing endpoint, coordinates, location ID, timezone, or model requires a new
state file. This prevents the deduplication state of one source from hiding
records from another.

The default endpoint is intended for personal, non-commercial use. Review
[Open-Meteo's terms and attribution requirements](https://open-meteo.com/en/terms)
for other uses. The current receiver does not configure a commercial API key.

## Tests

```sh
go test -race ./...
```

Tests use a local HTTP server and a shortened public API response. The
distribution's `scripts/test-weather-integration.py` also verifies restart
replay and corrections through the real OTLP HTTP exporter.
