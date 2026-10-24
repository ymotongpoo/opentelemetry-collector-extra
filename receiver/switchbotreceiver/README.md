# SwitchBot receiver

This metrics receiver reads SwitchBot OpenAPI v1.1 and, optionally, a separate [OHF Matter Server](https://github.com/matter-js/matterjs-server).
It sends no device commands and uses no direct BLE connection.

## Configuration

```yaml
receivers:
  switchbot:
    cloud:
      token: ${env:SWITCHBOT_TOKEN}
      secret: ${env:SWITCHBOT_SECRET}
      collection_interval: 5m
      discovery_interval: 1h
      timeout: 10s
      min_request_interval: 1s
      daily_request_limit: 8000
    battery_interval: 3h
    export_interval: 1m
exporters:
  otlp_http:
    endpoint: http://127.0.0.1:4318
service:
  pipelines:
    metrics:
      receivers: [switchbot]
      exporters: [otlp_http]
```

These intervals are the defaults. Cloud discovery runs at startup. `devices: [DEVICE_ID]` restricts collection to selected physical devices.
Device IDs are trimmed and normalized to uppercase. Device names never determine identity.
For a systemd installation, use the [encrypted credential helper](../../distributions/extra/README.md) instead of persistent plaintext environment variables.

The default cloud endpoint is `https://api.switch-bot.com/v1.1`. Requests use signed HMAC-SHA256 authentication and standard Base64.
Redirects are rejected. Both HTTP status and API `statusCode` must indicate success.
Requests share a minimum spacing and an in-memory rolling 24-hour budget. The default 8,000 leaves headroom below the documented 10,000 requests/day.
The budget belongs to this receiver instance; restarts reset it, and other clients using the account are outside it.
HTTP 429 applies a shared cooldown; authentication errors pause requests for an hour. Failed devices do not discard successful devices' results.

Environmental devices are queried every five minutes. Battery-only devices are queried every three hours.
Battery values returned with faster environmental calls are reused. `enableCloudService` in the inventory is not used to suppress status requests.
A Matter outage does not trigger extra cloud requests between scheduled cloud collections.
A slow collection delays the next collection, so requests do not overlap.

## Measurements and freshness

| Metric | Unit | Meaning |
| --- | --- | --- |
| `switchbot.temperature` | `Cel` | Temperature |
| `switchbot.humidity` | `%` | Relative humidity |
| `switchbot.co2` | `[ppm]` | Carbon dioxide concentration |
| `switchbot.battery` | `%` | Reported battery level, sometimes a coarse range |
| `switchbot.light_level` | `{level}` | Hub brightness stage 1–20; this is not lux |
| `switchbot.power` | `1` | Bot `switchMode` on/off only |
| `switchbot.observation.age` | `s` | Time since observation, with `switchbot.metric` |
| `switchbot.device.available` | `1` | At least one unexpired measurement exists |

Every metric has `switchbot.device.id`, `switchbot.device.name`, and `switchbot.device.type` as resource and datapoint attributes.
Datapoint attributes preserve device separation when a backend converts OTLP to Prometheus without copying resource attributes.
Source changes do not introduce another series: each field selects one value, preferring fresh Matter data and otherwise using fresh cloud data.

Cached values are emitted every minute by default, including battery values. The observation age continues to increase until a real read or value notification arrives.
Environmental values expire after twice their source collection interval plus its timeout. Battery values expire after twice `battery_interval` (six hours by default).
Expired measurements are omitted; their age can remain visible. Availability is data validity, not proof of network reachability.
An unchanged successful Matter read refreshes observation time. A node availability event or cached node snapshot does not.

Missing, null, invalid, and expired measurements are not filled with zero. A reported battery level of zero is retained.
Bots in press mode do not report appliance power or the physical position of a shutter or lock.
IR remotes do not provide observed appliance state. Unknown camera models are probed for a battery field at the battery interval, without assuming support.

## Grafana dashboard

The [importable dashboard example](../../examples/grafana/README.md) visualizes environmental measurements, batteries, device measurement status, and observation ages. Select your Prometheus data source and devices after importing its JSON into Grafana.

## Optional Matter source

Run Matter Server as a separate process and commission or share the bridge with that controller first.
Google Home account linking alone does not commission this controller. The Collector does not pair devices or manage a Matter fabric.
Follow the [Matter Server installation instructions](https://github.com/matter-js/matterjs-server#readme); the protocol reference used here is [v1.5.4](https://github.com/matter-js/matterjs-server/blob/v1.5.4/docs/websockets_api.md).
Keep its WebSocket port private. For bridge commissioning, follow its network commissioning instructions; this receiver needs no Bluetooth adapter.

Merge the following `matter` section into the receiver configuration. Paths below are examples: inspect the commissioned node and select the actual endpoints for each physical device.
Do not copy a neighboring bridged device's attributes. One binding may span several endpoints of the same node.

```yaml
matter:
  enabled: true
  endpoint: ws://127.0.0.1:5580/ws
  collection_interval: 1m
  timeout: 10s
  bindings:
    - device_id: DEVICE_ID_FROM_CLOUD
      node_id: 123
      name: Example meter
      device_type: MeterPro(CO2)
      reachability_attribute: 3/57/17
      attributes:
        temperature: 3/1026/0
        humidity: 4/1029/0
        battery: 5/47/12
        co2: 6/1037/0
```

Use the cloud device ID for the same physical device. `node_id` is a full unsigned 64-bit integer, including values above JavaScript's safe integer range.
For Matter-only operation set `cloud.enabled: false` and supply names/types in bindings.
Only explicitly mapped fields are read. The optional `reachability_attribute` must be true before that binding's values are accepted; use a bridged endpoint's Reachable attribute when available.
Node availability and this endpoint guard are separate checks.

The receiver subscribes with `start_listening`, handles attribute and topology events, and performs real `read_attribute` calls every minute.
Battery reads use `battery_interval`; faster battery notifications are accepted.
After reconnect it retrieves a full node snapshot and reads mapped attributes before emitting Matter measurements.
A read error, timeout, disconnection, node removal, or unreachable binding invalidates affected Matter values and allows cloud fallback.
Cloud-only startup and temporary Matter failures do not stop other Collector pipelines.

Supported Matter conversions are Temperature Measurement (cluster 1026, attribute 0, 0.01°C), Relative Humidity (1029/0, 0.01%), Power Source battery percent (47/12, 0.5%), and CO₂ concentration (1037/0).
CO₂ also reads MeasurementUnit (attribute 8) and MeasurementMedium (9). Only ppm/ppb/ppt in air are accepted and converted to ppm. Mass concentration units are rejected.
Matter illuminance is not substituted for SwitchBot's proprietary light stage. Missing fields continue to use cloud collection.

## Verification scope

Cloud parsing is covered by anonymized responses captured on 2026-10-24 from seven Bots, two Hub 2s, an outdoor meter, a CO₂ meter, and an Outdoor Spotlight Cam.
The CO₂ response contained temperature, humidity, battery, and CO₂ even though its inventory cloud flag was false.
The camera returned a successful empty body, so battery support was not confirmed. Several Bots reported battery zero; these values are preserved.
These observations do not guarantee support for other camera models or firmware.

Tests cover request signing, quotas, cancellation, partial failures, cadence, expiry, source selection, and a fake Matter WebSocket server with reconnects and precise node IDs.
Matter pairing and attribute exposure on the owned bridges still require live verification.
The implementation follows [SwitchBot OpenAPI](https://github.com/OpenWonderLabs/SwitchBotAPI/tree/dc9a6b0e4ad77882c72d186ecd5d134113acdad0) and [Matter Server protocol source](https://github.com/matter-js/matterjs-server/tree/46ea2f2c093d3a349259e67fe611eee25ca84e06).
