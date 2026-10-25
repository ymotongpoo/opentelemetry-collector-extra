# Smart Heim Navi receiver

The `smartheim` receiver polls a Smart Heim Navi information collection unit on the local LAN.
It collects instantaneous solar generation, household consumption, grid import/export, and power for each measured circuit.
The default interval is 30 seconds. The receiver sends HTTP GET requests and needs no cloud account or device configuration changes.

## Data source

The receiver reads these pages:

- `/cgi-bin/inst_val_sv.cgi`: solar generation, total consumption, grid flow, and the device number.
- `/cgi-bin/inst_val_all_branch_sv.cgi`: circuit names, circuit power, and the device number.

The tested unit serves Shift_JIS HTML. Its JavaScript reloads the summary page every 30 seconds, without a separate JSON request.
The receiver parses the HTML structure and decodes Japanese circuit names.
This implementation uses observed CGI behavior. Firmware with a different page layout may require parser changes.
The [manufacturer's troubleshooting guide](https://new.smart-heim.com/n/pub/hems_info/network_err/index.html) describes viewing instantaneous measurements from a computer on the home network.

The number at the bottom of each page identifies the unit. Configure it as `device_id` to select your unit during discovery.
The receiver verifies that the summary and circuit pages report the same number.

## Configuration

```yaml
receivers:
  smartheim:
    collection_interval: 30s
    timeout: 5s
    collect_branches: true
    # device_id: YOUR_DEVICE_NUMBER
    discovery:
      enabled: true
      interval: 1m
      timeout: 3s
      concurrency: 16
      # networks: [192.168.0.0/24]
```

Add `smartheim` to a metrics pipeline. [examples/smartheim.yaml](../../examples/smartheim.yaml) prints the collected metrics with the debug exporter:

```sh
make build
./otelcol-extra --config=examples/smartheim.yaml
```

For the Debian service, use [distributions/extra/smartheim.yaml](../../distributions/extra/smartheim.yaml) alongside the packaged main configuration.
It forwards metrics to the existing Alloy exporter. No credential registration is necessary.

| Setting | Default | Behavior |
| --- | --- | --- |
| `endpoint` | empty | HTTP(S) origin, such as `http://192.168.0.98`; setting it disables discovery |
| `device_id` | empty | Expected device number; discovery requires one matching unit |
| `collection_interval` | `30s` | Delay between the end of one collection and the next |
| `timeout` | `5s` | Timeout for each page request |
| `collect_branches` | `true` | Also fetch the circuit page |
| `discovery.enabled` | `true` | Allow discovery when `endpoint` is empty |
| `discovery.networks` | automatic | Up to four private IPv4 `/24` through `/30` networks |
| `discovery.interval` | `1m` | Minimum delay between discovery attempts |
| `discovery.timeout` | `3s` | Timeout for each discovery request; connections have a 250 ms timeout |
| `discovery.concurrency` | `16` | Number of parallel discovery requests; maximum 32 |

## IP address discovery

Discovery checks the summary CGI on port 80 in the selected networks.
It accepts a response only if the page contains valid generation and consumption values and a device number.
If `device_id` is set, the number must match. Multiple matches produce an error instead of selecting an arbitrary unit.

By default, discovery uses private IPv4 networks from active interfaces and excludes common virtual bridges and tunnels.
Automatic discovery supports interface prefixes from `/24` through `/30` and deduplicates networks shared by Ethernet and Wi-Fi.
Specify `discovery.networks` when your interface uses a larger subnet or the Collector runs in a container.
The Collector must have HTTP access to the device. Discovery needs no root access or external network tools.

After discovery, normal collection contacts only the selected address.
Three consecutive summary failures clear that address and allow another discovery attempt after `discovery.interval`.
The receiver retains the first successful device number for rediscovery during that process lifetime.
Set `device_id` to retain the selection across Collector restarts.
A configured `endpoint` remains fixed; automatic rediscovery applies only when you omit it.

Requests bypass HTTP proxies and reject redirects. Discovery can take several seconds and runs in the receiver's background goroutine.
Startup of other pipelines does not depend on a successful discovery.

## Metrics and missing values

All metrics are gauges in watts (`W`). The resource attribute `smartheim.device.id` identifies the unit independently of its IP address.

| OTLP metric | Meaning |
| --- | --- |
| `smartheim.solar.power` | Solar generation |
| `smartheim.consumption.power` | Total household consumption |
| `smartheim.grid.import.power` | Grid purchase; zero while selling |
| `smartheim.grid.export.power` | Grid sale; zero while purchasing |
| `smartheim.branch.power` | Circuit consumption, with `smartheim.branch.id` and `smartheim.branch.name` |

The displayed Japanese label determines grid direction. A layout without a grid section produces no grid metrics.
The receiver reads grid power directly; it does not derive it from generation minus consumption, which can misrepresent battery operation.
Battery charge/discharge power is outside the current metric set.

Circuit IDs follow slot order in the HTML, including slots marked as unmeasured.
Duplicate circuit names remain distinct through their IDs. Firmware or device configuration changes that reorder slots can change these IDs.
The receiver skips unmeasured values marked `－`; a measured value of zero remains zero.
Circuit totals can differ from whole-house consumption because the two pages are separate snapshots and some circuits lack measurements.

A failed summary request produces no measurement samples. A failed circuit request preserves the successful summary measurements and reports an error.
The receiver never re-exports cached measurements. Collector self-metrics and logs report collection errors.
Timestamps represent retrieval time because these pages expose no precise measurement timestamp.
These instantaneous readings do not provide energy in kWh or historical data.

## Grafana

Import [smartheim-dashboard.json](../../examples/grafana/smartheim-dashboard.json) and select your Prometheus data source.
See the [dashboard instructions](../../examples/grafana/smartheim-README.md) for OTLP resource labels and missing-data behavior.

## Verification

Normal tests use local HTTP servers and synthetic HTML with anonymous device numbers.
They cover Shift_JIS decoding, grid direction, circuit slots, partial failures, discovery ambiguity, request cancellation, and response limits.

```sh
cd receiver/smartheimreceiver
GOTOOLCHAIN=go1.27.3 go test -race -timeout=3m ./...
GOTOOLCHAIN=go1.27.3 go vet ./...
```

An optional test reads a real device and can verify discovery on an explicit LAN:

```sh
SMARTHEIM_TEST_ENDPOINT=http://192.168.0.98 \
SMARTHEIM_TEST_NETWORK=192.168.0.0/24 \
GOTOOLCHAIN=go1.27.3 go test -run TestLiveDevice -v .
```
