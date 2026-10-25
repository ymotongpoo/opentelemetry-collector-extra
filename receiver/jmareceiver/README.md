# Japan Meteorological Agency receiver

This pull receiver retrieves **hourly weather data** from JMA's
`hourly_s1.php` meteorological-station and `hourly_a1.php` AMeDAS tables.
It emits revision events as logs and distinguishes observations from estimates.
The initial station is configurable; the example selects Kofu, Yamanashi.
Real-time AMeDAS JSON is not used; both table formats follow the historical
retrieval and revision schedule below.

```yaml
receivers:
  jma:
    prec_no: "49"
    block_no: "47638"
    station_name: 甲府
    state_file: /var/lib/otelcol-extra/weather/jma.json
    recent_days: 7
    revision_days: 30
    revision_interval: 168h
```

Select the receiver in a `logs` pipeline. See the
[complete example](../../examples/weather.yaml) and
[weather history guide](../../examples/weather-README.md).

For AMeDAS, set the four-digit `block_no` shown in the URL of JMA's
[historical-data station selector](https://www.data.jma.go.jp/stats/etrn/index.php).
For example, Katsunuma in Yamanashi uses `prec_no: "49"`, `block_no: "0433"`,
and `station_name: 勝沼`. Preserve leading zeros by quoting the identifier.
The receiver automatically selects `hourly_a1.php` for four digits and
`hourly_s1.php` for five digits. The historical-search ID is **not** the
five-digit station ID used by the real-time AMeDAS JSON feed.

See the [standalone AMeDAS configuration](../../examples/jma-amedas.yaml).
Multiple instances such as `jma/kofu` and `jma/katsunuma` can run together;
give each a unique `state_file` and include both in the logs pipeline.
When changing a station or endpoint, use a new state file. The default effective
URL and state identity for existing five-digit station configurations are unchanged.

## Retrieval and revisions

The receiver checks the previous seven Japanese calendar days once per day.
Once a week it also checks the rest of the previous 30 days. Both passes run
on the first start. Completion checkpoints survive a restart. An unavailable
page or failed export leaves the pass incomplete and retries on the next poll.
Already delivered, unchanged records do not create new events on that retry.

JMA's historical table normally covers yesterday and earlier dates. A response
that says a date is not yet available is a collection failure, not an empty
observation. Date, optional station name, column headings, and all 24 rows must
match before the page is accepted. Schema drift fails closed.

| Setting | Default | Meaning |
| --- | --- | --- |
| `prec_no`, `block_no` | Required | Two-digit prefecture and four-digit AMeDAS or five-digit meteorological-station ID from historical data search. |
| `station_name` | Empty | Optional exact station-name check against the page heading. |
| `state_file` | Required | Durable outbox and checkpoints; unique per instance. |
| `endpoint` | Automatic | `https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php` or `hourly_s1.php`, selected by `block_no`. An explicit URL overrides the request destination, not the expected table format. No query string or credentials. |
| `collection_interval` | `1h` | Poll completion checkpoints and retry failed passes. |
| `recent_days` | `7` | Daily recheck window, 1–31 days. |
| `revision_days` | `30` | Weekly recheck window, at least `recent_days`, at most 366. |
| `revision_interval` | `168h` | Delay between completed wider passes; at least 24h. |
| `min_request_interval` | `2s` | Minimum delay between request starts; pages are fetched sequentially. |
| `timeout` | `30s` | Timeout per HTTP request. |
| `state_ttl` | `2880h` | Deduplication-state lifetime after the last change; not Cloud retention. |

For an older correction or initial import, set an explicit inclusive range:

```yaml
backfill:
  start_date: "2026-07-25"
  end_date: "2026-10-24"
  request_id: initial-three-months
```

The range must end before today in Japan and contain at most 366 days.
Progress is saved after each accepted day. A completed range does not run
again on restart. To recheck that same range later, change `request_id`.
Normal recent and revision passes run first. Weather data from before the
start of solar collection cannot reconstruct missing solar measurements.

JMA can revise data outside these windows. Thirty days is a polling policy,
not a guarantee that older observations are final. See JMA's
[revision notices](https://www.data.jma.go.jp/stats/etrn/kako_data.html).

## Fields and quality

The receiver preserves the original cell text in `raw_value` and emits:

| Variable | Tables | Unit | Time interpretation |
| --- | --- | --- | --- |
| `shortwave_radiation` | s1 | W/m² | Preceding-hour mean, converted from the hourly MJ/m² total: multiply by 1,000,000 / 3,600. |
| `sunshine_duration` | s1, a1 | seconds | Preceding-hour sum, converted from hours. |
| `precipitation` | s1, a1 | mm | Preceding-hour sum. |
| `air_temperature` | s1, a1 | °C | Instant at the table's hour. |
| `cloud_cover` | s1 | % | Instant; numeric tenths are multiplied by ten. |
| `weather_condition` | s1 | Text | Instant; preserves the Japanese weather-image description. |

AMeDAS does not provide the s1 radiation, cloud-cover, or weather-condition
columns. The receiver does not invent these records or derive weather categories
from rainfall. Rain-only stations are supported: temperature and sunshine cells
marked `///` remain null. Wind, humidity, and snow columns are not currently emitted.

From **2021-03-02 JST**, AMeDAS sunshine duration comes from JMA's estimated
weather distribution, based on satellite observations. These records use
`kind: estimate`, `details.method: jma_estimated_weather_distribution`, and
`quality: estimated` for normal numeric values. Missing and reduced-quality
markers retain their own quality values. Earlier AMeDAS sunshine records use
`kind: observation`. All AMeDAS records include `details.table_format: hourly_a1`.
Meteorological-station sunshine retains `kind: observation`.
Do not compare estimated and measured sunshine as if they used the same method;
see [JMA's explanation](https://www.data.jma.go.jp/stats/data/mdrr/man/sampling.html#amedassun2021).

Hour 24 is midnight at the beginning of the next day. All event intervals are
encoded in UTC; the table's dates and retrieval schedule use Asia/Tokyo.
The weather condition at a single hour does not establish how many minutes
within that hour were sunny or rainy.

The quality field distinguishes normal numeric values, `quasi_normal` (`)`),
`insufficient` (`]`), `missing` (`×`), `not_observed_or_missing` (`///`),
`not_available` (blank), and `suspect` (`#`). A `--` cell has value zero and
quality `no_phenomenon`, following JMA's definition. Cloud amounts `0+` and
`10-` have quality `bounded` and a null value; they are not exact 0% and 100%.
Unknown numeric or quality formats fail the page rather than inventing a value.

Missing or withdrawn values remain null. A later change from a numeric value
to missing is a new revision that supersedes the earlier number. Consumers
must select the latest revision **before** filtering by quality; otherwise an
older, withdrawn value can reappear.

References: [JMA's data FAQ](https://www.jma.go.jp/jma/kishou/know/faq/faq17.html),
[cell symbols](https://www.data.jma.go.jp/stats/data/mdrr/man/remark.html),
[Kofu hourly table](https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php?prec_no=49&block_no=47638&year=2026&month=10&day=24&view=p1).
Display attribution to JMA when using or publishing these data.

## Tests

```sh
go test -race ./...
```

Public table fixtures cover Kofu, Katsunuma (including pre-2021 sunshine), and
rain-only Uenohara: numeric conversions, midnight, missing cells, weather image
descriptions, and estimated sunshine. Local HTTP tests cover daily and weekly
passes, explicit backfill, rechecks, withdrawals, and unsuccessful handoffs.
The Collector integration test runs meteorological and AMeDAS instances together
and checks durable replay after restart.
