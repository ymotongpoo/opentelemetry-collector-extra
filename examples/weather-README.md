# Weather history for solar comparisons

The independent `openmeteo` and `jma` receivers emit OTLP **logs**. The
`smartheim` receiver continues to emit instantaneous power metrics. Grafana
Pages can compare their valid time intervals without changing the power
receiver or overwriting historical metric samples.

Run the standalone debug example:

```sh
./otelcol-extra validate --config=examples/weather.yaml
./otelcol-extra --config=examples/weather.yaml
```

This contacts Open-Meteo and JMA. The first JMA pass fetches 30 daily pages
sequentially, at least two seconds apart. It creates state files under
`build/weather-state/`. Stop the Collector normally before moving state files.

For the packaged service, merge the optional
[Open-Meteo](../distributions/extra/openmeteo.yaml) and
[JMA](../distributions/extra/jma.yaml) snippets into its configuration. Their
state files use the service's writable `/var/lib/otelcol-extra` directory.
Alloy must accept and forward **logs** on its OTLP HTTP endpoint. Existing
metric forwarding alone does not establish that logs reach Cloud.

The JMA receiver selects the historical table by `block_no`: four digits for
AMeDAS, five for meteorological stations. See the
[AMeDAS example](jma-amedas.yaml). AMeDAS sunshine from 2021-03-02 is an estimate,
identified by `kind: estimate`; retain that distinction when plotting it
alongside measured sunshine and Open-Meteo model values.

## Delivery boundary

Each instance requires a unique state file. The receiver takes an exclusive
lock, checks the schema and source identity, and writes updates through an
atomic file replacement. Corrupt state fails startup and is not overwritten.
It writes changed records to a durable outbox **before** calling the next
consumer. On a consumer error it retains the outbox and retries it before
fetching more data. A restart resumes pending delivery with the same event IDs.

Acknowledgement means that the **next consumer accepted the logs**, not that
every later service has committed them. The examples therefore omit a batch
processor and disable the Collector exporter's in-memory sending queue and
retries. The receiver retains failed handoffs and retries on its next poll.
If Alloy acknowledges before its Cloud export completes, configure durable
forwarding there to cover that additional failure boundary. Alternatively,
send this pipeline directly to the Cloud OTLP gateway with its credentials.

A crash after downstream acceptance but before local acknowledgement can
produce duplicates. Readers deduplicate by `event_id`. This is at-least-once
delivery at the consumer boundary, not an exactly-once guarantee.

The state file contains the latest content hashes, revision counters,
checkpoints, and pending events. It is **not a historical archive**. Its
120-day default TTL cannot extend Cloud retention. After an entry expires,
revision counters may start again; use `retrieved_at` and `event_id`, not a
global assumption that revision counters never reset.

## Event schema

The log body is a JSON object with `schema_version: 1`. Resource attributes
include `service.name: weather-openmeteo` or `weather-jma` and `weather.source`.
In Loki, select the corresponding `service_name` label. Record attributes
include the location, variable, and event ID. Do not promote observation
timestamps or event IDs to indexed labels.

| Field | Meaning |
| --- | --- |
| `source`, `kind`, `location_id`, `model` | Provenance and model selection. JMA records use `observation` or `estimate`; Open-Meteo uses `model` or `astronomical`. |
| `variable`, `unit`, `aggregation` | What is measured and whether it is an instant, mean, sum, or event time. |
| `period_start`, `period_end` | UTC valid time. Equal boundaries denote an instantaneous value. |
| `value`, `text_value` | Numeric or text value. An unavailable numeric value is explicitly null. |
| `quality`, `raw_value`, `details` | Provider quality, original JMA cell, or Open-Meteo grid information. |
| `retrieved_at` | UTC time this changed record was fetched. It is not a forecast run's initialisation time. |
| `observation_key` | Stable identity of provider, kind, location, model selection, variable, interval, and aggregation. |
| `event_id`, `revision`, `supersedes` | Unique change ID, per-key local counter, and previous event ID. A value changing A → B → A produces three events. |

The OTLP log timestamp is the **delivery attempt time**. Its observed timestamp
is `retrieved_at`. Replaying an old outbox refreshes only the transport timestamp,
so a historical correction does not have to enter Loki as an old log line.
The event body and ID stay unchanged on retry.

## Selecting revisions in Pages

Fetch the retrieval events from Loki, then:

1. Deduplicate identical `event_id` values.
2. Optionally exclude events retrieved after an analysis cutoff to reproduce
   what was known at that time.
3. Group by `observation_key` and choose the event with the latest
   `retrieved_at`. Keep missing-value revisions in this step.
4. Filter the resulting valid intervals to the requested day or period.
5. Apply the quality policy and combine with the solar series.

For example, a correction for October 25 can arrive on October 28. A Loki
query restricted to October 25's log timestamps will miss it. Query the
delivery-time window through the analysis cutoff, splitting long queries and
paginating all results as needed, then filter the JSON valid time. LogQL's
ordinary time bucketing uses log timestamps; `unwrap value` alone does not
move the values to their weather observation intervals. Pages must perform
that mapping explicitly.

An initial query can select:

```logql
{service_name=~"weather-(openmeteo|jma)"} | json
```

Keep JMA observations and Open-Meteo model values as separate series. Replacing
one with the other would conceal differences between an observation site and
a model grid. Open-Meteo's `best_match` is not a fixed model run, so this schema
supports retrieved model versions, not exact forecast-run reconstruction.

Power is in W and energy is the time integral in Wh or kWh. Compare the solar
power averaged over each weather interval with hourly radiation, and integrate
only intervals with acceptable solar coverage. Neither missing weather values
nor missing power measurements should become zero.

## Three-month retention

Retention is a backend setting. Configure both the Smart Heim metric series
and the weather log streams for the required comparison horizon. For at least
three calendar months, allow 93 days; a 90-day policy is only an approximation.
Corrections can be newer than their valid interval, so transport-time retention
and observation-time coverage differ.

Read the current Cloud Logs limit through the `grafanacloud-usage` data source:

```promql
grafanacloud_logs_instance_limits{limit_name="retention_period"} / 1e9 / 86400
```

Select the instance corresponding to the actual Loki data source; an account
can expose several stacks. Also inspect any per-stream retention overrides.
The usage limit is expressed in nanoseconds. Do not infer a metric retention
setting from a query lookback limit or from the age of the oldest available
sample.

If logs are retained for less than the desired horizon, ask Grafana Cloud
Support about an extension for just
`{service_name=~"weather-(openmeteo|jma)"}` and confirm the charge. A stack-wide
extension can affect unrelated logs. The receivers do not change billing or
retention settings. Their local state is not a substitute for such an extension.

See [Cloud usage limits](https://grafana.com/docs/grafana-cloud/platform/pricing-and-usage/usage-limits/),
[Logs pricing and retention](https://grafana.com/docs/grafana-cloud/platform/pricing-and-usage/logs/),
and [OTLP field mapping](https://grafana.com/docs/grafana-cloud/send-data/otlp/otlp-format-considerations/).

## Source attribution

Weather model data: [Open-Meteo](https://open-meteo.com/), CC BY 4.0.
Station observations: [Japan Meteorological Agency](https://www.data.jma.go.jp/).
Show the source and distinguish model values from station observations in
charts and exported analysis.
