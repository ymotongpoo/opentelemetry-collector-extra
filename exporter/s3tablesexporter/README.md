# S3 Tables Exporter

The S3 Tables exporter converts OpenTelemetry telemetry data (metrics, traces, and logs) to Parquet format and stores them in Amazon S3 Tables.

## Configuration

```yaml
exporters:
  s3tables:
    region: us-east-1
    namespace: my-namespace
    table_name: otel-data
```

### Configuration Parameters

- `region` (required): AWS region where the S3 Tables are located
- `namespace` (required): S3 Tables namespace
- `table_name` (required): Name of the table to store data

## Features

- Converts OTLP data to Parquet format
- Supports metrics, traces, and logs
- Integrates with Amazon S3 Tables service
- Efficient columnar storage for analytics

## Example Usage

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:

exporters:
  s3tables:
    region: us-east-1
    namespace: telemetry
    table_name: otel-data

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [s3tables]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [s3tables]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [s3tables]
```