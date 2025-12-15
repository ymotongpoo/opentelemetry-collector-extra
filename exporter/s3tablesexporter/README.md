# S3 Tables Exporter

The S3 Tables exporter converts OpenTelemetry telemetry data (metrics, traces, and logs) to Parquet format and stores them in Amazon S3 Tables.

## Configuration

```yaml
exporters:
  s3tables:
    table_bucket_arn: arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket
    region: us-east-1
    namespace: my-namespace
    table_name: otel-data
```

### Configuration Parameters

- `table_bucket_arn` (required): Amazon Resource Name (ARN) of the S3 Tables bucket. Must follow the format `arn:aws:s3tables:region:account-id:bucket/bucket-name`
- `region` (required): AWS region where the S3 Tables are located
- `namespace` (required): S3 Tables namespace
- `table_name` (required): Name of the table to store data

**Note:** The `table_bucket_arn` must be a valid S3 Tables bucket ARN. The ARN format consists of:
- Prefix: `arn:aws:s3tables`
- Region: AWS region identifier (e.g., `us-east-1`)
- Account ID: 12-digit AWS account number
- Bucket name: The name of your table bucket (3-63 characters, lowercase letters, numbers, and hyphens)

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
    table_bucket_arn: arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket
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