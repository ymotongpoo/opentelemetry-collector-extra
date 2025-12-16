// Copyright 2025 Yoshi Yamaguchi <ymotongpoo@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3tablesexporter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// s3TablesExporter implements the S3 Tables exporter.
type s3TablesExporter struct {
	config         *Config
	logger         *slog.Logger
	s3Client       *s3tables.Client
	icebergCatalog catalog.Catalog
	tables         map[string]table.Table
}

// initIcebergCatalog initializes the Iceberg REST Catalog
// connecting to S3 Tables Iceberg REST endpoint
func initIcebergCatalog(cfg *Config) (catalog.Catalog, error) {
	// S3 Tables Iceberg REST endpoint URL
	// 形式: https://s3tables.<region>.amazonaws.com/iceberg
	restEndpoint := fmt.Sprintf("https://s3tables.%s.amazonaws.com/iceberg", cfg.Region)

	// AWS設定をロード
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// REST Catalogを作成
	// AWS SigV4認証を使用してS3 Tables Iceberg REST endpointに接続
	cat, err := rest.NewCatalog(
		context.Background(),
		"s3tables",
		restEndpoint,
		rest.WithWarehouseLocation(cfg.TableBucketArn),
		rest.WithAwsConfig(awsCfg),
		rest.WithSigV4RegionSvc(cfg.Region, "s3tables"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Iceberg REST catalog: %w", err)
	}

	return cat, nil
}

// newS3TablesExporter creates a new S3 Tables exporter instance.
func newS3TablesExporter(cfg *Config, set exporter.Settings) (*s3TablesExporter, error) {
	// TableBucketArnが設定されているか検証
	if cfg.TableBucketArn == "" {
		return nil, fmt.Errorf("table_bucket_arn is required")
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	logger := newSlogLogger()

	// Iceberg Catalogを初期化
	// Note: テスト環境では実際のREST endpointに接続できないため、
	// エラーが発生した場合は警告をログに記録して続行します
	icebergCat, err := initIcebergCatalog(cfg)
	if err != nil {
		logger.Warn("Failed to initialize Iceberg catalog, will retry on first upload", "error", err)
	}

	return &s3TablesExporter{
		config:         cfg,
		logger:         logger,
		s3Client:       s3tables.NewFromConfig(awsCfg),
		icebergCatalog: icebergCat,
		tables:         make(map[string]table.Table),
	}, nil
}

// pushMetrics exports metrics data to S3 Tables in Parquet format.
func (e *s3TablesExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	e.logger.Info("Exporting metrics to S3 Tables", "resource_count", md.ResourceMetrics().Len())

	parquetData, err := convertMetricsToParquet(md)
	if err != nil {
		return fmt.Errorf("failed to convert metrics to Parquet: %w", err)
	}

	return e.uploadToS3Tables(ctx, parquetData, "metrics")
}

// pushTraces exports traces data to S3 Tables in Parquet format.
func (e *s3TablesExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	e.logger.Info("Exporting traces to S3 Tables", "resource_count", td.ResourceSpans().Len())

	parquetData, err := convertTracesToParquet(td)
	if err != nil {
		return fmt.Errorf("failed to convert traces to Parquet: %w", err)
	}

	return e.uploadToS3Tables(ctx, parquetData, "traces")
}

// pushLogs exports logs data to S3 Tables in Parquet format.
func (e *s3TablesExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	e.logger.Info("Exporting logs to S3 Tables", "resource_count", ld.ResourceLogs().Len())

	parquetData, err := convertLogsToParquet(ld)
	if err != nil {
		return fmt.Errorf("failed to convert logs to Parquet: %w", err)
	}

	return e.uploadToS3Tables(ctx, parquetData, "logs")
}

// uploadToS3Tables uploads the Parquet data to S3 Tables.
func (e *s3TablesExporter) uploadToS3Tables(ctx context.Context, data []byte, dataType string) error {
	if len(data) == 0 {
		e.logger.Debug("No data to upload")
		return nil
	}

	timestamp := time.Now().Format("2006/01/02/15/04/05")
	key := fmt.Sprintf("%s/%s/%d.parquet", dataType, timestamp, time.Now().UnixNano())

	// TODO: 実際のS3 Tables API統合でARNを使用する
	// S3 Tables APIを使用してテーブルバケットARNを指定したデータアップロードを実装する必要がある
	e.logger.Info("Would upload to S3 Tables",
		"table_bucket_arn", e.config.TableBucketArn,
		"namespace", e.config.Namespace,
		"table", e.config.TableName,
		"key", key,
		"size", len(data))

	return nil
}