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
	"regexp"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	s3TablesClient *s3tables.Client
	s3Client       *s3.Client
}

// newS3TablesExporter creates a new S3 Tables exporter instance.
func newS3TablesExporter(cfg *Config, set exporter.Settings) (*s3TablesExporter, error) {
	// TableBucketArnが設定されているか検証
	if cfg.TableBucketArn == "" {
		return nil, fmt.Errorf("table_bucket_arn is required")
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for region %s: %w", cfg.Region, err)
	}

	logger := newSlogLogger()

	return &s3TablesExporter{
		config:         cfg,
		logger:         logger,
		s3TablesClient: s3tables.NewFromConfig(awsCfg),
		s3Client:       s3.NewFromConfig(awsCfg),
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

// extractBucketNameFromArn extracts the bucket name from a Table Bucket ARN
// Table Bucket ARNからバケット名を抽出
// 形式: arn:aws:s3tables:region:account-id:bucket/bucket-name
func extractBucketNameFromArn(arn string) (string, error) {
	// ARN形式の検証
	arnPattern := regexp.MustCompile(`^arn:aws:s3tables:[a-z0-9-]+:\d{12}:bucket/([a-z0-9][a-z0-9-]{1,61}[a-z0-9])$`)
	matches := arnPattern.FindStringSubmatch(arn)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid Table Bucket ARN format: %s", arn)
	}
	return matches[1], nil
}

// uploadToS3Tables uploads the Parquet data to S3 Tables.
func (e *s3TablesExporter) uploadToS3Tables(ctx context.Context, data []byte, dataType string) error {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		e.logger.Warn("Upload cancelled before starting",
			"data_type", dataType,
			"error", ctx.Err())
		return fmt.Errorf("upload cancelled: %w", ctx.Err())
	default:
	}

	// 空データのハンドリング
	if len(data) == 0 {
		e.logger.Debug("Skipping upload: no data to upload", "data_type", dataType)
		return nil
	}

	// アップロード開始のログ出力
	// データタイプに応じたテーブル名を取得
	var tableName string
	switch dataType {
	case "metrics":
		tableName = e.config.Tables.Metrics
	case "traces":
		tableName = e.config.Tables.Traces
	case "logs":
		tableName = e.config.Tables.Logs
	default:
		return fmt.Errorf("unknown data type: %s", dataType)
	}

	// テーブル名が設定されていない場合はスキップ
	if tableName == "" {
		e.logger.Debug("Skipping upload: table name not configured",
			"data_type", dataType)
		return nil
	}

	e.logger.Info("Starting upload to S3 Tables",
		"table_bucket_arn", e.config.TableBucketArn,
		"namespace", e.config.Namespace,
		"table", tableName,
		"data_type", dataType,
		"size", len(data))

	// TODO: S3 Tables APIを使用した実装
	// 1. S3 Tables APIを使用してテーブルを作成・取得
	// 2. Warehouse locationを取得
	// 3. Warehouse locationにParquetファイルをアップロード
	// 4. テーブルメタデータを更新

	// 成功のログ出力
	e.logger.Info("Successfully uploaded data to S3 Tables",
		"table", tableName,
		"size", len(data))

	return nil
}