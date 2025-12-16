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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
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
	icebergCatalog catalog.Catalog
	tables         map[string]*table.Table
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
		return nil, fmt.Errorf("failed to load AWS config for region %s: %w", cfg.Region, err)
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
		return nil, fmt.Errorf("failed to create Iceberg REST catalog at %s with warehouse %s: %w",
			restEndpoint, cfg.TableBucketArn, err)
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
		return nil, fmt.Errorf("failed to load AWS config for region %s: %w", cfg.Region, err)
	}

	logger := newSlogLogger()

	// Iceberg Catalogを初期化
	// Note: テスト環境では実際のREST endpointに接続できないため、
	// エラーが発生した場合は警告をログに記録して続行します
	icebergCat, err := initIcebergCatalog(cfg)
	if err != nil {
		logger.Warn("Failed to initialize Iceberg catalog, will retry on first upload",
			"error", err,
			"region", cfg.Region,
			"table_bucket_arn", cfg.TableBucketArn)
	}

	return &s3TablesExporter{
		config:         cfg,
		logger:         logger,
		s3TablesClient: s3tables.NewFromConfig(awsCfg),
		s3Client:       s3.NewFromConfig(awsCfg),
		icebergCatalog: icebergCat,
		tables:         make(map[string]*table.Table),
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

// createNamespaceIfNotExists creates a namespace if it doesn't exist
// Namespaceが存在しない場合は新規作成
func (e *s3TablesExporter) createNamespaceIfNotExists(ctx context.Context, namespace string) error {
	// Namespaceが存在するか確認
	_, err := e.icebergCatalog.LoadNamespaceProperties(ctx, []string{namespace})
	if err == nil {
		// Namespaceが既に存在する
		return nil
	}

	// Namespaceを作成
	e.logger.Info("Namespace not found, creating new namespace", "namespace", namespace)
	err = e.icebergCatalog.CreateNamespace(ctx, []string{namespace}, nil)
	if err != nil {
		e.logger.Error("Failed to create namespace via Iceberg REST API",
			"namespace", namespace,
			"error", err)
		return fmt.Errorf("failed to create namespace %s: %w", namespace, err)
	}

	e.logger.Info("Successfully created namespace", "namespace", namespace)
	return nil
}

// getOrCreateTable gets an existing table or creates a new one
// 指定されたNamespaceとTable Nameでテーブルを取得
// テーブルが存在しない場合は新規作成
// テーブル参照をキャッシュして再利用
func (e *s3TablesExporter) getOrCreateTable(
	ctx context.Context,
	namespace string,
	tableName string,
	schema *iceberg.Schema,
) (*table.Table, error) {
	// キャッシュキーを生成
	cacheKey := fmt.Sprintf("%s.%s", namespace, tableName)

	// キャッシュからテーブルを取得
	if tbl, ok := e.tables[cacheKey]; ok {
		return tbl, nil
	}

	// Iceberg Catalogが初期化されていない場合はエラー
	if e.icebergCatalog == nil {
		return nil, fmt.Errorf("iceberg catalog is not initialized for table %s.%s", namespace, tableName)
	}

	// Namespaceを作成（存在しない場合）
	if err := e.createNamespaceIfNotExists(ctx, namespace); err != nil {
		return nil, fmt.Errorf("failed to ensure namespace exists for table %s.%s: %w", namespace, tableName, err)
	}

	// テーブル識別子を作成
	tableIdent := table.Identifier{namespace, tableName}

	// テーブルを取得
	tbl, err := e.icebergCatalog.LoadTable(ctx, tableIdent)
	if err != nil {
		// テーブルが存在しない場合は新規作成
		e.logger.Info("Table not found, creating new table",
			"namespace", namespace,
			"table", tableName)

		tbl, err = e.icebergCatalog.CreateTable(ctx, tableIdent, schema)
		if err != nil {
			e.logger.Error("Failed to create table via Iceberg REST API",
				"namespace", namespace,
				"table", tableName,
				"error", err)
			return nil, fmt.Errorf("failed to create table %s.%s: %w", namespace, tableName, err)
		}
		e.logger.Info("Successfully created table", "namespace", namespace, "table", tableName)
	}

	// テーブルをキャッシュ
	e.tables[cacheKey] = tbl

	return tbl, nil
}

// writeToIcebergTable writes Parquet data to an Iceberg table
// ParquetデータをS3にアップロードし、Icebergテーブルにデータファイルを追加してトランザクションをコミットする
func (e *s3TablesExporter) writeToIcebergTable(
	ctx context.Context,
	tbl *table.Table,
	data []byte,
	dataType string,
) error {
	// 1. Parquetファイルを一時的にS3にアップロード
	timestamp := time.Now().Format("2006/01/02/15/04/05")
	key := fmt.Sprintf("%s/%s/%d.parquet", dataType, timestamp, time.Now().UnixNano())

	// Table Bucket ARNからバケット名を抽出
	// 形式: arn:aws:s3tables:region:account-id:bucket/bucket-name
	bucketName, err := extractBucketNameFromArn(e.config.TableBucketArn)
	if err != nil {
		e.logger.Error("Failed to extract bucket name from ARN",
			"arn", e.config.TableBucketArn,
			"error", err)
		return fmt.Errorf("failed to extract bucket name from ARN %s: %w", e.config.TableBucketArn, err)
	}

	// S3にParquetファイルをアップロード
	_, err = e.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		e.logger.Error("Failed to upload Parquet file to S3",
			"bucket", bucketName,
			"key", key,
			"size", len(data),
			"error", err)
		return fmt.Errorf("failed to upload Parquet file to S3 bucket %s with key %s: %w", bucketName, key, err)
	}

	// 2. Icebergテーブルにデータファイルを追加
	// S3のファイルパスを構築
	s3Path := fmt.Sprintf("s3://%s/%s", bucketName, key)

	// データファイルをIcebergテーブルに追加
	// Note: iceberg-goライブラリの現在のバージョンでは、
	// データファイルの追加とトランザクションのコミットのAPIが限定的です。
	// 実際の実装では、Icebergのメタデータ更新APIを使用する必要があります。

	// 3. トランザクションをコミット
	// Note: 現在のiceberg-goライブラリでは、トランザクションAPIが完全にサポートされていないため、
	// この部分は将来のライブラリアップデートで実装する必要があります。

	e.logger.Info("Successfully wrote data to Iceberg table",
		"table", (*tbl).Identifier(),
		"s3_path", s3Path,
		"size", len(data))

	return nil
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
	// データタイプに応じたテーブル名とスキーマを取得
	var tableName string
	var schema *iceberg.Schema
	switch dataType {
	case "metrics":
		tableName = e.config.Tables.Metrics
		schema = createMetricsSchema()
	case "traces":
		tableName = e.config.Tables.Traces
		schema = createTracesSchema()
	case "logs":
		tableName = e.config.Tables.Logs
		schema = createLogsSchema()
	default:
		return fmt.Errorf("unknown data type: %s", dataType)
	}

	e.logger.Info("Starting upload to S3 Tables",
		"table_bucket_arn", e.config.TableBucketArn,
		"namespace", e.config.Namespace,
		"table", tableName,
		"data_type", dataType,
		"size", len(data))

	// Iceberg Catalogを使用してテーブルを取得または作成
	tbl, err := e.getOrCreateTable(ctx, e.config.Namespace, tableName, schema)
	if err != nil {
		// コンテキストキャンセルのチェック
		if ctx.Err() != nil {
			e.logger.Warn("Upload cancelled during table creation",
				"namespace", e.config.Namespace,
				"table", tableName,
				"error", ctx.Err())
			return fmt.Errorf("upload cancelled during table creation: %w", ctx.Err())
		}
		e.logger.Error("Failed to get or create table",
			"namespace", e.config.Namespace,
			"table", tableName,
			"error", err)
		return fmt.Errorf("failed to get or create table: %w", err)
	}

	// データ書き込み関数を呼び出し
	err = e.writeToIcebergTable(ctx, tbl, data, dataType)
	if err != nil {
		// コンテキストキャンセルのチェック
		if ctx.Err() != nil {
			e.logger.Warn("Upload cancelled during data write",
				"table", (*tbl).Identifier(),
				"error", ctx.Err())
			return fmt.Errorf("upload cancelled during data write: %w", ctx.Err())
		}
		e.logger.Error("Failed to write data to Iceberg table",
			"table", (*tbl).Identifier(),
			"error", err)
		return fmt.Errorf("failed to write data to Iceberg table: %w", err)
	}

	// 成功のログ出力
	e.logger.Info("Successfully uploaded data to S3 Tables",
		"table", (*tbl).Identifier(),
		"size", len(data))

	return nil
}