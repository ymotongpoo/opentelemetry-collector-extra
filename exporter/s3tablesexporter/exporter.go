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
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TableInfo holds cached table information
// テーブル情報をキャッシュするための構造体
type TableInfo struct {
	TableARN          string
	WarehouseLocation string
	VersionToken      string
}

// s3ClientInterface defines the S3 client interface for testing
// テスト用のS3クライアントインターフェース
type s3ClientInterface interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// s3TablesClientInterface defines the S3 Tables client interface for testing
// テスト用のS3 Tablesクライアントインターフェース
type s3TablesClientInterface interface {
	GetTable(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error)
	CreateTable(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error)
	GetNamespace(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error)
	CreateNamespace(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error)
	GetTableMetadataLocation(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error)
	UpdateTableMetadataLocation(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error)
}

// s3TablesExporter implements the S3 Tables exporter.
type s3TablesExporter struct {
	config         *Config
	logger         *slog.Logger
	s3TablesClient s3TablesClientInterface
	s3Client       s3ClientInterface
	tableCache     map[string]*TableInfo // キャッシュキーは "namespace.tableName" 形式
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
		tableCache:     make(map[string]*TableInfo),
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

// marshalSchemaToJSON converts a schema map to JSON string
// スキーママップをJSON文字列に変換
func marshalSchemaToJSON(schema map[string]interface{}) (string, error) {
	jsonBytes, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to marshal schema: %w", err)
	}
	return string(jsonBytes), nil
}

// convertToIcebergSchema converts a schema map to IcebergSchema
// スキーママップをIcebergSchema形式に変換
func convertToIcebergSchema(schema map[string]interface{}) (*IcebergSchema, error) {
	// スキーマのフィールドを抽出
	fieldsInterface, ok := schema["fields"]
	if !ok {
		return nil, fmt.Errorf("schema missing 'fields' key")
	}

	fieldsSlice, ok := fieldsInterface.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("schema 'fields' is not a slice of maps")
	}

	// SchemaFieldsに変換
	schemaFields := make([]IcebergSchemaField, 0, len(fieldsSlice))
	for _, fieldMap := range fieldsSlice {
		field, err := convertToSchemaField(fieldMap)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field: %w", err)
		}
		schemaFields = append(schemaFields, field)
	}

	return &IcebergSchema{
		SchemaID: 0, // デフォルトのスキーマID
		Fields:   schemaFields,
	}, nil
}

// convertToSchemaField converts a field map to IcebergSchemaField
// フィールドマップをIcebergSchemaField形式に変換
func convertToSchemaField(fieldMap map[string]interface{}) (IcebergSchemaField, error) {
	// IDを取得
	idInterface, ok := fieldMap["id"]
	if !ok {
		return IcebergSchemaField{}, fmt.Errorf("field missing 'id' key")
	}
	// IDはfloat64として解析される可能性があるため、型アサーションを試みる
	var id int
	switch v := idInterface.(type) {
	case int:
		id = v
	case float64:
		id = int(v)
	default:
		return IcebergSchemaField{}, fmt.Errorf("field 'id' is not a number")
	}

	// 名前を取得
	nameInterface, ok := fieldMap["name"]
	if !ok {
		return IcebergSchemaField{}, fmt.Errorf("field missing 'name' key")
	}
	name, ok := nameInterface.(string)
	if !ok {
		return IcebergSchemaField{}, fmt.Errorf("field 'name' is not a string")
	}

	// タイプを取得
	typeInterface, ok := fieldMap["type"]
	if !ok {
		return IcebergSchemaField{}, fmt.Errorf("field missing 'type' key")
	}

	// タイプの処理
	var fieldType interface{}
	if typeStr, ok := typeInterface.(string); ok {
		// プリミティブ型の場合、そのまま文字列として保持
		fieldType = typeStr
	} else if typeMap, ok := typeInterface.(map[string]interface{}); ok {
		// 複合型の場合、そのままmap[string]interface{}として保持
		fieldType = typeMap
	} else {
		// サポートされていない型の場合はエラー
		return IcebergSchemaField{}, fmt.Errorf("field 'type' must be string or map, got %T", typeInterface)
	}

	// requiredを取得
	required := false
	if requiredInterface, ok := fieldMap["required"]; ok {
		if req, ok := requiredInterface.(bool); ok {
			required = req
		}
	}

	return IcebergSchemaField{
		ID:       id,
		Name:     name,
		Required: required,
		Type:     fieldType,
	}, nil
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

// extractBucketFromWarehouseLocation extracts the bucket name from a warehouse location
// Warehouse locationからバケット名を抽出
// 形式: s3://bucket-name または s3://bucket-name/path
func extractBucketFromWarehouseLocation(warehouseLocation string) (string, error) {
	// s3://形式の検証
	s3Pattern := regexp.MustCompile(`^s3://([^/]+)`)
	matches := s3Pattern.FindStringSubmatch(warehouseLocation)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid warehouse location format: %s (expected s3://bucket-name)", warehouseLocation)
	}
	return matches[1], nil
}

// generateDataFileKey generates an Iceberg-format object key for data files
// Iceberg形式のデータファイル用オブジェクトキーを生成
// 形式: data/{timestamp}-{uuid}.parquet
func generateDataFileKey(dataType string) string {
	// タイムスタンプを生成（YYYYMMDDHHmmss形式）
	timestamp := time.Now().UTC().Format("20060102-150405")
	// UUIDを生成
	id := uuid.New().String()
	// キーを生成
	return fmt.Sprintf("data/%s-%s.parquet", timestamp, id)
}

// uploadToWarehouseLocation uploads Parquet data to the table's warehouse location
// Warehouse locationにParquetデータをアップロード
func (e *s3TablesExporter) uploadToWarehouseLocation(ctx context.Context, warehouseLocation string, data []byte, dataType string) (string, error) {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("upload cancelled: %w", ctx.Err())
	default:
	}

	// Warehouse locationからバケット名を抽出
	bucketName, err := extractBucketFromWarehouseLocation(warehouseLocation)
	if err != nil {
		return "", fmt.Errorf("failed to extract bucket name from warehouse location: %w", err)
	}

	// Iceberg形式のオブジェクトキーを生成
	objectKey := generateDataFileKey(dataType)

	// S3 PutObject APIを使用してアップロード
	putInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	}

	_, err = e.s3Client.PutObject(ctx, putInput)
	if err != nil {
		return "", fmt.Errorf("failed to upload Parquet file to warehouse location: %w", err)
	}

	// アップロードされたファイルのS3パスを返す
	s3Path := fmt.Sprintf("s3://%s/%s", bucketName, objectKey)

	e.logger.Debug("Uploaded Parquet file to warehouse location",
		"warehouse_location", warehouseLocation,
		"bucket", bucketName,
		"key", objectKey,
		"s3_path", s3Path,
		"size", len(data))

	return s3Path, nil
}

// getOrCreateTable gets an existing table or creates a new one
// テーブルが存在する場合は取得、存在しない場合は作成
func (e *s3TablesExporter) getOrCreateTable(ctx context.Context, namespace, tableName string, schema map[string]interface{}) (*TableInfo, error) {
	// キャッシュキーを生成
	cacheKey := fmt.Sprintf("%s.%s", namespace, tableName)

	// キャッシュをチェック
	if cachedInfo, ok := e.tableCache[cacheKey]; ok {
		e.logger.Debug("Using cached table information",
			"namespace", namespace,
			"table", tableName)
		return cachedInfo, nil
	}

	// テーブルの取得を試みる
	tableInfo, err := e.getTable(ctx, namespace, tableName)
	if err == nil {
		// テーブルが存在する場合はキャッシュに保存
		e.tableCache[cacheKey] = tableInfo
		return tableInfo, nil
	}

	// テーブルが存在しない場合は、まずNamespaceを作成
	if err := e.createNamespaceIfNotExists(ctx, namespace); err != nil {
		return nil, fmt.Errorf("failed to create namespace: %w", err)
	}

	// テーブルを作成
	tableInfo, err = e.createTable(ctx, namespace, tableName, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// キャッシュに保存
	e.tableCache[cacheKey] = tableInfo

	return tableInfo, nil
}

// getTable retrieves table information using S3 Tables API
// S3 Tables APIを使用してテーブル情報を取得
func (e *s3TablesExporter) getTable(ctx context.Context, namespace, tableName string) (*TableInfo, error) {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("table retrieval cancelled: %w", ctx.Err())
	default:
	}

	// テーブル情報を取得
	getInput := &s3tables.GetTableInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      &namespace,
		Name:           &tableName,
	}

	output, err := e.s3TablesClient.GetTable(ctx, getInput)
	if err != nil {
		return nil, fmt.Errorf("failed to get table %s.%s: %w", namespace, tableName, err)
	}

	// TableInfoを構築
	tableInfo := &TableInfo{
		TableARN:          *output.TableARN,
		WarehouseLocation: *output.WarehouseLocation,
		VersionToken:      *output.VersionToken,
	}

	e.logger.Debug("Retrieved table information",
		"namespace", namespace,
		"table", tableName,
		"table_arn", tableInfo.TableARN,
		"warehouse_location", tableInfo.WarehouseLocation)

	return tableInfo, nil
}

// createTable creates a new table using S3 Tables API
// S3 Tables APIを使用してテーブルを作成
func (e *s3TablesExporter) createTable(ctx context.Context, namespace, tableName string, schema map[string]interface{}) (*TableInfo, error) {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("table creation cancelled: %w", ctx.Err())
	default:
	}

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema to Iceberg format: %w", err)
	}

	// 独自のIcebergMetadataを作成
	metadata := createInitialIcebergMetadata(icebergSchema)

	// JSONにシリアライズ
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		e.logger.Error("Failed to marshal metadata to JSON",
			"namespace", namespace,
			"table", tableName,
			"error", err)
		return nil, fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	// デバッグログ: メタデータの内容を出力
	e.logger.Debug("Serialized Iceberg metadata",
		"namespace", namespace,
		"table", tableName,
		"metadata_json", string(metadataJSON))

	// JSON文字列としてメタデータを送信
	// 注: AWS SDKのtypes.IcebergMetadataはSchemaフィールドのみをサポート
	// 完全なIcebergメタデータを送信するには、独自のスキーマをAWS SDKの形式に変換
	awsSchemaFields := make([]types.SchemaField, 0, len(icebergSchema.Fields))
	for _, field := range icebergSchema.Fields {
		var typeStr string
		if field.IsPrimitiveType() {
			primitiveType, _ := field.GetPrimitiveType()
			typeStr = primitiveType
		} else {
			// 複合型の場合、JSON文字列として表現
			jsonBytes, err := json.Marshal(field.Type)
			if err != nil {
				e.logger.Error("Failed to marshal field type to JSON",
					"namespace", namespace,
					"table", tableName,
					"field", field.Name,
					"error", err)
				return nil, fmt.Errorf("failed to marshal field type to JSON: %w", err)
			}
			typeStr = string(jsonBytes)
		}
		awsSchemaFields = append(awsSchemaFields, types.SchemaField{
			Name:     &field.Name,
			Type:     &typeStr,
			Required: field.Required,
		})
	}

	// AWS SDKのIcebergMetadataを作成
	awsMetadata := types.IcebergMetadata{
		Schema: &types.IcebergSchema{
			Fields: awsSchemaFields,
		},
		Properties: map[string]string{
			// 完全なIcebergメタデータをPropertiesに格納
			"iceberg.metadata.json": string(metadataJSON),
		},
	}

	// テーブル作成
	createInput := &s3tables.CreateTableInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      &namespace,
		Name:           &tableName,
		Format:         types.OpenTableFormatIceberg,
		Metadata:       &types.TableMetadataMemberIceberg{Value: awsMetadata},
	}

	output, err := e.s3TablesClient.CreateTable(ctx, createInput)
	if err != nil {
		e.logger.Error("Failed to create table",
			"namespace", namespace,
			"table", tableName,
			"metadata_json", string(metadataJSON),
			"error", err)
		return nil, fmt.Errorf("failed to create table %s.%s: %w", namespace, tableName, err)
	}

	// GetTableを呼び出してwarehouse locationを取得
	getOutput, err := e.s3TablesClient.GetTable(ctx, &s3tables.GetTableInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      &namespace,
		Name:           &tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get table after creation %s.%s: %w", namespace, tableName, err)
	}

	// TableInfoを構築
	tableInfo := &TableInfo{
		TableARN:          *output.TableARN,
		WarehouseLocation: *getOutput.WarehouseLocation,
		VersionToken:      *output.VersionToken,
	}

	e.logger.Info("Created table",
		"namespace", namespace,
		"table", tableName,
		"table_arn", tableInfo.TableARN,
		"warehouse_location", tableInfo.WarehouseLocation)

	return tableInfo, nil
}



// createNamespaceIfNotExists creates a namespace if it doesn't exist
// Namespaceが存在しない場合は作成する
func (e *s3TablesExporter) createNamespaceIfNotExists(ctx context.Context, namespace string) error {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return fmt.Errorf("namespace creation cancelled: %w", ctx.Err())
	default:
	}

	// Table Bucket ARNからバケット名を抽出
	bucketName, err := extractBucketNameFromArn(e.config.TableBucketArn)
	if err != nil {
		return fmt.Errorf("failed to extract bucket name from ARN: %w", err)
	}

	// Namespaceの存在確認
	getInput := &s3tables.GetNamespaceInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      &namespace,
	}

	_, err = e.s3TablesClient.GetNamespace(ctx, getInput)
	if err == nil {
		// Namespaceが既に存在する
		e.logger.Debug("Namespace already exists",
			"namespace", namespace,
			"bucket", bucketName)
		return nil
	}

	// Namespaceが存在しない場合は作成
	// Namespaceは階層構造をサポートするため配列形式
	createInput := &s3tables.CreateNamespaceInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      []string{namespace},
	}

	_, err = e.s3TablesClient.CreateNamespace(ctx, createInput)
	if err != nil {
		return fmt.Errorf("failed to create namespace %s: %w", namespace, err)
	}

	e.logger.Info("Created namespace",
		"namespace", namespace,
		"bucket", bucketName)

	return nil
}

// commitSnapshot commits a new snapshot to the Iceberg table
// 新しいスナップショットをIcebergテーブルにコミット
//
// この関数は以下の処理を行います：
// 1. 既存メタデータの取得
// 2. マニフェストファイルの生成
// 3. 新しいスナップショットの作成
// 4. 新しいメタデータファイルの生成
// 5. メタデータファイルとマニフェストファイルのアップロード
// 6. UpdateTableMetadataLocation APIの呼び出し
// 7. バージョントークンを使用した楽観的並行性制御
// 8. ConflictException発生時のリトライ（最大3回）
func (e *s3TablesExporter) commitSnapshot(
	ctx context.Context,
	namespace string,
	tableName string,
	tableInfo *TableInfo,
	dataFilePaths []string,
	totalDataSize int64,
) error {
	// リトライロジック: ConflictException発生時に最大3回リトライ
	const maxRetries = 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := e.commitSnapshotAttempt(ctx, namespace, tableName, tableInfo, dataFilePaths, totalDataSize, attempt)
		if err == nil {
			// 成功
			return nil
		}

		lastErr = err

		// ConflictExceptionかどうかを確認
		// AWS SDK v2では、エラーメッセージに"ConflictException"が含まれているかチェック
		isConflict := false
		if err != nil {
			errMsg := err.Error()
			if len(errMsg) > 0 && (
				// AWS SDK v2のエラーメッセージパターン
				regexp.MustCompile(`ConflictException`).MatchString(errMsg) ||
				regexp.MustCompile(`conflict`).MatchString(errMsg) ||
				regexp.MustCompile(`version token`).MatchString(errMsg)) {
				isConflict = true
			}
		}

		if !isConflict {
			// ConflictException以外のエラーの場合はリトライしない
			e.logger.Error("Snapshot commit failed with non-retryable error",
				"namespace", namespace,
				"table", tableName,
				"attempt", attempt,
				"error", err)
			return err
		}

		// ConflictExceptionの場合
		if attempt < maxRetries {
			e.logger.Warn("Snapshot commit failed due to version conflict, retrying",
				"namespace", namespace,
				"table", tableName,
				"attempt", attempt,
				"max_retries", maxRetries,
				"error", err)

			// 最新のメタデータを再取得するため、次のリトライで新しいバージョントークンを使用
			// TableInfoのVersionTokenは次のgetTableMetadata呼び出しで更新される
		} else {
			// 最大リトライ回数に達した
			e.logger.Error("Snapshot commit failed after maximum retries",
				"namespace", namespace,
				"table", tableName,
				"attempts", maxRetries,
				"data_files_count", len(dataFilePaths),
				"total_data_size", totalDataSize,
				"error", err)
		}
	}

	// すべてのリトライが失敗した場合
	return fmt.Errorf("failed to commit snapshot after %d attempts: %w", maxRetries, lastErr)
}

// commitSnapshotAttempt performs a single attempt to commit a snapshot
// スナップショットコミットの1回の試行を実行
func (e *s3TablesExporter) commitSnapshotAttempt(
	ctx context.Context,
	namespace string,
	tableName string,
	tableInfo *TableInfo,
	dataFilePaths []string,
	totalDataSize int64,
	attempt int,
) error {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		e.logger.Warn("Snapshot commit cancelled",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", ctx.Err())
		return fmt.Errorf("snapshot commit cancelled: %w", ctx.Err())
	default:
	}

	e.logger.Info("Starting snapshot commit",
		"namespace", namespace,
		"table", tableName,
		"attempt", attempt,
		"data_files_count", len(dataFilePaths),
		"total_data_size", totalDataSize)

	// 1. 既存メタデータの取得
	existingMetadata, err := e.getTableMetadata(ctx, namespace, tableName, tableInfo)
	if err != nil {
		e.logger.Error("Failed to get table metadata",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 2. 新しいシーケンス番号を計算
	newSequenceNumber := existingMetadata.LastSequenceNumber + 1

	// 3. 新しいスナップショットIDを生成
	snapshotID := time.Now().UnixMilli()

	// 4. バージョン番号を計算（既存のスナップショット数 + 1）
	version := len(existingMetadata.Snapshots) + 1

	// 5. マニフェストファイル名を生成（アップロード前に決定）
	manifestFileName := generateManifestFileName(snapshotID)
	var manifestKey string
	// Warehouse locationからバケット名とプレフィックスを抽出
	bucket, prefix, err := extractBucketAndPrefixFromWarehouseLocation(tableInfo.WarehouseLocation)
	if err != nil {
		e.logger.Error("Failed to extract bucket and prefix from warehouse location",
			"warehouse_location", tableInfo.WarehouseLocation,
			"error", err)
		return fmt.Errorf("failed to extract bucket and prefix: %w", err)
	}
	if prefix != "" {
		manifestKey = fmt.Sprintf("%s/metadata/%s", prefix, manifestFileName)
	} else {
		manifestKey = fmt.Sprintf("metadata/%s", manifestFileName)
	}
	manifestPath := fmt.Sprintf("s3://%s/%s", bucket, manifestKey)

	// 6. マニフェストファイルの生成
	manifest, err := generateManifest(dataFilePaths, snapshotID, newSequenceNumber)
	if err != nil {
		e.logger.Error("Failed to generate manifest",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to generate manifest: %w", err)
	}

	// 7. 新しいスナップショットの作成
	addedFiles := len(dataFilePaths)
	addedRecords := int64(0) // TODO: 実際のレコード数を計算
	totalFiles := len(existingMetadata.Snapshots) + addedFiles
	totalRecords := int64(0) // TODO: 実際の合計レコード数を計算

	newSnapshot, err := generateSnapshot(
		manifestPath,
		newSequenceNumber,
		addedFiles,
		addedRecords,
		totalFiles,
		totalRecords,
	)
	if err != nil {
		e.logger.Error("Failed to generate snapshot",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to generate snapshot: %w", err)
	}

	// 8. メタデータファイル名を生成（アップロード前に決定）
	metadataFileName := generateMetadataFileName(version)
	var metadataKey string
	if prefix != "" {
		metadataKey = fmt.Sprintf("%s/metadata/%s", prefix, metadataFileName)
	} else {
		metadataKey = fmt.Sprintf("metadata/%s", metadataFileName)
	}
	metadataPath := fmt.Sprintf("s3://%s/%s", bucket, metadataKey)

	// 9. 新しいメタデータファイルの生成
	newMetadata := generateNewMetadata(existingMetadata, *newSnapshot, metadataPath)

	// 10. メタデータファイルとマニフェストファイルをアップロード（1回のみ）
	metadataPath, manifestPath, err = uploadMetadata(
		ctx,
		e.s3Client,
		tableInfo.WarehouseLocation,
		newMetadata,
		manifest,
		version,
	)
	if err != nil {
		e.logger.Error("Failed to upload metadata",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	// 11. UpdateTableMetadataLocation APIの呼び出し
	// バージョントークンを使用した楽観的並行性制御
	// 注: VersionTokenは値をコピーする必要がある（ポインタを直接使用すると後で値が変更される）
	currentVersionToken := tableInfo.VersionToken
	updateInput := &s3tables.UpdateTableMetadataLocationInput{
		TableBucketARN:   &e.config.TableBucketArn,
		Namespace:        &namespace,
		Name:             &tableName,
		MetadataLocation: &metadataPath,
		VersionToken:     &currentVersionToken,
	}

	updateOutput, err := e.s3TablesClient.UpdateTableMetadataLocation(ctx, updateInput)
	if err != nil {
		e.logger.Error("Failed to update table metadata location",
			"namespace", namespace,
			"table", tableName,
			"attempt", attempt,
			"metadata_location", metadataPath,
			"version_token", tableInfo.VersionToken,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to update table metadata location: %w", err)
	}

	// バージョントークンを更新
	tableInfo.VersionToken = *updateOutput.VersionToken

	e.logger.Info("Successfully committed snapshot",
		"namespace", namespace,
		"table", tableName,
		"attempt", attempt,
		"snapshot_id", newSnapshot.SnapshotID,
		"data_files_count", len(dataFilePaths),
		"total_data_size", totalDataSize,
		"metadata_location", metadataPath,
		"manifest_location", manifestPath,
		"new_version_token", tableInfo.VersionToken)

	return nil
}

// updateTableMetadata updates the table metadata after uploading data files
// データファイルアップロード後にテーブルメタデータを更新
func (e *s3TablesExporter) updateTableMetadata(ctx context.Context, tableInfo *TableInfo, dataFilePath string) error {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return fmt.Errorf("metadata update cancelled: %w", ctx.Err())
	default:
	}

	// AWS S3 Tablesの動作について:
	// S3 Tablesは自動的にwarehouse location内のデータファイルを認識し、
	// メタデータを管理します。データファイルをアップロードすると、
	// S3 Tablesは自動的にファイル圧縮とスナップショット管理を行います。
	//
	// UpdateTableMetadataLocation APIは、カスタムメタデータファイルを
	// 使用する場合にのみ必要です。現在の実装では、S3 Tablesの
	// 自動メタデータ管理機能を使用するため、明示的なメタデータ更新は
	// 不要です。
	//
	// 将来的に、カスタムメタデータ管理が必要になった場合は、
	// 以下のようにUpdateTableMetadataLocation APIを使用できます:
	//
	// updateInput := &s3tables.UpdateTableMetadataLocationInput{
	//     TableBucketARN:   &e.config.TableBucketArn,
	//     Namespace:        aws.String(namespace),
	//     Name:             aws.String(tableName),
	//     MetadataLocation: aws.String(metadataLocation),
	//     VersionToken:     &tableInfo.VersionToken,
	// }
	// _, err := e.s3TablesClient.UpdateTableMetadataLocation(ctx, updateInput)

	e.logger.Debug("Data file uploaded to warehouse location, S3 Tables will automatically manage metadata",
		"table_arn", tableInfo.TableARN,
		"data_file_path", dataFilePath)

	return nil
}

// getTableMetadata retrieves the current table metadata
// 現在のテーブルメタデータを取得
func (e *s3TablesExporter) getTableMetadata(ctx context.Context, namespace, tableName string, tableInfo *TableInfo) (*IcebergMetadata, error) {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("metadata retrieval cancelled: %w", ctx.Err())
	default:
	}

	e.logger.Debug("Retrieving table metadata",
		"namespace", namespace,
		"table", tableName,
		"table_arn", tableInfo.TableARN)

	// GetTableMetadataLocation APIを使用してメタデータロケーションとバージョントークンを取得
	getMetadataInput := &s3tables.GetTableMetadataLocationInput{
		TableBucketARN: &e.config.TableBucketArn,
		Namespace:      &namespace,
		Name:           &tableName,
	}

	metadataLocationOutput, err := e.s3TablesClient.GetTableMetadataLocation(ctx, getMetadataInput)
	if err != nil {
		e.logger.Error("Failed to get table metadata location",
			"namespace", namespace,
			"table", tableName,
			"error", err)
		return nil, fmt.Errorf("failed to get table metadata location for %s.%s: %w", namespace, tableName, err)
	}

	// メタデータロケーションとバージョントークンを取得
	metadataLocation := *metadataLocationOutput.MetadataLocation
	versionToken := *metadataLocationOutput.VersionToken

	e.logger.Debug("Retrieved metadata location",
		"namespace", namespace,
		"table", tableName,
		"metadata_location", metadataLocation,
		"version_token", versionToken)

	// TableInfoのVersionTokenを更新
	tableInfo.VersionToken = versionToken

	// S3 GetObject APIを使用してメタデータファイルをダウンロード
	// メタデータロケーションからバケット名とキーを抽出
	bucket, key, err := parseS3URI(metadataLocation)
	if err != nil {
		e.logger.Error("Failed to parse metadata location",
			"metadata_location", metadataLocation,
			"error", err)
		return nil, fmt.Errorf("failed to parse metadata location %s: %w", metadataLocation, err)
	}

	getObjectInput := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	getObjectOutput, err := e.s3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		e.logger.Error("Failed to download metadata file",
			"bucket", bucket,
			"key", key,
			"error", err)
		return nil, fmt.Errorf("failed to download metadata file from %s: %w", metadataLocation, err)
	}
	defer getObjectOutput.Body.Close()

	// メタデータファイルをIcebergMetadata構造体に解析
	var metadata IcebergMetadata
	decoder := json.NewDecoder(getObjectOutput.Body)
	if err := decoder.Decode(&metadata); err != nil {
		e.logger.Error("Failed to parse metadata file",
			"metadata_location", metadataLocation,
			"error", err)
		return nil, fmt.Errorf("failed to parse metadata file from %s: %w", metadataLocation, err)
	}

	e.logger.Info("Successfully retrieved and parsed table metadata",
		"namespace", namespace,
		"table", tableName,
		"format_version", metadata.FormatVersion,
		"current_snapshot_id", metadata.CurrentSnapshotID,
		"snapshots_count", len(metadata.Snapshots))

	return &metadata, nil
}

// parseS3URI parses an S3 URI and returns the bucket name and key
// S3 URIを解析してバケット名とキーを返す
// 形式: s3://bucket-name/key
func parseS3URI(uri string) (bucket, key string, err error) {
	s3Pattern := regexp.MustCompile(`^s3://([^/]+)/(.+)$`)
	matches := s3Pattern.FindStringSubmatch(uri)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid S3 URI format: %s (expected s3://bucket-name/key)", uri)
	}
	return matches[1], matches[2], nil
}

// generateSnapshot generates a new Iceberg snapshot
// 新しいIcebergスナップショットを生成
func generateSnapshot(manifestListPath string, sequenceNumber int64, addedFiles int, addedRecords int64, totalFiles int, totalRecords int64) (*IcebergSnapshot, error) {
	// マニフェストリストパスが空の場合はエラー
	if manifestListPath == "" {
		return nil, fmt.Errorf("manifestListPath cannot be empty")
	}

	// タイムスタンプベースのスナップショットIDを生成
	// ミリ秒単位のUnixタイムスタンプを使用
	timestampMS := time.Now().UnixMilli()
	snapshotID := timestampMS

	// サマリー情報を作成
	summary := map[string]string{
		"operation":     "append",
		"added-files":   fmt.Sprintf("%d", addedFiles),
		"added-records": fmt.Sprintf("%d", addedRecords),
		"total-files":   fmt.Sprintf("%d", totalFiles),
		"total-records": fmt.Sprintf("%d", totalRecords),
	}

	// スナップショットを作成
	snapshot := &IcebergSnapshot{
		SnapshotID:     snapshotID,
		TimestampMS:    timestampMS,
		SequenceNumber: sequenceNumber,
		Summary:        summary,
		ManifestList:   manifestListPath,
	}

	return snapshot, nil
}

// generateManifest generates a manifest file for data files
// データファイル用のマニフェストファイルを生成
func generateManifest(dataFilePaths []string, snapshotID int64, sequenceNumber int64) (*IcebergManifest, error) {
	// データファイルパスが空の場合はエラー
	if len(dataFilePaths) == 0 {
		return nil, fmt.Errorf("dataFilePaths cannot be empty")
	}

	// マニフェストエントリを生成
	entries := make([]IcebergManifestEntry, 0, len(dataFilePaths))
	for _, filePath := range dataFilePaths {
		// データファイル情報を作成
		// 注: 実際のファイルサイズとレコード数は、Parquetファイルから取得する必要があるが、
		// 現時点では簡略化のため、ダミー値を使用
		dataFile := IcebergDataFile{
			FilePath:      filePath,
			FileFormat:    "PARQUET",
			RecordCount:   0, // TODO: Parquetファイルから実際のレコード数を取得
			FileSizeBytes: 0, // TODO: S3から実際のファイルサイズを取得
		}

		// マニフェストエントリを作成
		entry := IcebergManifestEntry{
			Status:         1, // ADDED
			SnapshotID:     snapshotID,
			SequenceNumber: sequenceNumber,
			DataFile:       dataFile,
		}

		entries = append(entries, entry)
	}

	// マニフェストを作成
	manifest := &IcebergManifest{
		FormatVersion: 2,
		Content:       "data",
		Entries:       entries,
	}

	return manifest, nil
}

// uploadBatchToS3Tables uploads multiple Parquet data files to S3 Tables in a single snapshot.
// バッチコミット機能: 複数のデータファイルを単一のスナップショットにまとめる
func (e *s3TablesExporter) uploadBatchToS3Tables(ctx context.Context, dataList [][]byte, dataType string) error {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		e.logger.Warn("Batch upload cancelled before starting",
			"data_type", dataType,
			"error", ctx.Err())
		return fmt.Errorf("batch upload cancelled: %w", ctx.Err())
	default:
	}

	// 空データのハンドリング
	if len(dataList) == 0 {
		e.logger.Debug("Skipping batch upload: no data to upload", "data_type", dataType)
		return nil
	}

	// データタイプに応じたテーブル名を取得
	var tableName string
	var schema map[string]interface{}
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

	// テーブル名が設定されていない場合はスキップ
	if tableName == "" {
		e.logger.Debug("Skipping batch upload: table name not configured",
			"data_type", dataType)
		return nil
	}

	// 合計データサイズを計算
	totalSize := 0
	for _, data := range dataList {
		totalSize += len(data)
	}

	e.logger.Info("Starting batch upload to S3 Tables",
		"table_bucket_arn", e.config.TableBucketArn,
		"namespace", e.config.Namespace,
		"table", tableName,
		"data_type", dataType,
		"file_count", len(dataList),
		"total_size", totalSize)

	// 1. テーブルを取得または作成
	tableInfo, err := e.getOrCreateTable(ctx, e.config.Namespace, tableName, schema)
	if err != nil {
		e.logger.Error("Failed to get or create table",
			"namespace", e.config.Namespace,
			"table", tableName,
			"error", err)
		return fmt.Errorf("failed to get or create table: %w", err)
	}

	// 2. すべてのParquetファイルをWarehouse locationにアップロード
	dataFilePaths := make([]string, 0, len(dataList))
	for i, data := range dataList {
		// 空データはスキップ
		if len(data) == 0 {
			e.logger.Debug("Skipping empty data file in batch",
				"index", i,
				"data_type", dataType)
			continue
		}

		dataFilePath, err := e.uploadToWarehouseLocation(ctx, tableInfo.WarehouseLocation, data, dataType)
		if err != nil {
			e.logger.Error("Failed to upload data file in batch",
				"index", i,
				"warehouse_location", tableInfo.WarehouseLocation,
				"error", err)
			return fmt.Errorf("failed to upload data file %d to warehouse location: %w", i, err)
		}
		dataFilePaths = append(dataFilePaths, dataFilePath)
	}

	// アップロードされたファイルがない場合はスキップ
	if len(dataFilePaths) == 0 {
		e.logger.Debug("No files uploaded in batch, skipping snapshot commit",
			"data_type", dataType)
		return nil
	}

	// 3. すべてのデータファイルを単一のスナップショットにコミット
	if err := e.commitSnapshot(ctx, e.config.Namespace, tableName, tableInfo, dataFilePaths, int64(totalSize)); err != nil {
		e.logger.Error("Failed to commit batch snapshot",
			"namespace", e.config.Namespace,
			"table", tableName,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalSize,
			"error", err)
		return fmt.Errorf("failed to commit batch snapshot: %w", err)
	}

	// 成功のログ出力
	e.logger.Info("Successfully uploaded batch data to S3 Tables",
		"table", tableName,
		"warehouse_location", tableInfo.WarehouseLocation,
		"file_count", len(dataFilePaths),
		"total_size", totalSize)

	return nil
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
	var schema map[string]interface{}
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

	// 1. テーブルを取得または作成
	tableInfo, err := e.getOrCreateTable(ctx, e.config.Namespace, tableName, schema)
	if err != nil {
		e.logger.Error("Failed to get or create table",
			"namespace", e.config.Namespace,
			"table", tableName,
			"error", err)
		return fmt.Errorf("failed to get or create table: %w", err)
	}

	// 2. Warehouse locationにParquetファイルをアップロード
	dataFilePath, err := e.uploadToWarehouseLocation(ctx, tableInfo.WarehouseLocation, data, dataType)
	if err != nil {
		e.logger.Error("Failed to upload to warehouse location",
			"warehouse_location", tableInfo.WarehouseLocation,
			"error", err)
		return fmt.Errorf("failed to upload to warehouse location: %w", err)
	}

	// 3. スナップショットをコミット（バッチコミット対応）
	// 単一のデータファイルを含むスライスを作成
	dataFilePaths := []string{dataFilePath}
	totalDataSize := int64(len(data))

	if err := e.commitSnapshot(ctx, e.config.Namespace, tableName, tableInfo, dataFilePaths, totalDataSize); err != nil {
		e.logger.Error("Failed to commit snapshot",
			"namespace", e.config.Namespace,
			"table", tableName,
			"data_files_count", len(dataFilePaths),
			"total_data_size", totalDataSize,
			"error", err)
		return fmt.Errorf("failed to commit snapshot: %w", err)
	}

	// 成功のログ出力
	e.logger.Info("Successfully uploaded data to S3 Tables",
		"table", tableName,
		"warehouse_location", tableInfo.WarehouseLocation,
		"data_file_path", dataFilePath,
		"size", len(data))

	return nil
}
