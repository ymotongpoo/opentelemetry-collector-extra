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
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"go.opentelemetry.io/collector/component"
	exportertest "go.opentelemetry.io/collector/exporter/exportertest"
)

// TestSchemaIntegration_TracesSchema tests end-to-end table creation with traces schema
// トレーススキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.1
func TestSchemaIntegration_TracesSchema(t *testing.T) {
	// Exporterを作成
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// トレーススキーマを作成
	schema := createTracesSchema()

	// モックS3 Tablesクライアントを設定
	var capturedMetadata *types.IcebergMetadata
	var createTableCalled bool
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			createTableCalled = true
			// メタデータをキャプチャ
			if params.Metadata != nil {
				if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
					capturedMetadata = &member.Value
				}
			}
			return &s3tables.CreateTableOutput{
				TableARN:     aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_traces"),
				VersionToken: aws.String("version-token-1"),
			}, nil
		},
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_traces"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/otel_traces"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を実行
	tableInfo, err := exporter.createTable(context.Background(), cfg.Namespace, cfg.Tables.Traces, schema)
	if err != nil {
		t.Fatalf("createTable() failed: %v", err)
	}

	// CreateTableが呼ばれたことを確認
	if !createTableCalled {
		t.Fatal("CreateTable was not called")
	}

	// TableInfoを検証
	if tableInfo == nil {
		t.Fatal("tableInfo is nil")
	}
	if tableInfo.TableARN == "" {
		t.Error("TableARN is empty")
	}
	if tableInfo.WarehouseLocation == "" {
		t.Error("WarehouseLocation is empty")
	}

	// メタデータがキャプチャされたことを確認
	if capturedMetadata == nil {
		t.Fatal("metadata was not captured")
	}

	// Schemaフィールドが設定されていることを確認
	if capturedMetadata.Schema == nil {
		t.Fatal("Schema is nil")
	}

	// トレーススキーマは8つのフィールドを持つ
	expectedFieldCount := 8
	if len(capturedMetadata.Schema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(capturedMetadata.Schema.Fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"trace_id":   false,
		"span_id":    false,
		"name":       false,
		"start_time": false,
		"end_time":   false,
	}

	for _, field := range capturedMetadata.Schema.Fields {
		if field.Name == nil {
			continue
		}
		if _, ok := requiredFields[*field.Name]; ok {
			requiredFields[*field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（attributes、resource_attributes）の検証
	mapFields := []string{"attributes", "resource_attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *types.SchemaField
		for i := range capturedMetadata.Schema.Fields {
			if capturedMetadata.Schema.Fields[i].Name != nil && *capturedMetadata.Schema.Fields[i].Name == mapFieldName {
				mapField = &capturedMetadata.Schema.Fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型がJSON文字列として格納されていることを確認
		if mapField.Type == nil {
			t.Errorf("map field '%s' Type is nil", mapFieldName)
			continue
		}

		// JSON文字列をパース
		var mapType map[string]interface{}
		if err := json.Unmarshal([]byte(*mapField.Type), &mapType); err != nil {
			t.Errorf("failed to parse map type JSON for field '%s': %v", mapFieldName, err)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
		if mapType["key"] != "string" {
			t.Errorf("field '%s': expected key type 'string', got %v", mapFieldName, mapType["key"])
		}
		if mapType["value"] != "string" {
			t.Errorf("field '%s': expected value type 'string', got %v", mapFieldName, mapType["value"])
		}

		// key-idとvalue-idが含まれていることを確認
		if _, ok := mapType["key-id"]; !ok {
			t.Errorf("field '%s': key-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-id"]; !ok {
			t.Errorf("field '%s': value-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-required"]; !ok {
			t.Errorf("field '%s': value-required not found in map type", mapFieldName)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Traces schema successfully serialized and accepted by S3 Tables API")
}

// TestSchemaIntegration_MetricsSchema tests end-to-end table creation with metrics schema
// メトリクススキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.2
func TestSchemaIntegration_MetricsSchema(t *testing.T) {
	// Exporterを作成
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// メトリクススキーマを作成
	schema := createMetricsSchema()

	// モックS3 Tablesクライアントを設定
	var capturedMetadata *types.IcebergMetadata
	var createTableCalled bool
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			createTableCalled = true
			// メタデータをキャプチャ
			if params.Metadata != nil {
				if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
					capturedMetadata = &member.Value
				}
			}
			return &s3tables.CreateTableOutput{
				TableARN:     aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_metrics"),
				VersionToken: aws.String("version-token-1"),
			}, nil
		},
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_metrics"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/otel_metrics"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を実行
	tableInfo, err := exporter.createTable(context.Background(), cfg.Namespace, cfg.Tables.Metrics, schema)
	if err != nil {
		t.Fatalf("createTable() failed: %v", err)
	}

	// CreateTableが呼ばれたことを確認
	if !createTableCalled {
		t.Fatal("CreateTable was not called")
	}

	// TableInfoを検証
	if tableInfo == nil {
		t.Fatal("tableInfo is nil")
	}
	if tableInfo.TableARN == "" {
		t.Error("TableARN is empty")
	}
	if tableInfo.WarehouseLocation == "" {
		t.Error("WarehouseLocation is empty")
	}

	// メタデータがキャプチャされたことを確認
	if capturedMetadata == nil {
		t.Fatal("metadata was not captured")
	}

	// Schemaフィールドが設定されていることを確認
	if capturedMetadata.Schema == nil {
		t.Fatal("Schema is nil")
	}

	// メトリクススキーマは6つのフィールドを持つ
	expectedFieldCount := 6
	if len(capturedMetadata.Schema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(capturedMetadata.Schema.Fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp":   false,
		"metric_name": false,
		"metric_type": false,
		"value":       false,
	}

	for _, field := range capturedMetadata.Schema.Fields {
		if field.Name == nil {
			continue
		}
		if _, ok := requiredFields[*field.Name]; ok {
			requiredFields[*field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（resource_attributes、attributes）の検証
	mapFields := []string{"resource_attributes", "attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *types.SchemaField
		for i := range capturedMetadata.Schema.Fields {
			if capturedMetadata.Schema.Fields[i].Name != nil && *capturedMetadata.Schema.Fields[i].Name == mapFieldName {
				mapField = &capturedMetadata.Schema.Fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型がJSON文字列として格納されていることを確認
		if mapField.Type == nil {
			t.Errorf("map field '%s' Type is nil", mapFieldName)
			continue
		}

		// JSON文字列をパース
		var mapType map[string]interface{}
		if err := json.Unmarshal([]byte(*mapField.Type), &mapType); err != nil {
			t.Errorf("failed to parse map type JSON for field '%s': %v", mapFieldName, err)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
		if mapType["key"] != "string" {
			t.Errorf("field '%s': expected key type 'string', got %v", mapFieldName, mapType["key"])
		}
		if mapType["value"] != "string" {
			t.Errorf("field '%s': expected value type 'string', got %v", mapFieldName, mapType["value"])
		}

		// key-idとvalue-idが含まれていることを確認
		if _, ok := mapType["key-id"]; !ok {
			t.Errorf("field '%s': key-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-id"]; !ok {
			t.Errorf("field '%s': value-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-required"]; !ok {
			t.Errorf("field '%s': value-required not found in map type", mapFieldName)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Metrics schema successfully serialized and accepted by S3 Tables API")
}

// TestSchemaIntegration_LogsSchema tests end-to-end table creation with logs schema
// ログスキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.3
func TestSchemaIntegration_LogsSchema(t *testing.T) {
	// Exporterを作成
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// ログスキーマを作成
	schema := createLogsSchema()

	// モックS3 Tablesクライアントを設定
	var capturedMetadata *types.IcebergMetadata
	var createTableCalled bool
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			createTableCalled = true
			// メタデータをキャプチャ
			if params.Metadata != nil {
				if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
					capturedMetadata = &member.Value
				}
			}
			return &s3tables.CreateTableOutput{
				TableARN:     aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_logs"),
				VersionToken: aws.String("version-token-1"),
			}, nil
		},
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_logs"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/otel_logs"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を実行
	tableInfo, err := exporter.createTable(context.Background(), cfg.Namespace, cfg.Tables.Logs, schema)
	if err != nil {
		t.Fatalf("createTable() failed: %v", err)
	}

	// CreateTableが呼ばれたことを確認
	if !createTableCalled {
		t.Fatal("CreateTable was not called")
	}

	// TableInfoを検証
	if tableInfo == nil {
		t.Fatal("tableInfo is nil")
	}
	if tableInfo.TableARN == "" {
		t.Error("TableARN is empty")
	}
	if tableInfo.WarehouseLocation == "" {
		t.Error("WarehouseLocation is empty")
	}

	// メタデータがキャプチャされたことを確認
	if capturedMetadata == nil {
		t.Fatal("metadata was not captured")
	}

	// Schemaフィールドが設定されていることを確認
	if capturedMetadata.Schema == nil {
		t.Fatal("Schema is nil")
	}

	// ログスキーマは5つのフィールドを持つ
	expectedFieldCount := 5
	if len(capturedMetadata.Schema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(capturedMetadata.Schema.Fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp": false,
		"body":      false,
	}

	for _, field := range capturedMetadata.Schema.Fields {
		if field.Name == nil {
			continue
		}
		if _, ok := requiredFields[*field.Name]; ok {
			requiredFields[*field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（attributes、resource_attributes）の検証
	mapFields := []string{"attributes", "resource_attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *types.SchemaField
		for i := range capturedMetadata.Schema.Fields {
			if capturedMetadata.Schema.Fields[i].Name != nil && *capturedMetadata.Schema.Fields[i].Name == mapFieldName {
				mapField = &capturedMetadata.Schema.Fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型がJSON文字列として格納されていることを確認
		if mapField.Type == nil {
			t.Errorf("map field '%s' Type is nil", mapFieldName)
			continue
		}

		// JSON文字列をパース
		var mapType map[string]interface{}
		if err := json.Unmarshal([]byte(*mapField.Type), &mapType); err != nil {
			t.Errorf("failed to parse map type JSON for field '%s': %v", mapFieldName, err)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
		if mapType["key"] != "string" {
			t.Errorf("field '%s': expected key type 'string', got %v", mapFieldName, mapType["key"])
		}
		if mapType["value"] != "string" {
			t.Errorf("field '%s': expected value type 'string', got %v", mapFieldName, mapType["value"])
		}

		// key-idとvalue-idが含まれていることを確認
		if _, ok := mapType["key-id"]; !ok {
			t.Errorf("field '%s': key-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-id"]; !ok {
			t.Errorf("field '%s': value-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-required"]; !ok {
			t.Errorf("field '%s': value-required not found in map type", mapFieldName)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Logs schema successfully serialized and accepted by S3 Tables API")
}
