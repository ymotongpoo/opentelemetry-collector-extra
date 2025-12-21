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
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"go.opentelemetry.io/collector/component"
	exportertest "go.opentelemetry.io/collector/exporter/exportertest"
)

// TestCreateTable_PrimitiveTypes tests table creation with primitive types only
// プリミティブ型のみのスキーマでテーブル作成をテスト
// Requirements: 5.1, 5.2
func TestCreateTable_PrimitiveTypes(t *testing.T) {
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

	// プリミティブ型のみのスキーマを作成
	schema := map[string]interface{}{
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
			{
				"id":       2,
				"name":     "service_name",
				"required": false,
				"type":     "string",
			},
			{
				"id":       3,
				"name":     "value",
				"required": false,
				"type":     "double",
			},
		},
	}

	// モックS3 Tablesクライアントを設定
	var capturedMetadata *types.IcebergMetadata
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			// メタデータをキャプチャ
			if params.Metadata != nil {
				if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
					capturedMetadata = &member.Value
				}
			}
			return &s3tables.CreateTableOutput{
				TableARN:     aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				VersionToken: aws.String("version-token-1"),
			}, nil
		},
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/test-table"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を実行
	tableInfo, err := exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err != nil {
		t.Fatalf("createTable() failed: %v", err)
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
	if len(capturedMetadata.Schema.Fields) != 3 {
		t.Errorf("expected 3 fields in Schema, got %d", len(capturedMetadata.Schema.Fields))
	}

	// Propertiesフィールドに完全なメタデータが格納されていることを確認
	if capturedMetadata.Properties == nil {
		t.Fatal("Properties is nil")
	}
	metadataJSON, ok := capturedMetadata.Properties["iceberg.metadata.json"]
	if !ok {
		t.Fatal("iceberg.metadata.json not found in Properties")
	}

	// JSON文字列をパース
	var metadata IcebergMetadata
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		t.Fatalf("failed to parse metadata JSON: %v", err)
	}

	// メタデータの内容を検証
	if metadata.FormatVersion != 2 {
		t.Errorf("expected FormatVersion 2, got %d", metadata.FormatVersion)
	}
	if len(metadata.Schemas) != 1 {
		t.Errorf("expected 1 schema, got %d", len(metadata.Schemas))
	}
	if len(metadata.Schemas[0].Fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(metadata.Schemas[0].Fields))
	}

	// フィールドの型を検証
	for _, field := range metadata.Schemas[0].Fields {
		if !field.IsPrimitiveType() {
			t.Errorf("field %s should be primitive type", field.Name)
		}
	}
}

// TestCreateTable_ComplexTypes tests table creation with complex types (map)
// 複合型（map）を含むスキーマでテーブル作成をテスト
// Requirements: 5.1, 5.2
func TestCreateTable_ComplexTypes(t *testing.T) {
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

	// 複合型を含むスキーマを作成
	schema := map[string]interface{}{
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
			{
				"id":       2,
				"name":     "resource_attributes",
				"required": false,
				"type": map[string]interface{}{
					"type":           "map",
					"key-id":         3,
					"key":            "string",
					"value-id":       4,
					"value":          "string",
					"value-required": false,
				},
			},
		},
	}

	// モックS3 Tablesクライアントを設定
	var capturedMetadata *types.IcebergMetadata
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			// メタデータをキャプチャ
			if params.Metadata != nil {
				if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
					capturedMetadata = &member.Value
				}
			}
			return &s3tables.CreateTableOutput{
				TableARN:     aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				VersionToken: aws.String("version-token-1"),
			}, nil
		},
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/test-table"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を実行
	tableInfo, err := exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err != nil {
		t.Fatalf("createTable() failed: %v", err)
	}

	// TableInfoを検証
	if tableInfo == nil {
		t.Fatal("tableInfo is nil")
	}

	// メタデータがキャプチャされたことを確認
	if capturedMetadata == nil {
		t.Fatal("metadata was not captured")
	}

	// Schemaフィールドが設定されていることを確認
	if capturedMetadata.Schema == nil {
		t.Fatal("Schema is nil")
	}

	// Propertiesフィールドに完全なメタデータが格納されていることを確認
	if capturedMetadata.Properties == nil {
		t.Fatal("Properties is nil")
	}
	metadataJSON, ok := capturedMetadata.Properties["iceberg.metadata.json"]
	if !ok {
		t.Fatal("iceberg.metadata.json not found in Properties")
	}

	// JSON文字列をパース
	var metadata IcebergMetadata
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		t.Fatalf("failed to parse metadata JSON: %v", err)
	}

	// メタデータの内容を検証
	if len(metadata.Schemas) != 1 {
		t.Errorf("expected 1 schema, got %d", len(metadata.Schemas))
	}
	if len(metadata.Schemas[0].Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(metadata.Schemas[0].Fields))
	}

	// map型のフィールドを検証
	var mapField *IcebergSchemaField
	for i := range metadata.Schemas[0].Fields {
		if metadata.Schemas[0].Fields[i].Name == "resource_attributes" {
			mapField = &metadata.Schemas[0].Fields[i]
			break
		}
	}
	if mapField == nil {
		t.Fatal("map field not found")
	}

	// map型であることを確認
	if !mapField.IsMapType() {
		t.Error("field should be map type")
	}

	// map型の内容を検証
	mapType, err := mapField.GetMapType()
	if err != nil {
		t.Fatalf("failed to get map type: %v", err)
	}

	// map型がネストされたJSONオブジェクトとして保持されていることを確認
	if mapType["type"] != "map" {
		t.Errorf("expected type 'map', got %v", mapType["type"])
	}
	if mapType["key"] != "string" {
		t.Errorf("expected key type 'string', got %v", mapType["key"])
	}
	if mapType["value"] != "string" {
		t.Errorf("expected value type 'string', got %v", mapType["value"])
	}
}

// TestCreateTable_JSONSerializationError tests error handling when JSON serialization fails
// JSONシリアライゼーションエラーのハンドリングをテスト
// Requirements: 5.1, 5.2
func TestCreateTable_JSONSerializationError(t *testing.T) {
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

	// 無効なスキーマを作成（fieldsキーが欠落）
	schema := map[string]interface{}{
		"invalid": "schema",
	}

	// テーブル作成を試行
	_, err = exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from createTable with invalid schema")
	}

	// エラーメッセージを検証
	expectedSubstr := "failed to convert schema to Iceberg format"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}

// TestCreateTable_CreateTableAPIError tests error handling when CreateTable API fails
// CreateTable APIエラーのハンドリングをテスト
// Requirements: 5.1, 5.2
func TestCreateTable_CreateTableAPIError(t *testing.T) {
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

	// プリミティブ型のみのスキーマを作成
	schema := map[string]interface{}{
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
		},
	}

	// モックS3 Tablesクライアントを設定してエラーを返す
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			return nil, fmt.Errorf("BadRequestException: The specified metadata is not valid")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を試行
	_, err = exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from createTable when API fails")
	}

	// エラーメッセージを検証
	expectedSubstr := "failed to create table"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}
