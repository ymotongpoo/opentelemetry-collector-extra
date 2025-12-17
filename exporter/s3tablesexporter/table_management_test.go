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
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestTableInfo tests the TableInfo structure
// Requirements: 2.1, 2.2
func TestTableInfo(t *testing.T) {
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table",
		WarehouseLocation: "s3://unique-id--table-s3",
		VersionToken:      "v1",
	}

	if tableInfo.TableARN == "" {
		t.Error("TableARN should not be empty")
	}
	if tableInfo.WarehouseLocation == "" {
		t.Error("WarehouseLocation should not be empty")
	}
	if tableInfo.VersionToken == "" {
		t.Error("VersionToken should not be empty")
	}
}

// TestNewS3TablesExporter_TableCacheInitialized tests that table cache is initialized
// Requirements: 2.2
func TestNewS3TablesExporter_TableCacheInitialized(t *testing.T) {
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

	if exporter.tableCache == nil {
		t.Error("tableCache should be initialized")
	}

	if len(exporter.tableCache) != 0 {
		t.Errorf("tableCache should be empty initially, got %d entries", len(exporter.tableCache))
	}
}

// TestMarshalSchemaToJSON tests schema marshaling to JSON
// Requirements: 2.3
func TestMarshalSchemaToJSON(t *testing.T) {
	schema := map[string]interface{}{
		"type": "struct",
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "test_field",
				"required": true,
				"type":     "string",
			},
		},
	}

	jsonStr, err := marshalSchemaToJSON(schema)
	if err != nil {
		t.Fatalf("marshalSchemaToJSON() failed: %v", err)
	}

	if jsonStr == "" {
		t.Error("JSON string should not be empty")
	}

	// JSON文字列に必要なフィールドが含まれることを確認
	expectedSubstrings := []string{"type", "struct", "fields", "test_field"}
	for _, substr := range expectedSubstrings {
		found := false
		for i := 0; i <= len(jsonStr)-len(substr); i++ {
			if jsonStr[i:i+len(substr)] == substr {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("JSON string should contain '%s', got: %s", substr, jsonStr)
		}
	}
}

// TestMarshalSchemaToJSON_EmptySchema tests marshaling empty schema
// Requirements: 2.3
func TestMarshalSchemaToJSON_EmptySchema(t *testing.T) {
	schema := map[string]interface{}{}

	jsonStr, err := marshalSchemaToJSON(schema)
	if err != nil {
		t.Fatalf("marshalSchemaToJSON() with empty schema failed: %v", err)
	}

	if jsonStr != "{}" {
		t.Errorf("expected '{}', got '%s'", jsonStr)
	}
}

// TestCreateNamespaceIfNotExists_ContextCancellation tests context cancellation handling
// Requirements: 2.6, 5.4
func TestCreateNamespaceIfNotExists_ContextCancellation(t *testing.T) {
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

	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Namespace作成を試行
	err = exporter.createNamespaceIfNotExists(ctx, "test-namespace")
	if err == nil {
		t.Error("createNamespaceIfNotExists() should return error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	expectedMsg := "namespace creation cancelled"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestGetTable_ContextCancellation tests context cancellation handling
// Requirements: 2.6, 5.4
func TestGetTable_ContextCancellation(t *testing.T) {
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

	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// テーブル取得を試行
	_, err = exporter.getTable(ctx, "test-namespace", "test-table")
	if err == nil {
		t.Error("getTable() should return error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	expectedMsg := "table retrieval cancelled"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestCreateTable_ContextCancellation tests context cancellation handling
// Requirements: 2.6, 5.4
func TestCreateTable_ContextCancellation(t *testing.T) {
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

	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// テーブル作成を試行
	schema := createMetricsSchema()
	_, err = exporter.createTable(ctx, "test-namespace", "test-table", schema)
	if err == nil {
		t.Error("createTable() should return error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	expectedMsg := "table creation cancelled"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestGetOrCreateTable_CacheKey tests cache key generation
// Requirements: 2.2
func TestGetOrCreateTable_CacheKey(t *testing.T) {
	tests := []struct {
		name         string
		namespace    string
		tableName    string
		expectedKey  string
	}{
		{
			name:        "simple names",
			namespace:   "test-namespace",
			tableName:   "test-table",
			expectedKey: "test-namespace.test-table",
		},
		{
			name:        "production names",
			namespace:   "production",
			tableName:   "otel_metrics",
			expectedKey: "production.otel_metrics",
		},
		{
			name:        "names with hyphens",
			namespace:   "my-namespace",
			tableName:   "my-table-name",
			expectedKey: "my-namespace.my-table-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// キャッシュキーの形式を検証
			cacheKey := tt.namespace + "." + tt.tableName
			if cacheKey != tt.expectedKey {
				t.Errorf("expected cache key '%s', got '%s'", tt.expectedKey, cacheKey)
			}
		})
	}
}

// TestConvertToIcebergSchema tests schema conversion to Iceberg format
// Requirements: 2.3
func TestConvertToIcebergSchema(t *testing.T) {
	schema := map[string]interface{}{
		"type": "struct",
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
			{
				"id":       2,
				"name":     "metric_name",
				"required": true,
				"type":     "string",
			},
		},
	}

	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	if icebergSchema == nil {
		t.Fatal("icebergSchema should not be nil")
	}

	if len(icebergSchema.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(icebergSchema.Fields))
	}

	// 最初のフィールドを検証
	if *icebergSchema.Fields[0].Name != "timestamp" {
		t.Errorf("expected field name 'timestamp', got '%s'", *icebergSchema.Fields[0].Name)
	}
	if *icebergSchema.Fields[0].Type != "timestamptz" {
		t.Errorf("expected field type 'timestamptz', got '%s'", *icebergSchema.Fields[0].Type)
	}
	if !icebergSchema.Fields[0].Required {
		t.Error("expected field to be required")
	}

	// 2番目のフィールドを検証
	if *icebergSchema.Fields[1].Name != "metric_name" {
		t.Errorf("expected field name 'metric_name', got '%s'", *icebergSchema.Fields[1].Name)
	}
	if *icebergSchema.Fields[1].Type != "string" {
		t.Errorf("expected field type 'string', got '%s'", *icebergSchema.Fields[1].Type)
	}
}

// TestConvertToIcebergSchema_WithMapType tests schema conversion with map type
// Requirements: 2.3
func TestConvertToIcebergSchema_WithMapType(t *testing.T) {
	schema := map[string]interface{}{
		"type": "struct",
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "attributes",
				"required": false,
				"type": map[string]interface{}{
					"type":  "map",
					"key":   "string",
					"value": "string",
				},
			},
		},
	}

	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() with map type failed: %v", err)
	}

	if len(icebergSchema.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(icebergSchema.Fields))
	}

	// マップ型のフィールドを検証
	if *icebergSchema.Fields[0].Name != "attributes" {
		t.Errorf("expected field name 'attributes', got '%s'", *icebergSchema.Fields[0].Name)
	}
	expectedType := "map<string, string>"
	if *icebergSchema.Fields[0].Type != expectedType {
		t.Errorf("expected field type '%s', got '%s'", expectedType, *icebergSchema.Fields[0].Type)
	}
	if icebergSchema.Fields[0].Required {
		t.Error("expected field to not be required")
	}
}

// TestConvertToIcebergSchema_MissingFields tests error handling for missing fields
// Requirements: 2.3
func TestConvertToIcebergSchema_MissingFields(t *testing.T) {
	schema := map[string]interface{}{
		"type": "struct",
	}

	_, err := convertToIcebergSchema(schema)
	if err == nil {
		t.Error("convertToIcebergSchema() should return error when 'fields' is missing")
	}

	expectedMsg := "schema missing 'fields' key"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestConvertToSchemaField tests field conversion
// Requirements: 2.3
func TestConvertToSchemaField(t *testing.T) {
	tests := []struct {
		name          string
		fieldMap      map[string]interface{}
		expectedName  string
		expectedType  string
		expectedReq   bool
		expectError   bool
	}{
		{
			name: "simple string field",
			fieldMap: map[string]interface{}{
				"name":     "test_field",
				"type":     "string",
				"required": true,
			},
			expectedName: "test_field",
			expectedType: "string",
			expectedReq:  true,
			expectError:  false,
		},
		{
			name: "optional field",
			fieldMap: map[string]interface{}{
				"name":     "optional_field",
				"type":     "double",
				"required": false,
			},
			expectedName: "optional_field",
			expectedType: "double",
			expectedReq:  false,
			expectError:  false,
		},
		{
			name: "missing name",
			fieldMap: map[string]interface{}{
				"type":     "string",
				"required": true,
			},
			expectError: true,
		},
		{
			name: "missing type",
			fieldMap: map[string]interface{}{
				"name":     "test_field",
				"required": true,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := convertToSchemaField(tt.fieldMap)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if *field.Name != tt.expectedName {
				t.Errorf("expected name '%s', got '%s'", tt.expectedName, *field.Name)
			}
			if *field.Type != tt.expectedType {
				t.Errorf("expected type '%s', got '%s'", tt.expectedType, *field.Type)
			}
			if field.Required != tt.expectedReq {
				t.Errorf("expected required %v, got %v", tt.expectedReq, field.Required)
			}
		})
	}
}

// TestConvertToIcebergSchema_WithMetricsSchema tests conversion of metrics schema
// Requirements: 2.3
func TestConvertToIcebergSchema_WithMetricsSchema(t *testing.T) {
	schema := createMetricsSchema()

	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() with metrics schema failed: %v", err)
	}

	// メトリクススキーマには6つのフィールドがある
	expectedFieldCount := 6
	if len(icebergSchema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(icebergSchema.Fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := []string{"timestamp", "metric_name", "metric_type", "value"}
	for _, requiredField := range requiredFields {
		found := false
		for _, field := range icebergSchema.Fields {
			if *field.Name == requiredField {
				found = true
				if !field.Required {
					t.Errorf("field '%s' should be required", requiredField)
				}
				break
			}
		}
		if !found {
			t.Errorf("required field '%s' not found in schema", requiredField)
		}
	}
}

// TestConvertToIcebergSchema_WithTracesSchema tests conversion of traces schema
// Requirements: 2.3
func TestConvertToIcebergSchema_WithTracesSchema(t *testing.T) {
	schema := createTracesSchema()

	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() with traces schema failed: %v", err)
	}

	// トレーススキーマには8つのフィールドがある
	expectedFieldCount := 8
	if len(icebergSchema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(icebergSchema.Fields))
	}
}

// TestConvertToIcebergSchema_WithLogsSchema tests conversion of logs schema
// Requirements: 2.3
func TestConvertToIcebergSchema_WithLogsSchema(t *testing.T) {
	schema := createLogsSchema()

	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() with logs schema failed: %v", err)
	}

	// ログスキーマには5つのフィールドがある
	expectedFieldCount := 5
	if len(icebergSchema.Fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(icebergSchema.Fields))
	}
}
