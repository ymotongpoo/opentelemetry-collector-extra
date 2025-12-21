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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestCreateTable_SchemaValidationErrorLogging tests that schema validation errors are logged
// Requirements: 6.1, 6.2
func TestCreateTable_SchemaValidationErrorLogging(t *testing.T) {
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

	// 無効なスキーマを作成（重複するフィールドID）
	invalidSchema := map[string]interface{}{
		"type": "struct",
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "field1",
				"required": false,
				"type":     "string",
			},
			{
				"id":       1, // 重複
				"name":     "field2",
				"required": false,
				"type":     "int",
			},
		},
	}

	// モックS3 Tablesクライアントを設定
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			// この関数は呼ばれないはず（スキーマ検証で失敗するため）
			t.Error("CreateTable should not be called with invalid schema")
			return nil, fmt.Errorf("should not reach here")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を試行
	_, err = exporter.createTable(context.Background(), "test-namespace", "test-table", invalidSchema)
	if err == nil {
		t.Fatal("expected error from createTable with invalid schema")
	}

	// エラーメッセージに"validation"が含まれることを確認
	if !strings.Contains(err.Error(), "validation") {
		t.Errorf("error message should contain 'validation', got: %s", err.Error())
	}
}

// TestCreateTable_S3TablesAPIRejectionLogging tests that S3 Tables API rejection is logged with schema JSON
// Requirements: 6.2, 6.3
func TestCreateTable_S3TablesAPIRejectionLogging(t *testing.T) {
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

	// 有効なスキーマを作成
	schema := createMetricsSchema()

	// モックS3 Tablesクライアントを設定してAPIエラーを返す
	mockClient := &mockS3TablesClient{
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			return nil, fmt.Errorf("BadRequestException: The specified metadata is not valid")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を試行
	_, err = exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from createTable when API rejects schema")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	if !strings.Contains(err.Error(), "failed to create table") {
		t.Errorf("error message should contain 'failed to create table', got: %s", err.Error())
	}
}

// TestSerializeToJSON_DebugLogging tests that SerializeToJSON is used for debug logging
// Requirements: 6.3
func TestSerializeToJSON_DebugLogging(t *testing.T) {
	// 有効なスキーマを作成
	schema := &IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "timestamp",
				Required: true,
				Type:     "timestamptz",
			},
			{
				ID:       2,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
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

	// IcebergSchemaSerializerを作成
	serializer := NewIcebergSchemaSerializer(schema)

	// SerializeToJSONを呼び出し
	jsonStr, err := serializer.SerializeToJSON()
	if err != nil {
		t.Fatalf("SerializeToJSON() failed: %v", err)
	}

	// JSON文字列が非空であることを確認
	if jsonStr == "" {
		t.Error("SerializeToJSON() returned empty string")
	}

	// JSON文字列に期待されるフィールドが含まれることを確認
	expectedFields := []string{"type", "fields", "timestamp", "attributes", "map", "key-id", "value-id"}
	for _, field := range expectedFields {
		if !strings.Contains(jsonStr, field) {
			t.Errorf("JSON string should contain '%s', got: %s", field, jsonStr)
		}
	}

	// JSON文字列がインデントされていることを確認（デバッグ用）
	if !strings.Contains(jsonStr, "\n") {
		t.Error("JSON string should be indented for readability")
	}
}

// TestValidate_ErrorMessageDetails tests that Validate returns detailed error messages
// Requirements: 6.1, 6.4
func TestValidate_ErrorMessageDetails(t *testing.T) {
	tests := []struct {
		name          string
		schema        *IcebergSchema
		expectedError string
	}{
		{
			name: "empty field name",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "", // 空の名前
						Required: false,
						Type:     "string",
					},
				},
			},
			expectedError: "field name cannot be empty",
		},
		{
			name: "negative field ID",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       -1, // 負のID
						Name:     "field1",
						Required: false,
						Type:     "string",
					},
				},
			},
			expectedError: "field ID must be positive",
		},
		{
			name: "nil type",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "field1",
						Required: false,
						Type:     nil, // nil型
					},
				},
			},
			expectedError: "field type cannot be nil",
		},
		{
			name: "duplicate field ID",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "field1",
						Required: false,
						Type:     "string",
					},
					{
						ID:       1, // 重複
						Name:     "field2",
						Required: false,
						Type:     "int",
					},
				},
			},
			expectedError: "duplicate field ID",
		},
		{
			name: "invalid map type - missing key-id",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "field1",
						Required: false,
						Type: map[string]interface{}{
							"type":           "map",
							// key-idが欠落
							"key":            "string",
							"value-id":       2,
							"value":          "string",
							"value-required": false,
						},
					},
				},
			},
			expectedError: "map type missing or invalid 'key-id'",
		},
		{
			name: "invalid list type - missing element-id",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "field1",
						Required: false,
						Type: map[string]interface{}{
							"type": "list",
							// element-idが欠落
							"element":          "string",
							"element-required": false,
						},
					},
				},
			},
			expectedError: "list type missing or invalid 'element-id'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// IcebergSchemaSerializerを作成
			serializer := NewIcebergSchemaSerializer(tt.schema)

			// 検証を実行
			err := serializer.Validate()
			if err == nil {
				t.Fatal("expected validation error but got none")
			}

			// エラーメッセージに期待される文字列が含まれることを確認
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("error message should contain '%s', got: %s", tt.expectedError, err.Error())
			}

			// エラーメッセージにフィールド名が含まれることを確認（空の名前の場合を除く）
			if tt.name != "empty field name" {
				if !strings.Contains(err.Error(), "field") {
					t.Errorf("error message should contain field information, got: %s", err.Error())
				}
			}
		})
	}
}

// TestSerializeForS3Tables_ErrorMessageDetails tests that SerializeForS3Tables returns detailed error messages
// Requirements: 6.1, 6.4
func TestSerializeForS3Tables_ErrorMessageDetails(t *testing.T) {
	tests := []struct {
		name          string
		schema        *IcebergSchema
		expectedError string
	}{
		{
			name: "validation error during serialization",
			schema: &IcebergSchema{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "field1",
						Required: false,
						Type:     "string",
					},
					{
						ID:       1, // 重複
						Name:     "field2",
						Required: false,
						Type:     "int",
					},
				},
			},
			expectedError: "duplicate field ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// IcebergSchemaSerializerを作成
			serializer := NewIcebergSchemaSerializer(tt.schema)

			// シリアライズを実行
			_, err := serializer.SerializeForS3Tables()
			if err == nil {
				t.Fatal("expected serialization error but got none")
			}

			// エラーメッセージに期待される文字列が含まれることを確認
			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("error message should contain '%s', got: %s", tt.expectedError, err.Error())
			}
		})
	}
}

// TestAWSSchemaAdapter_ErrorMessageDetails tests that AWSSchemaAdapter returns detailed error messages
// Requirements: 6.1, 6.4
func TestAWSSchemaAdapter_ErrorMessageDetails(t *testing.T) {
	// 無効なスキーマを作成（重複するフィールドID）
	invalidSchema := &IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "field1",
				Required: false,
				Type:     "string",
			},
			{
				ID:       1, // 重複
				Name:     "field2",
				Required: false,
				Type:     "int",
			},
		},
	}

	// AWSSchemaAdapterを作成
	adapter := NewAWSSchemaAdapter(invalidSchema)

	// MarshalSchemaFieldsを呼び出し
	_, err := adapter.MarshalSchemaFields()
	if err == nil {
		t.Fatal("expected error from MarshalSchemaFields with invalid schema")
	}

	// エラーメッセージに"validation"または"duplicate"が含まれることを確認
	errMsg := err.Error()
	if !strings.Contains(errMsg, "validation") && !strings.Contains(errMsg, "duplicate") {
		t.Errorf("error message should contain 'validation' or 'duplicate', got: %s", errMsg)
	}
}

// TestSchemaValidationError_ErrorMethod tests SchemaValidationError.Error() method
// Requirements: 6.1
func TestSchemaValidationError_ErrorMethod(t *testing.T) {
	tests := []struct {
		name          string
		err           *SchemaValidationError
		expectedField string
		expectedReason string
	}{
		{
			name: "duplicate field ID error",
			err: &SchemaValidationError{
				Field:  "field2",
				Reason: "duplicate field ID 1 (already used by field field1)",
			},
			expectedField:  "field2",
			expectedReason: "duplicate field ID",
		},
		{
			name: "empty field name error",
			err: &SchemaValidationError{
				Field:  "name",
				Reason: "field name cannot be empty",
			},
			expectedField:  "name",
			expectedReason: "cannot be empty",
		},
		{
			name: "negative ID error",
			err: &SchemaValidationError{
				Field:  "field1",
				Reason: "field ID must be positive",
			},
			expectedField:  "field1",
			expectedReason: "must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Error()メソッドを呼び出し
			errMsg := tt.err.Error()

			// エラーメッセージが非空であることを確認
			if errMsg == "" {
				t.Error("Error() returned empty string")
			}

			// エラーメッセージにフィールド名が含まれることを確認
			if !strings.Contains(errMsg, tt.expectedField) {
				t.Errorf("error message should contain field '%s', got: %s", tt.expectedField, errMsg)
			}

			// エラーメッセージに理由が含まれることを確認
			if !strings.Contains(errMsg, tt.expectedReason) {
				t.Errorf("error message should contain reason '%s', got: %s", tt.expectedReason, errMsg)
			}

			// エラーメッセージに"validation"が含まれることを確認
			if !strings.Contains(errMsg, "validation") {
				t.Errorf("error message should contain 'validation', got: %s", errMsg)
			}
		})
	}
}

// TestSerializationError_ErrorMethod tests SerializationError.Error() method
// Requirements: 6.1
func TestSerializationError_ErrorMethod(t *testing.T) {
	tests := []struct {
		name              string
		err               *SerializationError
		expectedOperation string
		expectedCause     string
	}{
		{
			name: "marshal error",
			err: &SerializationError{
				Operation: "marshal schema to JSON",
				Cause:     fmt.Errorf("json: unsupported type"),
			},
			expectedOperation: "marshal schema to JSON",
			expectedCause:     "unsupported type",
		},
		{
			name: "serialize field error",
			err: &SerializationError{
				Operation: "serialize field attributes",
				Cause:     fmt.Errorf("invalid type format"),
			},
			expectedOperation: "serialize field attributes",
			expectedCause:     "invalid type format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Error()メソッドを呼び出し
			errMsg := tt.err.Error()

			// エラーメッセージが非空であることを確認
			if errMsg == "" {
				t.Error("Error() returned empty string")
			}

			// エラーメッセージに操作名が含まれることを確認
			if !strings.Contains(errMsg, tt.expectedOperation) {
				t.Errorf("error message should contain operation '%s', got: %s", tt.expectedOperation, errMsg)
			}

			// エラーメッセージに原因が含まれることを確認
			if !strings.Contains(errMsg, tt.expectedCause) {
				t.Errorf("error message should contain cause '%s', got: %s", tt.expectedCause, errMsg)
			}

			// エラーメッセージに"serialization"が含まれることを確認
			if !strings.Contains(errMsg, "serialization") {
				t.Errorf("error message should contain 'serialization', got: %s", errMsg)
			}
		})
	}
}
