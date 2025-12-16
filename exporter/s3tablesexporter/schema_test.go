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
	"testing"

	"github.com/apache/iceberg-go"
)

// TestCreateMetricsSchema tests that metrics schema is created correctly
// Requirements: 2.3
func TestCreateMetricsSchema(t *testing.T) {
	schema := createMetricsSchema()
	if schema == nil {
		t.Fatal("createMetricsSchema() returned nil")
	}

	// スキーマのフィールド数を確認
	fields := schema.Fields()
	expectedFieldCount := 6 // timestamp, resource_attributes, metric_name, metric_type, value, attributes
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := []string{"timestamp", "metric_name", "metric_type", "value"}
	for _, fieldName := range requiredFields {
		found := false
		for _, field := range fields {
			if field.Name == fieldName {
				found = true
				if !field.Required {
					t.Errorf("field %s should be required", fieldName)
				}
				break
			}
		}
		if !found {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// timestampフィールドの型を確認
	timestampField := fields[0]
	if timestampField.Name != "timestamp" {
		t.Errorf("expected first field to be 'timestamp', got '%s'", timestampField.Name)
	}
	if timestampField.Type != iceberg.PrimitiveTypes.TimestampTz {
		t.Errorf("timestamp field should be TimestampTz type")
	}

	// valueフィールドの型を確認
	var valueField *iceberg.NestedField
	for i := range fields {
		if fields[i].Name == "value" {
			valueField = &fields[i]
			break
		}
	}
	if valueField == nil {
		t.Fatal("value field not found")
	}
	if valueField.Type != iceberg.PrimitiveTypes.Float64 {
		t.Errorf("value field should be Float64 type")
	}
}

// TestCreateTracesSchema tests that traces schema is created correctly
// Requirements: 2.3
func TestCreateTracesSchema(t *testing.T) {
	schema := createTracesSchema()
	if schema == nil {
		t.Fatal("createTracesSchema() returned nil")
	}

	// スキーマのフィールド数を確認
	fields := schema.Fields()
	expectedFieldCount := 8 // trace_id, span_id, parent_span_id, name, start_time, end_time, attributes, resource_attributes
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := []string{"trace_id", "span_id", "name", "start_time", "end_time"}
	for _, fieldName := range requiredFields {
		found := false
		for _, field := range fields {
			if field.Name == fieldName {
				found = true
				if !field.Required {
					t.Errorf("field %s should be required", fieldName)
				}
				break
			}
		}
		if !found {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// trace_idフィールドの型を確認
	traceIDField := fields[0]
	if traceIDField.Name != "trace_id" {
		t.Errorf("expected first field to be 'trace_id', got '%s'", traceIDField.Name)
	}
	if traceIDField.Type != iceberg.PrimitiveTypes.Binary {
		t.Errorf("trace_id field should be Binary type")
	}

	// parent_span_idはオプショナルであることを確認
	var parentSpanIDField *iceberg.NestedField
	for i := range fields {
		if fields[i].Name == "parent_span_id" {
			parentSpanIDField = &fields[i]
			break
		}
	}
	if parentSpanIDField == nil {
		t.Fatal("parent_span_id field not found")
	}
	if parentSpanIDField.Required {
		t.Error("parent_span_id field should be optional")
	}
}

// TestCreateLogsSchema tests that logs schema is created correctly
// Requirements: 2.3
func TestCreateLogsSchema(t *testing.T) {
	schema := createLogsSchema()
	if schema == nil {
		t.Fatal("createLogsSchema() returned nil")
	}

	// スキーマのフィールド数を確認
	fields := schema.Fields()
	expectedFieldCount := 5 // timestamp, severity, body, attributes, resource_attributes
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := []string{"timestamp", "body"}
	for _, fieldName := range requiredFields {
		found := false
		for _, field := range fields {
			if field.Name == fieldName {
				found = true
				if !field.Required {
					t.Errorf("field %s should be required", fieldName)
				}
				break
			}
		}
		if !found {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// timestampフィールドの型を確認
	timestampField := fields[0]
	if timestampField.Name != "timestamp" {
		t.Errorf("expected first field to be 'timestamp', got '%s'", timestampField.Name)
	}
	if timestampField.Type != iceberg.PrimitiveTypes.TimestampTz {
		t.Errorf("timestamp field should be TimestampTz type")
	}

	// severityはオプショナルであることを確認
	var severityField *iceberg.NestedField
	for i := range fields {
		if fields[i].Name == "severity" {
			severityField = &fields[i]
			break
		}
	}
	if severityField == nil {
		t.Fatal("severity field not found")
	}
	if severityField.Required {
		t.Error("severity field should be optional")
	}
}

// **Feature: s3tables-upload-implementation, Property 1: スキーマ変換の正確性**
// **Validates: Requirements 2.3**
// TestProperty_SchemaConversionAccuracy tests that all schema creation functions
// produce valid Iceberg schemas with correct field types and requirements
func TestProperty_SchemaConversionAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property test in short mode")
	}

	// プロパティ: すべてのスキーマ作成関数は有効なIcebergスキーマを生成する
	schemas := []struct {
		name   string
		schema *iceberg.Schema
	}{
		{"metrics", createMetricsSchema()},
		{"traces", createTracesSchema()},
		{"logs", createLogsSchema()},
	}

	for _, tc := range schemas {
		t.Run(tc.name, func(t *testing.T) {
			// プロパティ1: スキーマはnilではない
			if tc.schema == nil {
				t.Errorf("%s schema should not be nil", tc.name)
				return
			}

			// プロパティ2: スキーマは少なくとも1つのフィールドを持つ
			fields := tc.schema.Fields()
			if len(fields) == 0 {
				t.Errorf("%s schema should have at least one field", tc.name)
			}

			// プロパティ3: すべてのフィールドは一意のIDを持つ
			fieldIDs := make(map[int]bool)
			for _, field := range fields {
				if fieldIDs[field.ID] {
					t.Errorf("%s schema has duplicate field ID: %d", tc.name, field.ID)
				}
				fieldIDs[field.ID] = true
			}

			// プロパティ4: すべてのフィールドは名前を持つ
			for _, field := range fields {
				if field.Name == "" {
					t.Errorf("%s schema has field with empty name", tc.name)
				}
			}

			// プロパティ5: すべてのフィールドは型を持つ
			for _, field := range fields {
				if field.Type == nil {
					t.Errorf("%s schema field '%s' has nil type", tc.name, field.Name)
				}
			}

			// プロパティ6: timestampフィールドは存在し、TimestampTz型である
			var timestampField *iceberg.NestedField
			for i := range fields {
				if fields[i].Name == "timestamp" {
					timestampField = &fields[i]
					break
				}
			}
			if timestampField != nil {
				if timestampField.Type != iceberg.PrimitiveTypes.TimestampTz {
					t.Errorf("%s schema timestamp field should be TimestampTz type", tc.name)
				}
			}

			// プロパティ7: Map型のフィールドは正しい構造を持つ
			for _, field := range fields {
				if mapType, ok := field.Type.(*iceberg.MapType); ok {
					if mapType.KeyType == nil {
						t.Errorf("%s schema map field '%s' has nil KeyType", tc.name, field.Name)
					}
					if mapType.ValueType == nil {
						t.Errorf("%s schema map field '%s' has nil ValueType", tc.name, field.Name)
					}
				}
			}
		})
	}
}
