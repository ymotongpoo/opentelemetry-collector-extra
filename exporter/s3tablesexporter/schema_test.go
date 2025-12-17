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
)

// TestCreateMetricsSchema tests that metrics schema is created correctly
// Requirements: 2.3
func TestCreateMetricsSchema(t *testing.T) {
	schema := createMetricsSchema()
	if schema == nil {
		t.Fatal("createMetricsSchema() returned nil")
	}

	// スキーマのトップレベル構造を検証
	if schema["type"] != "struct" {
		t.Errorf("expected type 'struct', got %v", schema["type"])
	}

	fields, ok := schema["fields"].([]map[string]interface{})
	if !ok {
		t.Fatal("fields is not a slice of maps")
	}

	// 必須フィールドの存在を検証
	requiredFields := map[string]string{
		"timestamp":   "timestamptz",
		"metric_name": "string",
		"metric_type": "string",
		"value":       "double",
	}

	foundFields := make(map[string]bool)
	for _, field := range fields {
		name, ok := field["name"].(string)
		if !ok {
			continue
		}
		foundFields[name] = true

		// 必須フィールドの型を検証
		if expectedType, exists := requiredFields[name]; exists {
			if field["type"] != expectedType {
				t.Errorf("field %s: expected type %s, got %v", name, expectedType, field["type"])
			}
			if required, ok := field["required"].(bool); !ok || !required {
				t.Errorf("field %s should be required", name)
			}
		}
	}

	// すべての必須フィールドが存在することを確認
	for fieldName := range requiredFields {
		if !foundFields[fieldName] {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// オプションフィールドの存在を検証
	optionalFields := []string{"resource_attributes", "attributes"}
	for _, fieldName := range optionalFields {
		if !foundFields[fieldName] {
			t.Errorf("optional field %s not found in schema", fieldName)
		}
	}
}

// TestCreateTracesSchema tests that traces schema is created correctly
// Requirements: 2.3
func TestCreateTracesSchema(t *testing.T) {
	schema := createTracesSchema()
	if schema == nil {
		t.Fatal("createTracesSchema() returned nil")
	}

	// スキーマのトップレベル構造を検証
	if schema["type"] != "struct" {
		t.Errorf("expected type 'struct', got %v", schema["type"])
	}

	fields, ok := schema["fields"].([]map[string]interface{})
	if !ok {
		t.Fatal("fields is not a slice of maps")
	}

	// 必須フィールドの存在を検証
	requiredFields := map[string]string{
		"trace_id":   "binary",
		"span_id":    "binary",
		"name":       "string",
		"start_time": "timestamptz",
		"end_time":   "timestamptz",
	}

	foundFields := make(map[string]bool)
	for _, field := range fields {
		name, ok := field["name"].(string)
		if !ok {
			continue
		}
		foundFields[name] = true

		// 必須フィールドの型を検証
		if expectedType, exists := requiredFields[name]; exists {
			if field["type"] != expectedType {
				t.Errorf("field %s: expected type %s, got %v", name, expectedType, field["type"])
			}
			if required, ok := field["required"].(bool); !ok || !required {
				t.Errorf("field %s should be required", name)
			}
		}
	}

	// すべての必須フィールドが存在することを確認
	for fieldName := range requiredFields {
		if !foundFields[fieldName] {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// オプションフィールドの存在を検証
	optionalFields := []string{"parent_span_id", "attributes", "resource_attributes"}
	for _, fieldName := range optionalFields {
		if !foundFields[fieldName] {
			t.Errorf("optional field %s not found in schema", fieldName)
		}
	}
}

// TestCreateLogsSchema tests that logs schema is created correctly
// Requirements: 2.3
func TestCreateLogsSchema(t *testing.T) {
	schema := createLogsSchema()
	if schema == nil {
		t.Fatal("createLogsSchema() returned nil")
	}

	// スキーマのトップレベル構造を検証
	if schema["type"] != "struct" {
		t.Errorf("expected type 'struct', got %v", schema["type"])
	}

	fields, ok := schema["fields"].([]map[string]interface{})
	if !ok {
		t.Fatal("fields is not a slice of maps")
	}

	// 必須フィールドの存在を検証
	requiredFields := map[string]string{
		"timestamp": "timestamptz",
		"body":      "string",
	}

	foundFields := make(map[string]bool)
	for _, field := range fields {
		name, ok := field["name"].(string)
		if !ok {
			continue
		}
		foundFields[name] = true

		// 必須フィールドの型を検証
		if expectedType, exists := requiredFields[name]; exists {
			if field["type"] != expectedType {
				t.Errorf("field %s: expected type %s, got %v", name, expectedType, field["type"])
			}
			if required, ok := field["required"].(bool); !ok || !required {
				t.Errorf("field %s should be required", name)
			}
		}
	}

	// すべての必須フィールドが存在することを確認
	for fieldName := range requiredFields {
		if !foundFields[fieldName] {
			t.Errorf("required field %s not found in schema", fieldName)
		}
	}

	// オプションフィールドの存在を検証
	optionalFields := []string{"severity", "attributes", "resource_attributes"}
	for _, fieldName := range optionalFields {
		if !foundFields[fieldName] {
			t.Errorf("optional field %s not found in schema", fieldName)
		}
	}
}
