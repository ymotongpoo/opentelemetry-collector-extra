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

// TestHTTPAPIIntegration_TracesSchema tests schema serialization for traces with pre-existing table
// 事前に存在するテーブルを使用したトレーススキーマのシリアライゼーションをテスト
// Requirements: 3.1, 3.2
func TestHTTPAPIIntegration_TracesSchema(t *testing.T) {
	// トレーススキーマを作成
	schema := createTracesSchema()

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// スキーマフィールドを検証
	fields := customFields

	// トレーススキーマは8つのフィールドを持つ
	expectedFieldCount := 8
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"trace_id":   false,
		"span_id":    false,
		"name":       false,
		"start_time": false,
		"end_time":   false,
	}

	for _, field := range fields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
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
		var mapField *CustomSchemaField
		for i := range fields {
			if fields[i].Name == mapFieldName {
				mapField = &fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
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

	// プリミティブ型フィールドの検証
	primitiveFields := map[string]string{
		"trace_id":   "binary",
		"span_id":    "binary",
		"name":       "string",
		"start_time": "timestamptz",
		"end_time":   "timestamptz",
	}

	for fieldName, expectedType := range primitiveFields {
		var field *CustomSchemaField
		for i := range fields {
			if fields[i].Name == fieldName {
				field = &fields[i]
				break
			}
		}

		if field == nil {
			t.Errorf("primitive field '%s' not found", fieldName)
			continue
		}

		// プリミティブ型が文字列として格納されていることを確認
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field '%s': expected Type to be string, got %T", fieldName, field.Type)
			continue
		}

		if typeStr != expectedType {
			t.Errorf("field '%s': expected type '%s', got '%s'", fieldName, expectedType, typeStr)
		}
	}

	// スキーマが正しくシリアライズされることを確認
	t.Log("Traces schema successfully serialized")
}

// TestHTTPAPIIntegration_MetricsSchema tests schema serialization for metrics with pre-existing table
// 事前に存在するテーブルを使用したメトリクススキーマのシリアライゼーションをテスト
// Requirements: 3.1, 3.2
func TestHTTPAPIIntegration_MetricsSchema(t *testing.T) {
	// メトリクススキーマを作成
	schema := createMetricsSchema()

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// スキーマフィールドを検証
	fields := customFields

	// メトリクススキーマは6つのフィールドを持つ
	expectedFieldCount := 6
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp":   false,
		"metric_name": false,
		"metric_type": false,
		"value":       false,
	}

	for _, field := range fields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
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
		var mapField *CustomSchemaField
		for i := range fields {
			if fields[i].Name == mapFieldName {
				mapField = &fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
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

	// プリミティブ型フィールドの検証
	primitiveFields := map[string]string{
		"timestamp":   "timestamptz",
		"metric_name": "string",
		"metric_type": "string",
		"value":       "double",
	}

	for fieldName, expectedType := range primitiveFields {
		var field *CustomSchemaField
		for i := range fields {
			if fields[i].Name == fieldName {
				field = &fields[i]
				break
			}
		}

		if field == nil {
			t.Errorf("primitive field '%s' not found", fieldName)
			continue
		}

		// プリミティブ型が文字列として格納されていることを確認
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field '%s': expected Type to be string, got %T", fieldName, field.Type)
			continue
		}

		if typeStr != expectedType {
			t.Errorf("field '%s': expected type '%s', got '%s'", fieldName, expectedType, typeStr)
		}
	}

	// スキーマが正しくシリアライズされることを確認
	t.Log("Metrics schema successfully serialized")
}

// TestHTTPAPIIntegration_LogsSchema tests schema serialization for logs with pre-existing table
// 事前に存在するテーブルを使用したログスキーマのシリアライゼーションをテスト
// Requirements: 3.1, 3.2
func TestHTTPAPIIntegration_LogsSchema(t *testing.T) {
	// ログスキーマを作成
	schema := createLogsSchema()

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// スキーマフィールドを検証
	fields := customFields

	// ログスキーマは5つのフィールドを持つ
	expectedFieldCount := 5
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp": false,
		"severity":  false,
		"body":      false,
	}

	for _, field := range fields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
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
		var mapField *CustomSchemaField
		for i := range fields {
			if fields[i].Name == mapFieldName {
				mapField = &fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
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

	// プリミティブ型フィールドの検証
	primitiveFields := map[string]string{
		"timestamp": "timestamptz",
		"severity":  "string",
		"body":      "string",
	}

	for fieldName, expectedType := range primitiveFields {
		var field *CustomSchemaField
		for i := range fields {
			if fields[i].Name == fieldName {
				field = &fields[i]
				break
			}
		}

		if field == nil {
			t.Errorf("primitive field '%s' not found", fieldName)
			continue
		}

		// プリミティブ型が文字列として格納されていることを確認
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field '%s': expected Type to be string, got %T", fieldName, field.Type)
			continue
		}

		if typeStr != expectedType {
			t.Errorf("field '%s': expected type '%s', got '%s'", fieldName, expectedType, typeStr)
		}
	}

	// スキーマが正しくシリアライズされることを確認
	t.Log("Logs schema successfully serialized")
}
