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
	"encoding/json"
	"testing"
)

// TestAWSSchemaAdapter_TracesSchema はトレーススキーマでAWSSchemaAdapterをテストする
func TestAWSSchemaAdapter_TracesSchema(t *testing.T) {
	// トレーススキーマを作成
	schemaMap := createTracesSchema()

	// IcebergSchemaに変換
	icebergSchema, err := convertToIcebergSchema(schemaMap)
	if err != nil {
		t.Fatalf("Failed to convert to IcebergSchema: %v", err)
	}

	// AWSSchemaAdapterを作成
	adapter := NewAWSSchemaAdapter(icebergSchema)

	// CustomSchemaFieldsを取得
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("Failed to marshal schema fields: %v", err)
	}

	// フィールド数を確認
	if len(customFields) == 0 {
		t.Fatal("Expected non-empty custom fields")
	}

	// JSON出力を確認
	jsonBytes, err := json.MarshalIndent(customFields, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal custom fields to JSON: %v", err)
	}

	// JSON構造を検証
	var jsonFields []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &jsonFields); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// 各フィールドの構造を確認
	for _, field := range jsonFields {
		// nameフィールドの存在確認
		if _, ok := field["name"]; !ok {
			t.Errorf("Field missing 'name': %+v", field)
		}

		// typeフィールドの存在確認
		if _, ok := field["type"]; !ok {
			t.Errorf("Field missing 'type': %+v", field)
		}

		// requiredフィールドの存在確認
		if _, ok := field["required"]; !ok {
			t.Errorf("Field missing 'required': %+v", field)
		}

		// map型フィールドの構造を確認
		if name, ok := field["name"].(string); ok && (name == "attributes" || name == "resource_attributes") {
			// typeがmap[string]interface{}であることを確認
			typeField, ok := field["type"].(map[string]interface{})
			if !ok {
				t.Errorf("Expected map type for field %s, got %T", name, field["type"])
				continue
			}

			// map型の必須フィールドを確認
			requiredKeys := []string{"type", "key-id", "key", "value-id", "value", "value-required"}
			for _, key := range requiredKeys {
				if _, ok := typeField[key]; !ok {
					t.Errorf("Map type for field %s missing '%s'", name, key)
				}
			}

			// typeが"map"であることを確認
			if typeVal, ok := typeField["type"].(string); !ok || typeVal != "map" {
				t.Errorf("Expected type='map' for field %s, got %v", name, typeField["type"])
			}
		}
	}

	t.Logf("Traces schema JSON:\n%s", string(jsonBytes))
}

// TestAWSSchemaAdapter_MetricsSchema はメトリクススキーマでAWSSchemaAdapterをテストする
func TestAWSSchemaAdapter_MetricsSchema(t *testing.T) {
	// メトリクススキーマを作成
	schemaMap := createMetricsSchema()

	// IcebergSchemaに変換
	icebergSchema, err := convertToIcebergSchema(schemaMap)
	if err != nil {
		t.Fatalf("Failed to convert to IcebergSchema: %v", err)
	}

	// AWSSchemaAdapterを作成
	adapter := NewAWSSchemaAdapter(icebergSchema)

	// CustomSchemaFieldsを取得
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("Failed to marshal schema fields: %v", err)
	}

	// フィールド数を確認
	if len(customFields) == 0 {
		t.Fatal("Expected non-empty custom fields")
	}

	// JSON出力を確認
	jsonBytes, err := json.MarshalIndent(customFields, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal custom fields to JSON: %v", err)
	}

	// JSON構造を検証
	var jsonFields []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &jsonFields); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// 各フィールドの構造を確認
	for _, field := range jsonFields {
		// 基本フィールドの存在確認
		if _, ok := field["name"]; !ok {
			t.Errorf("Field missing 'name': %+v", field)
		}
		if _, ok := field["type"]; !ok {
			t.Errorf("Field missing 'type': %+v", field)
		}
		if _, ok := field["required"]; !ok {
			t.Errorf("Field missing 'required': %+v", field)
		}
	}

	t.Logf("Metrics schema JSON:\n%s", string(jsonBytes))
}

// TestAWSSchemaAdapter_LogsSchema はログスキーマでAWSSchemaAdapterをテストする
func TestAWSSchemaAdapter_LogsSchema(t *testing.T) {
	// ログスキーマを作成
	schemaMap := createLogsSchema()

	// IcebergSchemaに変換
	icebergSchema, err := convertToIcebergSchema(schemaMap)
	if err != nil {
		t.Fatalf("Failed to convert to IcebergSchema: %v", err)
	}

	// AWSSchemaAdapterを作成
	adapter := NewAWSSchemaAdapter(icebergSchema)

	// CustomSchemaFieldsを取得
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("Failed to marshal schema fields: %v", err)
	}

	// フィールド数を確認
	if len(customFields) == 0 {
		t.Fatal("Expected non-empty custom fields")
	}

	// JSON出力を確認
	jsonBytes, err := json.MarshalIndent(customFields, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal custom fields to JSON: %v", err)
	}

	// JSON構造を検証
	var jsonFields []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &jsonFields); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// 各フィールドの構造を確認
	for _, field := range jsonFields {
		// 基本フィールドの存在確認
		if _, ok := field["name"]; !ok {
			t.Errorf("Field missing 'name': %+v", field)
		}
		if _, ok := field["type"]; !ok {
			t.Errorf("Field missing 'type': %+v", field)
		}
		if _, ok := field["required"]; !ok {
			t.Errorf("Field missing 'required': %+v", field)
		}
	}

	t.Logf("Logs schema JSON:\n%s", string(jsonBytes))
}

// TestAWSSchemaAdapter_ComplexTypeStructure は複雑型の構造が正しくシリアライズされることをテストする
func TestAWSSchemaAdapter_ComplexTypeStructure(t *testing.T) {
	// map型を含むシンプルなスキーマを作成
	schemaMap := map[string]interface{}{
		"type": "struct",
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "simple_field",
				"required": true,
				"type":     "string",
			},
			{
				"id":       2,
				"name":     "map_field",
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

	// IcebergSchemaに変換
	icebergSchema, err := convertToIcebergSchema(schemaMap)
	if err != nil {
		t.Fatalf("Failed to convert to IcebergSchema: %v", err)
	}

	// AWSSchemaAdapterを作成
	adapter := NewAWSSchemaAdapter(icebergSchema)

	// CustomSchemaFieldsを取得
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("Failed to marshal schema fields: %v", err)
	}

	// フィールド数を確認
	if len(customFields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(customFields))
	}

	// simple_fieldの確認
	simpleField := customFields[0]
	if simpleField.Name != "simple_field" {
		t.Errorf("Expected name 'simple_field', got '%s'", simpleField.Name)
	}
	if typeStr, ok := simpleField.Type.(string); !ok || typeStr != "string" {
		t.Errorf("Expected type 'string', got %v", simpleField.Type)
	}
	if !simpleField.Required {
		t.Error("Expected required=true for simple_field")
	}

	// map_fieldの確認
	mapField := customFields[1]
	if mapField.Name != "map_field" {
		t.Errorf("Expected name 'map_field', got '%s'", mapField.Name)
	}
	if mapField.Required {
		t.Error("Expected required=false for map_field")
	}

	// map型の構造を確認
	mapType, ok := mapField.Type.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map type, got %T", mapField.Type)
	}

	// map型の必須フィールドを確認
	expectedKeys := map[string]interface{}{
		"type":           "map",
		"key-id":         3,
		"key":            "string",
		"value-id":       4,
		"value":          "string",
		"value-required": false,
	}

	for key, expectedValue := range expectedKeys {
		actualValue, ok := mapType[key]
		if !ok {
			t.Errorf("Map type missing key '%s'", key)
			continue
		}

		// 型に応じて値を比較
		switch v := expectedValue.(type) {
		case string:
			if actualValue != v {
				t.Errorf("Expected %s='%s', got '%v'", key, v, actualValue)
			}
		case int:
			// JSON unmarshalではintがfloat64になる可能性があるため、両方をチェック
			if actualInt, ok := actualValue.(int); ok {
				if actualInt != v {
					t.Errorf("Expected %s=%d, got %d", key, v, actualInt)
				}
			} else if actualFloat, ok := actualValue.(float64); ok {
				if int(actualFloat) != v {
					t.Errorf("Expected %s=%d, got %f", key, v, actualFloat)
				}
			} else {
				t.Errorf("Expected %s=%d, got %v (type %T)", key, v, actualValue, actualValue)
			}
		case bool:
			if actualValue != v {
				t.Errorf("Expected %s=%v, got %v", key, v, actualValue)
			}
		}
	}

	// JSON出力を確認
	jsonBytes, err := json.MarshalIndent(customFields, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal custom fields to JSON: %v", err)
	}

	t.Logf("Complex type structure JSON:\n%s", string(jsonBytes))
}
