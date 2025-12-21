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

// TestDeserialize_PrimitiveTypes tests deserialization of primitive types
// プリミティブ型のデシリアライゼーションをテスト
// 要件: 5.1
func TestDeserialize_PrimitiveTypes(t *testing.T) {
	// シリアライズされたスキーマJSON
	schemaJSON := `{
		"type": "struct",
		"fields": [
			{
				"id": 1,
				"name": "timestamp",
				"required": true,
				"type": "timestamptz"
			},
			{
				"id": 2,
				"name": "trace_id",
				"required": true,
				"type": "string"
			}
		]
	}`

	// デシリアライズ
	serializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	schema, err := serializer.Deserialize([]byte(schemaJSON))
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// フィールド数の検証
	if len(schema.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(schema.Fields))
	}

	// 最初のフィールドの検証
	field1 := schema.Fields[0]
	if field1.ID != 1 {
		t.Errorf("field1.ID: expected 1, got %d", field1.ID)
	}
	if field1.Name != "timestamp" {
		t.Errorf("field1.Name: expected 'timestamp', got '%s'", field1.Name)
	}
	if !field1.Required {
		t.Error("field1.Required: expected true, got false")
	}
	if !field1.IsPrimitiveType() {
		t.Error("field1 should be primitive type")
	}
	primitiveType, _ := field1.GetPrimitiveType()
	if primitiveType != "timestamptz" {
		t.Errorf("field1 type: expected 'timestamptz', got '%s'", primitiveType)
	}

	// 2番目のフィールドの検証
	field2 := schema.Fields[1]
	if field2.ID != 2 {
		t.Errorf("field2.ID: expected 2, got %d", field2.ID)
	}
	if field2.Name != "trace_id" {
		t.Errorf("field2.Name: expected 'trace_id', got '%s'", field2.Name)
	}
	if !field2.Required {
		t.Error("field2.Required: expected true, got false")
	}
	if !field2.IsPrimitiveType() {
		t.Error("field2 should be primitive type")
	}
	primitiveType2, _ := field2.GetPrimitiveType()
	if primitiveType2 != "string" {
		t.Errorf("field2 type: expected 'string', got '%s'", primitiveType2)
	}
}

// TestDeserialize_MapType tests deserialization of map types
// map型のデシリアライゼーションをテスト
// 要件: 5.1, 5.2
func TestDeserialize_MapType(t *testing.T) {
	// シリアライズされたスキーマJSON
	schemaJSON := `{
		"type": "struct",
		"fields": [
			{
				"id": 1,
				"name": "attributes",
				"required": false,
				"type": {
					"type": "map",
					"key-id": 2,
					"key": "string",
					"value-id": 3,
					"value": "string",
					"value-required": false
				}
			}
		]
	}`

	// デシリアライズ
	serializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	schema, err := serializer.Deserialize([]byte(schemaJSON))
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// フィールド数の検証
	if len(schema.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(schema.Fields))
	}

	// フィールドの検証
	field := schema.Fields[0]
	if field.ID != 1 {
		t.Errorf("field.ID: expected 1, got %d", field.ID)
	}
	if field.Name != "attributes" {
		t.Errorf("field.Name: expected 'attributes', got '%s'", field.Name)
	}
	if field.Required {
		t.Error("field.Required: expected false, got true")
	}

	// map型の検証
	if !field.IsMapType() {
		t.Fatal("field should be map type")
	}

	mapType, err := field.GetMapType()
	if err != nil {
		t.Fatalf("GetMapType failed: %v", err)
	}

	// map型のプロパティを検証
	if mapType["type"] != "map" {
		t.Errorf("map type: expected 'map', got '%v'", mapType["type"])
	}

	keyID, ok := mapType["key-id"].(float64)
	if !ok {
		t.Fatalf("key-id is not float64: %T", mapType["key-id"])
	}
	if int(keyID) != 2 {
		t.Errorf("key-id: expected 2, got %d", int(keyID))
	}

	if mapType["key"] != "string" {
		t.Errorf("key: expected 'string', got '%v'", mapType["key"])
	}

	valueID, ok := mapType["value-id"].(float64)
	if !ok {
		t.Fatalf("value-id is not float64: %T", mapType["value-id"])
	}
	if int(valueID) != 3 {
		t.Errorf("value-id: expected 3, got %d", int(valueID))
	}

	if mapType["value"] != "string" {
		t.Errorf("value: expected 'string', got '%v'", mapType["value"])
	}

	valueRequired, ok := mapType["value-required"].(bool)
	if !ok {
		t.Fatalf("value-required is not bool: %T", mapType["value-required"])
	}
	if valueRequired {
		t.Error("value-required: expected false, got true")
	}
}

// TestDeserialize_InvalidJSON tests deserialization with invalid JSON
// 無効なJSONでのデシリアライゼーションをテスト
// 要件: 5.1
func TestDeserialize_InvalidJSON(t *testing.T) {
	invalidJSON := `{invalid json`

	serializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	_, err := serializer.Deserialize([]byte(invalidJSON))
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

// TestDeserialize_MissingType tests deserialization with missing type field
// typeフィールドが欠落している場合のデシリアライゼーションをテスト
// 要件: 5.1
func TestDeserialize_MissingType(t *testing.T) {
	schemaJSON := `{
		"fields": [
			{
				"id": 1,
				"name": "test",
				"required": true,
				"type": "string"
			}
		]
	}`

	serializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	_, err := serializer.Deserialize([]byte(schemaJSON))
	if err == nil {
		t.Error("expected error for missing type field, got nil")
	}
}

// TestDeserialize_MissingFields tests deserialization with missing fields
// fieldsフィールドが欠落している場合のデシリアライゼーションをテスト
// 要件: 5.1
func TestDeserialize_MissingFields(t *testing.T) {
	schemaJSON := `{
		"type": "struct"
	}`

	serializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	_, err := serializer.Deserialize([]byte(schemaJSON))
	if err == nil {
		t.Error("expected error for missing fields, got nil")
	}
}

// TestDeserialize_RoundTrip tests serialization followed by deserialization
// シリアライズ→デシリアライズのラウンドトリップをテスト
// 要件: 5.1, 5.2, 5.3
func TestDeserialize_RoundTrip(t *testing.T) {
	// 元のスキーマを作成
	originalSchema := &IcebergSchema{
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

	// シリアライズ
	serializer := NewIcebergSchemaSerializer(originalSchema)
	serialized, err := serializer.SerializeForS3Tables()
	if err != nil {
		t.Fatalf("SerializeForS3Tables failed: %v", err)
	}

	// JSONに変換
	jsonBytes, err := json.Marshal(serialized)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// デシリアライズ
	deserializer := NewIcebergSchemaSerializer(&IcebergSchema{})
	deserializedSchema, err := deserializer.Deserialize(jsonBytes)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// フィールド数の検証
	if len(deserializedSchema.Fields) != len(originalSchema.Fields) {
		t.Errorf("field count mismatch: expected %d, got %d",
			len(originalSchema.Fields), len(deserializedSchema.Fields))
	}

	// 各フィールドの検証
	for i, originalField := range originalSchema.Fields {
		deserializedField := deserializedSchema.Fields[i]

		if deserializedField.ID != originalField.ID {
			t.Errorf("field %d ID mismatch: expected %d, got %d",
				i, originalField.ID, deserializedField.ID)
		}

		if deserializedField.Name != originalField.Name {
			t.Errorf("field %d Name mismatch: expected '%s', got '%s'",
				i, originalField.Name, deserializedField.Name)
		}

		if deserializedField.Required != originalField.Required {
			t.Errorf("field %d Required mismatch: expected %v, got %v",
				i, originalField.Required, deserializedField.Required)
		}

		// 型の検証
		if originalField.IsPrimitiveType() {
			if !deserializedField.IsPrimitiveType() {
				t.Errorf("field %d should be primitive type", i)
				continue
			}
			originalType, _ := originalField.GetPrimitiveType()
			deserializedType, _ := deserializedField.GetPrimitiveType()
			if deserializedType != originalType {
				t.Errorf("field %d type mismatch: expected '%s', got '%s'",
					i, originalType, deserializedType)
			}
		} else if originalField.IsMapType() {
			if !deserializedField.IsMapType() {
				t.Errorf("field %d should be map type", i)
				continue
			}
			// map型の詳細な検証は省略（基本的な構造が保持されていることを確認）
		}
	}
}
