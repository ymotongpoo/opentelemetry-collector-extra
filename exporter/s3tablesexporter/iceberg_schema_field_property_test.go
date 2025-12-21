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
	"fmt"
	"testing"
)

// TestProperty_JSONSerializationAccuracy tests JSON serialization accuracy property
// Feature: iceberg-schema-complex-types, Property 2: JSON シリアライゼーションの正確性
// Validates: Requirements 4.1, 4.2
func TestProperty_JSONSerializationAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のIcebergSchemaFieldをJSONにシリアライズした場合、
	// プリミティブ型は文字列値として、複合型はネストされたJSONオブジェクトとして出力されるべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// プリミティブ型のテスト
		primitiveField := generateRandomPrimitiveField(i)

		// JSONにマーシャル
		jsonData, err := json.Marshal(primitiveField)
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to marshal field: %v", i, err)
			continue
		}

		// JSONからアンマーシャル
		var unmarshalledPrimitive IcebergSchemaField
		err = json.Unmarshal(jsonData, &unmarshalledPrimitive)
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to unmarshal field: %v", i, err)
			continue
		}

		// プリミティブ型が文字列として保持されていることを確認
		if !unmarshalledPrimitive.IsPrimitiveType() {
			t.Errorf("iteration %d (primitive): type is not a primitive type after round-trip", i)
			continue
		}

		primitiveType, err := unmarshalledPrimitive.GetPrimitiveType()
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to get primitive type: %v", i, err)
			continue
		}

		originalType, _ := primitiveField.GetPrimitiveType()
		if primitiveType != originalType {
			t.Errorf("iteration %d (primitive): type mismatch after round-trip, got %s, want %s",
				i, primitiveType, originalType)
		}

		// 他のフィールドも保持されていることを確認
		if unmarshalledPrimitive.ID != primitiveField.ID {
			t.Errorf("iteration %d (primitive): ID mismatch, got %d, want %d",
				i, unmarshalledPrimitive.ID, primitiveField.ID)
		}
		if unmarshalledPrimitive.Name != primitiveField.Name {
			t.Errorf("iteration %d (primitive): Name mismatch, got %s, want %s",
				i, unmarshalledPrimitive.Name, primitiveField.Name)
		}
		if unmarshalledPrimitive.Required != primitiveField.Required {
			t.Errorf("iteration %d (primitive): Required mismatch, got %v, want %v",
				i, unmarshalledPrimitive.Required, primitiveField.Required)
		}

		// 複合型（map）のテスト
		mapField := generateRandomMapField(i + 1000)

		// JSONにマーシャル
		jsonData, err = json.Marshal(mapField)
		if err != nil {
			t.Errorf("iteration %d (map): failed to marshal field: %v", i, err)
			continue
		}

		// JSONからアンマーシャル
		var unmarshalledMap IcebergSchemaField
		err = json.Unmarshal(jsonData, &unmarshalledMap)
		if err != nil {
			t.Errorf("iteration %d (map): failed to unmarshal field: %v", i, err)
			continue
		}

		// 複合型がmap型として保持されていることを確認
		if !unmarshalledMap.IsMapType() {
			t.Errorf("iteration %d (map): type is not a map type after round-trip", i)
			continue
		}

		mapType, err := unmarshalledMap.GetMapType()
		if err != nil {
			t.Errorf("iteration %d (map): failed to get map type: %v", i, err)
			continue
		}

		originalMapType, _ := mapField.GetMapType()

		// map型の必須フィールドが保持されていることを確認
		if mapType["type"] != originalMapType["type"] {
			t.Errorf("iteration %d (map): type field mismatch, got %v, want %v",
				i, mapType["type"], originalMapType["type"])
		}

		// JSONアンマーシャル時に数値はfloat64に変換されるため、型変換して比較
		keyIDFloat, ok := mapType["key-id"].(float64)
		if !ok {
			t.Errorf("iteration %d (map): key-id is not a number", i)
		} else {
			originalKeyID := originalMapType["key-id"].(int)
			if int(keyIDFloat) != originalKeyID {
				t.Errorf("iteration %d (map): key-id mismatch, got %d, want %d",
					i, int(keyIDFloat), originalKeyID)
			}
		}

		if mapType["key"] != originalMapType["key"] {
			t.Errorf("iteration %d (map): key mismatch, got %v, want %v",
				i, mapType["key"], originalMapType["key"])
		}

		// JSONアンマーシャル時に数値はfloat64に変換されるため、型変換して比較
		valueIDFloat, ok := mapType["value-id"].(float64)
		if !ok {
			t.Errorf("iteration %d (map): value-id is not a number", i)
		} else {
			originalValueID := originalMapType["value-id"].(int)
			if int(valueIDFloat) != originalValueID {
				t.Errorf("iteration %d (map): value-id mismatch, got %d, want %d",
					i, int(valueIDFloat), originalValueID)
			}
		}

		if mapType["value"] != originalMapType["value"] {
			t.Errorf("iteration %d (map): value mismatch, got %v, want %v",
				i, mapType["value"], originalMapType["value"])
		}
		if mapType["value-required"] != originalMapType["value-required"] {
			t.Errorf("iteration %d (map): value-required mismatch, got %v, want %v",
				i, mapType["value-required"], originalMapType["value-required"])
		}

		// 他のフィールドも保持されていることを確認
		if unmarshalledMap.ID != mapField.ID {
			t.Errorf("iteration %d (map): ID mismatch, got %d, want %d",
				i, unmarshalledMap.ID, mapField.ID)
		}
		if unmarshalledMap.Name != mapField.Name {
			t.Errorf("iteration %d (map): Name mismatch, got %s, want %s",
				i, unmarshalledMap.Name, mapField.Name)
		}
		if unmarshalledMap.Required != mapField.Required {
			t.Errorf("iteration %d (map): Required mismatch, got %v, want %v",
				i, unmarshalledMap.Required, mapField.Required)
		}
	}
}

// generateRandomPrimitiveField generates a random primitive type field for testing
// テスト用のランダムなプリミティブ型フィールドを生成
func generateRandomPrimitiveField(seed int) IcebergSchemaField {
	primitiveTypes := []string{
		"boolean",
		"int",
		"long",
		"float",
		"double",
		"decimal(10,2)",
		"date",
		"time",
		"timestamp",
		"timestamptz",
		"string",
		"uuid",
		"fixed(16)",
		"binary",
	}

	typeIndex := seed % len(primitiveTypes)

	return IcebergSchemaField{
		ID:       seed + 1,
		Name:     fmt.Sprintf("field_%d", seed),
		Required: seed%2 == 0,
		Type:     primitiveTypes[typeIndex],
	}
}

// generateRandomMapField generates a random map type field for testing
// テスト用のランダムなmap型フィールドを生成
func generateRandomMapField(seed int) IcebergSchemaField {
	keyTypes := []string{"string", "int", "long"}
	valueTypes := []string{"string", "int", "long", "double", "boolean"}

	keyType := keyTypes[seed%len(keyTypes)]
	valueType := valueTypes[seed%len(valueTypes)]

	return IcebergSchemaField{
		ID:       seed + 1,
		Name:     fmt.Sprintf("map_field_%d", seed),
		Required: seed%3 == 0,
		Type: map[string]interface{}{
			"type":           "map",
			"key-id":         seed + 100,
			"key":            keyType,
			"value-id":       seed + 200,
			"value":          valueType,
			"value-required": seed%2 == 0,
		},
	}
}
