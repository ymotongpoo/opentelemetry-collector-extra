// Copyright 2025 Yoshii <ymotongpoo@gmail.com>
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

// TestProperty_SchemaConversionRoundtrip tests schema conversion roundtrip property
// Feature: iceberg-schema-complex-types, Property 4: スキーマ変換のラウンドトリップ
// Validates: Requirements 3.1, 3.2, 3.3, 3.4
func TestProperty_SchemaConversionRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の有効なスキーマ定義（map[string]interface{}）をIcebergSchemaに変換し、
	// JSONにシリアライズした場合、元のスキーマ構造と等価な出力が得られるべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// プリミティブ型のラウンドトリップテスト
		primitiveFieldMap := generateRandomPrimitiveFieldMap(i)

		// map[string]interface{}からIcebergSchemaFieldに変換
		primitiveField, err := convertToSchemaField(primitiveFieldMap)
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to convert field: %v", i, err)
			continue
		}

		// IcebergSchemaFieldをJSONにシリアライズ
		jsonData, err := json.Marshal(primitiveField)
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to marshal field: %v", i, err)
			continue
		}

		// JSONをmap[string]interface{}にデシリアライズ
		var resultMap map[string]interface{}
		err = json.Unmarshal(jsonData, &resultMap)
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to unmarshal JSON: %v", i, err)
			continue
		}

		// 元のマップと結果のマップを比較
		if !compareFieldMaps(primitiveFieldMap, resultMap) {
			t.Errorf("iteration %d (primitive): field map mismatch after roundtrip\noriginal: %+v\nresult: %+v",
				i, primitiveFieldMap, resultMap)
		}

		// 複合型（map）のラウンドトリップテスト
		mapFieldMap := generateRandomMapFieldMap(i + 1000)

		// map[string]interface{}からIcebergSchemaFieldに変換
		mapField, err := convertToSchemaField(mapFieldMap)
		if err != nil {
			t.Errorf("iteration %d (map): failed to convert field: %v", i, err)
			continue
		}

		// IcebergSchemaFieldをJSONにシリアライズ
		jsonData, err = json.Marshal(mapField)
		if err != nil {
			t.Errorf("iteration %d (map): failed to marshal field: %v", i, err)
			continue
		}

		// JSONをmap[string]interface{}にデシリアライズ
		err = json.Unmarshal(jsonData, &resultMap)
		if err != nil {
			t.Errorf("iteration %d (map): failed to unmarshal JSON: %v", i, err)
			continue
		}

		// 元のマップと結果のマップを比較
		if !compareFieldMaps(mapFieldMap, resultMap) {
			t.Errorf("iteration %d (map): field map mismatch after roundtrip\noriginal: %+v\nresult: %+v",
				i, mapFieldMap, resultMap)
		}
	}
}

// generateRandomPrimitiveFieldMap generates a random primitive field map for testing
// テスト用のランダムなプリミティブ型フィールドマップを生成
func generateRandomPrimitiveFieldMap(seed int) map[string]interface{} {
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

	return map[string]interface{}{
		"id":       seed + 1,
		"name":     "field_" + string(rune('a'+seed%26)),
		"required": seed%2 == 0,
		"type":     primitiveTypes[typeIndex],
	}
}

// generateRandomMapFieldMap generates a random map field map for testing
// テスト用のランダムなmap型フィールドマップを生成
func generateRandomMapFieldMap(seed int) map[string]interface{} {
	keyTypes := []string{"string", "int", "long"}
	valueTypes := []string{"string", "int", "long", "double", "boolean"}

	keyType := keyTypes[seed%len(keyTypes)]
	valueType := valueTypes[seed%len(valueTypes)]

	return map[string]interface{}{
		"id":       seed + 1,
		"name":     "map_field_" + string(rune('a'+seed%26)),
		"required": seed%3 == 0,
		"type": map[string]interface{}{
			"type":           "map",
			"key-id":         seed + 100,
			"key":            keyType,
			"value-id":       seed + 200,
			"value":          valueType,
			"value-required": seed%2 == 0,
		},
	}
}

// compareFieldMaps compares two field maps for equality
// 2つのフィールドマップが等しいかどうかを比較
func compareFieldMaps(original, result map[string]interface{}) bool {
	// IDの比較（float64に変換される可能性があるため）
	originalID := getIntValue(original["id"])
	resultID := getIntValue(result["id"])
	if originalID != resultID {
		return false
	}

	// 名前の比較
	if original["name"] != result["name"] {
		return false
	}

	// requiredの比較
	if original["required"] != result["required"] {
		return false
	}

	// 型の比較
	originalType := original["type"]
	resultType := result["type"]

	// プリミティブ型の場合
	if originalTypeStr, ok := originalType.(string); ok {
		resultTypeStr, ok := resultType.(string)
		if !ok {
			return false
		}
		return originalTypeStr == resultTypeStr
	}

	// 複合型（map）の場合
	if originalTypeMap, ok := originalType.(map[string]interface{}); ok {
		resultTypeMap, ok := resultType.(map[string]interface{})
		if !ok {
			return false
		}
		return compareMapTypes(originalTypeMap, resultTypeMap)
	}

	return false
}

// compareMapTypes compares two map type definitions for equality
// 2つのmap型定義が等しいかどうかを比較
func compareMapTypes(original, result map[string]interface{}) bool {
	// typeフィールドの比較
	if original["type"] != result["type"] {
		return false
	}

	// key-idの比較
	originalKeyID := getIntValue(original["key-id"])
	resultKeyID := getIntValue(result["key-id"])
	if originalKeyID != resultKeyID {
		return false
	}

	// keyの比較
	if original["key"] != result["key"] {
		return false
	}

	// value-idの比較
	originalValueID := getIntValue(original["value-id"])
	resultValueID := getIntValue(result["value-id"])
	if originalValueID != resultValueID {
		return false
	}

	// valueの比較
	if original["value"] != result["value"] {
		return false
	}

	// value-requiredの比較
	if original["value-required"] != result["value-required"] {
		return false
	}

	return true
}

// getIntValue extracts an int value from an interface{} (handles both int and float64)
// interface{}からint値を抽出（intとfloat64の両方を処理）
func getIntValue(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	default:
		return 0
	}
}
