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

	"github.com/google/uuid"
)

// TestProperty_MetadataJSONSerializationAccuracy tests metadata JSON serialization accuracy
// Feature: fix-aws-sdk-metadata-serialization, Property 1: メタデータJSONシリアライゼーションの正確性
// Validates: Requirements 2.1, 2.2
func TestProperty_MetadataJSONSerializationAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のIcebergMetadataをJSONにシリアライズした場合、
	// 複合型はネストされたJSONオブジェクトとして出力され、JSON文字列として出力されないべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなスキーマを生成（プリミティブ型と複合型を含む）
		schema := generateRandomSchemaWithComplexTypes(i)

		// IcebergMetadataを作成
		metadata := createInitialIcebergMetadata(schema)

		// JSONにシリアライズ
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			t.Fatalf("iteration %d: failed to marshal metadata to JSON: %v", i, err)
		}

		// JSONをmap[string]interface{}にデシリアライズして検証
		var metadataMap map[string]interface{}
		err = json.Unmarshal(metadataJSON, &metadataMap)
		if err != nil {
			t.Fatalf("iteration %d: failed to unmarshal metadata JSON: %v", i, err)
		}

		// スキーマフィールドを検証
		schemas, ok := metadataMap["schemas"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: schemas is not an array", i)
		}
		if len(schemas) == 0 {
			t.Fatalf("iteration %d: schemas array is empty", i)
		}

		schemaMap, ok := schemas[0].(map[string]interface{})
		if !ok {
			t.Fatalf("iteration %d: schema is not a map", i)
		}

		fields, ok := schemaMap["fields"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: fields is not an array", i)
		}

		// 各フィールドの型を検証
		for j, fieldInterface := range fields {
			fieldMap, ok := fieldInterface.(map[string]interface{})
			if !ok {
				t.Errorf("iteration %d, field %d: field is not a map", i, j)
				continue
			}

			fieldType := fieldMap["type"]

			// プリミティブ型の場合は文字列であることを確認
			if typeStr, ok := fieldType.(string); ok {
				// プリミティブ型は文字列として正しくシリアライズされている
				if typeStr == "" {
					t.Errorf("iteration %d, field %d: primitive type is empty string", i, j)
				}
				continue
			}

			// 複合型の場合はネストされたオブジェクトであることを確認
			if typeMap, ok := fieldType.(map[string]interface{}); ok {
				// 複合型はネストされたJSONオブジェクトとして正しくシリアライズされている
				typeValue, hasType := typeMap["type"]
				if !hasType {
					t.Errorf("iteration %d, field %d: complex type missing 'type' field", i, j)
					continue
				}

				// map型の場合、必須フィールドを検証
				if typeValue == "map" {
					requiredFields := []string{"key-id", "key", "value-id", "value", "value-required"}
					for _, requiredField := range requiredFields {
						if _, hasField := typeMap[requiredField]; !hasField {
							t.Errorf("iteration %d, field %d: map type missing required field '%s'", i, j, requiredField)
						}
					}

					// key-idとvalue-idが数値であることを確認
					if keyID, ok := typeMap["key-id"].(float64); !ok || keyID <= 0 {
						t.Errorf("iteration %d, field %d: map type key-id is not a positive number", i, j)
					}
					if valueID, ok := typeMap["value-id"].(float64); !ok || valueID <= 0 {
						t.Errorf("iteration %d, field %d: map type value-id is not a positive number", i, j)
					}

					// keyとvalueが文字列であることを確認
					if _, ok := typeMap["key"].(string); !ok {
						t.Errorf("iteration %d, field %d: map type key is not a string", i, j)
					}
					if _, ok := typeMap["value"].(string); !ok {
						t.Errorf("iteration %d, field %d: map type value is not a string", i, j)
					}

					// value-requiredがブール値であることを確認
					if _, ok := typeMap["value-required"].(bool); !ok {
						t.Errorf("iteration %d, field %d: map type value-required is not a boolean", i, j)
					}
				}

				continue
			}

			// 型が文字列でもマップでもない場合はエラー
			t.Errorf("iteration %d, field %d: type is neither string nor map, got %T", i, j, fieldType)
		}

		// メタデータJSON全体が有効なJSONであることを確認
		var testUnmarshal interface{}
		if err := json.Unmarshal(metadataJSON, &testUnmarshal); err != nil {
			t.Errorf("iteration %d: metadata JSON is not valid JSON: %v", i, err)
		}
	}
}

// generateRandomSchemaWithComplexTypes generates a random schema with complex types for testing
// テスト用の複合型を含むランダムなスキーマを生成
func generateRandomSchemaWithComplexTypes(seed int) *IcebergSchema {
	// プリミティブ型のフィールドを生成
	primitiveTypes := []string{
		"boolean",
		"int",
		"long",
		"float",
		"double",
		"string",
		"timestamp",
		"timestamptz",
	}

	fields := make([]IcebergSchemaField, 0)
	fieldID := 1

	// プリミティブ型のフィールドを追加（1-3個）
	numPrimitiveFields := 1 + (seed % 3)
	for i := 0; i < numPrimitiveFields; i++ {
		typeIndex := (seed + i) % len(primitiveTypes)
		fields = append(fields, IcebergSchemaField{
			ID:       fieldID,
			Name:     fmt.Sprintf("primitive_field_%d", i),
			Required: i == 0, // 最初のフィールドのみ必須
			Type:     primitiveTypes[typeIndex],
		})
		fieldID++
	}

	// map型のフィールドを追加（1-2個）
	numMapFields := 1 + (seed % 2)
	for i := 0; i < numMapFields; i++ {
		keyTypes := []string{"string", "int", "long"}
		valueTypes := []string{"string", "int", "long", "double", "boolean"}

		keyType := keyTypes[(seed+i)%len(keyTypes)]
		valueType := valueTypes[(seed+i)%len(valueTypes)]

		mapType := map[string]interface{}{
			"type":           "map",
			"key-id":         fieldID,
			"key":            keyType,
			"value-id":       fieldID + 1,
			"value":          valueType,
			"value-required": (seed+i)%2 == 0,
		}

		fields = append(fields, IcebergSchemaField{
			ID:       fieldID + 2,
			Name:     fmt.Sprintf("map_field_%d", i),
			Required: false,
			Type:     mapType,
		})
		fieldID += 3 // map型は3つのIDを使用（field ID, key-id, value-id）
	}

	return &IcebergSchema{
		SchemaID: 0,
		Fields:   fields,
	}
}

// createInitialIcebergMetadata creates an initial Iceberg metadata for testing
// テスト用の初期Icebergメタデータを作成
func createInitialIcebergMetadata(schema *IcebergSchema) *IcebergMetadata {
	return &IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          uuid.New().String(),
		Location:           "", // S3 Tablesが自動的に設定
		LastSequenceNumber: 0,
		LastUpdatedMS:      0, // S3 Tablesが自動的に設定
		LastColumnID:       getLastColumnID(schema),
		Schemas:            []IcebergSchema{*schema},
		CurrentSchemaID:    0,
		PartitionSpecs:     []IcebergPartitionSpec{{SpecID: 0, Fields: []IcebergPartitionField{}}},
		DefaultSpecID:      0,
		LastPartitionID:    0,
		Properties:         map[string]string{},
		CurrentSnapshotID:  -1,
		Snapshots:          []IcebergSnapshot{},
		SnapshotLog:        []IcebergSnapshotLog{},
		MetadataLog:        []IcebergMetadataLog{},
	}
}

// getLastColumnID returns the last column ID from the schema
// スキーマから最後のカラムIDを取得
func getLastColumnID(schema *IcebergSchema) int {
	maxID := 0
	for _, field := range schema.Fields {
		if field.ID > maxID {
			maxID = field.ID
		}
		// map型の場合、key-idとvalue-idも考慮
		if field.IsMapType() {
			mapType, _ := field.GetMapType()
			if keyID, ok := mapType["key-id"].(int); ok && keyID > maxID {
				maxID = keyID
			}
			if keyID, ok := mapType["key-id"].(float64); ok && int(keyID) > maxID {
				maxID = int(keyID)
			}
			if valueID, ok := mapType["value-id"].(int); ok && valueID > maxID {
				maxID = valueID
			}
			if valueID, ok := mapType["value-id"].(float64); ok && int(valueID) > maxID {
				maxID = int(valueID)
			}
		}
	}
	return maxID
}
