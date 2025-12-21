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
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

// SchemaGenerator はランダムなIcebergスキーマを生成する
// すべてのIDが一意になるように生成される
type SchemaGenerator struct {
	FieldCount int
}

// Generate はランダムなSchemaGeneratorを生成する
func (SchemaGenerator) Generate(r *rand.Rand, size int) reflect.Value {
	// フィールド数は1から10の範囲でランダムに生成
	fieldCount := r.Intn(10) + 1

	return reflect.ValueOf(SchemaGenerator{
		FieldCount: fieldCount,
	})
}

// GenerateSchema はランダムなIcebergスキーマを生成する
// すべてのIDが一意になるように生成される
func (g SchemaGenerator) GenerateSchema(r *rand.Rand) *IcebergSchema {
	fields := make([]IcebergSchemaField, 0, g.FieldCount)
	nextID := 1

	for i := 0; i < g.FieldCount; i++ {
		// フィールドの型をランダムに選択
		typeKind := r.Intn(4) // 0: primitive, 1: map, 2: list, 3: struct

		var fieldType interface{}
		fieldID := nextID
		nextID++

		switch typeKind {
		case 0:
			// プリミティブ型
			primitiveTypes := []string{"string", "int", "long", "double", "timestamptz", "binary"}
			fieldType = primitiveTypes[r.Intn(len(primitiveTypes))]

		case 1:
			// map型
			keyID := nextID
			nextID++
			valueID := nextID
			nextID++

			fieldType = map[string]interface{}{
				"type":           "map",
				"key-id":         keyID,
				"key":            "string",
				"value-id":       valueID,
				"value":          "string",
				"value-required": r.Intn(2) == 1,
			}

		case 2:
			// list型
			elementID := nextID
			nextID++

			fieldType = map[string]interface{}{
				"type":             "list",
				"element-id":       elementID,
				"element":          "string",
				"element-required": r.Intn(2) == 1,
			}

		case 3:
			// struct型（簡単な1フィールドのみ）
			innerFieldID := nextID
			nextID++

			fieldType = map[string]interface{}{
				"type": "struct",
				"fields": []map[string]interface{}{
					{
						"id":       innerFieldID,
						"name":     fmt.Sprintf("inner_field_%d", innerFieldID),
						"required": r.Intn(2) == 1,
						"type":     "string",
					},
				},
			}
		}

		field := IcebergSchemaField{
			ID:       fieldID,
			Name:     fmt.Sprintf("field_%d", fieldID),
			Required: r.Intn(2) == 1,
			Type:     fieldType,
		}

		fields = append(fields, field)
	}

	return &IcebergSchema{
		SchemaID: 0,
		Fields:   fields,
	}
}

// Feature: iceberg-schema-serialization-fix, Property 3: フィールドIDの一意性
// すべてのスキーマに対して、すべてのフィールドID（フィールドID、key-id、value-id、element-id等）は
// スキーマ全体で一意でなければならない
// 検証: 要件 3.1, 3.2
func TestProperty_FieldIDUniqueness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(schemaGen SchemaGenerator) bool {
		// ランダムなスキーマを生成
		r := rand.New(rand.NewSource(rand.Int63()))
		schema := schemaGen.GenerateSchema(r)

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// 検証を実行
		err := serializer.Validate()
		if err != nil {
			// エラーが発生した場合、それがID重複エラーでないことを確認
			// （生成されたスキーマはすべてのIDが一意であるべき）
			t.Logf("Validation failed unexpectedly: %v", err)
			return false
		}

		// すべてのIDを収集して一意性を確認
		idSet := make(map[int]bool)
		for _, field := range schema.Fields {
			// フィールドIDをチェック
			if idSet[field.ID] {
				t.Logf("Duplicate field ID found: %d", field.ID)
				return false
			}
			idSet[field.ID] = true

			// 複雑型のIDもチェック
			if field.IsMapType() {
				mapType, _ := field.GetMapType()
				keyID := mapType["key-id"].(int)
				valueID := mapType["value-id"].(int)

				if idSet[keyID] {
					t.Logf("Duplicate key-id found: %d", keyID)
					return false
				}
				idSet[keyID] = true

				if idSet[valueID] {
					t.Logf("Duplicate value-id found: %d", valueID)
					return false
				}
				idSet[valueID] = true
			} else if field.IsListType() {
				listType, _ := field.GetListType()
				elementID := listType["element-id"].(int)

				if idSet[elementID] {
					t.Logf("Duplicate element-id found: %d", elementID)
					return false
				}
				idSet[elementID] = true
			} else if field.IsStructType() {
				structType, _ := field.GetStructType()
				fieldsSlice := structType["fields"].([]map[string]interface{})

				for _, fieldMap := range fieldsSlice {
					nestedID := fieldMap["id"].(int)

					if idSet[nestedID] {
						t.Logf("Duplicate nested field ID found: %d", nestedID)
						return false
					}
					idSet[nestedID] = true
				}
			}
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}

// Feature: iceberg-schema-serialization-fix, Property 4: Iceberg仕様v2への準拠
// すべてのシリアライズされたスキーマに対して、出力はApache Iceberg Schema Specification v2の
// 構造要件を満たさなければならない
// 検証: 要件 3.3
func TestProperty_IcebergSpecV2Compliance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(schemaGen SchemaGenerator) bool {
		// ランダムなスキーマを生成
		r := rand.New(rand.NewSource(rand.Int63()))
		schema := schemaGen.GenerateSchema(r)

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// スキーマをシリアライズ
		serialized, err := serializer.SerializeForS3Tables()
		if err != nil {
			t.Logf("Serialization failed: %v", err)
			return false
		}

		// 結果が構造化オブジェクトであることを確認
		serializedMap, ok := serialized.(map[string]interface{})
		if !ok {
			t.Logf("Serialized result is not a map[string]interface{}, got %T", serialized)
			return false
		}

		// Iceberg仕様v2の必須フィールドを確認
		// 1. typeフィールドが"struct"であること
		typeValue, exists := serializedMap["type"]
		if !exists {
			t.Logf("Missing 'type' field in serialized schema")
			return false
		}
		if typeValue != "struct" {
			t.Logf("Expected type='struct', got %v", typeValue)
			return false
		}

		// 2. fieldsフィールドが存在すること
		fieldsValue, exists := serializedMap["fields"]
		if !exists {
			t.Logf("Missing 'fields' field in serialized schema")
			return false
		}

		// 3. fieldsが配列であること
		fieldsSlice, ok := fieldsValue.([]map[string]interface{})
		if !ok {
			t.Logf("'fields' is not a slice, got %T", fieldsValue)
			return false
		}

		// 4. 各フィールドが必須プロパティを持つこと
		for i, field := range fieldsSlice {
			// idフィールドの存在と型を確認
			if _, exists := field["id"]; !exists {
				t.Logf("Field %d missing 'id'", i)
				return false
			}
			if _, ok := field["id"].(int); !ok {
				t.Logf("Field %d 'id' is not an int, got %T", i, field["id"])
				return false
			}

			// nameフィールドの存在と型を確認
			if _, exists := field["name"]; !exists {
				t.Logf("Field %d missing 'name'", i)
				return false
			}
			if _, ok := field["name"].(string); !ok {
				t.Logf("Field %d 'name' is not a string, got %T", i, field["name"])
				return false
			}

			// requiredフィールドの存在と型を確認
			if _, exists := field["required"]; !exists {
				t.Logf("Field %d missing 'required'", i)
				return false
			}
			if _, ok := field["required"].(bool); !ok {
				t.Logf("Field %d 'required' is not a bool, got %T", i, field["required"])
				return false
			}

			// typeフィールドの存在を確認
			if _, exists := field["type"]; !exists {
				t.Logf("Field %d missing 'type'", i)
				return false
			}

			// typeフィールドが文字列（プリミティブ型）またはmap（複雑型）であることを確認
			switch fieldType := field["type"].(type) {
			case string:
				// プリミティブ型の場合、何もしない
			case map[string]interface{}:
				// 複雑型の場合、typeフィールドが存在することを確認
				if _, exists := fieldType["type"]; !exists {
					t.Logf("Field %d complex type missing 'type' field", i)
					return false
				}
			default:
				t.Logf("Field %d 'type' has unexpected type: %T", i, fieldType)
				return false
			}
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}

// Feature: iceberg-schema-serialization-fix, Property 5: Requiredフラグの正確性
// すべてのフィールドに対して、シリアライズ結果のrequiredプロパティは
// 元のフィールド定義のrequired値と一致しなければならない
// 検証: 要件 3.4, 3.5
func TestProperty_RequiredFlagAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(schemaGen SchemaGenerator) bool {
		// ランダムなスキーマを生成
		r := rand.New(rand.NewSource(rand.Int63()))
		schema := schemaGen.GenerateSchema(r)

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// スキーマをシリアライズ
		serialized, err := serializer.SerializeForS3Tables()
		if err != nil {
			t.Logf("Serialization failed: %v", err)
			return false
		}

		// 結果が構造化オブジェクトであることを確認
		serializedMap, ok := serialized.(map[string]interface{})
		if !ok {
			t.Logf("Serialized result is not a map[string]interface{}, got %T", serialized)
			return false
		}

		// fieldsを取得
		fieldsValue, exists := serializedMap["fields"]
		if !exists {
			t.Logf("Missing 'fields' field in serialized schema")
			return false
		}

		fieldsSlice, ok := fieldsValue.([]map[string]interface{})
		if !ok {
			t.Logf("'fields' is not a slice, got %T", fieldsValue)
			return false
		}

		// 元のスキーマとシリアライズされたスキーマのフィールド数が一致することを確認
		if len(fieldsSlice) != len(schema.Fields) {
			t.Logf("Field count mismatch: expected %d, got %d", len(schema.Fields), len(fieldsSlice))
			return false
		}

		// 各フィールドのrequiredフラグが保持されていることを確認
		for i, originalField := range schema.Fields {
			serializedField := fieldsSlice[i]

			// requiredフィールドの存在を確認
			requiredValue, exists := serializedField["required"]
			if !exists {
				t.Logf("Field %d missing 'required' field", i)
				return false
			}

			// requiredフィールドの型を確認
			requiredBool, ok := requiredValue.(bool)
			if !ok {
				t.Logf("Field %d 'required' is not a bool, got %T", i, requiredValue)
				return false
			}

			// requiredフラグが元の値と一致することを確認
			if requiredBool != originalField.Required {
				t.Logf("Field %d required flag mismatch: expected %v, got %v", i, originalField.Required, requiredBool)
				return false
			}
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
