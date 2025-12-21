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
	"math/rand"
	"testing"
)

// TestProperty_SchemaRoundtripPreservation tests schema roundtrip preservation property
// Feature: iceberg-schema-serialization-fix, Property 6: スキーマのラウンドトリップ保持
// Validates: Requirements 5.1, 5.2, 5.3
//
// プロパティ: すべての有効なIcebergスキーマに対して、シリアライズしてからデシリアライズした結果は、
// 元のスキーマと同等の構造（フィールド名、型、ID、required等）を持たなければならない
func TestProperty_SchemaRoundtripPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなスキーマを生成
		originalSchema := generateRandomSchema(i)

		// シリアライズ
		serializer := NewIcebergSchemaSerializer(originalSchema)
		serialized, err := serializer.SerializeForS3Tables()
		if err != nil {
			t.Errorf("iteration %d: SerializeForS3Tables failed: %v", i, err)
			continue
		}

		// JSONに変換
		jsonBytes, err := json.Marshal(serialized)
		if err != nil {
			t.Errorf("iteration %d: json.Marshal failed: %v", i, err)
			continue
		}

		// デシリアライズ
		deserializer := NewIcebergSchemaSerializer(&IcebergSchema{})
		deserializedSchema, err := deserializer.Deserialize(jsonBytes)
		if err != nil {
			t.Errorf("iteration %d: Deserialize failed: %v", i, err)
			continue
		}

		// スキーマの構造が保持されているか検証
		if !compareSchemasStructure(originalSchema, deserializedSchema) {
			t.Errorf("iteration %d: schema structure not preserved after roundtrip\noriginal: %+v\ndeserialized: %+v",
				i, originalSchema, deserializedSchema)
		}
	}
}

// generateRandomSchema generates a random Iceberg schema for testing
// テスト用のランダムなIcebergスキーマを生成
func generateRandomSchema(seed int) *IcebergSchema {
	rng := rand.New(rand.NewSource(int64(seed)))

	// フィールド数をランダムに決定（1〜5個）
	fieldCount := rng.Intn(5) + 1
	fields := make([]IcebergSchemaField, fieldCount)

	nextID := 1
	for i := 0; i < fieldCount; i++ {
		// フィールドタイプをランダムに選択
		// 0: primitive, 1: map, 2: list, 3: struct
		fieldType := rng.Intn(4)

		switch fieldType {
		case 0:
			// プリミティブ型
			fields[i] = generateRandomPrimitiveFieldForRoundtrip(rng, &nextID)
		case 1:
			// map型
			fields[i] = generateRandomMapFieldForRoundtrip(rng, &nextID)
		case 2:
			// list型
			fields[i] = generateRandomListFieldForRoundtrip(rng, &nextID)
		case 3:
			// struct型（簡略版：ネストは1レベルまで）
			fields[i] = generateRandomStructFieldForRoundtrip(rng, &nextID)
		}
	}

	return &IcebergSchema{
		SchemaID: 0,
		Fields:   fields,
	}
}

// generateRandomPrimitiveFieldForRoundtrip generates a random primitive field
// ランダムなプリミティブ型フィールドを生成
func generateRandomPrimitiveFieldForRoundtrip(rng *rand.Rand, nextID *int) IcebergSchemaField {
	primitiveTypes := []string{"string", "int", "long", "double", "timestamptz", "binary", "boolean"}
	typeIndex := rng.Intn(len(primitiveTypes))

	field := IcebergSchemaField{
		ID:       *nextID,
		Name:     fmt.Sprintf("field_%d", *nextID),
		Required: rng.Intn(2) == 1,
		Type:     primitiveTypes[typeIndex],
	}

	*nextID++
	return field
}

// generateRandomMapFieldForRoundtrip generates a random map field
// ランダムなmap型フィールドを生成
func generateRandomMapFieldForRoundtrip(rng *rand.Rand, nextID *int) IcebergSchemaField {
	fieldID := *nextID
	*nextID++

	keyID := *nextID
	*nextID++

	valueID := *nextID
	*nextID++

	// keyは常にプリミティブ型
	keyTypes := []string{"string", "int", "long"}
	keyType := keyTypes[rng.Intn(len(keyTypes))]

	// valueはプリミティブ型
	valueTypes := []string{"string", "int", "long", "double", "boolean"}
	valueType := valueTypes[rng.Intn(len(valueTypes))]

	field := IcebergSchemaField{
		ID:       fieldID,
		Name:     fmt.Sprintf("map_field_%d", fieldID),
		Required: rng.Intn(2) == 1,
		Type: map[string]interface{}{
			"type":           "map",
			"key-id":         keyID,
			"key":            keyType,
			"value-id":       valueID,
			"value":          valueType,
			"value-required": rng.Intn(2) == 1,
		},
	}

	return field
}

// generateRandomListFieldForRoundtrip generates a random list field
// ランダムなlist型フィールドを生成
func generateRandomListFieldForRoundtrip(rng *rand.Rand, nextID *int) IcebergSchemaField {
	fieldID := *nextID
	*nextID++

	elementID := *nextID
	*nextID++

	// elementはプリミティブ型
	elementTypes := []string{"string", "int", "long", "double", "boolean"}
	elementType := elementTypes[rng.Intn(len(elementTypes))]

	field := IcebergSchemaField{
		ID:       fieldID,
		Name:     fmt.Sprintf("list_field_%d", fieldID),
		Required: rng.Intn(2) == 1,
		Type: map[string]interface{}{
			"type":             "list",
			"element-id":       elementID,
			"element":          elementType,
			"element-required": rng.Intn(2) == 1,
		},
	}

	return field
}

// generateRandomStructFieldForRoundtrip generates a random struct field
// ランダムなstruct型フィールドを生成（ネストは1レベルまで）
func generateRandomStructFieldForRoundtrip(rng *rand.Rand, nextID *int) IcebergSchemaField {
	fieldID := *nextID
	*nextID++

	// ネストされたフィールド数をランダムに決定（1〜3個）
	nestedFieldCount := rng.Intn(3) + 1
	nestedFields := make([]map[string]interface{}, nestedFieldCount)

	for i := 0; i < nestedFieldCount; i++ {
		nestedID := *nextID
		*nextID++

		primitiveTypes := []string{"string", "int", "long", "double", "boolean"}
		typeIndex := rng.Intn(len(primitiveTypes))

		nestedFields[i] = map[string]interface{}{
			"id":       nestedID,
			"name":     fmt.Sprintf("nested_%d", nestedID),
			"required": rng.Intn(2) == 1,
			"type":     primitiveTypes[typeIndex],
		}
	}

	field := IcebergSchemaField{
		ID:       fieldID,
		Name:     fmt.Sprintf("struct_field_%d", fieldID),
		Required: rng.Intn(2) == 1,
		Type: map[string]interface{}{
			"type":   "struct",
			"fields": nestedFields,
		},
	}

	return field
}

// compareSchemasStructure compares two schemas for structural equality
// 2つのスキーマの構造的な等価性を比較
func compareSchemasStructure(schema1, schema2 *IcebergSchema) bool {
	// フィールド数の比較
	if len(schema1.Fields) != len(schema2.Fields) {
		return false
	}

	// 各フィールドを比較
	for i := 0; i < len(schema1.Fields); i++ {
		if !compareFieldsStructure(&schema1.Fields[i], &schema2.Fields[i]) {
			return false
		}
	}

	return true
}

// compareFieldsStructure compares two fields for structural equality
// 2つのフィールドの構造的な等価性を比較
func compareFieldsStructure(field1, field2 *IcebergSchemaField) bool {
	// ID、Name、Requiredの比較
	if field1.ID != field2.ID {
		return false
	}
	if field1.Name != field2.Name {
		return false
	}
	if field1.Required != field2.Required {
		return false
	}

	// 型の比較
	if field1.IsPrimitiveType() && field2.IsPrimitiveType() {
		type1, _ := field1.GetPrimitiveType()
		type2, _ := field2.GetPrimitiveType()
		return type1 == type2
	}

	if field1.IsMapType() && field2.IsMapType() {
		return compareMapTypesForRoundtrip(field1, field2)
	}

	if field1.IsListType() && field2.IsListType() {
		return compareListTypesForRoundtrip(field1, field2)
	}

	if field1.IsStructType() && field2.IsStructType() {
		return compareStructTypesForRoundtrip(field1, field2)
	}

	// 型が一致しない場合
	return false
}

// compareMapTypesForRoundtrip compares two map type fields
// 2つのmap型フィールドを比較
func compareMapTypesForRoundtrip(field1, field2 *IcebergSchemaField) bool {
	mapType1, _ := field1.GetMapType()
	mapType2, _ := field2.GetMapType()

	// key-idの比較
	keyID1 := getIntValueForRoundtrip(mapType1["key-id"])
	keyID2 := getIntValueForRoundtrip(mapType2["key-id"])
	if keyID1 != keyID2 {
		return false
	}

	// keyの比較
	if mapType1["key"] != mapType2["key"] {
		return false
	}

	// value-idの比較
	valueID1 := getIntValueForRoundtrip(mapType1["value-id"])
	valueID2 := getIntValueForRoundtrip(mapType2["value-id"])
	if valueID1 != valueID2 {
		return false
	}

	// valueの比較
	if mapType1["value"] != mapType2["value"] {
		return false
	}

	// value-requiredの比較
	valueRequired1, _ := mapType1["value-required"].(bool)
	valueRequired2, _ := mapType2["value-required"].(bool)
	if valueRequired1 != valueRequired2 {
		return false
	}

	return true
}

// compareListTypesForRoundtrip compares two list type fields
// 2つのlist型フィールドを比較
func compareListTypesForRoundtrip(field1, field2 *IcebergSchemaField) bool {
	listType1, _ := field1.GetListType()
	listType2, _ := field2.GetListType()

	// element-idの比較
	elementID1 := getIntValueForRoundtrip(listType1["element-id"])
	elementID2 := getIntValueForRoundtrip(listType2["element-id"])
	if elementID1 != elementID2 {
		return false
	}

	// elementの比較
	if listType1["element"] != listType2["element"] {
		return false
	}

	// element-requiredの比較
	elementRequired1, _ := listType1["element-required"].(bool)
	elementRequired2, _ := listType2["element-required"].(bool)
	if elementRequired1 != elementRequired2 {
		return false
	}

	return true
}

// compareStructTypesForRoundtrip compares two struct type fields
// 2つのstruct型フィールドを比較
func compareStructTypesForRoundtrip(field1, field2 *IcebergSchemaField) bool {
	structType1, _ := field1.GetStructType()
	structType2, _ := field2.GetStructType()

	// fieldsの取得
	fieldsInterface1, ok1 := structType1["fields"]
	fieldsInterface2, ok2 := structType2["fields"]

	if !ok1 || !ok2 {
		return false
	}

	// 両方のfieldsを[]map[string]interface{}に変換
	var fields1, fields2 []map[string]interface{}

	// fieldsInterface1を変換
	if slice1, ok := fieldsInterface1.([]map[string]interface{}); ok {
		fields1 = slice1
	} else if slice1, ok := fieldsInterface1.([]interface{}); ok {
		fields1 = make([]map[string]interface{}, len(slice1))
		for i, item := range slice1 {
			if fieldMap, ok := item.(map[string]interface{}); ok {
				fields1[i] = fieldMap
			} else {
				return false
			}
		}
	} else {
		return false
	}

	// fieldsInterface2を変換
	if slice2, ok := fieldsInterface2.([]map[string]interface{}); ok {
		fields2 = slice2
	} else if slice2, ok := fieldsInterface2.([]interface{}); ok {
		fields2 = make([]map[string]interface{}, len(slice2))
		for i, item := range slice2 {
			if fieldMap, ok := item.(map[string]interface{}); ok {
				fields2[i] = fieldMap
			} else {
				return false
			}
		}
	} else {
		return false
	}

	// フィールド数の比較
	if len(fields1) != len(fields2) {
		return false
	}

	// 各ネストされたフィールドを比較
	for i := 0; i < len(fields1); i++ {
		if !compareNestedFieldMaps(fields1[i], fields2[i]) {
			return false
		}
	}

	return true
}

// compareNestedFieldMaps compares two nested field maps
// 2つのネストされたフィールドマップを比較
func compareNestedFieldMaps(field1, field2 map[string]interface{}) bool {
	// IDの比較
	id1 := getIntValueForRoundtrip(field1["id"])
	id2 := getIntValueForRoundtrip(field2["id"])
	if id1 != id2 {
		return false
	}

	// nameの比較
	if field1["name"] != field2["name"] {
		return false
	}

	// requiredの比較
	required1, _ := field1["required"].(bool)
	required2, _ := field2["required"].(bool)
	if required1 != required2 {
		return false
	}

	// typeの比較
	if field1["type"] != field2["type"] {
		return false
	}

	return true
}

// getIntValueForRoundtrip extracts an int value from interface{} (handles both int and float64)
// interface{}からint値を抽出（intとfloat64の両方に対応）
func getIntValueForRoundtrip(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		return 0
	}
}
