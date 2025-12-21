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

// MapFieldGenerator はランダムなmap型フィールドを生成する
type MapFieldGenerator struct {
	ID            int
	Name          string
	Required      bool
	KeyID         int
	ValueID       int
	ValueRequired bool
}

// Generate はランダムなMapFieldGeneratorを生成する
func (MapFieldGenerator) Generate(r *rand.Rand, size int) reflect.Value {
	// IDは1から1000の範囲でランダムに生成
	id := r.Intn(1000) + 1
	keyID := id + 1
	valueID := id + 2

	// 名前はランダムな文字列
	name := fmt.Sprintf("field_%d", id)

	// requiredはランダムなブール値
	required := r.Intn(2) == 1
	valueRequired := r.Intn(2) == 1

	return reflect.ValueOf(MapFieldGenerator{
		ID:            id,
		Name:          name,
		Required:      required,
		KeyID:         keyID,
		ValueID:       valueID,
		ValueRequired: valueRequired,
	})
}

// Feature: iceberg-schema-serialization-fix, Property 1: Map型の構造化シリアライゼーション
// すべてのmap型フィールドに対して、シリアライズ結果のTypeフィールドは文字列ではなく、
// type、key-id、key、value-id、value、value-requiredプロパティを含む構造化オブジェクトでなければならない
// 検証: 要件 1.1, 1.2, 1.3
func TestProperty_MapTypeStructuredSerialization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(mapField MapFieldGenerator) bool {
		// MapTypeSerializerを作成
		serializer := NewMapTypeSerializer(
			mapField.KeyID,
			"string", // keyはプリミティブ型
			mapField.ValueID,
			"string", // valueはプリミティブ型
			mapField.ValueRequired,
		)

		// シリアライズを実行
		result, err := serializer.Serialize()
		if err != nil {
			t.Logf("Serialization failed: %v", err)
			return false
		}

		// 結果が構造化オブジェクト（map[string]interface{}）であることを確認
		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Logf("Result is not a map[string]interface{}, got %T", result)
			return false
		}

		// 必須プロパティの存在を確認
		requiredKeys := []string{"type", "key-id", "key", "value-id", "value", "value-required"}
		for _, key := range requiredKeys {
			if _, exists := resultMap[key]; !exists {
				t.Logf("Missing required key: %s", key)
				return false
			}
		}

		// typeが"map"であることを確認
		if resultMap["type"] != "map" {
			t.Logf("Expected type='map', got %v", resultMap["type"])
			return false
		}

		// key-idが正しいことを確認
		if resultMap["key-id"] != mapField.KeyID {
			t.Logf("Expected key-id=%d, got %v", mapField.KeyID, resultMap["key-id"])
			return false
		}

		// value-idが正しいことを確認
		if resultMap["value-id"] != mapField.ValueID {
			t.Logf("Expected value-id=%d, got %v", mapField.ValueID, resultMap["value-id"])
			return false
		}

		// value-requiredが正しいことを確認
		if resultMap["value-required"] != mapField.ValueRequired {
			t.Logf("Expected value-required=%v, got %v", mapField.ValueRequired, resultMap["value-required"])
			return false
		}

		// keyが文字列であることを確認（プリミティブ型の場合）
		if resultMap["key"] != "string" {
			t.Logf("Expected key='string', got %v", resultMap["key"])
			return false
		}

		// valueが文字列であることを確認（プリミティブ型の場合）
		if resultMap["value"] != "string" {
			t.Logf("Expected value='string', got %v", resultMap["value"])
			return false
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}


// ComplexTypeGenerator はランダムな複雑型（map、list、struct）を生成する
type ComplexTypeGenerator struct {
	TypeKind string // "map", "list", "struct"
	ID       int
	Name     string
	Required bool
}

// Generate はランダムなComplexTypeGeneratorを生成する
func (ComplexTypeGenerator) Generate(r *rand.Rand, size int) reflect.Value {
	// 型の種類をランダムに選択
	typeKinds := []string{"map", "list", "struct"}
	typeKind := typeKinds[r.Intn(len(typeKinds))]

	// IDは1から1000の範囲でランダムに生成
	id := r.Intn(1000) + 1

	// 名前はランダムな文字列
	name := fmt.Sprintf("field_%d", id)

	// requiredはランダムなブール値
	required := r.Intn(2) == 1

	return reflect.ValueOf(ComplexTypeGenerator{
		TypeKind: typeKind,
		ID:       id,
		Name:     name,
		Required: required,
	})
}

// Feature: iceberg-schema-serialization-fix, Property 2: 複雑型の構造化シリアライゼーション
// すべての複雑型フィールド（map、list、struct）に対して、シリアライズ結果のTypeフィールドは
// 文字列ではなく、型に応じた適切なプロパティを含む構造化オブジェクトでなければならない
// 検証: 要件 2.1, 2.2, 2.3
func TestProperty_ComplexTypeStructuredSerialization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(complexType ComplexTypeGenerator) bool {
		var serializer IcebergTypeSerializer
		var err error

		// 型の種類に応じてシリアライザーを作成
		switch complexType.TypeKind {
		case "map":
			serializer = NewMapTypeSerializer(
				complexType.ID+1,
				"string",
				complexType.ID+2,
				"string",
				false,
			)
		case "list":
			serializer = NewListTypeSerializer(
				complexType.ID+1,
				"string",
				false,
			)
		case "struct":
			// 簡単なstructを作成（1つのフィールドのみ）
			fields := []IcebergSchemaField{
				{
					ID:       complexType.ID + 1,
					Name:     "inner_field",
					Required: true,
					Type:     "string",
				},
			}
			serializer = NewStructTypeSerializer(fields)
		default:
			t.Logf("Unknown type kind: %s", complexType.TypeKind)
			return false
		}

		// シリアライズを実行
		result, err := serializer.Serialize()
		if err != nil {
			t.Logf("Serialization failed for %s: %v", complexType.TypeKind, err)
			return false
		}

		// 結果が構造化オブジェクト（map[string]interface{}）であることを確認
		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Logf("Result is not a map[string]interface{} for %s, got %T", complexType.TypeKind, result)
			return false
		}

		// typeフィールドが存在することを確認
		typeValue, exists := resultMap["type"]
		if !exists {
			t.Logf("Missing 'type' field for %s", complexType.TypeKind)
			return false
		}

		// typeフィールドが期待される値であることを確認
		if typeValue != complexType.TypeKind {
			t.Logf("Expected type='%s', got %v", complexType.TypeKind, typeValue)
			return false
		}

		// 型に応じた必須フィールドの存在を確認
		switch complexType.TypeKind {
		case "map":
			requiredKeys := []string{"key-id", "key", "value-id", "value", "value-required"}
			for _, key := range requiredKeys {
				if _, exists := resultMap[key]; !exists {
					t.Logf("Missing required key '%s' for map type", key)
					return false
				}
			}
		case "list":
			requiredKeys := []string{"element-id", "element", "element-required"}
			for _, key := range requiredKeys {
				if _, exists := resultMap[key]; !exists {
					t.Logf("Missing required key '%s' for list type", key)
					return false
				}
			}
		case "struct":
			if _, exists := resultMap["fields"]; !exists {
				t.Logf("Missing 'fields' key for struct type")
				return false
			}
			// fieldsが配列であることを確認
			fields, ok := resultMap["fields"].([]map[string]interface{})
			if !ok {
				t.Logf("'fields' is not an array for struct type")
				return false
			}
			// 少なくとも1つのフィールドが存在することを確認
			if len(fields) == 0 {
				t.Logf("struct has no fields")
				return false
			}
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
