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

// TestCalculateLastColumnID_PrimitiveTypes tests calculateLastColumnID with primitive types only
// プリミティブ型のみのスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_PrimitiveTypes(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "name",
				Required: true,
				Type:     "string",
			},
			{
				ID:       5,
				Name:     "timestamp",
				Required: true,
				Type:     "timestamptz",
			},
			{
				ID:       3,
				Name:     "value",
				Required: false,
				Type:     "double",
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは5であることを確認
	if maxID != 5 {
		t.Errorf("Expected maxID 5, got %d", maxID)
	}
}

// TestCalculateLastColumnID_EmptySchema tests calculateLastColumnID with empty schema
// 空のスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_EmptySchema(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields:   []IcebergSchemaField{},
	}

	maxID := calculateLastColumnID(schema)

	// 空のスキーマの場合、maxIDは0であることを確認
	if maxID != 0 {
		t.Errorf("Expected maxID 0 for empty schema, got %d", maxID)
	}
}

// TestCalculateLastColumnID_MapType tests calculateLastColumnID with map type
// map型を含むスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_MapType(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":     "map",
					"key-id":   10,
					"key":      "string",
					"value-id": 11,
					"value":    "string",
					"value-required": false,
				},
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは11（map型のvalue-id）であることを確認
	if maxID != 11 {
		t.Errorf("Expected maxID 11, got %d", maxID)
	}
}

// TestCalculateLastColumnID_ListType tests calculateLastColumnID with list type
// list型を含むスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_ListType(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "tags",
				Required: false,
				Type: map[string]interface{}{
					"type":             "list",
					"element-id":       15,
					"element":          "string",
					"element-required": false,
				},
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは15（list型のelement-id）であることを確認
	if maxID != 15 {
		t.Errorf("Expected maxID 15, got %d", maxID)
	}
}

// TestCalculateLastColumnID_StructType tests calculateLastColumnID with struct type
// struct型を含むスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_StructType(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "location",
				Required: false,
				Type: map[string]interface{}{
					"type": "struct",
					"fields": []map[string]interface{}{
						{
							"id":       20,
							"name":     "latitude",
							"required": true,
							"type":     "double",
						},
						{
							"id":       21,
							"name":     "longitude",
							"required": true,
							"type":     "double",
						},
					},
				},
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは21（struct型のネストされたフィールド）であることを確認
	if maxID != 21 {
		t.Errorf("Expected maxID 21, got %d", maxID)
	}
}

// TestCalculateLastColumnID_ComplexNested tests calculateLastColumnID with complex nested types
// 複雑なネストされた型を含むスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_ComplexNested(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":     "map",
					"key-id":   10,
					"key":      "string",
					"value-id": 11,
					"value":    "string",
					"value-required": false,
				},
			},
			{
				ID:       3,
				Name:     "tags",
				Required: false,
				Type: map[string]interface{}{
					"type":             "list",
					"element-id":       15,
					"element":          "string",
					"element-required": false,
				},
			},
			{
				ID:       4,
				Name:     "location",
				Required: false,
				Type: map[string]interface{}{
					"type": "struct",
					"fields": []map[string]interface{}{
						{
							"id":       25,
							"name":     "latitude",
							"required": true,
							"type":     "double",
						},
						{
							"id":       26,
							"name":     "longitude",
							"required": true,
							"type":     "double",
						},
					},
				},
			},
			{
				ID:       5,
				Name:     "timestamp",
				Required: true,
				Type:     "timestamptz",
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは26（struct型のネストされたフィールド）であることを確認
	if maxID != 26 {
		t.Errorf("Expected maxID 26, got %d", maxID)
	}
}

// TestCalculateLastColumnID_SingleField tests calculateLastColumnID with single field
// 単一フィールドのスキーマでcalculateLastColumnIDをテスト
func TestCalculateLastColumnID_SingleField(t *testing.T) {
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       100,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
		},
	}

	maxID := calculateLastColumnID(schema)

	// 最大IDは100であることを確認
	if maxID != 100 {
		t.Errorf("Expected maxID 100, got %d", maxID)
	}
}
