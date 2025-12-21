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

// TestProperty_TypeFieldPreservation tests type field preservation property
// Feature: iceberg-schema-complex-types, Property 1: Type フィールドの型保持
// Validates: Requirements 1.1, 1.2, 1.3
func TestProperty_TypeFieldPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のスキーマフィールド定義において、
	// プリミティブ型は文字列として、複合型は構造化オブジェクトとして保持されるべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// プリミティブ型のテスト
		primitiveField := generateRandomPrimitiveField(i)

		// プリミティブ型が文字列として保持されていることを確認
		if !primitiveField.IsPrimitiveType() {
			t.Errorf("iteration %d (primitive): field should be primitive type", i)
			continue
		}

		primitiveType, err := primitiveField.GetPrimitiveType()
		if err != nil {
			t.Errorf("iteration %d (primitive): failed to get primitive type: %v", i, err)
			continue
		}

		// 型が文字列であることを確認
		if primitiveType == "" {
			t.Errorf("iteration %d (primitive): primitive type should not be empty", i)
		}

		// 複合型（map）のテスト
		mapField := generateRandomMapField(i + 1000)

		// 複合型が構造化オブジェクトとして保持されていることを確認
		if !mapField.IsMapType() {
			t.Errorf("iteration %d (map): field should be map type", i)
			continue
		}

		mapType, err := mapField.GetMapType()
		if err != nil {
			t.Errorf("iteration %d (map): failed to get map type: %v", i, err)
			continue
		}

		// map型の必須フィールドが存在することを確認
		if mapType["type"] != "map" {
			t.Errorf("iteration %d (map): type field should be 'map', got %v", i, mapType["type"])
		}

		if _, ok := mapType["key-id"]; !ok {
			t.Errorf("iteration %d (map): key-id field is missing", i)
		}

		if _, ok := mapType["key"]; !ok {
			t.Errorf("iteration %d (map): key field is missing", i)
		}

		if _, ok := mapType["value-id"]; !ok {
			t.Errorf("iteration %d (map): value-id field is missing", i)
		}

		if _, ok := mapType["value"]; !ok {
			t.Errorf("iteration %d (map): value field is missing", i)
		}

		if _, ok := mapType["value-required"]; !ok {
			t.Errorf("iteration %d (map): value-required field is missing", i)
		}
	}
}
