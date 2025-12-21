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

// TestProperty_MapStructurePreservation tests map structure preservation property
// Feature: iceberg-schema-complex-types, Property 3: Map型の構造保持
// Validates: Requirements 2.1, 2.2, 2.3, 2.4
func TestProperty_MapStructurePreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のmap型のフィールドにおいて、
	// type, key-id, key, value-id, value, value-requiredのすべてのフィールドが保持されるべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなmap型フィールドを生成
		mapField := generateRandomMapField(i)

		// map型であることを確認
		if !mapField.IsMapType() {
			t.Errorf("iteration %d: field should be map type", i)
			continue
		}

		mapType, err := mapField.GetMapType()
		if err != nil {
			t.Errorf("iteration %d: failed to get map type: %v", i, err)
			continue
		}

		// 必須フィールド: type
		typeVal, ok := mapType["type"]
		if !ok {
			t.Errorf("iteration %d: type field is missing", i)
			continue
		}
		if typeVal != "map" {
			t.Errorf("iteration %d: type field should be 'map', got %v", i, typeVal)
		}

		// 必須フィールド: key-id
		keyID, ok := mapType["key-id"]
		if !ok {
			t.Errorf("iteration %d: key-id field is missing", i)
			continue
		}
		if _, ok := keyID.(int); !ok {
			t.Errorf("iteration %d: key-id should be int, got %T", i, keyID)
		}

		// 必須フィールド: key
		key, ok := mapType["key"]
		if !ok {
			t.Errorf("iteration %d: key field is missing", i)
			continue
		}
		if _, ok := key.(string); !ok {
			t.Errorf("iteration %d: key should be string, got %T", i, key)
		}

		// 必須フィールド: value-id
		valueID, ok := mapType["value-id"]
		if !ok {
			t.Errorf("iteration %d: value-id field is missing", i)
			continue
		}
		if _, ok := valueID.(int); !ok {
			t.Errorf("iteration %d: value-id should be int, got %T", i, valueID)
		}

		// 必須フィールド: value
		value, ok := mapType["value"]
		if !ok {
			t.Errorf("iteration %d: value field is missing", i)
			continue
		}
		if _, ok := value.(string); !ok {
			t.Errorf("iteration %d: value should be string, got %T", i, value)
		}

		// 必須フィールド: value-required
		valueRequired, ok := mapType["value-required"]
		if !ok {
			t.Errorf("iteration %d: value-required field is missing", i)
			continue
		}
		if _, ok := valueRequired.(bool); !ok {
			t.Errorf("iteration %d: value-required should be bool, got %T", i, valueRequired)
		}
	}
}
