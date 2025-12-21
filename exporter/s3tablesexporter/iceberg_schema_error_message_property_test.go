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
	"strings"
	"testing"
	"testing/quick"
)

// InvalidSchemaGenerator はランダムな無効なIcebergスキーマを生成する
// 様々な種類の検証エラーを含むスキーマを生成する
type InvalidSchemaGenerator struct {
	ErrorType int // 0: 重複ID, 1: 空のフィールド名, 2: 負のID, 3: nil型, 4: 無効なmap型
}

// Generate はランダムなInvalidSchemaGeneratorを生成する
func (InvalidSchemaGenerator) Generate(r *rand.Rand, size int) reflect.Value {
	// エラータイプをランダムに選択
	errorType := r.Intn(5)

	return reflect.ValueOf(InvalidSchemaGenerator{
		ErrorType: errorType,
	})
}

// GenerateInvalidSchema はランダムな無効なIcebergスキーマを生成する
// ErrorTypeに応じて異なる種類の検証エラーを含むスキーマを生成する
func (g InvalidSchemaGenerator) GenerateInvalidSchema(r *rand.Rand) *IcebergSchema {
	switch g.ErrorType {
	case 0:
		// 重複するフィールドIDを持つスキーマ
		duplicateID := r.Intn(100) + 1
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       duplicateID,
					Name:     "field1",
					Required: false,
					Type:     "string",
				},
				{
					ID:       duplicateID, // 重複
					Name:     "field2",
					Required: false,
					Type:     "int",
				},
			},
		}

	case 1:
		// 空のフィールド名を持つスキーマ
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     "", // 空の名前
					Required: false,
					Type:     "string",
				},
			},
		}

	case 2:
		// 負のフィールドIDを持つスキーマ
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       -1, // 負のID
					Name:     "field1",
					Required: false,
					Type:     "string",
				},
			},
		}

	case 3:
		// nil型を持つスキーマ
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     "field1",
					Required: false,
					Type:     nil, // nil型
				},
			},
		}

	case 4:
		// 無効なmap型（重複するkey-idとvalue-id）を持つスキーマ
		duplicateID := r.Intn(100) + 2
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     "field1",
					Required: false,
					Type: map[string]interface{}{
						"type":           "map",
						"key-id":         duplicateID,
						"key":            "string",
						"value-id":       duplicateID, // key-idと同じ
						"value":          "string",
						"value-required": false,
					},
				},
			},
		}

	default:
		// デフォルトは重複IDのケース
		return &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     "field1",
					Required: false,
					Type:     "string",
				},
				{
					ID:       1, // 重複
					Name:     "field2",
					Required: false,
					Type:     "int",
				},
			},
		}
	}
}

// Feature: iceberg-schema-serialization-fix, Property 7: エラーメッセージの明確性
// すべての無効なスキーマまたは未サポートの型に対して、Schema_Serializerは
// 失敗の理由を説明する非空のエラーメッセージを返さなければならない
// 検証: 要件 6.1, 6.4
func TestProperty_ErrorMessageClarity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(invalidGen InvalidSchemaGenerator) bool {
		// ランダムな無効なスキーマを生成
		r := rand.New(rand.NewSource(rand.Int63()))
		schema := invalidGen.GenerateInvalidSchema(r)

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// 検証を実行（エラーが返されることを期待）
		err := serializer.Validate()

		// エラーが返されることを確認
		if err == nil {
			t.Logf("Expected validation error for error type %d, but got nil", invalidGen.ErrorType)
			return false
		}

		// エラーメッセージが非空であることを確認
		errMsg := err.Error()
		if errMsg == "" {
			t.Logf("Error message is empty for error type %d", invalidGen.ErrorType)
			return false
		}

		// エラーメッセージが意味のある情報を含むことを確認
		// 最低限、"validation"または"field"という単語を含むべき
		if !strings.Contains(strings.ToLower(errMsg), "validation") &&
			!strings.Contains(strings.ToLower(errMsg), "field") {
			t.Logf("Error message lacks meaningful information: %s", errMsg)
			return false
		}

		// エラーの種類に応じて、適切なキーワードが含まれることを確認
		switch invalidGen.ErrorType {
		case 0:
			// 重複IDエラーの場合、"duplicate"が含まれるべき
			if !strings.Contains(strings.ToLower(errMsg), "duplicate") {
				t.Logf("Duplicate ID error message should contain 'duplicate': %s", errMsg)
				return false
			}

		case 1:
			// 空のフィールド名エラーの場合、"empty"または"cannot be empty"が含まれるべき
			if !strings.Contains(strings.ToLower(errMsg), "empty") {
				t.Logf("Empty field name error message should contain 'empty': %s", errMsg)
				return false
			}

		case 2:
			// 負のIDエラーの場合、"positive"が含まれるべき
			if !strings.Contains(strings.ToLower(errMsg), "positive") {
				t.Logf("Negative ID error message should contain 'positive': %s", errMsg)
				return false
			}

		case 3:
			// nil型エラーの場合、"nil"または"cannot be nil"が含まれるべき
			if !strings.Contains(strings.ToLower(errMsg), "nil") {
				t.Logf("Nil type error message should contain 'nil': %s", errMsg)
				return false
			}

		case 4:
			// 無効なmap型エラーの場合、"duplicate"または"different"が含まれるべき
			if !strings.Contains(strings.ToLower(errMsg), "duplicate") &&
				!strings.Contains(strings.ToLower(errMsg), "different") {
				t.Logf("Invalid map type error message should contain 'duplicate' or 'different': %s", errMsg)
				return false
			}
		}

		// SchemaValidationErrorまたはSerializationError型であることを確認
		switch err.(type) {
		case *SchemaValidationError, *SerializationError:
			// 正しいエラー型
		default:
			t.Logf("Error is not of expected type (SchemaValidationError or SerializationError): %T", err)
			return false
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}

// Feature: iceberg-schema-serialization-fix, Property 7: エラーメッセージの明確性（シリアライゼーション）
// すべての無効なスキーマに対して、SerializeForS3Tablesは
// 失敗の理由を説明する非空のエラーメッセージを返さなければならない
// 検証: 要件 6.1, 6.4
func TestProperty_SerializationErrorMessageClarity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(invalidGen InvalidSchemaGenerator) bool {
		// ランダムな無効なスキーマを生成
		r := rand.New(rand.NewSource(rand.Int63()))
		schema := invalidGen.GenerateInvalidSchema(r)

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// シリアライズを実行（エラーが返されることを期待）
		_, err := serializer.SerializeForS3Tables()

		// エラーが返されることを確認
		if err == nil {
			t.Logf("Expected serialization error for error type %d, but got nil", invalidGen.ErrorType)
			return false
		}

		// エラーメッセージが非空であることを確認
		errMsg := err.Error()
		if errMsg == "" {
			t.Logf("Error message is empty for error type %d", invalidGen.ErrorType)
			return false
		}

		// エラーメッセージが意味のある情報を含むことを確認
		// 最低限、"validation"、"serialization"、または"field"という単語を含むべき
		if !strings.Contains(strings.ToLower(errMsg), "validation") &&
			!strings.Contains(strings.ToLower(errMsg), "serialization") &&
			!strings.Contains(strings.ToLower(errMsg), "field") {
			t.Logf("Error message lacks meaningful information: %s", errMsg)
			return false
		}

		return true
	}

	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}

// Feature: iceberg-schema-serialization-fix, Property 7: エラーメッセージの明確性（未サポート型）
// 未サポートの型を含むスキーマに対して、明確なエラーメッセージを返さなければならない
// 検証: 要件 6.1, 6.4
func TestProperty_UnsupportedTypeErrorMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))

		// 未サポートの型を持つスキーマを生成
		// 例: 数値型、配列型、その他の予期しない型
		unsupportedTypes := []interface{}{
			123,                    // 数値
			[]string{"a", "b"},     // 配列
			struct{ X int }{X: 1},  // 構造体
			true,                   // ブール値
		}

		unsupportedType := unsupportedTypes[r.Intn(len(unsupportedTypes))]

		schema := &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     "field1",
					Required: false,
					Type:     unsupportedType,
				},
			},
		}

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// シリアライズを実行（エラーが返されることを期待）
		_, err := serializer.SerializeForS3Tables()

		// エラーが返されることを確認
		if err == nil {
			t.Logf("Expected error for unsupported type %T, but got nil", unsupportedType)
			return false
		}

		// エラーメッセージが非空であることを確認
		errMsg := err.Error()
		if errMsg == "" {
			t.Logf("Error message is empty for unsupported type %T", unsupportedType)
			return false
		}

		// エラーメッセージが"unsupported"または"invalid"を含むことを確認
		if !strings.Contains(strings.ToLower(errMsg), "unsupported") &&
			!strings.Contains(strings.ToLower(errMsg), "invalid") &&
			!strings.Contains(strings.ToLower(errMsg), "failed") {
			t.Logf("Error message should indicate unsupported/invalid type: %s", errMsg)
			return false
		}

		return true
	}

	// int64をシードとして使用するためのラッパー
	propertyWrapper := func(seed int64) bool {
		return property(seed)
	}

	if err := quick.Check(propertyWrapper, config); err != nil {
		t.Error(err)
	}
}

// Feature: iceberg-schema-serialization-fix, Property 7: エラーメッセージの明確性（フィールド情報）
// エラーメッセージには、どのフィールドで問題が発生したかの情報が含まれなければならない
// 検証: 要件 6.1, 6.4
func TestProperty_ErrorMessageContainsFieldInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	config := &quick.Config{MaxCount: 100}

	property := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))

		// ランダムなフィールド名を生成
		fieldName := fmt.Sprintf("test_field_%d", r.Intn(1000))

		// 無効なスキーマを生成（空の型）
		schema := &IcebergSchema{
			SchemaID: 0,
			Fields: []IcebergSchemaField{
				{
					ID:       1,
					Name:     fieldName,
					Required: false,
					Type:     nil, // nil型
				},
			},
		}

		// IcebergSchemaSerializerを作成
		serializer := NewIcebergSchemaSerializer(schema)

		// 検証を実行
		err := serializer.Validate()

		// エラーが返されることを確認
		if err == nil {
			t.Logf("Expected validation error, but got nil")
			return false
		}

		// エラーメッセージにフィールド名が含まれることを確認
		errMsg := err.Error()
		if !strings.Contains(errMsg, fieldName) {
			t.Logf("Error message should contain field name '%s': %s", fieldName, errMsg)
			return false
		}

		// SchemaValidationError型の場合、Fieldプロパティが設定されていることを確認
		if validationErr, ok := err.(*SchemaValidationError); ok {
			if validationErr.Field == "" {
				t.Logf("SchemaValidationError.Field should not be empty")
				return false
			}
			if validationErr.Reason == "" {
				t.Logf("SchemaValidationError.Reason should not be empty")
				return false
			}
		}

		return true
	}

	// int64をシードとして使用するためのラッパー
	propertyWrapper := func(seed int64) bool {
		return property(seed)
	}

	if err := quick.Check(propertyWrapper, config); err != nil {
		t.Error(err)
	}
}
