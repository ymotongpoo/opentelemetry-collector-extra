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
)

// IcebergSchemaSerializer はIcebergスキーマを様々な形式にシリアライズするための中心的なコンポーネント
// スキーマ全体の検証とシリアライゼーションを担当する
type IcebergSchemaSerializer struct {
	schema *IcebergSchema
}

// NewIcebergSchemaSerializer は新しいIcebergSchemaSerializerを作成する
// schema: シリアライズ対象のIcebergスキーマ
func NewIcebergSchemaSerializer(schema *IcebergSchema) *IcebergSchemaSerializer {
	return &IcebergSchemaSerializer{
		schema: schema,
	}
}

// Validate はスキーマの妥当性を検証する
// 以下の検証を行う：
// - フィールドIDの一意性チェック
// - map型のkey-id、value-idの一意性チェック
// - list型のelement-idの一意性チェック
// - 必須フィールド（name、type、id）の存在チェック
func (s *IcebergSchemaSerializer) Validate() error {
	// すべてのIDを追跡するためのマップ
	idSet := make(map[int]string) // ID -> フィールド名のマッピング

	// 各フィールドを検証
	for _, field := range s.schema.Fields {
		// 必須フィールドの存在チェック
		if field.Name == "" {
			return &SchemaValidationError{
				Field:  "name",
				Reason: "field name cannot be empty",
			}
		}

		if field.ID <= 0 {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "field ID must be positive",
			}
		}

		if field.Type == nil {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "field type cannot be nil",
			}
		}

		// フィールドIDの一意性チェック
		if existingField, exists := idSet[field.ID]; exists {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "duplicate field ID " + string(rune(field.ID)) + " (already used by field " + existingField + ")",
			}
		}
		idSet[field.ID] = field.Name

		// 型に応じた追加の検証
		if field.IsMapType() {
			// map型の検証
			if err := s.validateMapType(&field, idSet); err != nil {
				return err
			}
		} else if field.IsListType() {
			// list型の検証
			if err := s.validateListType(&field, idSet); err != nil {
				return err
			}
		} else if field.IsStructType() {
			// struct型の検証
			if err := s.validateStructType(&field, idSet); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateMapType はmap型フィールドの妥当性を検証する
func (s *IcebergSchemaSerializer) validateMapType(field *IcebergSchemaField, idSet map[int]string) error {
	mapType, err := field.GetMapType()
	if err != nil {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "failed to get map type: " + err.Error(),
		}
	}

	// key-idの取得と検証
	keyID, ok := mapType["key-id"].(int)
	if !ok {
		if keyIDFloat, ok := mapType["key-id"].(float64); ok {
			keyID = int(keyIDFloat)
		} else {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "map type missing or invalid 'key-id'",
			}
		}
	}

	if keyID <= 0 {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "map key-id must be positive",
		}
	}

	// key-idの一意性チェック
	if existingField, exists := idSet[keyID]; exists {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "duplicate key-id " + string(rune(keyID)) + " (already used by field " + existingField + ")",
		}
	}
	idSet[keyID] = field.Name + ".key"

	// value-idの取得と検証
	valueID, ok := mapType["value-id"].(int)
	if !ok {
		if valueIDFloat, ok := mapType["value-id"].(float64); ok {
			valueID = int(valueIDFloat)
		} else {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "map type missing or invalid 'value-id'",
			}
		}
	}

	if valueID <= 0 {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "map value-id must be positive",
		}
	}

	// value-idの一意性チェック
	if existingField, exists := idSet[valueID]; exists {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "duplicate value-id " + string(rune(valueID)) + " (already used by field " + existingField + ")",
		}
	}
	idSet[valueID] = field.Name + ".value"

	// keyとvalueの存在チェック
	if _, ok := mapType["key"]; !ok {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "map type missing 'key'",
		}
	}

	if _, ok := mapType["value"]; !ok {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "map type missing 'value'",
		}
	}

	return nil
}

// validateListType はlist型フィールドの妥当性を検証する
func (s *IcebergSchemaSerializer) validateListType(field *IcebergSchemaField, idSet map[int]string) error {
	listType, err := field.GetListType()
	if err != nil {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "failed to get list type: " + err.Error(),
		}
	}

	// element-idの取得と検証
	elementID, ok := listType["element-id"].(int)
	if !ok {
		if elementIDFloat, ok := listType["element-id"].(float64); ok {
			elementID = int(elementIDFloat)
		} else {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "list type missing or invalid 'element-id'",
			}
		}
	}

	if elementID <= 0 {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "list element-id must be positive",
		}
	}

	// element-idの一意性チェック
	if existingField, exists := idSet[elementID]; exists {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "duplicate element-id " + string(rune(elementID)) + " (already used by field " + existingField + ")",
		}
	}
	idSet[elementID] = field.Name + ".element"

	// elementの存在チェック
	if _, ok := listType["element"]; !ok {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "list type missing 'element'",
		}
	}

	return nil
}

// validateStructType はstruct型フィールドの妥当性を検証する
func (s *IcebergSchemaSerializer) validateStructType(field *IcebergSchemaField, idSet map[int]string) error {
	structType, err := field.GetStructType()
	if err != nil {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "failed to get struct type: " + err.Error(),
		}
	}

	// fieldsの存在チェック
	fieldsInterface, ok := structType["fields"]
	if !ok {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "struct type missing 'fields'",
		}
	}

	// fieldsを[]map[string]interface{}に変換
	fieldsSlice, ok := fieldsInterface.([]map[string]interface{})
	if !ok {
		return &SchemaValidationError{
			Field:  field.Name,
			Reason: "struct 'fields' is not a valid array",
		}
	}

	// 各ネストされたフィールドを検証
	for _, fieldMap := range fieldsSlice {
		// IDの取得と検証
		var nestedID int
		if id, ok := fieldMap["id"].(int); ok {
			nestedID = id
		} else if idFloat, ok := fieldMap["id"].(float64); ok {
			nestedID = int(idFloat)
		} else {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "nested field missing or invalid 'id'",
			}
		}

		if nestedID <= 0 {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "nested field ID must be positive",
			}
		}

		// IDの一意性チェック
		if existingField, exists := idSet[nestedID]; exists {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "duplicate nested field ID " + string(rune(nestedID)) + " (already used by field " + existingField + ")",
			}
		}

		// nameの取得
		nestedName, ok := fieldMap["name"].(string)
		if !ok || nestedName == "" {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: "nested field missing or invalid 'name'",
			}
		}

		idSet[nestedID] = field.Name + "." + nestedName

		// typeの存在チェック
		if _, ok := fieldMap["type"]; !ok {
			return &SchemaValidationError{
				Field:  field.Name + "." + nestedName,
				Reason: "nested field missing 'type'",
			}
		}
	}

	return nil
}

// SerializeForS3Tables はスキーマをS3 Tables API形式にシリアライズする
// 各フィールドに対してGetTypeSerializer()を呼び出し、構造化オブジェクトを返す
// 返される構造はJSON marshalingが可能
func (s *IcebergSchemaSerializer) SerializeForS3Tables() (interface{}, error) {
	// スキーマの検証
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// フィールドをシリアライズ
	serializedFields := make([]map[string]interface{}, 0, len(s.schema.Fields))

	for _, field := range s.schema.Fields {
		// フィールドの型をシリアライズ
		var serializedType interface{}

		if field.IsPrimitiveType() {
			// プリミティブ型の場合、文字列として返す
			primitiveType, err := field.GetPrimitiveType()
			if err != nil {
				return nil, &SerializationError{
					Operation: "get primitive type for field " + field.Name,
					Cause:     err,
				}
			}
			serializedType = primitiveType
		} else {
			// 複雑型の場合、TypeSerializerを使用
			typeSerializer, err := field.GetTypeSerializer()
			if err != nil {
				return nil, &SerializationError{
					Operation: "get type serializer for field " + field.Name,
					Cause:     err,
				}
			}

			// 型を検証
			if err := typeSerializer.Validate(); err != nil {
				return nil, err
			}

			// 型をシリアライズ
			serializedType, err = typeSerializer.Serialize()
			if err != nil {
				return nil, &SerializationError{
					Operation: "serialize field " + field.Name,
					Cause:     err,
				}
			}
		}

		// フィールドを構造化オブジェクトとして追加
		serializedField := map[string]interface{}{
			"id":       field.ID,
			"name":     field.Name,
			"required": field.Required,
			"type":     serializedType,
		}

		serializedFields = append(serializedFields, serializedField)
	}

	// スキーマ全体を構造化オブジェクトとして返す
	return map[string]interface{}{
		"type":   "struct",
		"fields": serializedFields,
	}, nil
}

// SerializeToJSON はSerializeForS3Tables()の結果をJSON文字列に変換する
// デバッグログ用に使用される
func (s *IcebergSchemaSerializer) SerializeToJSON() (string, error) {
	// スキーマをシリアライズ
	serialized, err := s.SerializeForS3Tables()
	if err != nil {
		return "", err
	}

	// JSON文字列に変換
	jsonBytes, err := json.MarshalIndent(serialized, "", "  ")
	if err != nil {
		return "", &SerializationError{
			Operation: "marshal schema to JSON",
			Cause:     err,
		}
	}

	return string(jsonBytes), nil
}
