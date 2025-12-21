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
)

// IcebergTypeSerializer は個々のIceberg型をシリアライズするためのインターフェース
// 各型（プリミティブ、map、list、struct）に対して、このインターフェースを実装する
type IcebergTypeSerializer interface {
	// Serialize は型をJSON互換の構造に変換する
	// プリミティブ型の場合は文字列を返し、複雑型の場合は構造化オブジェクトを返す
	Serialize() (interface{}, error)

	// Validate は型定義の妥当性を検証する
	// 例: map型のkey-idとvalue-idが有効かどうかをチェック
	Validate() error
}


// PrimitiveTypeSerializer はプリミティブ型（string、int、long、double、timestamptz、binary等）のシリアライザー
type PrimitiveTypeSerializer struct {
	typeName string
}

// NewPrimitiveTypeSerializer は新しいPrimitiveTypeSerializerを作成する
func NewPrimitiveTypeSerializer(typeName string) *PrimitiveTypeSerializer {
	return &PrimitiveTypeSerializer{
		typeName: typeName,
	}
}

// Serialize はプリミティブ型名を文字列として返す
func (s *PrimitiveTypeSerializer) Serialize() (interface{}, error) {
	return s.typeName, nil
}

// Validate はプリミティブ型名が空でないことを検証する
func (s *PrimitiveTypeSerializer) Validate() error {
	if s.typeName == "" {
		return &SchemaValidationError{
			Field:  "type",
			Reason: "primitive type name cannot be empty",
		}
	}
	return nil
}


// MapTypeSerializer はmap型のシリアライザー
type MapTypeSerializer struct {
	keyID         int
	key           interface{} // string（プリミティブ型）またはIcebergTypeSerializer（複雑型）
	valueID       int
	value         interface{} // string（プリミティブ型）またはIcebergTypeSerializer（複雑型）
	valueRequired bool
}

// NewMapTypeSerializer は新しいMapTypeSerializerを作成する
func NewMapTypeSerializer(keyID int, key interface{}, valueID int, value interface{}, valueRequired bool) *MapTypeSerializer {
	return &MapTypeSerializer{
		keyID:         keyID,
		key:           key,
		valueID:       valueID,
		value:         value,
		valueRequired: valueRequired,
	}
}

// Serialize はmap型を構造化オブジェクトとして返す
// 返される構造: {"type":"map","key-id":N,"key":"...","value-id":M,"value":"...","value-required":bool}
func (s *MapTypeSerializer) Serialize() (interface{}, error) {
	// keyのシリアライズ
	var serializedKey interface{}
	if keyStr, ok := s.key.(string); ok {
		// プリミティブ型の場合
		serializedKey = keyStr
	} else if keySerializer, ok := s.key.(IcebergTypeSerializer); ok {
		// 複雑型の場合
		var err error
		serializedKey, err = keySerializer.Serialize()
		if err != nil {
			return nil, &SerializationError{
				Operation: "serialize map key",
				Cause:     err,
			}
		}
	} else {
		return nil, &SerializationError{
			Operation: "serialize map key",
			Cause:     fmt.Errorf("unsupported key type: %T", s.key),
		}
	}

	// valueのシリアライズ
	var serializedValue interface{}
	if valueStr, ok := s.value.(string); ok {
		// プリミティブ型の場合
		serializedValue = valueStr
	} else if valueSerializer, ok := s.value.(IcebergTypeSerializer); ok {
		// 複雑型の場合
		var err error
		serializedValue, err = valueSerializer.Serialize()
		if err != nil {
			return nil, &SerializationError{
				Operation: "serialize map value",
				Cause:     err,
			}
		}
	} else {
		return nil, &SerializationError{
			Operation: "serialize map value",
			Cause:     fmt.Errorf("unsupported value type: %T", s.value),
		}
	}

	// 構造化オブジェクトを返す
	return map[string]interface{}{
		"type":           "map",
		"key-id":         s.keyID,
		"key":            serializedKey,
		"value-id":       s.valueID,
		"value":          serializedValue,
		"value-required": s.valueRequired,
	}, nil
}

// Validate はmap型定義の妥当性を検証する
func (s *MapTypeSerializer) Validate() error {
	// key-idの検証
	if s.keyID <= 0 {
		return &SchemaValidationError{
			Field:  "key-id",
			Reason: fmt.Sprintf("key-id must be positive, got %d", s.keyID),
		}
	}

	// value-idの検証
	if s.valueID <= 0 {
		return &SchemaValidationError{
			Field:  "value-id",
			Reason: fmt.Sprintf("value-id must be positive, got %d", s.valueID),
		}
	}

	// key-idとvalue-idが異なることを検証
	if s.keyID == s.valueID {
		return &SchemaValidationError{
			Field:  "map",
			Reason: fmt.Sprintf("key-id and value-id must be different, both are %d", s.keyID),
		}
	}

	// keyの検証
	if s.key == nil {
		return &SchemaValidationError{
			Field:  "key",
			Reason: "key cannot be nil",
		}
	}
	if keySerializer, ok := s.key.(IcebergTypeSerializer); ok {
		if err := keySerializer.Validate(); err != nil {
			return err
		}
	}

	// valueの検証
	if s.value == nil {
		return &SchemaValidationError{
			Field:  "value",
			Reason: "value cannot be nil",
		}
	}
	if valueSerializer, ok := s.value.(IcebergTypeSerializer); ok {
		if err := valueSerializer.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ListTypeSerializer はlist型のシリアライザー
type ListTypeSerializer struct {
	elementID       int
	element         interface{} // string（プリミティブ型）またはIcebergTypeSerializer（複雑型）
	elementRequired bool
}

// NewListTypeSerializer は新しいListTypeSerializerを作成する
func NewListTypeSerializer(elementID int, element interface{}, elementRequired bool) *ListTypeSerializer {
	return &ListTypeSerializer{
		elementID:       elementID,
		element:         element,
		elementRequired: elementRequired,
	}
}

// Serialize はlist型を構造化オブジェクトとして返す
// 返される構造: {"type":"list","element-id":N,"element":"...","element-required":bool}
func (s *ListTypeSerializer) Serialize() (interface{}, error) {
	// elementのシリアライズ
	var serializedElement interface{}
	if elementStr, ok := s.element.(string); ok {
		// プリミティブ型の場合
		serializedElement = elementStr
	} else if elementSerializer, ok := s.element.(IcebergTypeSerializer); ok {
		// 複雑型の場合
		var err error
		serializedElement, err = elementSerializer.Serialize()
		if err != nil {
			return nil, &SerializationError{
				Operation: "serialize list element",
				Cause:     err,
			}
		}
	} else {
		return nil, &SerializationError{
			Operation: "serialize list element",
			Cause:     fmt.Errorf("unsupported element type: %T", s.element),
		}
	}

	// 構造化オブジェクトを返す
	return map[string]interface{}{
		"type":             "list",
		"element-id":       s.elementID,
		"element":          serializedElement,
		"element-required": s.elementRequired,
	}, nil
}

// Validate はlist型定義の妥当性を検証する
func (s *ListTypeSerializer) Validate() error {
	// element-idの検証
	if s.elementID <= 0 {
		return &SchemaValidationError{
			Field:  "element-id",
			Reason: fmt.Sprintf("element-id must be positive, got %d", s.elementID),
		}
	}

	// elementの検証
	if s.element == nil {
		return &SchemaValidationError{
			Field:  "element",
			Reason: "element cannot be nil",
		}
	}
	if elementSerializer, ok := s.element.(IcebergTypeSerializer); ok {
		if err := elementSerializer.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// StructTypeSerializer はstruct型のシリアライザー
type StructTypeSerializer struct {
	fields []IcebergSchemaField
}

// NewStructTypeSerializer は新しいStructTypeSerializerを作成する
func NewStructTypeSerializer(fields []IcebergSchemaField) *StructTypeSerializer {
	return &StructTypeSerializer{
		fields: fields,
	}
}

// Serialize はstruct型を構造化オブジェクトとして返す
// 返される構造: {"type":"struct","fields":[...]}
func (s *StructTypeSerializer) Serialize() (interface{}, error) {
	// フィールドをシリアライズ
	serializedFields := make([]map[string]interface{}, 0, len(s.fields))
	for _, field := range s.fields {
		// フィールドの型をシリアライズ
		var serializedType interface{}
		if field.IsPrimitiveType() {
			// プリミティブ型の場合
			primitiveType, _ := field.GetPrimitiveType()
			serializedType = primitiveType
		} else {
			// 複雑型の場合、再帰的にシリアライズ
			typeSerializer, err := field.GetTypeSerializer()
			if err != nil {
				return nil, &SerializationError{
					Operation: fmt.Sprintf("get type serializer for field %s", field.Name),
					Cause:     err,
				}
			}
			serializedType, err = typeSerializer.Serialize()
			if err != nil {
				return nil, &SerializationError{
					Operation: fmt.Sprintf("serialize field %s", field.Name),
					Cause:     err,
				}
			}
		}

		// フィールドを構造化オブジェクトとして追加
		serializedFields = append(serializedFields, map[string]interface{}{
			"id":       field.ID,
			"name":     field.Name,
			"required": field.Required,
			"type":     serializedType,
		})
	}

	// 構造化オブジェクトを返す
	return map[string]interface{}{
		"type":   "struct",
		"fields": serializedFields,
	}, nil
}

// Validate はstruct型定義の妥当性を検証する
func (s *StructTypeSerializer) Validate() error {
	// フィールドが空でないことを検証
	if len(s.fields) == 0 {
		return &SchemaValidationError{
			Field:  "fields",
			Reason: "struct must have at least one field",
		}
	}

	// 各フィールドの検証
	fieldIDs := make(map[int]bool)
	for _, field := range s.fields {
		// フィールドIDの一意性チェック
		if fieldIDs[field.ID] {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: fmt.Sprintf("duplicate field ID: %d", field.ID),
			}
		}
		fieldIDs[field.ID] = true

		// フィールド名が空でないことを検証
		if field.Name == "" {
			return &SchemaValidationError{
				Field:  "name",
				Reason: "field name cannot be empty",
			}
		}

		// フィールドIDが正の値であることを検証
		if field.ID <= 0 {
			return &SchemaValidationError{
				Field:  field.Name,
				Reason: fmt.Sprintf("field ID must be positive, got %d", field.ID),
			}
		}

		// フィールドの型を検証
		if !field.IsPrimitiveType() {
			typeSerializer, err := field.GetTypeSerializer()
			if err != nil {
				return &SchemaValidationError{
					Field:  field.Name,
					Reason: fmt.Sprintf("failed to get type serializer: %v", err),
				}
			}
			if err := typeSerializer.Validate(); err != nil {
				return err
			}
		}
	}

	return nil
}


// SchemaValidationError はスキーマ検証エラーを表す
type SchemaValidationError struct {
	Field  string
	Reason string
}

// Error はエラーメッセージを返す
func (e *SchemaValidationError) Error() string {
	return fmt.Sprintf("schema validation failed for field %s: %s", e.Field, e.Reason)
}

// SerializationError はシリアライゼーションエラーを表す
type SerializationError struct {
	Operation string
	Cause     error
}

// Error はエラーメッセージを返す
func (e *SerializationError) Error() string {
	return fmt.Sprintf("serialization failed during %s: %v", e.Operation, e.Cause)
}
