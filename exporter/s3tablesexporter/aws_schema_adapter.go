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

// AWSSchemaAdapter はIcebergスキーマをAWS SDK形式に変換するアダプター層
// AWS SDKの型制約（types.SchemaField.Typeが*string型）を回避するために使用される
type AWSSchemaAdapter struct {
	serializer *IcebergSchemaSerializer
}

// NewAWSSchemaAdapter は新しいAWSSchemaAdapterを作成する
// schema: 変換対象のIcebergスキーマ
func NewAWSSchemaAdapter(schema *IcebergSchema) *AWSSchemaAdapter {
	return &AWSSchemaAdapter{
		serializer: NewIcebergSchemaSerializer(schema),
	}
}

// CustomSchemaField はAWS SDKのtypes.SchemaFieldの代替
// Typeフィールドをinterface{}型にすることで、複雑型（map、list、struct）をサポート
// カスタムJSON marshalerを実装してAWS SDKの型制約を回避
type CustomSchemaField struct {
	Name     string      `json:"name"`
	Type     interface{} `json:"type"` // string（プリミティブ型）またはmap[string]interface{}（複雑型）
	Required bool        `json:"required"`
}

// MarshalSchemaFields はIcebergSchemaSerializerを使用してスキーマをシリアライズし、
// 各フィールドをCustomSchemaFieldに変換する
// JSON marshaling可能な構造を返す
func (a *AWSSchemaAdapter) MarshalSchemaFields() ([]CustomSchemaField, error) {
	// スキーマをシリアライズ
	serialized, err := a.serializer.SerializeForS3Tables()
	if err != nil {
		return nil, err
	}

	// シリアライズ結果をmap[string]interface{}に変換
	schemaMap, ok := serialized.(map[string]interface{})
	if !ok {
		return nil, &SerializationError{
			Operation: "convert serialized schema to map",
			Cause:     nil,
		}
	}

	// fieldsを取得
	fieldsInterface, ok := schemaMap["fields"]
	if !ok {
		return nil, &SerializationError{
			Operation: "get fields from serialized schema",
			Cause:     nil,
		}
	}

	// fieldsを[]interface{}に変換
	fieldsSlice, ok := fieldsInterface.([]map[string]interface{})
	if !ok {
		return nil, &SerializationError{
			Operation: "convert fields to slice",
			Cause:     nil,
		}
	}

	// 各フィールドをCustomSchemaFieldに変換
	customFields := make([]CustomSchemaField, 0, len(fieldsSlice))
	for _, fieldMap := range fieldsSlice {
		// nameを取得
		name, ok := fieldMap["name"].(string)
		if !ok {
			return nil, &SerializationError{
				Operation: "get field name",
				Cause:     nil,
			}
		}

		// typeを取得（プリミティブ型の場合はstring、複雑型の場合はmap[string]interface{}）
		fieldType := fieldMap["type"]

		// requiredを取得
		required, ok := fieldMap["required"].(bool)
		if !ok {
			required = false
		}

		// CustomSchemaFieldを作成
		customField := CustomSchemaField{
			Name:     name,
			Type:     fieldType,
			Required: required,
		}

		customFields = append(customFields, customField)
	}

	return customFields, nil
}
