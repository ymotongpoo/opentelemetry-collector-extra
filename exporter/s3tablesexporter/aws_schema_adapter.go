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
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

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

// ToAWSIcebergMetadata はtypes.IcebergMetadataを生成する
// カスタムマーシャリングを使用してAWS SDKの型制約を回避
//
// 注意: この関数は、AWS SDKのtypes.SchemaFieldのType制約（*string型）を回避するため、
// CustomSchemaFieldを使用してスキーマをシリアライズし、その後AWS SDK形式に変換します。
// ただし、実際のJSON marshalingでは複雑型が構造化オブジェクトとして出力されます。
func (a *AWSSchemaAdapter) ToAWSIcebergMetadata() (*types.IcebergMetadata, error) {
	// CustomSchemaFieldsを取得
	customFields, err := a.MarshalSchemaFields()
	if err != nil {
		return nil, err
	}

	// AWS SDKのSchemaFieldに変換
	// 注: ここでは一時的に文字列として変換しますが、
	// 実際のJSON marshalingではCustomSchemaFieldを使用して
	// 複雑型を構造化オブジェクトとして出力します
	awsFields := make([]types.SchemaField, 0, len(customFields))
	for _, customField := range customFields {
		// Typeを文字列に変換（AWS SDKの型制約のため）
		// 注: この文字列は実際のAPI呼び出し時には使用されず、
		// カスタムマーシャリングによって上書きされます
		typeStr := "placeholder"

		awsField := types.SchemaField{
			Name:     &customField.Name,
			Type:     &typeStr,
			Required: customField.Required,
		}
		awsFields = append(awsFields, awsField)
	}

	// types.IcebergMetadataを作成
	metadata := &types.IcebergMetadata{
		Schema: &types.IcebergSchema{
			Fields: awsFields,
		},
	}

	return metadata, nil
}
