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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_MetadataJSONSerializationAccuracy tests metadata JSON serialization accuracy
// Feature: fix-aws-sdk-metadata-serialization, Property 1: メタデータJSONシリアライゼーションの正確性
// Validates: Requirements 2.1, 2.2
func TestProperty_MetadataJSONSerializationAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のIcebergMetadataをJSONにシリアライズした場合、
	// 複合型はネストされたJSONオブジェクトとして出力され、JSON文字列として出力されないべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなスキーマを生成（プリミティブ型と複合型を含む）
		schema := generateRandomSchemaWithComplexTypes(i)

		// IcebergMetadataを作成
		metadata := createInitialIcebergMetadata(schema)

		// JSONにシリアライズ
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			t.Fatalf("iteration %d: failed to marshal metadata to JSON: %v", i, err)
		}

		// JSONをmap[string]interface{}にデシリアライズして検証
		var metadataMap map[string]interface{}
		err = json.Unmarshal(metadataJSON, &metadataMap)
		if err != nil {
			t.Fatalf("iteration %d: failed to unmarshal metadata JSON: %v", i, err)
		}

		// スキーマフィールドを検証
		schemas, ok := metadataMap["schemas"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: schemas is not an array", i)
		}
		if len(schemas) == 0 {
			t.Fatalf("iteration %d: schemas array is empty", i)
		}

		schemaMap, ok := schemas[0].(map[string]interface{})
		if !ok {
			t.Fatalf("iteration %d: schema is not a map", i)
		}

		fields, ok := schemaMap["fields"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: fields is not an array", i)
		}

		// 各フィールドの型を検証
		for j, fieldInterface := range fields {
			fieldMap, ok := fieldInterface.(map[string]interface{})
			if !ok {
				t.Errorf("iteration %d, field %d: field is not a map", i, j)
				continue
			}

			fieldType := fieldMap["type"]

			// プリミティブ型の場合は文字列であることを確認
			if typeStr, ok := fieldType.(string); ok {
				// プリミティブ型は文字列として正しくシリアライズされている
				if typeStr == "" {
					t.Errorf("iteration %d, field %d: primitive type is empty string", i, j)
				}
				continue
			}

			// 複合型の場合はネストされたオブジェクトであることを確認
			if typeMap, ok := fieldType.(map[string]interface{}); ok {
				// 複合型はネストされたJSONオブジェクトとして正しくシリアライズされている
				typeValue, hasType := typeMap["type"]
				if !hasType {
					t.Errorf("iteration %d, field %d: complex type missing 'type' field", i, j)
					continue
				}

				// map型の場合、必須フィールドを検証
				if typeValue == "map" {
					requiredFields := []string{"key-id", "key", "value-id", "value", "value-required"}
					for _, requiredField := range requiredFields {
						if _, hasField := typeMap[requiredField]; !hasField {
							t.Errorf("iteration %d, field %d: map type missing required field '%s'", i, j, requiredField)
						}
					}

					// key-idとvalue-idが数値であることを確認
					if keyID, ok := typeMap["key-id"].(float64); !ok || keyID <= 0 {
						t.Errorf("iteration %d, field %d: map type key-id is not a positive number", i, j)
					}
					if valueID, ok := typeMap["value-id"].(float64); !ok || valueID <= 0 {
						t.Errorf("iteration %d, field %d: map type value-id is not a positive number", i, j)
					}

					// keyとvalueが文字列であることを確認
					if _, ok := typeMap["key"].(string); !ok {
						t.Errorf("iteration %d, field %d: map type key is not a string", i, j)
					}
					if _, ok := typeMap["value"].(string); !ok {
						t.Errorf("iteration %d, field %d: map type value is not a string", i, j)
					}

					// value-requiredがブール値であることを確認
					if _, ok := typeMap["value-required"].(bool); !ok {
						t.Errorf("iteration %d, field %d: map type value-required is not a boolean", i, j)
					}
				}

				continue
			}

			// 型が文字列でもマップでもない場合はエラー
			t.Errorf("iteration %d, field %d: type is neither string nor map, got %T", i, j, fieldType)
		}

		// メタデータJSON全体が有効なJSONであることを確認
		var testUnmarshal interface{}
		if err := json.Unmarshal(metadataJSON, &testUnmarshal); err != nil {
			t.Errorf("iteration %d: metadata JSON is not valid JSON: %v", i, err)
		}
	}
}

// generateRandomSchemaWithComplexTypes generates a random schema with complex types for testing
// テスト用の複合型を含むランダムなスキーマを生成
func generateRandomSchemaWithComplexTypes(seed int) *IcebergSchema {
	// プリミティブ型のフィールドを生成
	primitiveTypes := []string{
		"boolean",
		"int",
		"long",
		"float",
		"double",
		"string",
		"timestamp",
		"timestamptz",
	}

	fields := make([]IcebergSchemaField, 0)
	fieldID := 1

	// プリミティブ型のフィールドを追加（1-3個）
	numPrimitiveFields := 1 + (seed % 3)
	for i := 0; i < numPrimitiveFields; i++ {
		typeIndex := (seed + i) % len(primitiveTypes)
		fields = append(fields, IcebergSchemaField{
			ID:       fieldID,
			Name:     fmt.Sprintf("primitive_field_%d", i),
			Required: i == 0, // 最初のフィールドのみ必須
			Type:     primitiveTypes[typeIndex],
		})
		fieldID++
	}

	// map型のフィールドを追加（1-2個）
	numMapFields := 1 + (seed % 2)
	for i := 0; i < numMapFields; i++ {
		keyTypes := []string{"string", "int", "long"}
		valueTypes := []string{"string", "int", "long", "double", "boolean"}

		keyType := keyTypes[(seed+i)%len(keyTypes)]
		valueType := valueTypes[(seed+i)%len(valueTypes)]

		mapType := map[string]interface{}{
			"type":           "map",
			"key-id":         fieldID,
			"key":            keyType,
			"value-id":       fieldID + 1,
			"value":          valueType,
			"value-required": (seed+i)%2 == 0,
		}

		fields = append(fields, IcebergSchemaField{
			ID:       fieldID + 2,
			Name:     fmt.Sprintf("map_field_%d", i),
			Required: false,
			Type:     mapType,
		})
		fieldID += 3 // map型は3つのIDを使用（field ID, key-id, value-id）
	}

	return &IcebergSchema{
		SchemaID: 0,
		Fields:   fields,
	}
}

// TestProperty_TableCreationSuccess tests table creation success with random schemas
// Feature: fix-aws-sdk-metadata-serialization, Property 2: テーブル作成の成功
// Validates: Requirements 1.3, 2.4
func TestProperty_TableCreationSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の有効なスキーマ（プリミティブ型と複合型を含む）でテーブルを作成した場合、
	// S3 Tables APIはエラーを返さず、テーブルが正常に作成されるべきである

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなスキーマを生成
		schema := generateRandomSchemaWithComplexTypes(i)

		// スキーマをmap[string]interface{}形式に変換
		schemaMap := map[string]interface{}{
			"fields": convertSchemaToFieldMaps(schema),
		}

		// エクスポーターを作成
		cfg := &Config{
			TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
			Region:         "us-east-1",
			Namespace:      "test-namespace",
			Tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "otel_metrics",
				Logs:    "otel_logs",
			},
		}
		set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
		exporter, err := newS3TablesExporter(cfg, set)
		if err != nil {
			t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
		}

		// モックS3 Tablesクライアントを設定
		tableName := fmt.Sprintf("test_table_%d", i)
		tableARN := fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/%s", tableName)
		warehouseLocation := fmt.Sprintf("s3://test-bucket/warehouse/%s", tableName)
		versionToken := fmt.Sprintf("version-token-%d", i)

		mockS3TablesClient := &mockS3TablesClient{
			getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
				// Namespaceが既に存在すると仮定
				return &s3tables.GetNamespaceOutput{}, nil
			},
			createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
				// テーブル作成が成功
				return &s3tables.CreateTableOutput{
					TableARN:     &tableARN,
					VersionToken: &versionToken,
				}, nil
			},
			getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
				// GetTableを呼び出してwarehouse locationを返す
				return &s3tables.GetTableOutput{
					TableARN:          &tableARN,
					WarehouseLocation: &warehouseLocation,
					VersionToken:      &versionToken,
				}, nil
			},
		}
		exporter.s3TablesClient = mockS3TablesClient

		// テーブルを作成
		tableInfo, err := exporter.createTable(context.Background(), "test-namespace", tableName, schemaMap)
		if err != nil {
			t.Errorf("iteration %d: createTable() failed: %v", i, err)
			continue
		}

		// テーブル情報を検証
		if tableInfo.TableARN != tableARN {
			t.Errorf("iteration %d: expected TableARN '%s', got '%s'", i, tableARN, tableInfo.TableARN)
		}
		if tableInfo.WarehouseLocation != warehouseLocation {
			t.Errorf("iteration %d: expected WarehouseLocation '%s', got '%s'", i, warehouseLocation, tableInfo.WarehouseLocation)
		}
		if tableInfo.VersionToken != versionToken {
			t.Errorf("iteration %d: expected VersionToken '%s', got '%s'", i, versionToken, tableInfo.VersionToken)
		}
	}
}

// convertSchemaToFieldMaps converts IcebergSchema to []map[string]interface{} format
// IcebergSchemaを[]map[string]interface{}形式に変換
func convertSchemaToFieldMaps(schema *IcebergSchema) []map[string]interface{} {
	fieldMaps := make([]map[string]interface{}, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		fieldMap := map[string]interface{}{
			"id":       field.ID,
			"name":     field.Name,
			"required": field.Required,
			"type":     field.Type,
		}
		fieldMaps = append(fieldMaps, fieldMap)
	}
	return fieldMaps
}

// TestProperty_BackwardCompatibility tests backward compatibility with existing tests
// Feature: fix-aws-sdk-metadata-serialization, Property 3: 後方互換性の保持
// Validates: Requirements 3.1, 3.2, 3.3, 3.4
func TestProperty_BackwardCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の既存のテストケースにおいて、createTable関数の修正後も同じ結果が得られるべきである

	// 既存のテストケースを再実行して、同じ結果が得られることを確認
	testCases := []struct {
		name   string
		schema map[string]interface{}
	}{
		{
			name: "primitive_types_only",
			schema: map[string]interface{}{
				"fields": []map[string]interface{}{
					{
						"id":       1,
						"name":     "timestamp",
						"required": true,
						"type":     "timestamptz",
					},
					{
						"id":       2,
						"name":     "service_name",
						"required": false,
						"type":     "string",
					},
					{
						"id":       3,
						"name":     "value",
						"required": false,
						"type":     "double",
					},
				},
			},
		},
		{
			name: "complex_types_with_map",
			schema: map[string]interface{}{
				"fields": []map[string]interface{}{
					{
						"id":       1,
						"name":     "timestamp",
						"required": true,
						"type":     "timestamptz",
					},
					{
						"id":       4,
						"name":     "resource_attributes",
						"required": false,
						"type": map[string]interface{}{
							"type":           "map",
							"key-id":         2,
							"key":            "string",
							"value-id":       3,
							"value":          "string",
							"value-required": false,
						},
					},
				},
			},
		},
		{
			name: "multiple_complex_types",
			schema: map[string]interface{}{
				"fields": []map[string]interface{}{
					{
						"id":       1,
						"name":     "timestamp",
						"required": true,
						"type":     "timestamptz",
					},
					{
						"id":       4,
						"name":     "attributes",
						"required": false,
						"type": map[string]interface{}{
							"type":           "map",
							"key-id":         2,
							"key":            "string",
							"value-id":       3,
							"value":          "string",
							"value-required": false,
						},
					},
					{
						"id":       7,
						"name":     "labels",
						"required": false,
						"type": map[string]interface{}{
							"type":           "map",
							"key-id":         5,
							"key":            "string",
							"value-id":       6,
							"value":          "string",
							"value-required": false,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// エクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("newS3TablesExporter() failed: %v", err)
			}

			// モックS3 Tablesクライアントを設定
			tableName := "test_table"
			tableARN := fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/%s", tableName)
			warehouseLocation := fmt.Sprintf("s3://test-bucket/warehouse/%s", tableName)
			versionToken := "version-token-1"

			var capturedMetadata *types.IcebergMetadata
			mockS3TablesClient := &mockS3TablesClient{
				getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
					return &s3tables.GetNamespaceOutput{}, nil
				},
				createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
					// メタデータをキャプチャ
					if params.Metadata != nil {
						if member, ok := params.Metadata.(*types.TableMetadataMemberIceberg); ok {
							capturedMetadata = &member.Value
						}
					}
					return &s3tables.CreateTableOutput{
						TableARN:     &tableARN,
						VersionToken: &versionToken,
					}, nil
				},
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					return &s3tables.GetTableOutput{
						TableARN:          &tableARN,
						WarehouseLocation: &warehouseLocation,
						VersionToken:      &versionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// テーブルを作成
			tableInfo, err := exporter.createTable(context.Background(), "test-namespace", tableName, tc.schema)
			if err != nil {
				t.Fatalf("createTable() failed: %v", err)
			}

			// テーブル情報を検証（既存のテストと同じ結果が得られることを確認）
			if tableInfo.TableARN != tableARN {
				t.Errorf("expected TableARN '%s', got '%s'", tableARN, tableInfo.TableARN)
			}
			if tableInfo.WarehouseLocation != warehouseLocation {
				t.Errorf("expected WarehouseLocation '%s', got '%s'", warehouseLocation, tableInfo.WarehouseLocation)
			}
			if tableInfo.VersionToken != versionToken {
				t.Errorf("expected VersionToken '%s', got '%s'", versionToken, tableInfo.VersionToken)
			}

			// メタデータが正しくキャプチャされたことを確認
			if capturedMetadata == nil {
				t.Fatal("metadata was not captured")
			}

			// スキーマが正しく変換されたことを確認
			if capturedMetadata.Schema == nil {
				t.Fatal("schema is nil")
			}

			// フィールド数が一致することを確認
			expectedFieldCount := len(tc.schema["fields"].([]map[string]interface{}))
			actualFieldCount := len(capturedMetadata.Schema.Fields)
			if actualFieldCount != expectedFieldCount {
				t.Errorf("expected %d fields, got %d", expectedFieldCount, actualFieldCount)
			}

			// 各フィールドの基本情報が正しく変換されたことを確認
			for i, expectedFieldMap := range tc.schema["fields"].([]map[string]interface{}) {
				if i >= len(capturedMetadata.Schema.Fields) {
					t.Errorf("field %d is missing in captured metadata", i)
					continue
				}

				actualField := capturedMetadata.Schema.Fields[i]

				// 名前の検証
				expectedName := expectedFieldMap["name"].(string)
				if *actualField.Name != expectedName {
					t.Errorf("field %d: expected name '%s', got '%s'", i, expectedName, *actualField.Name)
				}

				// requiredの検証
				expectedRequired := expectedFieldMap["required"].(bool)
				if actualField.Required != expectedRequired {
					t.Errorf("field %d: expected required %v, got %v", i, expectedRequired, actualField.Required)
				}

				// 型の検証
				expectedType := expectedFieldMap["type"]
				if typeStr, ok := expectedType.(string); ok {
					// プリミティブ型の場合
					if *actualField.Type != typeStr {
						t.Errorf("field %d: expected type '%s', got '%s'", i, typeStr, *actualField.Type)
					}
				} else if typeMap, ok := expectedType.(map[string]interface{}); ok {
					// 複合型の場合、JSON文字列として変換されていることを確認
					var actualTypeMap map[string]interface{}
					if err := json.Unmarshal([]byte(*actualField.Type), &actualTypeMap); err != nil {
						t.Errorf("field %d: failed to unmarshal type JSON: %v", i, err)
						continue
					}

					// 型の種類を確認
					if actualTypeMap["type"] != typeMap["type"] {
						t.Errorf("field %d: expected type '%s', got '%s'", i, typeMap["type"], actualTypeMap["type"])
					}
				}
			}

			// Propertiesに完全なIcebergメタデータが格納されていることを確認
			if capturedMetadata.Properties == nil {
				t.Fatal("properties is nil")
			}

			metadataJSON, ok := capturedMetadata.Properties["iceberg.metadata.json"]
			if !ok {
				t.Fatal("iceberg.metadata.json property is missing")
			}

			// メタデータJSONが有効なJSONであることを確認
			var fullMetadata IcebergMetadata
			if err := json.Unmarshal([]byte(metadataJSON), &fullMetadata); err != nil {
				t.Fatalf("failed to unmarshal full metadata JSON: %v", err)
			}

			// 完全なメタデータの基本フィールドを検証
			if fullMetadata.FormatVersion != 2 {
				t.Errorf("expected format-version 2, got %d", fullMetadata.FormatVersion)
			}
			if fullMetadata.TableUUID == "" {
				t.Error("table-uuid is empty")
			}
			if len(fullMetadata.Schemas) == 0 {
				t.Error("schemas is empty")
			}
			if fullMetadata.CurrentSnapshotID != -1 {
				t.Errorf("expected current-snapshot-id -1, got %d", fullMetadata.CurrentSnapshotID)
			}
		})
	}
}
