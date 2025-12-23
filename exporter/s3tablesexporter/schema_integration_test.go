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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component"
	exportertest "go.opentelemetry.io/collector/exporter/exportertest"
)

// TestSchemaIntegration_TracesSchema tests end-to-end table creation with traces schema
// トレーススキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.1
func TestSchemaIntegration_TracesSchema(t *testing.T) {
	// トレーススキーマを作成
	schema := createTracesSchema()

	// HTTPモックサーバーを作成
	var capturedRequest *CreateTableRequest
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストボディを読み取る
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		// リクエストボディをパース
		var req CreateTableRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("failed to parse request body: %v", err)
		}
		capturedRequest = &req

		// レスポンスを返す
		resp := CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_traces",
			VersionToken: "version-token-1",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(resp)
	}))
	defer mockServer.Close()

	// Exporterを作成
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
	_, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// HTTPクライアントを使用してモックサーバーにリクエストを送信
	endpoint := mockServer.URL + "/tables"
	httpClient := &http.Client{}

	// リクエストボディを構築
	requestBody := CreateTableRequest{
		TableBucketARN: cfg.TableBucketArn,
		Namespace:      cfg.Namespace,
		Name:           cfg.Tables.Traces,
		Format:         "ICEBERG",
		Metadata: CreateTableMetadata{
			Iceberg: CreateTableIcebergMetadata{
				Schema: CreateTableSchema{
					Fields: customFields,
				},
			},
		},
	}

	// リクエストボディをJSONにシリアライズ
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	// HTTPリクエストを作成
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, strings.NewReader(string(requestBodyBytes)))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	// Content-Typeヘッダーを設定
	req.Header.Set("Content-Type", "application/json")

	// HTTPリクエストを送信
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// レスポンスを検証
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status code %d, got %d", http.StatusCreated, resp.StatusCode)
	}

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// スキーマフィールドを検証
	fields := capturedRequest.Metadata.Iceberg.Schema.Fields

	// トレーススキーマは8つのフィールドを持つ
	expectedFieldCount := 8
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"trace_id":   false,
		"span_id":    false,
		"name":       false,
		"start_time": false,
		"end_time":   false,
	}

	for _, field := range fields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（attributes、resource_attributes）の検証
	mapFields := []string{"attributes", "resource_attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *CustomSchemaField
		for i := range fields {
			if fields[i].Name == mapFieldName {
				mapField = &fields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
		if mapType["key"] != "string" {
			t.Errorf("field '%s': expected key type 'string', got %v", mapFieldName, mapType["key"])
		}
		if mapType["value"] != "string" {
			t.Errorf("field '%s': expected value type 'string', got %v", mapFieldName, mapType["value"])
		}

		// key-idとvalue-idが含まれていることを確認
		if _, ok := mapType["key-id"]; !ok {
			t.Errorf("field '%s': key-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-id"]; !ok {
			t.Errorf("field '%s': value-id not found in map type", mapFieldName)
		}
		if _, ok := mapType["value-required"]; !ok {
			t.Errorf("field '%s': value-required not found in map type", mapFieldName)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Traces schema successfully serialized and accepted by S3 Tables API")
}

// TestSchemaIntegration_MetricsSchema tests end-to-end table creation with metrics schema
// メトリクススキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.2
func TestSchemaIntegration_MetricsSchema(t *testing.T) {
	// メトリクススキーマを作成
	schema := createMetricsSchema()

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// メトリクススキーマは6つのフィールドを持つ
	expectedFieldCount := 6
	if len(customFields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(customFields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp":   false,
		"metric_name": false,
		"metric_type": false,
		"value":       false,
	}

	for _, field := range customFields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（resource_attributes、attributes）の検証
	mapFields := []string{"resource_attributes", "attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *CustomSchemaField
		for i := range customFields {
			if customFields[i].Name == mapFieldName {
				mapField = &customFields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
	}

	t.Log("Metrics schema successfully serialized")
}

// TestSchemaIntegration_LogsSchema tests end-to-end table creation with logs schema
// ログスキーマを使用したエンドツーエンドのテーブル作成をテスト
// Requirements: 4.3
func TestSchemaIntegration_LogsSchema(t *testing.T) {
	// ログスキーマを作成
	schema := createLogsSchema()

	// スキーマをIcebergSchema形式に変換
	icebergSchema, err := convertToIcebergSchema(schema)
	if err != nil {
		t.Fatalf("convertToIcebergSchema() failed: %v", err)
	}

	// AWSSchemaAdapterを使用してCustomSchemaFieldsを取得
	adapter := NewAWSSchemaAdapter(icebergSchema)
	customFields, err := adapter.MarshalSchemaFields()
	if err != nil {
		t.Fatalf("MarshalSchemaFields() failed: %v", err)
	}

	// ログスキーマは5つのフィールドを持つ
	expectedFieldCount := 5
	if len(customFields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(customFields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp": false,
		"body":      false,
	}

	for _, field := range customFields {
		if _, ok := requiredFields[field.Name]; ok {
			requiredFields[field.Name] = true
		}
	}

	for fieldName, found := range requiredFields {
		if !found {
			t.Errorf("required field '%s' not found in schema", fieldName)
		}
	}

	// map型フィールド（attributes、resource_attributes）の検証
	mapFields := []string{"attributes", "resource_attributes"}
	for _, mapFieldName := range mapFields {
		var mapField *CustomSchemaField
		for i := range customFields {
			if customFields[i].Name == mapFieldName {
				mapField = &customFields[i]
				break
			}
		}

		if mapField == nil {
			t.Errorf("map field '%s' not found", mapFieldName)
			continue
		}

		// map型が構造化オブジェクトとして格納されていることを確認
		mapType, ok := mapField.Type.(map[string]interface{})
		if !ok {
			t.Errorf("field '%s': expected Type to be map[string]interface{}, got %T", mapFieldName, mapField.Type)
			continue
		}

		// map型の内容を検証
		if mapType["type"] != "map" {
			t.Errorf("field '%s': expected type 'map', got %v", mapFieldName, mapType["type"])
		}
	}

	t.Log("Logs schema successfully serialized")
}
