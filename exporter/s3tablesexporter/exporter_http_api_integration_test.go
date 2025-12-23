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
	"strings"
	"testing"
)

// TestHTTPAPIIntegration_TracesSchema tests end-to-end table creation with traces schema using HTTP API
// トレーススキーマを使用したHTTP APIによるエンドツーエンドのテーブル作成をテスト
// Requirements: 4.1
func TestHTTPAPIIntegration_TracesSchema(t *testing.T) {
	// トレーススキーマを作成
	schema := createTracesSchema()

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

	// HTTPモックサーバーを作成
	var capturedRequest *CreateTableRequest
	mockServer := NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_traces",
			VersionToken: "version-token-1",
		},
		CaptureCreateTableRequest: &capturedRequest,
		ValidateRequest: ValidateCreateTableRequest("POST", "/tables"),
	})
	defer mockServer.Close()

	// リクエストボディを構築
	requestBody := CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Namespace:      "test-namespace",
		Name:           "otel_traces",
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
	endpoint := mockServer.URL + "/tables"
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, strings.NewReader(string(requestBodyBytes)))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	// Content-Typeヘッダーを設定
	req.Header.Set("Content-Type", "application/json")

	// HTTPリクエストを送信
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// レスポンスステータスコードを検証
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status code %d, got %d", http.StatusCreated, resp.StatusCode)
	}

	// レスポンスボディを読み取る
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	// レスポンスボディをパース
	var createResp CreateTableResponse
	if err := json.Unmarshal(respBody, &createResp); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	// レスポンスを検証
	expectedTableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_traces"
	if createResp.TableARN != expectedTableARN {
		t.Errorf("expected TableARN %s, got %s", expectedTableARN, createResp.TableARN)
	}

	expectedVersionToken := "version-token-1"
	if createResp.VersionToken != expectedVersionToken {
		t.Errorf("expected VersionToken %s, got %s", expectedVersionToken, createResp.VersionToken)
	}

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// リクエストボディの構造を検証
	if capturedRequest.TableBucketARN != requestBody.TableBucketARN {
		t.Errorf("expected TableBucketARN %s, got %s", requestBody.TableBucketARN, capturedRequest.TableBucketARN)
	}

	if capturedRequest.Namespace != requestBody.Namespace {
		t.Errorf("expected Namespace %s, got %s", requestBody.Namespace, capturedRequest.Namespace)
	}

	if capturedRequest.Name != requestBody.Name {
		t.Errorf("expected Name %s, got %s", requestBody.Name, capturedRequest.Name)
	}

	if capturedRequest.Format != "ICEBERG" {
		t.Errorf("expected Format ICEBERG, got %s", capturedRequest.Format)
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

	// プリミティブ型フィールドの検証
	primitiveFields := map[string]string{
		"trace_id":   "binary",
		"span_id":    "binary",
		"name":       "string",
		"start_time": "timestamptz",
		"end_time":   "timestamptz",
	}

	for fieldName, expectedType := range primitiveFields {
		var field *CustomSchemaField
		for i := range fields {
			if fields[i].Name == fieldName {
				field = &fields[i]
				break
			}
		}

		if field == nil {
			t.Errorf("primitive field '%s' not found", fieldName)
			continue
		}

		// プリミティブ型が文字列として格納されていることを確認
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field '%s': expected Type to be string, got %T", fieldName, field.Type)
			continue
		}

		if typeStr != expectedType {
			t.Errorf("field '%s': expected type '%s', got '%s'", fieldName, expectedType, typeStr)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Traces schema successfully serialized and accepted by S3 Tables API")
}

// TestHTTPAPIIntegration_MetricsSchema tests end-to-end table creation with metrics schema using HTTP API
// メトリクススキーマを使用したHTTP APIによるエンドツーエンドのテーブル作成をテスト
// Requirements: 4.2
func TestHTTPAPIIntegration_MetricsSchema(t *testing.T) {
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

	// HTTPモックサーバーを作成
	var capturedRequest *CreateTableRequest
	mockServer := NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_metrics",
			VersionToken: "version-token-1",
		},
		CaptureCreateTableRequest: &capturedRequest,
		ValidateRequest: ValidateCreateTableRequest("POST", "/tables"),
	})
	defer mockServer.Close()

	// リクエストボディを構築
	requestBody := CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Namespace:      "test-namespace",
		Name:           "otel_metrics",
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
	endpoint := mockServer.URL + "/tables"
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, strings.NewReader(string(requestBodyBytes)))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	// Content-Typeヘッダーを設定
	req.Header.Set("Content-Type", "application/json")

	// HTTPリクエストを送信
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// レスポンスステータスコードを検証
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status code %d, got %d", http.StatusCreated, resp.StatusCode)
	}

	// レスポンスボディを読み取る
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	// レスポンスボディをパース
	var createResp CreateTableResponse
	if err := json.Unmarshal(respBody, &createResp); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	// レスポンスを検証
	expectedTableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_metrics"
	if createResp.TableARN != expectedTableARN {
		t.Errorf("expected TableARN %s, got %s", expectedTableARN, createResp.TableARN)
	}

	expectedVersionToken := "version-token-1"
	if createResp.VersionToken != expectedVersionToken {
		t.Errorf("expected VersionToken %s, got %s", expectedVersionToken, createResp.VersionToken)
	}

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// リクエストボディの構造を検証
	if capturedRequest.TableBucketARN != requestBody.TableBucketARN {
		t.Errorf("expected TableBucketARN %s, got %s", requestBody.TableBucketARN, capturedRequest.TableBucketARN)
	}

	if capturedRequest.Namespace != requestBody.Namespace {
		t.Errorf("expected Namespace %s, got %s", requestBody.Namespace, capturedRequest.Namespace)
	}

	if capturedRequest.Name != requestBody.Name {
		t.Errorf("expected Name %s, got %s", requestBody.Name, capturedRequest.Name)
	}

	if capturedRequest.Format != "ICEBERG" {
		t.Errorf("expected Format ICEBERG, got %s", capturedRequest.Format)
	}

	// スキーマフィールドを検証
	fields := capturedRequest.Metadata.Iceberg.Schema.Fields

	// メトリクススキーマは6つのフィールドを持つ
	expectedFieldCount := 6
	if len(fields) != expectedFieldCount {
		t.Errorf("expected %d fields in Schema, got %d", expectedFieldCount, len(fields))
	}

	// 必須フィールドの存在を確認
	requiredFields := map[string]bool{
		"timestamp":   false,
		"metric_name": false,
		"metric_type": false,
		"value":       false,
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

	// プリミティブ型フィールドの検証
	primitiveFields := map[string]string{
		"timestamp":   "timestamptz",
		"metric_name": "string",
		"metric_type": "string",
		"value":       "double",
	}

	for fieldName, expectedType := range primitiveFields {
		var field *CustomSchemaField
		for i := range fields {
			if fields[i].Name == fieldName {
				field = &fields[i]
				break
			}
		}

		if field == nil {
			t.Errorf("primitive field '%s' not found", fieldName)
			continue
		}

		// プリミティブ型が文字列として格納されていることを確認
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field '%s': expected Type to be string, got %T", fieldName, field.Type)
			continue
		}

		if typeStr != expectedType {
			t.Errorf("field '%s': expected type '%s', got '%s'", fieldName, expectedType, typeStr)
		}
	}

	// シリアライズされたスキーマがS3 Tables APIに受け入れられることを確認
	// （モックが正常に完了したことで確認済み）
	t.Log("Metrics schema successfully serialized and accepted by S3 Tables API")
}
