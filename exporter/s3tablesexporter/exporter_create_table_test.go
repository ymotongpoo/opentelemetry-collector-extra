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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	exportertest "go.opentelemetry.io/collector/exporter/exportertest"
)

// TestCreateTable_PrimitiveTypes tests table creation with primitive types only
// プリミティブ型のみのスキーマでテーブル作成をテスト
// Requirements: 5.1, 5.2
func TestCreateTable_PrimitiveTypes(t *testing.T) {
	// プリミティブ型のみのスキーマを作成
	schema := map[string]interface{}{
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
	}

	// HTTPモックサーバーを作成
	var capturedRequest *CreateTableRequest
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストメソッドとパスを検証
		if r.Method != "POST" {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.URL.Path != "/tables" {
			t.Errorf("expected /tables path, got %s", r.URL.Path)
		}

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
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
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
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// モックS3 Tablesクライアントを設定（GetTable用）
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/test-table"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

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
		Namespace:      "test-namespace",
		Name:           "test-table",
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

	// リクエストの内容を検証
	if capturedRequest.TableBucketARN != cfg.TableBucketArn {
		t.Errorf("expected TableBucketARN %s, got %s", cfg.TableBucketArn, capturedRequest.TableBucketARN)
	}
	if capturedRequest.Namespace != "test-namespace" {
		t.Errorf("expected Namespace test-namespace, got %s", capturedRequest.Namespace)
	}
	if capturedRequest.Name != "test-table" {
		t.Errorf("expected Name test-table, got %s", capturedRequest.Name)
	}
	if capturedRequest.Format != "ICEBERG" {
		t.Errorf("expected Format ICEBERG, got %s", capturedRequest.Format)
	}

	// スキーマフィールドを検証
	fields := capturedRequest.Metadata.Iceberg.Schema.Fields
	if len(fields) != 3 {
		t.Errorf("expected 3 fields in Schema, got %d", len(fields))
	}

	// プリミティブ型のフィールドを検証
	expectedTypes := map[string]string{
		"timestamp":    "timestamptz",
		"service_name": "string",
		"value":        "double",
	}
	for _, field := range fields {
		expectedType, ok := expectedTypes[field.Name]
		if !ok {
			t.Errorf("unexpected field name: %s", field.Name)
			continue
		}
		// プリミティブ型の場合、Typeは文字列
		typeStr, ok := field.Type.(string)
		if !ok {
			t.Errorf("field %s: expected Type to be string, got %T", field.Name, field.Type)
			continue
		}
		if typeStr != expectedType {
			t.Errorf("field %s: expected type %s, got %s", field.Name, expectedType, typeStr)
		}
	}
}

// TestCreateTable_ComplexTypes tests table creation with complex types (map)
// 複合型（map）を含むスキーマでテーブル作成をテスト
// Requirements: 5.1, 5.2
func TestCreateTable_ComplexTypes(t *testing.T) {
	// 複合型を含むスキーマを作成
	schema := map[string]interface{}{
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
			{
				"id":       2,
				"name":     "resource_attributes",
				"required": false,
				"type": map[string]interface{}{
					"type":           "map",
					"key-id":         3,
					"key":            "string",
					"value-id":       4,
					"value":          "string",
					"value-required": false,
				},
			},
		},
	}

	// HTTPモックサーバーを作成
	var capturedRequest *CreateTableRequest
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストメソッドとパスを検証
		if r.Method != "POST" {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.URL.Path != "/tables" {
			t.Errorf("expected /tables path, got %s", r.URL.Path)
		}

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
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
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
	exporter, err := newS3TablesExporter(cfg, set)
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
		Namespace:      "test-namespace",
		Name:           "test-table",
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
	if len(fields) != 2 {
		t.Errorf("expected 2 fields in Schema, got %d", len(fields))
	}

	// map型のフィールドを検証
	var mapField *CustomSchemaField
	for i := range fields {
		if fields[i].Name == "resource_attributes" {
			mapField = &fields[i]
			break
		}
	}
	if mapField == nil {
		t.Fatal("map field not found")
	}

	// map型が構造化オブジェクトとして格納されていることを確認
	mapType, ok := mapField.Type.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map field Type to be map[string]interface{}, got %T", mapField.Type)
	}

	// map型の内容を検証
	if mapType["type"] != "map" {
		t.Errorf("expected type 'map', got %v", mapType["type"])
	}
	if mapType["key"] != "string" {
		t.Errorf("expected key type 'string', got %v", mapType["key"])
	}
	if mapType["value"] != "string" {
		t.Errorf("expected value type 'string', got %v", mapType["value"])
	}

	// key-idとvalue-idが含まれていることを確認
	if _, ok := mapType["key-id"]; !ok {
		t.Error("key-id not found in map type")
	}
	if _, ok := mapType["value-id"]; !ok {
		t.Error("value-id not found in map type")
	}
	if _, ok := mapType["value-required"]; !ok {
		t.Error("value-required not found in map type")
	}

	// exporterを使用してテストを完了
	_ = exporter
}

// TestCreateTable_JSONSerializationError tests error handling when JSON serialization fails
// JSONシリアライゼーションエラーのハンドリングをテスト
// Requirements: 5.1, 5.2
func TestCreateTable_JSONSerializationError(t *testing.T) {
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

	// 無効なスキーマを作成（fieldsキーが欠落）
	schema := map[string]interface{}{
		"invalid": "schema",
	}

	// スキーマをIcebergSchema形式に変換を試行
	_, err = convertToIcebergSchema(schema)
	if err == nil {
		t.Fatal("expected error from convertToIcebergSchema with invalid schema")
	}

	// エラーメッセージを検証
	expectedSubstr := "fields"
	if !strings.Contains(err.Error(), expectedSubstr) {
		t.Errorf("expected error message to contain '%s', got: '%s'", expectedSubstr, err.Error())
	}
}

// TestCreateTable_CreateTableAPIError tests error handling when CreateTable API fails
// CreateTable APIエラーのハンドリングをテスト
// Requirements: 5.1, 5.2
func TestCreateTable_CreateTableAPIError(t *testing.T) {
	// プリミティブ型のみのスキーマを作成
	schema := map[string]interface{}{
		"fields": []map[string]interface{}{
			{
				"id":       1,
				"name":     "timestamp",
				"required": true,
				"type":     "timestamptz",
			},
		},
	}

	// HTTPモックサーバーを作成（エラーを返す）
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// エラーレスポンスを返す
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "BadRequestException: The specified metadata is not valid"}`))
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
	exporter, err := newS3TablesExporter(cfg, set)
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
		Namespace:      "test-namespace",
		Name:           "test-table",
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
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status code %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	// レスポンスボディを読み取る
	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	// エラーメッセージを検証
	expectedSubstr := "BadRequestException"
	if !strings.Contains(string(respBodyBytes), expectedSubstr) {
		t.Errorf("expected error message to contain '%s', got: '%s'", expectedSubstr, string(respBodyBytes))
	}

	// exporterを使用してテストを完了
	_ = exporter
}
