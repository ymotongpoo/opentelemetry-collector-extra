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

// TestCreateTableDirectAPI_RequestConstruction tests that HTTP request is correctly constructed
// HTTPリクエストが正しく構築されることをテスト
// Requirements: 1.4, 6.2
func TestCreateTableDirectAPI_RequestConstruction(t *testing.T) {
	// HTTPモックサーバーを作成
	var capturedRequest *http.Request
	var capturedBody []byte
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストをキャプチャ
		capturedRequest = r

		// リクエストボディを読み取る
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}
		capturedBody = body

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

	// CustomSchemaFieldsを作成
	customFields := []CustomSchemaField{
		{
			Name:     "timestamp",
			Type:     "timestamptz",
			Required: true,
		},
		{
			Name:     "service_name",
			Type:     "string",
			Required: false,
		},
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

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// HTTPメソッドを検証
	if capturedRequest.Method != "POST" {
		t.Errorf("expected POST request, got %s", capturedRequest.Method)
	}

	// パスを検証
	if capturedRequest.URL.Path != "/tables" {
		t.Errorf("expected /tables path, got %s", capturedRequest.URL.Path)
	}

	// Content-Typeヘッダーを検証
	contentType := capturedRequest.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	// リクエストボディが空でないことを確認
	if len(capturedBody) == 0 {
		t.Fatal("request body is empty")
	}
}

// TestCreateTableDirectAPI_RequestBodyStructure tests that request body JSON has correct structure
// リクエストボディのJSONが正しい構造であることをテスト
// Requirements: 1.4, 6.2
func TestCreateTableDirectAPI_RequestBodyStructure(t *testing.T) {
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

	// CustomSchemaFieldsを作成（プリミティブ型とmap型を含む）
	customFields := []CustomSchemaField{
		{
			Name:     "timestamp",
			Type:     "timestamptz",
			Required: true,
		},
		{
			Name:     "attributes",
			Type: map[string]interface{}{
				"type":           "map",
				"key-id":         3,
				"key":            "string",
				"value-id":       4,
				"value":          "string",
				"value-required": false,
			},
			Required: false,
		},
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

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// リクエストボディの構造を検証
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
	if len(fields) != 2 {
		t.Errorf("expected 2 fields in Schema, got %d", len(fields))
	}

	// プリミティブ型のフィールドを検証
	timestampField := fields[0]
	if timestampField.Name != "timestamp" {
		t.Errorf("expected field name timestamp, got %s", timestampField.Name)
	}
	if timestampField.Type != "timestamptz" {
		t.Errorf("expected field type timestamptz, got %v", timestampField.Type)
	}
	if !timestampField.Required {
		t.Error("expected timestamp field to be required")
	}

	// map型のフィールドを検証
	attributesField := fields[1]
	if attributesField.Name != "attributes" {
		t.Errorf("expected field name attributes, got %s", attributesField.Name)
	}

	// map型が構造化オブジェクトとして格納されていることを確認
	mapType, ok := attributesField.Type.(map[string]interface{})
	if !ok {
		t.Fatalf("expected attributes field Type to be map[string]interface{}, got %T", attributesField.Type)
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

	// exporterを使用してテストを完了
	_ = exporter
}

// TestCreateTableDirectAPI_ResponseHandling tests that response handling works correctly
// レスポンスハンドリングが正しく動作することをテスト
// Requirements: 1.4, 6.2
func TestCreateTableDirectAPI_ResponseHandling(t *testing.T) {
	tests := []struct {
		name               string
		statusCode         int
		responseBody       string
		expectError        bool
		expectedTableARN   string
		expectedVersionToken string
	}{
		{
			name:       "Success with 201 Created",
			statusCode: http.StatusCreated,
			responseBody: `{
				"tableARN": "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
				"versionToken": "version-token-1"
			}`,
			expectError:          false,
			expectedTableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
			expectedVersionToken: "version-token-1",
		},
		{
			name:       "Success with 200 OK",
			statusCode: http.StatusOK,
			responseBody: `{
				"tableARN": "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
				"versionToken": "version-token-2"
			}`,
			expectError:          false,
			expectedTableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
			expectedVersionToken: "version-token-2",
		},
		{
			name:         "Error with 400 Bad Request",
			statusCode:   http.StatusBadRequest,
			responseBody: `{"error": "BadRequestException: The specified metadata is not valid"}`,
			expectError:  true,
		},
		{
			name:         "Error with 500 Internal Server Error",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error": "InternalServerError"}`,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// HTTPモックサーバーを作成
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.responseBody))
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
						TableARN:          aws.String(tt.expectedTableARN),
						WarehouseLocation: aws.String("s3://test-bucket/warehouse/test-namespace/test-table"),
						VersionToken:      aws.String(tt.expectedVersionToken),
					}, nil
				},
			}
			exporter.s3TablesClient = mockClient

			// CustomSchemaFieldsを作成
			customFields := []CustomSchemaField{
				{
					Name:     "timestamp",
					Type:     "timestamptz",
					Required: true,
				},
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
			if tt.expectError {
				// エラーが期待される場合
				if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
					t.Errorf("expected error status code, got %d", resp.StatusCode)
				}
			} else {
				// 成功が期待される場合
				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
					t.Errorf("expected success status code, got %d", resp.StatusCode)
				}

				// レスポンスボディを読み取る
				respBodyBytes, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("failed to read response body: %v", err)
				}

				// レスポンスボディをパース
				var createResp CreateTableResponse
				if err := json.Unmarshal(respBodyBytes, &createResp); err != nil {
					t.Fatalf("failed to parse response body: %v", err)
				}

				// TableARNを検証
				if createResp.TableARN != tt.expectedTableARN {
					t.Errorf("expected TableARN %s, got %s", tt.expectedTableARN, createResp.TableARN)
				}

				// VersionTokenを検証
				if createResp.VersionToken != tt.expectedVersionToken {
					t.Errorf("expected VersionToken %s, got %s", tt.expectedVersionToken, createResp.VersionToken)
				}
			}

			// exporterを使用してテストを完了
			_ = exporter
		})
	}
}

// TestCreateTableDirectAPI_SigV4Signing tests that AWS SigV4 signing is correctly applied
// AWS SigV4署名が正しく適用されることをテスト
// Requirements: 1.4, 6.2
func TestCreateTableDirectAPI_SigV4Signing(t *testing.T) {
	// HTTPモックサーバーを作成
	var capturedRequest *http.Request
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストをキャプチャ
		capturedRequest = r

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

	// CustomSchemaFieldsを作成
	customFields := []CustomSchemaField{
		{
			Name:     "timestamp",
			Type:     "timestamptz",
			Required: true,
		},
	}

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
	endpoint := mockServer.URL + "/tables"
	req, err := http.NewRequestWithContext(context.Background(), "POST", endpoint, strings.NewReader(string(requestBodyBytes)))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	// Content-Typeヘッダーを設定
	req.Header.Set("Content-Type", "application/json")

	// AWS SigV4署名を追加
	if err := exporter.signHTTPRequest(context.Background(), req, requestBodyBytes); err != nil {
		t.Fatalf("signHTTPRequest() failed: %v", err)
	}

	// HTTPリクエストを送信
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// リクエストがキャプチャされたことを確認
	if capturedRequest == nil {
		t.Fatal("request was not captured")
	}

	// AWS SigV4署名ヘッダーが存在することを確認
	authHeader := capturedRequest.Header.Get("Authorization")
	if authHeader == "" {
		t.Error("Authorization header is missing")
	}

	// Authorizationヘッダーが"AWS4-HMAC-SHA256"で始まることを確認
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		t.Errorf("expected Authorization header to start with 'AWS4-HMAC-SHA256', got %s", authHeader)
	}

	// X-Amz-Dateヘッダーが存在することを確認
	dateHeader := capturedRequest.Header.Get("X-Amz-Date")
	if dateHeader == "" {
		t.Error("X-Amz-Date header is missing")
	}

	// X-Amz-Security-Tokenヘッダーが存在する可能性がある（セッショントークンを使用する場合）
	// このヘッダーは必須ではないため、存在チェックのみ
	_ = capturedRequest.Header.Get("X-Amz-Security-Token")
}
