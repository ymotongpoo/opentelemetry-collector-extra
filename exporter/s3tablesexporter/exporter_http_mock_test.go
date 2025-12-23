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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// MockS3TablesServerConfig は、モックS3 Tables APIサーバーの設定を表します
type MockS3TablesServerConfig struct {
	// ValidateRequest は、リクエストを検証するためのコールバック関数です
	// リクエストが無効な場合はエラーを返します
	ValidateRequest func(r *http.Request, body []byte) error

	// StatusCode は、レスポンスのHTTPステータスコードです
	StatusCode int

	// ResponseBody は、レスポンスボディのJSONオブジェクトです
	ResponseBody interface{}

	// CaptureRequest は、リクエストをキャプチャするためのポインタです
	// nilでない場合、リクエストがキャプチャされます
	CaptureRequest **http.Request

	// CaptureBody は、リクエストボディをキャプチャするためのポインタです
	// nilでない場合、リクエストボディがキャプチャされます
	CaptureBody *[]byte

	// CaptureCreateTableRequest は、CreateTableRequestをキャプチャするためのポインタです
	// nilでない場合、パースされたCreateTableRequestがキャプチャされます
	CaptureCreateTableRequest **CreateTableRequest
}

// NewMockS3TablesServer は、モックS3 Tables APIサーバーを作成します
// このサーバーは、リクエストを検証し、設定されたレスポンスを返します
// Requirements: 1.4
func NewMockS3TablesServer(t *testing.T, config MockS3TablesServerConfig) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// リクエストボディを読み取る
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}

		// リクエストをキャプチャ
		if config.CaptureRequest != nil {
			*config.CaptureRequest = r
		}

		// リクエストボディをキャプチャ
		if config.CaptureBody != nil {
			*config.CaptureBody = body
		}

		// CreateTableRequestをパースしてキャプチャ
		if config.CaptureCreateTableRequest != nil {
			var req CreateTableRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatalf("failed to parse CreateTableRequest: %v", err)
			}
			*config.CaptureCreateTableRequest = &req
		}

		// リクエストを検証
		if config.ValidateRequest != nil {
			if err := config.ValidateRequest(r, body); err != nil {
				t.Fatalf("request validation failed: %v", err)
			}
		}

		// レスポンスを返す
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(config.StatusCode)

		if config.ResponseBody != nil {
			if err := json.NewEncoder(w).Encode(config.ResponseBody); err != nil {
				t.Fatalf("failed to encode response body: %v", err)
			}
		}
	}))
}

// NewSuccessfulMockS3TablesServer は、成功レスポンスを返すモックS3 Tables APIサーバーを作成します
// これは、テーブル作成が成功する場合のテストに使用されます
// Requirements: 1.4
func NewSuccessfulMockS3TablesServer(t *testing.T, tableARN, versionToken string) *httptest.Server {
	return NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     tableARN,
			VersionToken: versionToken,
		},
	})
}

// NewErrorMockS3TablesServer は、エラーレスポンスを返すモックS3 Tables APIサーバーを作成します
// これは、テーブル作成が失敗する場合のテストに使用されます
// Requirements: 1.4
func NewErrorMockS3TablesServer(t *testing.T, statusCode int, errorMessage string) *httptest.Server {
	return NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: statusCode,
		ResponseBody: map[string]string{
			"error": errorMessage,
		},
	})
}

// ValidateCreateTableRequest は、CreateTableリクエストの基本的な検証を行います
// これは、MockS3TablesServerConfigのValidateRequestコールバックとして使用できます
// Requirements: 1.4
func ValidateCreateTableRequest(expectedMethod, expectedPath string) func(r *http.Request, body []byte) error {
	return func(r *http.Request, body []byte) error {
		// HTTPメソッドを検証
		if r.Method != expectedMethod {
			return &ValidationError{
				Field:  "Method",
				Reason: "expected " + expectedMethod + ", got " + r.Method,
			}
		}

		// パスを検証
		if r.URL.Path != expectedPath {
			return &ValidationError{
				Field:  "Path",
				Reason: "expected " + expectedPath + ", got " + r.URL.Path,
			}
		}

		// Content-Typeヘッダーを検証
		contentType := r.Header.Get("Content-Type")
		if contentType != "application/json" {
			return &ValidationError{
				Field:  "Content-Type",
				Reason: "expected application/json, got " + contentType,
			}
		}

		// リクエストボディが空でないことを確認
		if len(body) == 0 {
			return &ValidationError{
				Field:  "Body",
				Reason: "request body is empty",
			}
		}

		return nil
	}
}

// ValidationError は、リクエスト検証エラーを表します
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return "validation failed for " + e.Field + ": " + e.Reason
}

// TestNewMockS3TablesServer_BasicFunctionality は、モックサーバーの基本機能をテストします
func TestNewMockS3TablesServer_BasicFunctionality(t *testing.T) {
	// リクエストとボディをキャプチャするための変数
	var capturedRequest *http.Request
	var capturedBody []byte

	// モックサーバーを作成
	mockServer := NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
			VersionToken: "version-token-1",
		},
		CaptureRequest: &capturedRequest,
		CaptureBody:    &capturedBody,
	})
	defer mockServer.Close()

	// テストリクエストを送信
	requestBody := CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Namespace:      "test-namespace",
		Name:           "test-table",
		Format:         "ICEBERG",
		Metadata: CreateTableMetadata{
			Iceberg: CreateTableIcebergMetadata{
				Schema: CreateTableSchema{
					Fields: []CustomSchemaField{
						{
							Name:     "timestamp",
							Type:     "timestamptz",
							Required: true,
						},
					},
				},
			},
		},
	}

	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	// HTTPリクエストを送信
	req, err := http.NewRequest("POST", mockServer.URL+"/tables", bytes.NewReader(requestBodyBytes))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
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
	expectedTableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"
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

	// リクエストボディがキャプチャされたことを確認
	if len(capturedBody) == 0 {
		t.Fatal("request body was not captured")
	}
}

// TestNewMockS3TablesServer_WithValidation は、リクエスト検証機能をテストします
func TestNewMockS3TablesServer_WithValidation(t *testing.T) {
	// モックサーバーを作成（リクエスト検証付き）
	mockServer := NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
			VersionToken: "version-token-1",
		},
		ValidateRequest: ValidateCreateTableRequest("POST", "/tables"),
	})
	defer mockServer.Close()

	// テストリクエストを送信
	requestBody := CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Namespace:      "test-namespace",
		Name:           "test-table",
		Format:         "ICEBERG",
		Metadata: CreateTableMetadata{
			Iceberg: CreateTableIcebergMetadata{
				Schema: CreateTableSchema{
					Fields: []CustomSchemaField{
						{
							Name:     "timestamp",
							Type:     "timestamptz",
							Required: true,
						},
					},
				},
			},
		},
	}

	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	// HTTPリクエストを送信
	req, err := http.NewRequest("POST", mockServer.URL+"/tables", bytes.NewReader(requestBodyBytes))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// レスポンスステータスコードを検証
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status code %d, got %d", http.StatusCreated, resp.StatusCode)
	}

	// 未使用の変数を使用
	_ = requestBodyBytes
}

// TestNewMockS3TablesServer_CaptureCreateTableRequest は、CreateTableRequestのキャプチャ機能をテストします
func TestNewMockS3TablesServer_CaptureCreateTableRequest(t *testing.T) {
	// CreateTableRequestをキャプチャするための変数
	var capturedCreateTableRequest *CreateTableRequest

	// モックサーバーを作成
	mockServer := NewMockS3TablesServer(t, MockS3TablesServerConfig{
		StatusCode: http.StatusCreated,
		ResponseBody: CreateTableResponse{
			TableARN:     "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table",
			VersionToken: "version-token-1",
		},
		CaptureCreateTableRequest: &capturedCreateTableRequest,
	})
	defer mockServer.Close()

	// テストリクエストを送信
	requestBody := CreateTableRequest{
		TableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Namespace:      "test-namespace",
		Name:           "test-table",
		Format:         "ICEBERG",
		Metadata: CreateTableMetadata{
			Iceberg: CreateTableIcebergMetadata{
				Schema: CreateTableSchema{
					Fields: []CustomSchemaField{
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
					},
				},
			},
		},
	}

	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	// HTTPリクエストを送信
	req, err := http.NewRequest("POST", mockServer.URL+"/tables", bytes.NewReader(requestBodyBytes))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// CreateTableRequestがキャプチャされたことを確認
	if capturedCreateTableRequest == nil {
		t.Fatal("CreateTableRequest was not captured")
	}

	// キャプチャされたリクエストを検証
	if capturedCreateTableRequest.TableBucketARN != requestBody.TableBucketARN {
		t.Errorf("expected TableBucketARN %s, got %s", requestBody.TableBucketARN, capturedCreateTableRequest.TableBucketARN)
	}

	if capturedCreateTableRequest.Namespace != requestBody.Namespace {
		t.Errorf("expected Namespace %s, got %s", requestBody.Namespace, capturedCreateTableRequest.Namespace)
	}

	if capturedCreateTableRequest.Name != requestBody.Name {
		t.Errorf("expected Name %s, got %s", requestBody.Name, capturedCreateTableRequest.Name)
	}

	// スキーマフィールドを検証
	fields := capturedCreateTableRequest.Metadata.Iceberg.Schema.Fields
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}

	// プリミティブ型のフィールドを検証
	if fields[0].Name != "timestamp" {
		t.Errorf("expected field name timestamp, got %s", fields[0].Name)
	}

	// map型のフィールドを検証
	if fields[1].Name != "attributes" {
		t.Errorf("expected field name attributes, got %s", fields[1].Name)
	}

	// map型が構造化オブジェクトとして格納されていることを確認
	mapType, ok := fields[1].Type.(map[string]interface{})
	if !ok {
		t.Fatalf("expected attributes field Type to be map[string]interface{}, got %T", fields[1].Type)
	}

	if mapType["type"] != "map" {
		t.Errorf("expected type 'map', got %v", mapType["type"])
	}
}

// TestNewSuccessfulMockS3TablesServer は、成功レスポンスを返すモックサーバーをテストします
func TestNewSuccessfulMockS3TablesServer(t *testing.T) {
	// モックサーバーを作成
	tableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"
	versionToken := "version-token-1"
	mockServer := NewSuccessfulMockS3TablesServer(t, tableARN, versionToken)
	defer mockServer.Close()

	// HTTPリクエストを送信
	resp, err := http.Post(mockServer.URL+"/tables", "application/json", bytes.NewReader([]byte("{}")))
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
	if createResp.TableARN != tableARN {
		t.Errorf("expected TableARN %s, got %s", tableARN, createResp.TableARN)
	}

	if createResp.VersionToken != versionToken {
		t.Errorf("expected VersionToken %s, got %s", versionToken, createResp.VersionToken)
	}
}

// TestNewErrorMockS3TablesServer は、エラーレスポンスを返すモックサーバーをテストします
func TestNewErrorMockS3TablesServer(t *testing.T) {
	// モックサーバーを作成
	statusCode := http.StatusBadRequest
	errorMessage := "BadRequestException: The specified metadata is not valid"
	mockServer := NewErrorMockS3TablesServer(t, statusCode, errorMessage)
	defer mockServer.Close()

	// HTTPリクエストを送信
	resp, err := http.Post(mockServer.URL+"/tables", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	// レスポンスステータスコードを検証
	if resp.StatusCode != statusCode {
		t.Errorf("expected status code %d, got %d", statusCode, resp.StatusCode)
	}

	// レスポンスボディを読み取る
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	// レスポンスボディをパース
	var errorResp map[string]string
	if err := json.Unmarshal(respBody, &errorResp); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	// エラーメッセージを検証
	if errorResp["error"] != errorMessage {
		t.Errorf("expected error message %s, got %s", errorMessage, errorResp["error"])
	}
}
