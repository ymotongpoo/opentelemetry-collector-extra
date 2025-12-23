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
}

// NewMockS3TablesServer は、モックS3 Tables APIサーバーを作成します
// このサーバーは、リクエストを検証し、設定されたレスポンスを返します
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

// ValidationError は、リクエスト検証エラーを表します
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return "validation failed for " + e.Field + ": " + e.Reason
}

