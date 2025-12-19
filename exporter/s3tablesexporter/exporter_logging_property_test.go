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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// logEntry represents a captured log entry
// キャプチャされたログエントリを表す
type logEntry struct {
	level   slog.Level
	message string
	attrs   map[string]interface{}
}

// testLogHandler is a custom slog.Handler that captures log entries
// ログエントリをキャプチャするカスタムslog.Handler
type testLogHandler struct {
	mu      sync.Mutex
	entries []logEntry
}

func newTestLogHandler() *testLogHandler {
	return &testLogHandler{
		entries: make([]logEntry, 0),
	}
}

func (h *testLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *testLogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	attrs := make(map[string]interface{})
	record.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})

	h.entries = append(h.entries, logEntry{
		level:   record.Level,
		message: record.Message,
		attrs:   attrs,
	})

	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *testLogHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *testLogHandler) getEntries() []logEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logEntry{}, h.entries...)
}

func (h *testLogHandler) clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = make([]logEntry, 0)
}

// TestProperty_LogOutputCompleteness tests log output completeness property
// Feature: iceberg-snapshot-commit, Property 8: ログ出力の完全性
// Validates: Requirements 3.1, 3.2
func TestProperty_LogOutputCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のスナップショットコミットに対して、システムはTable Name、データファイル数、
	// データサイズを含むログメッセージを出力し、成功時には新しいスナップショットIDを含むログを出力するべきである

	// テストケース1: 成功時のログ出力の完全性
	t.Run("successful_commit_log_completeness", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（1-10個）
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			totalDataSize := int64(0)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
				// ランダムなファイルサイズを生成（1KB-1MB）
				fileSize := int64(1024 + (i*j)%1048576)
				totalDataSize += fileSize
			}

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

			// カスタムログハンドラーを設定
			logHandler := newTestLogHandler()
			exporter.logger = slog.New(logHandler)

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)
			existingMetadata.CurrentSnapshotID = -1

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-initial.metadata.json", i)
			versionToken := fmt.Sprintf("version-token-%d", i)
			newVersionToken := fmt.Sprintf("version-token-%d-new", i)

			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			namespace := "test-namespace"
			tableName := fmt.Sprintf("test-table-%d", i)
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), namespace, tableName, tableInfo, dataFilePaths, totalDataSize)
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// ログエントリを取得
			entries := logHandler.getEntries()

			// 要件3.1: スナップショットコミット開始時のログ（Table Name、データファイル数、データサイズ）
			foundStartLog := false
			for _, entry := range entries {
				if strings.Contains(entry.message, "Starting snapshot commit") {
					foundStartLog = true

					// Table Nameが含まれているか確認
					if tableAttr, ok := entry.attrs["table"]; !ok || tableAttr != tableName {
						t.Errorf("iteration %d: start log missing or incorrect 'table' attribute: expected '%s', got '%v'", i, tableName, tableAttr)
					}

					// データファイル数が含まれているか確認
					if _, ok := entry.attrs["data_files_count"]; !ok {
						t.Errorf("iteration %d: start log missing 'data_files_count' attribute", i)
					}

					// データサイズが含まれているか確認
					if _, ok := entry.attrs["total_data_size"]; !ok {
						t.Errorf("iteration %d: start log missing 'total_data_size' attribute", i)
					}

					break
				}
			}

			if !foundStartLog {
				t.Errorf("iteration %d: start log not found", i)
			}

			// 要件3.2: スナップショットコミット成功時のログ（新しいスナップショットID）
			foundSuccessLog := false
			for _, entry := range entries {
				if strings.Contains(entry.message, "Successfully committed snapshot") {
					foundSuccessLog = true

					// Table Nameが含まれているか確認
					if tableAttr, ok := entry.attrs["table"]; !ok || tableAttr != tableName {
						t.Errorf("iteration %d: success log missing or incorrect 'table' attribute: expected '%s', got '%v'", i, tableName, tableAttr)
					}

					// スナップショットIDが含まれているか確認
					if snapshotIDAttr, ok := entry.attrs["snapshot_id"]; !ok {
						t.Errorf("iteration %d: success log missing 'snapshot_id' attribute", i)
					} else {
						// スナップショットIDが有効な値であることを確認（-1以外）
						if snapshotID, ok := snapshotIDAttr.(int64); !ok || snapshotID == -1 {
							t.Errorf("iteration %d: success log has invalid 'snapshot_id': %v", i, snapshotIDAttr)
						}
					}

					// データファイル数が含まれているか確認
					if _, ok := entry.attrs["data_files_count"]; !ok {
						t.Errorf("iteration %d: success log missing 'data_files_count' attribute", i)
					}

					// データサイズが含まれているか確認
					if _, ok := entry.attrs["total_data_size"]; !ok {
						t.Errorf("iteration %d: success log missing 'total_data_size' attribute", i)
					}

					break
				}
			}

			if !foundSuccessLog {
				t.Errorf("iteration %d: success log not found", i)
			}
		}
	})

	// テストケース2: 失敗時のログ出力の完全性（要件3.3）
	t.Run("failed_commit_log_completeness", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（1-5個）
			numFiles := 1 + (i % 5)
			dataFilePaths := make([]string, numFiles)
			totalDataSize := int64(0)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
				fileSize := int64(1024 + (i*j)%1048576)
				totalDataSize += fileSize
			}

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

			// カスタムログハンドラーを設定
			logHandler := newTestLogHandler()
			exporter.logger = slog.New(logHandler)

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)
			existingMetadata.CurrentSnapshotID = -1

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定（エラーを返す）
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-initial.metadata.json", i)
			versionToken := fmt.Sprintf("version-token-%d", i)

			// 異なるエラーケースをテスト
			var expectedErrorType string
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					// エラーを返す
					expectedErrorType = "UpdateTableMetadataLocation"
					return nil, fmt.Errorf("simulated UpdateTableMetadataLocation error")
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			namespace := "test-namespace"
			tableName := fmt.Sprintf("test-table-%d", i)
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// スナップショットをコミット（エラーが発生するはず）
			err = exporter.commitSnapshot(context.Background(), namespace, tableName, tableInfo, dataFilePaths, totalDataSize)
			if err == nil {
				t.Fatalf("iteration %d: commitSnapshot() should have failed but succeeded", i)
			}

			// ログエントリを取得
			entries := logHandler.getEntries()

			// 要件3.3: スナップショットコミット失敗時のログ（エラーメッセージと理由）
			foundErrorLog := false
			for _, entry := range entries {
				if entry.level == slog.LevelError && strings.Contains(entry.message, "Failed to") {
					foundErrorLog = true

					// Table Nameが含まれているか確認
					if tableAttr, ok := entry.attrs["table"]; ok && tableAttr != tableName {
						t.Errorf("iteration %d: error log has incorrect 'table' attribute: expected '%s', got '%v'", i, tableName, tableAttr)
					}

					// データファイル数が含まれているか確認（エラーログでは必須ではない）
					// 属性が存在する場合のみチェック

					// データサイズが含まれているか確認（エラーログでは必須ではない）
					// 属性が存在する場合のみチェック

					// エラー情報が含まれているか確認
					if errorAttr, ok := entry.attrs["error"]; !ok {
						t.Errorf("iteration %d: error log missing 'error' attribute", i)
					} else {
						// エラーメッセージが空でないことを確認
						if errorStr, ok := errorAttr.(error); !ok || errorStr.Error() == "" {
							t.Errorf("iteration %d: error log has empty or invalid 'error' attribute: %v", i, errorAttr)
						}
					}

					break
				}
			}

			if !foundErrorLog {
				t.Errorf("iteration %d: error log not found (expected error type: %s)", i, expectedErrorType)
			}
		}
	})

	// テストケース3: 異なるエラーケースでのログ出力
	t.Run("different_error_cases_log_completeness", func(t *testing.T) {
		testCases := []struct {
			name                string
			setupMock           func(*mockS3TablesClient, *mockS3Client)
			expectedErrorInLogs string
		}{
			{
				name: "metadata_retrieval_error",
				setupMock: func(s3tablesClient *mockS3TablesClient, s3Client *mockS3Client) {
					s3tablesClient.getTableMetadataLocationFunc = func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
						return nil, fmt.Errorf("simulated GetTableMetadataLocation error")
					}
				},
				expectedErrorInLogs: "Failed to get table metadata",
			},
			{
				name: "metadata_upload_error",
				setupMock: func(s3tablesClient *mockS3TablesClient, s3Client *mockS3Client) {
					metadataLocation := "s3://test-bucket/metadata/test.metadata.json"
					versionToken := "test-version-token"
					s3tablesClient.getTableMetadataLocationFunc = func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
						return &s3tables.GetTableMetadataLocationOutput{
							MetadataLocation: &metadataLocation,
							VersionToken:     &versionToken,
						}, nil
					}
					s3Client.putObjectFunc = func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						return nil, fmt.Errorf("simulated PutObject error")
					}
				},
				expectedErrorInLogs: "Failed to upload metadata",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
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

				// カスタムログハンドラーを設定
				logHandler := newTestLogHandler()
				exporter.logger = slog.New(logHandler)

				// 既存のメタデータを作成
				existingMetadata := generateRandomMetadata(0)
				metadataJSON, _ := json.Marshal(existingMetadata)

				// モッククライアントを設定
				mockS3TablesClient := &mockS3TablesClient{}
				mockS3Client := &mockS3Client{
					getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(metadataJSON)),
						}, nil
					},
				}

				tc.setupMock(mockS3TablesClient, mockS3Client)

				exporter.s3TablesClient = mockS3TablesClient
				exporter.s3Client = mockS3Client

				// テーブル情報を作成
				tableInfo := &TableInfo{
					TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
					WarehouseLocation: "s3://test-bucket",
					VersionToken:      "test-version-token",
				}

				// スナップショットをコミット（エラーが発生するはず）
				dataFilePaths := []string{"s3://test-bucket/data/file1.parquet"}
				err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, 1024)
				if err == nil {
					t.Fatal("commitSnapshot() should have failed but succeeded")
				}

				// ログエントリを取得
				entries := logHandler.getEntries()

				// エラーログが含まれているか確認
				foundExpectedError := false
				for _, entry := range entries {
					if entry.level == slog.LevelError && strings.Contains(entry.message, tc.expectedErrorInLogs) {
						foundExpectedError = true

						// エラー情報が含まれているか確認
						if _, ok := entry.attrs["error"]; !ok {
							t.Errorf("error log missing 'error' attribute")
						}

						break
					}
				}

				if !foundExpectedError {
					t.Errorf("expected error log with message '%s' not found", tc.expectedErrorInLogs)
				}
			})
		}
	})
}
