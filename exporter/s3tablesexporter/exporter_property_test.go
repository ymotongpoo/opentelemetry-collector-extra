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
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// mockS3TablesClient is a mock implementation of S3 Tables client for testing
type mockS3TablesClient struct {
	getTableFunc                 func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error)
	createTableFunc              func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error)
	getNamespaceFunc             func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error)
	createNamespaceFunc          func(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error)
	getTableMetadataLocationFunc func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error)
}

func (m *mockS3TablesClient) GetTable(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	if m.getTableFunc != nil {
		return m.getTableFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetTable not implemented")
}

func (m *mockS3TablesClient) CreateTable(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
	if m.createTableFunc != nil {
		return m.createTableFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("CreateTable not implemented")
}

func (m *mockS3TablesClient) GetNamespace(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
	if m.getNamespaceFunc != nil {
		return m.getNamespaceFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetNamespace not implemented")
}

func (m *mockS3TablesClient) CreateNamespace(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error) {
	if m.createNamespaceFunc != nil {
		return m.createNamespaceFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("CreateNamespace not implemented")
}

func (m *mockS3TablesClient) GetTableMetadataLocation(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
	if m.getTableMetadataLocationFunc != nil {
		return m.getTableMetadataLocationFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetTableMetadataLocation not implemented")
}

// mockS3Client is a mock implementation of S3 client for testing
type mockS3Client struct {
	putObjectFunc func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	getObjectFunc func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putObjectFunc != nil {
		return m.putObjectFunc(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getObjectFunc != nil {
		return m.getObjectFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetObject not implemented")
}

// TestUploadToWarehouseLocation tests uploading data to warehouse location
// Requirements: 1.3, 2.3
func TestUploadToWarehouseLocation(t *testing.T) {
	tests := []struct {
		name              string
		warehouseLocation string
		data              []byte
		dataType          string
		mockFunc          func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
		expectError       bool
		validateFunc      func(t *testing.T, params *s3.PutObjectInput)
	}{
		{
			name:              "successful upload",
			warehouseLocation: "s3://test-warehouse-bucket",
			data:              []byte("test parquet data"),
			dataType:          "metrics",
			mockFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
				return &s3.PutObjectOutput{}, nil
			},
			expectError: false,
			validateFunc: func(t *testing.T, params *s3.PutObjectInput) {
				if *params.Bucket != "test-warehouse-bucket" {
					t.Errorf("expected bucket 'test-warehouse-bucket', got '%s'", *params.Bucket)
				}
				if len(*params.Key) < 5 || (*params.Key)[:5] != "data/" {
					t.Errorf("expected key to start with 'data/', got '%s'", *params.Key)
				}
			},
		},
		{
			name:              "upload with complex warehouse location",
			warehouseLocation: "s3://63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3",
			data:              []byte("test data"),
			dataType:          "traces",
			mockFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
				return &s3.PutObjectOutput{}, nil
			},
			expectError: false,
			validateFunc: func(t *testing.T, params *s3.PutObjectInput) {
				expectedBucket := "63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3"
				if *params.Bucket != expectedBucket {
					t.Errorf("expected bucket '%s', got '%s'", expectedBucket, *params.Bucket)
				}
			},
		},
		{
			name:              "S3 PutObject error",
			warehouseLocation: "s3://test-bucket",
			data:              []byte("test data"),
			dataType:          "logs",
			mockFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
				return nil, fmt.Errorf("S3 PutObject failed")
			},
			expectError: true,
		},
		{
			name:              "invalid warehouse location",
			warehouseLocation: "invalid-location",
			data:              []byte("test data"),
			dataType:          "metrics",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// モックS3クライアントを設定
			var capturedParams *s3.PutObjectInput
			mockClient := &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					capturedParams = params
					if tt.mockFunc != nil {
						return tt.mockFunc(ctx, params, optFns...)
					}
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockClient

			// アップロードを実行
			s3Path, err := exporter.uploadToWarehouseLocation(context.Background(), tt.warehouseLocation, tt.data, tt.dataType)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if s3Path == "" {
					t.Error("expected non-empty S3 path")
				}
				// S3パスが正しい形式であることを確認
				if len(s3Path) < 5 || s3Path[:5] != "s3://" {
					t.Errorf("expected S3 path to start with 's3://', got '%s'", s3Path)
				}
				// バリデーション関数が提供されている場合は実行
				if tt.validateFunc != nil && capturedParams != nil {
					tt.validateFunc(t, capturedParams)
				}
			}
		})
	}
}

// TestUploadToWarehouseLocation_ContextCancellation tests context cancellation handling
// Requirements: 2.6, 5.4
func TestUploadToWarehouseLocation_ContextCancellation(t *testing.T) {
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

	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// アップロードを試行
	_, err = exporter.uploadToWarehouseLocation(ctx, "s3://test-bucket", []byte("test data"), "metrics")
	if err == nil {
		t.Error("expected error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	expectedMsg := "upload cancelled"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestProperty_WarehouseLocationExtraction tests warehouse location extraction property
// Feature: s3tables-upload-implementation, Property 2: Warehouse Location抽出の正確性
// Validates: Requirements 1.2
func TestProperty_WarehouseLocationExtraction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の有効なwarehouse location文字列（s3://bucket-name形式）に対して、
	// バケット名抽出関数は正しいバケット名を返すべきである

	// テストケース: 様々な有効なwarehouse locationを生成してテスト
	testCases := []struct {
		bucketName  string
		path        string
		description string
	}{
		{
			bucketName:  "simple-bucket",
			path:        "",
			description: "シンプルなバケット名",
		},
		{
			bucketName:  "bucket-with-hyphens",
			path:        "",
			description: "ハイフンを含むバケット名",
		},
		{
			bucketName:  "bucket123",
			path:        "",
			description: "数字を含むバケット名",
		},
		{
			bucketName:  "63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3",
			path:        "",
			description: "複雑なバケット名（S3 Tablesの実際の形式）",
		},
		{
			bucketName:  "my-warehouse-bucket",
			path:        "/data/path",
			description: "パスを含むwarehouse location",
		},
		{
			bucketName:  "test-bucket-01",
			path:        "/namespace/table/data",
			description: "複数階層のパスを含むwarehouse location",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// warehouse locationを構築
			warehouseLocation := fmt.Sprintf("s3://%s%s", tc.bucketName, tc.path)

			// バケット名を抽出
			extractedBucket, err := extractBucketFromWarehouseLocation(warehouseLocation)
			if err != nil {
				t.Errorf("extractBucketFromWarehouseLocation() failed for '%s': %v", warehouseLocation, err)
				return
			}

			// 抽出されたバケット名が期待値と一致することを確認
			if extractedBucket != tc.bucketName {
				t.Errorf("expected bucket '%s', got '%s' for warehouse location '%s'",
					tc.bucketName, extractedBucket, warehouseLocation)
			}
		})
	}

	// 追加のランダムテスト: 100回の反復
	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなバケット名を生成（英数字とハイフン）
		bucketName := generateRandomBucketName()
		warehouseLocation := fmt.Sprintf("s3://%s", bucketName)

		extractedBucket, err := extractBucketFromWarehouseLocation(warehouseLocation)
		if err != nil {
			t.Errorf("iteration %d: extractBucketFromWarehouseLocation() failed for '%s': %v",
				i, warehouseLocation, err)
			continue
		}

		if extractedBucket != bucketName {
			t.Errorf("iteration %d: expected bucket '%s', got '%s'",
				i, bucketName, extractedBucket)
		}
	}
}

// generateRandomBucketName generates a random valid S3 bucket name
// ランダムな有効なS3バケット名を生成
func generateRandomBucketName() string {
	// S3バケット名の規則:
	// - 3-63文字
	// - 小文字、数字、ハイフンのみ
	// - 先頭と末尾は英数字
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const charsetWithHyphen = "abcdefghijklmnopqrstuvwxyz0123456789-"

	// 長さをランダムに決定（10-30文字）
	length := 10 + (len(charset) % 21)

	result := make([]byte, length)
	// 先頭は英数字
	result[0] = charset[len(charset)%len(charset)]

	// 中間はハイフンも含む
	for i := 1; i < length-1; i++ {
		result[i] = charsetWithHyphen[len(charsetWithHyphen)%len(charsetWithHyphen)]
	}

	// 末尾は英数字
	result[length-1] = charset[len(charset)%len(charset)]

	return string(result)
}

// TestProperty_ObjectKeyUniqueness tests object key uniqueness property
// Feature: s3tables-upload-implementation, Property 3: オブジェクトキーの一意性
// Validates: Requirements 1.3
func TestProperty_ObjectKeyUniqueness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の2つの連続したアップロード呼び出しに対して、
	// 生成されるオブジェクトキーは異なるべきである

	// テストケース1: 同じデータタイプで連続して生成
	dataTypes := []string{"metrics", "traces", "logs"}
	for _, dataType := range dataTypes {
		t.Run(fmt.Sprintf("consecutive_calls_for_%s", dataType), func(t *testing.T) {
			keys := make(map[string]bool)
			iterations := 100

			for i := 0; i < iterations; i++ {
				key := generateDataFileKey(dataType)

				// キーが既に存在する場合はエラー
				if keys[key] {
					t.Errorf("duplicate key generated: '%s' (iteration %d)", key, i)
				}
				keys[key] = true

				// キーの形式を検証
				if len(key) < 5 || key[:5] != "data/" {
					t.Errorf("invalid key format: '%s' (expected to start with 'data/')", key)
				}
				if len(key) < 8 || key[len(key)-8:] != ".parquet" {
					t.Errorf("invalid key format: '%s' (expected to end with '.parquet')", key)
				}
			}

			// すべてのキーが一意であることを確認
			if len(keys) != iterations {
				t.Errorf("expected %d unique keys, got %d", iterations, len(keys))
			}
		})
	}

	// テストケース2: 異なるデータタイプで生成されたキーも一意であることを確認
	t.Run("keys_across_data_types_are_unique", func(t *testing.T) {
		keys := make(map[string]bool)
		iterations := 100

		for i := 0; i < iterations; i++ {
			// 各データタイプでキーを生成
			for _, dataType := range dataTypes {
				key := generateDataFileKey(dataType)

				// キーが既に存在する場合はエラー
				if keys[key] {
					t.Errorf("duplicate key generated across data types: '%s' (iteration %d, data_type %s)",
						key, i, dataType)
				}
				keys[key] = true
			}
		}

		expectedTotal := iterations * len(dataTypes)
		if len(keys) != expectedTotal {
			t.Errorf("expected %d unique keys across all data types, got %d", expectedTotal, len(keys))
		}
	})

	// テストケース3: 並行生成でも一意性が保たれることを確認
	t.Run("concurrent_key_generation", func(t *testing.T) {
		keys := make(map[string]bool)
		keysChan := make(chan string, 100)
		done := make(chan bool)

		// 複数のゴルーチンで並行してキーを生成
		goroutines := 10
		keysPerGoroutine := 10

		for g := 0; g < goroutines; g++ {
			go func() {
				for i := 0; i < keysPerGoroutine; i++ {
					key := generateDataFileKey("metrics")
					keysChan <- key
				}
			}()
		}

		// キーを収集
		go func() {
			expectedKeys := goroutines * keysPerGoroutine
			for i := 0; i < expectedKeys; i++ {
				key := <-keysChan
				if keys[key] {
					t.Errorf("duplicate key generated in concurrent test: '%s'", key)
				}
				keys[key] = true
			}
			done <- true
		}()

		<-done

		expectedTotal := goroutines * keysPerGoroutine
		if len(keys) != expectedTotal {
			t.Errorf("expected %d unique keys in concurrent test, got %d", expectedTotal, len(keys))
		}
	})

	// テストケース4: タイムスタンプとUUIDの組み合わせが一意性を保証することを確認
	t.Run("timestamp_and_uuid_combination", func(t *testing.T) {
		keys := make(map[string]bool)
		iterations := 1000 // より多くの反復でテスト

		for i := 0; i < iterations; i++ {
			key := generateDataFileKey("metrics")

			// キーが既に存在する場合はエラー
			if keys[key] {
				t.Errorf("duplicate key generated: '%s' (iteration %d)", key, i)
			}
			keys[key] = true

			// キーの構造を検証
			// 形式: data/{timestamp}-{uuid}.parquet
			// タイムスタンプ: 20060102-150405 (15文字)
			// UUID: 36文字
			// 最小長: 5 (data/) + 15 (timestamp) + 1 (-) + 36 (uuid) + 8 (.parquet) = 65
			if len(key) < 65 {
				t.Errorf("key length too short: %d (expected at least 65) for key '%s'", len(key), key)
			}
		}

		// すべてのキーが一意であることを確認
		if len(keys) != iterations {
			t.Errorf("expected %d unique keys, got %d", iterations, len(keys))
		}
	})
}

// TestUpdateTableMetadata tests the updateTableMetadata function
// Requirements: 1.4
func TestUpdateTableMetadata(t *testing.T) {
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

	// テーブル情報を作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket",
		VersionToken:      "test-version-token",
	}

	// メタデータ更新を実行
	err = exporter.updateTableMetadata(context.Background(), tableInfo, "s3://test-warehouse-bucket/data/test-file.parquet")
	if err != nil {
		t.Errorf("updateTableMetadata() failed: %v", err)
	}
}

// TestUpdateTableMetadata_ContextCancellation tests context cancellation handling
// Requirements: 2.6, 5.4
func TestUpdateTableMetadata_ContextCancellation(t *testing.T) {
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

	// テーブル情報を作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket",
		VersionToken:      "test-version-token",
	}

	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// メタデータ更新を試行
	err = exporter.updateTableMetadata(ctx, tableInfo, "s3://test-warehouse-bucket/data/test-file.parquet")
	if err == nil {
		t.Error("expected error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	expectedMsg := "metadata update cancelled"
	if len(err.Error()) < len(expectedMsg) || err.Error()[:len(expectedMsg)] != expectedMsg {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestUpdateTableMetadata_LogsDebugMessage tests that debug logs are output
// Requirements: 1.4
func TestUpdateTableMetadata_LogsDebugMessage(t *testing.T) {
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

	// カスタムロガーを設定してログメッセージをキャプチャ
	capture := &logCapture{}
	exporter.logger = slog.New(capture)

	// テーブル情報を作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket",
		VersionToken:      "test-version-token",
	}

	// メタデータ更新を実行
	err = exporter.updateTableMetadata(context.Background(), tableInfo, "s3://test-warehouse-bucket/data/test-file.parquet")
	if err != nil {
		t.Errorf("updateTableMetadata() failed: %v", err)
	}

	// デバッグログが出力されたことを確認
	found := false
	for _, msg := range capture.messages {
		if msg.level == slog.LevelDebug && msg.message == "Data file uploaded to warehouse location, S3 Tables will automatically manage metadata" {
			found = true
			// 必要な属性が含まれることを確認
			if msg.attrs["table_arn"] != tableInfo.TableARN {
				t.Errorf("expected table_arn '%s', got '%v'", tableInfo.TableARN, msg.attrs["table_arn"])
			}
			if msg.attrs["data_file_path"] != "s3://test-warehouse-bucket/data/test-file.parquet" {
				t.Errorf("expected data_file_path 's3://test-warehouse-bucket/data/test-file.parquet', got '%v'", msg.attrs["data_file_path"])
			}
			break
		}
	}
	if !found {
		t.Error("expected debug log message about S3 Tables automatic metadata management")
	}
}

// TestProperty_ErrorPropagation tests error propagation property
// Feature: s3tables-upload-implementation, Property 4: エラー伝播
// Validates: Requirements 1.6, 5.1, 5.2
func TestProperty_ErrorPropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のS3 Tables APIまたはS3 PutObject APIエラーに対して、
	// アップロード関数はエラーをラップして返すべきである

	// テストケース1: S3 Tables APIエラーの伝播
	t.Run("S3_Tables_API_errors_are_propagated", func(t *testing.T) {
		// 様々なS3 Tables APIエラーを生成してテスト
		apiErrors := []struct {
			name      string
			errorMsg  string
			operation string
		}{
			{
				name:      "GetTable error",
				errorMsg:  "table not found",
				operation: "GetTable",
			},
			{
				name:      "CreateTable error",
				errorMsg:  "insufficient permissions",
				operation: "CreateTable",
			},
			{
				name:      "CreateNamespace error",
				errorMsg:  "quota exceeded",
				operation: "CreateNamespace",
			},
			{
				name:      "GetNamespace error",
				errorMsg:  "namespace not found",
				operation: "GetNamespace",
			},
		}

		for _, apiErr := range apiErrors {
			t.Run(apiErr.name, func(t *testing.T) {
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

				// モックS3 Tablesクライアントを設定してエラーを返す
				mockClient := &mockS3TablesClient{
					getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
						if apiErr.operation == "GetTable" {
							return nil, fmt.Errorf("%s", apiErr.errorMsg)
						}
						return nil, fmt.Errorf("table not found")
					},
					createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
						if apiErr.operation == "CreateTable" {
							return nil, fmt.Errorf("%s", apiErr.errorMsg)
						}
						return nil, fmt.Errorf("CreateTable failed")
					},
					getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
						if apiErr.operation == "GetNamespace" {
							return nil, fmt.Errorf("%s", apiErr.errorMsg)
						}
						return nil, fmt.Errorf("namespace not found")
					},
					createNamespaceFunc: func(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error) {
						if apiErr.operation == "CreateNamespace" {
							return nil, fmt.Errorf("%s", apiErr.errorMsg)
						}
						return nil, fmt.Errorf("CreateNamespace failed")
					},
				}
				exporter.s3TablesClient = mockClient

				// アップロードを試行
				err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "metrics")
				if err == nil {
					t.Errorf("expected error from uploadToS3Tables for %s", apiErr.name)
					return
				}

				// エラーが適切にラップされていることを確認
				// エラーメッセージに元のエラーメッセージが含まれることを確認
				if len(err.Error()) == 0 {
					t.Error("error message should not be empty")
				}
			})
		}
	})

	// テストケース2: S3 PutObject APIエラーの伝播
	t.Run("S3_PutObject_API_errors_are_propagated", func(t *testing.T) {
		// 様々なS3 PutObject APIエラーを生成してテスト
		s3Errors := []struct {
			name     string
			errorMsg string
		}{
			{
				name:     "access denied",
				errorMsg: "AccessDenied: User does not have permission",
			},
			{
				name:     "bucket not found",
				errorMsg: "NoSuchBucket: The specified bucket does not exist",
			},
			{
				name:     "invalid request",
				errorMsg: "InvalidRequest: The request is invalid",
			},
			{
				name:     "service unavailable",
				errorMsg: "ServiceUnavailable: Service is temporarily unavailable",
			},
		}

		for _, s3Err := range s3Errors {
			t.Run(s3Err.name, func(t *testing.T) {
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

				// モックS3 Tablesクライアントを設定してテーブル情報を返す
				tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
				warehouseLocation := "s3://test-warehouse-bucket"
				versionToken := "test-version-token"

				mockS3TablesClient := &mockS3TablesClient{
					getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
						return &s3tables.GetTableOutput{
							TableARN:          &tableArn,
							WarehouseLocation: &warehouseLocation,
							VersionToken:      &versionToken,
						}, nil
					},
				}
				exporter.s3TablesClient = mockS3TablesClient

				// モックS3クライアントを設定してエラーを返す
				mockS3Client := &mockS3Client{
					putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						return nil, fmt.Errorf("%s", s3Err.errorMsg)
					},
				}
				exporter.s3Client = mockS3Client

				// アップロードを試行
				err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "metrics")
				if err == nil {
					t.Errorf("expected error from uploadToS3Tables for %s", s3Err.name)
					return
				}

				// エラーが適切にラップされていることを確認
				expectedSubstr := "failed to upload to warehouse location"
				if len(err.Error()) < len(expectedSubstr) {
					t.Errorf("error message should contain '%s', got '%s'", expectedSubstr, err.Error())
				}
			})
		}
	})

	// テストケース3: ランダムエラーメッセージでのエラー伝播
	t.Run("random_error_messages_are_propagated", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			// ランダムなエラーメッセージを生成
			randomErrorMsg := fmt.Sprintf("Random error %d: %s", i, generateRandomString(20))

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

			// モックS3 Tablesクライアントを設定してエラーを返す
			mockClient := &mockS3TablesClient{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					return nil, fmt.Errorf("%s", randomErrorMsg)
				},
				getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
					return &s3tables.GetNamespaceOutput{}, nil
				},
				createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
					return nil, fmt.Errorf("%s", randomErrorMsg)
				},
			}
			exporter.s3TablesClient = mockClient

			// アップロードを試行
			err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "metrics")
			if err == nil {
				t.Errorf("iteration %d: expected error from uploadToS3Tables", i)
				continue
			}

			// エラーが適切にラップされていることを確認
			if len(err.Error()) == 0 {
				t.Errorf("iteration %d: error message should not be empty", i)
			}
		}
	})
}

// generateRandomString generates a random string of the specified length
// 指定された長さのランダムな文字列を生成
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}

// TestProperty_ContextCancellation tests context cancellation handling property
// Feature: s3tables-upload-implementation, Property 5: コンテキストキャンセルの処理
// Validates: Requirements 2.6, 5.4
func TestProperty_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のキャンセルされたコンテキストに対して、
	// アップロード関数は適切にキャンセルを処理し、エラーを返すべきである

	// テストケース1: uploadToS3Tablesでのコンテキストキャンセル
	t.Run("uploadToS3Tables_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// アップロードを試行
			err = exporter.uploadToS3Tables(ctx, []byte("test data"), "metrics")
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "upload cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース2: uploadToWarehouseLocationでのコンテキストキャンセル
	t.Run("uploadToWarehouseLocation_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// アップロードを試行
			_, err = exporter.uploadToWarehouseLocation(ctx, "s3://test-bucket", []byte("test data"), "metrics")
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "upload cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース3: getTableでのコンテキストキャンセル
	t.Run("getTable_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// テーブル取得を試行
			_, err = exporter.getTable(ctx, "test-namespace", "test-table")
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "table retrieval cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース4: createTableでのコンテキストキャンセル
	t.Run("createTable_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// テーブル作成を試行
			schema := createMetricsSchema()
			_, err = exporter.createTable(ctx, "test-namespace", "test-table", schema)
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "table creation cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース5: createNamespaceIfNotExistsでのコンテキストキャンセル
	t.Run("createNamespaceIfNotExists_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Namespace作成を試行
			err = exporter.createNamespaceIfNotExists(ctx, "test-namespace")
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "namespace creation cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース6: updateTableMetadataでのコンテキストキャンセル
	t.Run("updateTableMetadata_handles_cancelled_context", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
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

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-warehouse-bucket",
				VersionToken:      "test-version-token",
			}

			// キャンセル済みのコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// メタデータ更新を試行
			err = exporter.updateTableMetadata(ctx, tableInfo, "s3://test-warehouse-bucket/data/test-file.parquet")
			if err == nil {
				t.Errorf("iteration %d: expected error when context is cancelled", i)
				continue
			}

			// エラーメッセージにコンテキストキャンセルが含まれることを確認
			expectedSubstr := "metadata update cancelled"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("iteration %d: error message should contain '%s', got '%s'", i, expectedSubstr, err.Error())
			}
		}
	})

	// テストケース7: 様々なデータタイプでのコンテキストキャンセル
	t.Run("context_cancellation_across_data_types", func(t *testing.T) {
		dataTypes := []string{"metrics", "traces", "logs"}
		iterations := 30

		for _, dataType := range dataTypes {
			for i := 0; i < iterations; i++ {
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
					t.Fatalf("dataType %s, iteration %d: newS3TablesExporter() failed: %v", dataType, i, err)
				}

				// キャンセル済みのコンテキストを作成
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				// アップロードを試行
				err = exporter.uploadToS3Tables(ctx, []byte("test data"), dataType)
				if err == nil {
					t.Errorf("dataType %s, iteration %d: expected error when context is cancelled", dataType, i)
					continue
				}

				// エラーメッセージにコンテキストキャンセルが含まれることを確認
				expectedSubstr := "upload cancelled"
				if len(err.Error()) < len(expectedSubstr) {
					t.Errorf("dataType %s, iteration %d: error message should contain '%s', got '%s'",
						dataType, i, expectedSubstr, err.Error())
				}
			}
		}
	})
}

// TestProperty_MetadataRetrievalConsistency tests metadata retrieval and update consistency
// Feature: iceberg-snapshot-commit, Property 10: メタデータ取得と更新の一貫性
// Validates: Requirements 9.1, 9.2
func TestProperty_MetadataRetrievalConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のテーブルに対して、GetTableMetadataLocation APIを使用して
	// メタデータロケーションとバージョントークンを取得し、S3 GetObject APIを使用して
	// メタデータファイルをダウンロードし、IcebergMetadata構造体に正しく解析できるべきである

	// テストケース1: 有効なメタデータファイルの取得と解析
	t.Run("valid_metadata_retrieval_and_parsing", func(t *testing.T) {
		// 様々なメタデータ構造をテスト
		testCases := []struct {
			name     string
			metadata IcebergMetadata
		}{
			{
				name: "basic_metadata",
				metadata: IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          "test-uuid-1",
					Location:           "s3://test-bucket/test-table",
					LastSequenceNumber: 0,
					LastUpdatedMS:      1234567890000,
					LastColumnID:       1,
					Schemas: []IcebergSchema{
						{
							SchemaID: 0,
							Fields: []IcebergSchemaField{
								{ID: 1, Name: "field1", Required: true, Type: "string"},
							},
						},
					},
					CurrentSchemaID: 0,
					PartitionSpecs: []IcebergPartitionSpec{
						{
							SpecID: 0,
							Fields: []IcebergPartitionField{},
						},
					},
					DefaultSpecID:     0,
					LastPartitionID:   0,
					Properties:        map[string]string{},
					CurrentSnapshotID: -1,
					Snapshots:         []IcebergSnapshot{},
					SnapshotLog:       []IcebergSnapshotLog{},
					MetadataLog:       []IcebergMetadataLog{},
				},
			},
			{
				name: "metadata_with_snapshot",
				metadata: IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          "test-uuid-2",
					Location:           "s3://test-bucket/test-table-2",
					LastSequenceNumber: 1,
					LastUpdatedMS:      1234567890000,
					LastColumnID:       2,
					Schemas: []IcebergSchema{
						{
							SchemaID: 0,
							Fields: []IcebergSchemaField{
								{ID: 1, Name: "field1", Required: true, Type: "string"},
								{ID: 2, Name: "field2", Required: false, Type: "long"},
							},
						},
					},
					CurrentSchemaID: 0,
					PartitionSpecs: []IcebergPartitionSpec{
						{
							SpecID: 0,
							Fields: []IcebergPartitionField{},
						},
					},
					DefaultSpecID:   0,
					LastPartitionID: 0,
					Properties:      map[string]string{"key1": "value1"},
					CurrentSnapshotID: 1234567890000,
					Snapshots: []IcebergSnapshot{
						{
							SnapshotID:     1234567890000,
							TimestampMS:    1234567890000,
							SequenceNumber: 1,
							Summary: map[string]string{
								"operation":      "append",
								"added-files":    "1",
								"added-records":  "100",
								"total-files":    "1",
								"total-records":  "100",
							},
							ManifestList: "s3://test-bucket/test-table-2/metadata/snap-1234567890000-1-manifest-list.avro",
						},
					},
					SnapshotLog: []IcebergSnapshotLog{
						{
							TimestampMS: 1234567890000,
							SnapshotID:  1234567890000,
						},
					},
					MetadataLog: []IcebergMetadataLog{
						{
							TimestampMS:  1234567890000,
							MetadataFile: "s3://test-bucket/test-table-2/metadata/00001-test-uuid.metadata.json",
						},
					},
				},
			},
			{
				name: "metadata_with_multiple_snapshots",
				metadata: IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          "test-uuid-3",
					Location:           "s3://test-bucket/test-table-3",
					LastSequenceNumber: 3,
					LastUpdatedMS:      1234567890000,
					LastColumnID:       3,
					Schemas: []IcebergSchema{
						{
							SchemaID: 0,
							Fields: []IcebergSchemaField{
								{ID: 1, Name: "field1", Required: true, Type: "string"},
								{ID: 2, Name: "field2", Required: false, Type: "long"},
								{ID: 3, Name: "field3", Required: false, Type: "double"},
							},
						},
					},
					CurrentSchemaID: 0,
					PartitionSpecs: []IcebergPartitionSpec{
						{
							SpecID: 0,
							Fields: []IcebergPartitionField{},
						},
					},
					DefaultSpecID:   0,
					LastPartitionID: 0,
					Properties:      map[string]string{"key1": "value1", "key2": "value2"},
					CurrentSnapshotID: 1234567892000,
					Snapshots: []IcebergSnapshot{
						{
							SnapshotID:     1234567890000,
							TimestampMS:    1234567890000,
							SequenceNumber: 1,
							Summary: map[string]string{
								"operation":      "append",
								"added-files":    "1",
								"added-records":  "100",
							},
							ManifestList: "s3://test-bucket/test-table-3/metadata/snap-1234567890000-1-manifest-list.avro",
						},
						{
							SnapshotID:     1234567891000,
							TimestampMS:    1234567891000,
							SequenceNumber: 2,
							Summary: map[string]string{
								"operation":      "append",
								"added-files":    "1",
								"added-records":  "200",
							},
							ManifestList: "s3://test-bucket/test-table-3/metadata/snap-1234567891000-2-manifest-list.avro",
						},
						{
							SnapshotID:     1234567892000,
							TimestampMS:    1234567892000,
							SequenceNumber: 3,
							Summary: map[string]string{
								"operation":      "append",
								"added-files":    "1",
								"added-records":  "300",
							},
							ManifestList: "s3://test-bucket/test-table-3/metadata/snap-1234567892000-3-manifest-list.avro",
						},
					},
					SnapshotLog: []IcebergSnapshotLog{
						{TimestampMS: 1234567890000, SnapshotID: 1234567890000},
						{TimestampMS: 1234567891000, SnapshotID: 1234567891000},
						{TimestampMS: 1234567892000, SnapshotID: 1234567892000},
					},
					MetadataLog: []IcebergMetadataLog{
						{
							TimestampMS:  1234567890000,
							MetadataFile: "s3://test-bucket/test-table-3/metadata/00001-test-uuid.metadata.json",
						},
						{
							TimestampMS:  1234567891000,
							MetadataFile: "s3://test-bucket/test-table-3/metadata/00002-test-uuid.metadata.json",
						},
						{
							TimestampMS:  1234567892000,
							MetadataFile: "s3://test-bucket/test-table-3/metadata/00003-test-uuid.metadata.json",
						},
					},
				},
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

				// メタデータをJSONにシリアライズ
				metadataJSON, err := json.Marshal(tc.metadata)
				if err != nil {
					t.Fatalf("failed to marshal metadata: %v", err)
				}

				// モックS3 Tablesクライアントを設定
				metadataLocation := "s3://test-bucket/metadata/00001-test-uuid.metadata.json"
				versionToken := "test-version-token"
				mockS3TablesClient := &mockS3TablesClient{
					getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
						return &s3tables.GetTableMetadataLocationOutput{
							MetadataLocation: &metadataLocation,
							VersionToken:     &versionToken,
						}, nil
					},
				}
				exporter.s3TablesClient = mockS3TablesClient

				// モックS3クライアントを設定
				mockS3Client := &mockS3Client{
					getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						// バケット名とキーを検証
						if *params.Bucket != "test-bucket" {
							t.Errorf("expected bucket 'test-bucket', got '%s'", *params.Bucket)
						}
						if *params.Key != "metadata/00001-test-uuid.metadata.json" {
							t.Errorf("expected key 'metadata/00001-test-uuid.metadata.json', got '%s'", *params.Key)
						}

						// メタデータJSONを返す
						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(metadataJSON)),
						}, nil
					},
				}
				exporter.s3Client = mockS3Client

				// テーブル情報を作成
				tableInfo := &TableInfo{
					TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
					WarehouseLocation: "s3://test-bucket",
					VersionToken:      "old-version-token",
				}

				// メタデータを取得
				retrievedMetadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "test-table", tableInfo)
				if err != nil {
					t.Fatalf("getTableMetadata() failed: %v", err)
				}

				// 取得したメタデータが元のメタデータと一致することを確認
				if retrievedMetadata.FormatVersion != tc.metadata.FormatVersion {
					t.Errorf("expected FormatVersion %d, got %d", tc.metadata.FormatVersion, retrievedMetadata.FormatVersion)
				}
				if retrievedMetadata.TableUUID != tc.metadata.TableUUID {
					t.Errorf("expected TableUUID '%s', got '%s'", tc.metadata.TableUUID, retrievedMetadata.TableUUID)
				}
				if retrievedMetadata.Location != tc.metadata.Location {
					t.Errorf("expected Location '%s', got '%s'", tc.metadata.Location, retrievedMetadata.Location)
				}
				if retrievedMetadata.CurrentSnapshotID != tc.metadata.CurrentSnapshotID {
					t.Errorf("expected CurrentSnapshotID %d, got %d", tc.metadata.CurrentSnapshotID, retrievedMetadata.CurrentSnapshotID)
				}
				if len(retrievedMetadata.Snapshots) != len(tc.metadata.Snapshots) {
					t.Errorf("expected %d snapshots, got %d", len(tc.metadata.Snapshots), len(retrievedMetadata.Snapshots))
				}

				// バージョントークンが更新されたことを確認
				if tableInfo.VersionToken != versionToken {
					t.Errorf("expected VersionToken to be updated to '%s', got '%s'", versionToken, tableInfo.VersionToken)
				}
			})
		}
	})

	// テストケース2: ランダムなメタデータ構造での取得と解析
	t.Run("random_metadata_structures", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなメタデータを生成
			metadata := generateRandomMetadata(i)

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

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(metadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-test-uuid.metadata.json", i)
			versionToken := fmt.Sprintf("version-token-%d", i)
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
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
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "old-version-token",
			}

			// メタデータを取得
			retrievedMetadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "test-table", tableInfo)
			if err != nil {
				t.Fatalf("iteration %d: getTableMetadata() failed: %v", i, err)
			}

			// 取得したメタデータが元のメタデータと一致することを確認
			if retrievedMetadata.FormatVersion != metadata.FormatVersion {
				t.Errorf("iteration %d: expected FormatVersion %d, got %d", i, metadata.FormatVersion, retrievedMetadata.FormatVersion)
			}
			if retrievedMetadata.TableUUID != metadata.TableUUID {
				t.Errorf("iteration %d: expected TableUUID '%s', got '%s'", i, metadata.TableUUID, retrievedMetadata.TableUUID)
			}
			if retrievedMetadata.CurrentSnapshotID != metadata.CurrentSnapshotID {
				t.Errorf("iteration %d: expected CurrentSnapshotID %d, got %d", i, metadata.CurrentSnapshotID, retrievedMetadata.CurrentSnapshotID)
			}

			// バージョントークンが更新されたことを確認
			if tableInfo.VersionToken != versionToken {
				t.Errorf("iteration %d: expected VersionToken to be updated to '%s', got '%s'", i, versionToken, tableInfo.VersionToken)
			}
		}
	})

	// テストケース3: エラーケースのテスト
	t.Run("error_cases", func(t *testing.T) {
		// GetTableMetadataLocation APIエラー
		t.Run("GetTableMetadataLocation_error", func(t *testing.T) {
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

			// モックS3 Tablesクライアントを設定してエラーを返す
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return nil, fmt.Errorf("GetTableMetadataLocation failed")
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "test-version-token",
			}

			// メタデータ取得を試行
			_, err = exporter.getTableMetadata(context.Background(), "test-namespace", "test-table", tableInfo)
			if err == nil {
				t.Error("expected error from getTableMetadata")
			}
		})

		// S3 GetObject APIエラー
		t.Run("S3_GetObject_error", func(t *testing.T) {
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
			metadataLocation := "s3://test-bucket/metadata/00001-test-uuid.metadata.json"
			versionToken := "test-version-token"
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定してエラーを返す
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return nil, fmt.Errorf("S3 GetObject failed")
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "old-version-token",
			}

			// メタデータ取得を試行
			_, err = exporter.getTableMetadata(context.Background(), "test-namespace", "test-table", tableInfo)
			if err == nil {
				t.Error("expected error from getTableMetadata")
			}
		})

		// 不正なメタデータJSON
		t.Run("invalid_metadata_json", func(t *testing.T) {
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
			metadataLocation := "s3://test-bucket/metadata/00001-test-uuid.metadata.json"
			versionToken := "test-version-token"
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定して不正なJSONを返す
			invalidJSON := []byte("{invalid json")
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(invalidJSON)),
					}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "old-version-token",
			}

			// メタデータ取得を試行
			_, err = exporter.getTableMetadata(context.Background(), "test-namespace", "test-table", tableInfo)
			if err == nil {
				t.Error("expected error from getTableMetadata for invalid JSON")
			}
		})
	})
}

// generateRandomMetadata generates random metadata for testing
// テスト用のランダムなメタデータを生成
func generateRandomMetadata(seed int) IcebergMetadata {
	// シード値を使用して決定的なランダムデータを生成
	numSnapshots := seed % 5 // 0-4個のスナップショット
	numFields := 1 + (seed % 5) // 1-5個のフィールド

	// スキーマフィールドを生成
	fields := make([]IcebergSchemaField, numFields)
	for i := 0; i < numFields; i++ {
		fields[i] = IcebergSchemaField{
			ID:       i + 1,
			Name:     fmt.Sprintf("field%d", i+1),
			Required: i == 0, // 最初のフィールドのみ必須
			Type:     []string{"string", "long", "double", "boolean"}[i%4],
		}
	}

	// スナップショットを生成
	snapshots := make([]IcebergSnapshot, numSnapshots)
	snapshotLog := make([]IcebergSnapshotLog, numSnapshots)
	metadataLog := make([]IcebergMetadataLog, numSnapshots)
	var currentSnapshotID int64 = -1

	for i := 0; i < numSnapshots; i++ {
		snapshotID := int64(1234567890000 + i*1000)
		snapshots[i] = IcebergSnapshot{
			SnapshotID:     snapshotID,
			TimestampMS:    snapshotID,
			SequenceNumber: int64(i + 1),
			Summary: map[string]string{
				"operation":     "append",
				"added-files":   fmt.Sprintf("%d", i+1),
				"added-records": fmt.Sprintf("%d", (i+1)*100),
			},
			ManifestList: fmt.Sprintf("s3://test-bucket/metadata/snap-%d-%d-manifest-list.avro", snapshotID, i+1),
		}
		snapshotLog[i] = IcebergSnapshotLog{
			TimestampMS: snapshotID,
			SnapshotID:  snapshotID,
		}
		metadataLog[i] = IcebergMetadataLog{
			TimestampMS:  snapshotID,
			MetadataFile: fmt.Sprintf("s3://test-bucket/metadata/%05d-test-uuid.metadata.json", i+1),
		}
		currentSnapshotID = snapshotID
	}

	return IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          fmt.Sprintf("test-uuid-%d", seed),
		Location:           fmt.Sprintf("s3://test-bucket/test-table-%d", seed),
		LastSequenceNumber: int64(numSnapshots),
		LastUpdatedMS:      1234567890000 + int64(seed*1000),
		LastColumnID:       numFields,
		Schemas: []IcebergSchema{
			{
				SchemaID: 0,
				Fields:   fields,
			},
		},
		CurrentSchemaID: 0,
		PartitionSpecs: []IcebergPartitionSpec{
			{
				SpecID: 0,
				Fields: []IcebergPartitionField{},
			},
		},
		DefaultSpecID:     0,
		LastPartitionID:   0,
		Properties:        map[string]string{"seed": fmt.Sprintf("%d", seed)},
		CurrentSnapshotID: currentSnapshotID,
		Snapshots:         snapshots,
		SnapshotLog:       snapshotLog,
		MetadataLog:       metadataLog,
	}
}
