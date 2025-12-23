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
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_DataWriteRetention tests that data is uploaded to warehouse location when table exists
// Feature: remove-table-creation, Property 4: データ書き込みの保持
// Validates: Requirements 4.1, 4.2, 4.3
func TestProperty_DataWriteRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: すべての有効なテレメトリーデータに対して、テーブルが存在する場合、
	// データがParquet形式に変換されてwarehouse locationにアップロードされること

	// テストケース1: メトリクスデータがwarehouse locationにアップロードされることを検証
	t.Run("metrics_data_is_uploaded_to_warehouse_location", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなメトリクスデータを生成
			metrics := generateRandomMetrics()

			// アップロードを追跡するモッククライアントを作成
			uploadTracker := &uploadTracker{
				uploads: make([]uploadInfo, 0),
				mu:      sync.Mutex{},
			}

			// エクスポーターを作成
			exporter := createExporterWithUploadTracker(t, uploadTracker)

			// メトリクスをエクスポート
			err := exporter.pushMetrics(context.Background(), metrics)

			// スナップショットコミットのエラーは無視（このテストはデータアップロードのみを検証）
			// エラーが発生しても、データがアップロードされていれば成功とみなす
			_ = err

			// データがアップロードされたことを確認
			uploadTracker.mu.Lock()
			uploadCount := len(uploadTracker.uploads)
			uploadTracker.mu.Unlock()

			if uploadCount == 0 {
				t.Errorf("iteration %d: expected at least one upload, but got none", i)
				continue
			}

			// アップロードされたデータがwarehouse locationに送信されたことを確認
			uploadTracker.mu.Lock()
			for j, upload := range uploadTracker.uploads {
				// バケット名が正しいことを確認
				if upload.bucket != "test-warehouse-bucket" {
					t.Errorf("iteration %d, upload %d: expected bucket 'test-warehouse-bucket', got '%s'", i, j, upload.bucket)
				}

				// キーがdata/で始まることを確認
				if len(upload.key) < 5 || upload.key[:5] != "data/" {
					t.Errorf("iteration %d, upload %d: expected key to start with 'data/', got '%s'", i, j, upload.key)
				}

				// キーが.parquetで終わることを確認
				if len(upload.key) < 8 || upload.key[len(upload.key)-8:] != ".parquet" {
					t.Errorf("iteration %d, upload %d: expected key to end with '.parquet', got '%s'", i, j, upload.key)
				}

				// データが空でないことを確認
				if len(upload.data) == 0 {
					t.Errorf("iteration %d, upload %d: expected non-empty data", i, j)
				}
			}
			uploadTracker.mu.Unlock()
		}
	})

	// テストケース2: トレースデータがwarehouse locationにアップロードされることを検証
	t.Run("traces_data_is_uploaded_to_warehouse_location", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなトレースデータを生成
			traces := generateRandomTraces()

			// アップロードを追跡するモッククライアントを作成
			uploadTracker := &uploadTracker{
				uploads: make([]uploadInfo, 0),
				mu:      sync.Mutex{},
			}

			// エクスポーターを作成
			exporter := createExporterWithUploadTracker(t, uploadTracker)

			// トレースをエクスポート
			err := exporter.pushTraces(context.Background(), traces)

			// スナップショットコミットのエラーは無視（このテストはデータアップロードのみを検証）
			// エラーが発生しても、データがアップロードされていれば成功とみなす
			_ = err

			// データがアップロードされたことを確認
			uploadTracker.mu.Lock()
			uploadCount := len(uploadTracker.uploads)
			uploadTracker.mu.Unlock()

			if uploadCount == 0 {
				t.Errorf("iteration %d: expected at least one upload, but got none", i)
				continue
			}

			// アップロードされたデータがwarehouse locationに送信されたことを確認
			uploadTracker.mu.Lock()
			for j, upload := range uploadTracker.uploads {
				// バケット名が正しいことを確認
				if upload.bucket != "test-warehouse-bucket" {
					t.Errorf("iteration %d, upload %d: expected bucket 'test-warehouse-bucket', got '%s'", i, j, upload.bucket)
				}

				// キーがdata/で始まることを確認
				if len(upload.key) < 5 || upload.key[:5] != "data/" {
					t.Errorf("iteration %d, upload %d: expected key to start with 'data/', got '%s'", i, j, upload.key)
				}

				// キーが.parquetで終わることを確認
				if len(upload.key) < 8 || upload.key[len(upload.key)-8:] != ".parquet" {
					t.Errorf("iteration %d, upload %d: expected key to end with '.parquet', got '%s'", i, j, upload.key)
				}

				// データが空でないことを確認
				if len(upload.data) == 0 {
					t.Errorf("iteration %d, upload %d: expected non-empty data", i, j)
				}
			}
			uploadTracker.mu.Unlock()
		}
	})

	// テストケース3: ログデータがwarehouse locationにアップロードされることを検証
	t.Run("logs_data_is_uploaded_to_warehouse_location", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなログデータを生成
			logs := generateRandomLogs()

			// アップロードを追跡するモッククライアントを作成
			uploadTracker := &uploadTracker{
				uploads: make([]uploadInfo, 0),
				mu:      sync.Mutex{},
			}

			// エクスポーターを作成
			exporter := createExporterWithUploadTracker(t, uploadTracker)

			// ログをエクスポート
			err := exporter.pushLogs(context.Background(), logs)

			// スナップショットコミットのエラーは無視（このテストはデータアップロードのみを検証）
			// エラーが発生しても、データがアップロードされていれば成功とみなす
			_ = err

			// データがアップロードされたことを確認
			uploadTracker.mu.Lock()
			uploadCount := len(uploadTracker.uploads)
			uploadTracker.mu.Unlock()

			if uploadCount == 0 {
				t.Errorf("iteration %d: expected at least one upload, but got none", i)
				continue
			}

			// アップロードされたデータがwarehouse locationに送信されたことを確認
			uploadTracker.mu.Lock()
			for j, upload := range uploadTracker.uploads {
				// バケット名が正しいことを確認
				if upload.bucket != "test-warehouse-bucket" {
					t.Errorf("iteration %d, upload %d: expected bucket 'test-warehouse-bucket', got '%s'", i, j, upload.bucket)
				}

				// キーがdata/で始まることを確認
				if len(upload.key) < 5 || upload.key[:5] != "data/" {
					t.Errorf("iteration %d, upload %d: expected key to start with 'data/', got '%s'", i, j, upload.key)
				}

				// キーが.parquetで終わることを確認
				if len(upload.key) < 8 || upload.key[len(upload.key)-8:] != ".parquet" {
					t.Errorf("iteration %d, upload %d: expected key to end with '.parquet', got '%s'", i, j, upload.key)
				}

				// データが空でないことを確認
				if len(upload.data) == 0 {
					t.Errorf("iteration %d, upload %d: expected non-empty data", i, j)
				}
			}
			uploadTracker.mu.Unlock()
		}
	})
}


// uploadInfo tracks information about an upload
// アップロードに関する情報を追跡
type uploadInfo struct {
	bucket string
	key    string
	data   []byte
}

// uploadTracker tracks all uploads to S3
// S3へのすべてのアップロードを追跡
type uploadTracker struct {
	uploads []uploadInfo
	mu      sync.Mutex
}

// mockS3TablesClientWithUploadTracking is a mock S3 Tables client that returns table info
// テーブル情報を返すモックS3 Tablesクライアント
type mockS3TablesClientWithUploadTracking struct{}

func (m *mockS3TablesClientWithUploadTracking) GetTable(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	// テーブルが存在することを返す
	tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
	warehouseLocation := "s3://test-warehouse-bucket"
	versionToken := "test-version-token"

	return &s3tables.GetTableOutput{
		TableARN:          &tableArn,
		WarehouseLocation: &warehouseLocation,
		VersionToken:      &versionToken,
	}, nil
}

func (m *mockS3TablesClientWithUploadTracking) GetNamespace(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
	return &s3tables.GetNamespaceOutput{}, nil
}

func (m *mockS3TablesClientWithUploadTracking) GetTableMetadataLocation(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("GetTableMetadataLocation not implemented")
}

func (m *mockS3TablesClientWithUploadTracking) UpdateTableMetadataLocation(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("UpdateTableMetadataLocation not implemented")
}

// mockS3ClientWithUploadTracking is a mock S3 client that tracks uploads
// アップロードを追跡するモックS3クライアント
type mockS3ClientWithUploadTracking struct {
	tracker *uploadTracker
}

func (m *mockS3ClientWithUploadTracking) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	// アップロード情報を記録
	data := make([]byte, 0)
	if params.Body != nil {
		buf := make([]byte, 1024*1024) // 1MB buffer
		for {
			n, err := params.Body.Read(buf)
			if n > 0 {
				data = append(data, buf[:n]...)
			}
			if err != nil {
				break
			}
		}
	}

	m.tracker.mu.Lock()
	m.tracker.uploads = append(m.tracker.uploads, uploadInfo{
		bucket: *params.Bucket,
		key:    *params.Key,
		data:   data,
	})
	m.tracker.mu.Unlock()

	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3ClientWithUploadTracking) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, fmt.Errorf("GetObject not implemented")
}

// createExporterWithUploadTracker creates an exporter with upload tracking
// アップロード追跡機能を持つエクスポーターを作成
func createExporterWithUploadTracker(t *testing.T, tracker *uploadTracker) *s3TablesExporter {
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

	// モックS3 Tablesクライアントを設定（テーブルが存在することを返す）
	exporter.s3TablesClient = &mockS3TablesClientWithUploadTracking{}

	// モックS3クライアントを設定（アップロードを追跡）
	exporter.s3Client = &mockS3ClientWithUploadTracking{
		tracker: tracker,
	}

	return exporter
}
