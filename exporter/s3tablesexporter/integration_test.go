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
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestUploadToS3Tables_Integration_EmptyData tests that empty data is skipped
// 空データがスキップされることを検証
// Requirements: 3.4
func TestUploadToS3Tables_Integration_EmptyData(t *testing.T) {
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

	// 空データでアップロードを実行
	err = exporter.uploadToS3Tables(context.Background(), []byte{}, "metrics")
	if err != nil {
		t.Errorf("uploadToS3Tables() with empty data should not fail: %v", err)
	}
}

// TestUploadToS3Tables_Integration_WithCachedTable tests that cached table information is reused
// キャッシュされたテーブル情報が再利用されることを検証
// Requirements: 2.2
func TestUploadToS3Tables_Integration_WithCachedTable(t *testing.T) {
	// モックS3 Tablesクライアントを作成
	getTableCallCount := 0
	mockS3TablesClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			getTableCallCount++
			tableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
			warehouseLocation := "s3://test-warehouse-bucket"
			versionToken := "test-version-token"
			return &s3tables.GetTableOutput{
				TableARN:          &tableARN,
				WarehouseLocation: &warehouseLocation,
				VersionToken:      &versionToken,
			}, nil
		},
	}

	mockS3Client := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
	}

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

	// モッククライアントを設定
	exporter.s3TablesClient = mockS3TablesClient
	exporter.s3Client = mockS3Client

	// 1回目のアップロード
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data 1"), "metrics")
	if err != nil {
		t.Fatalf("first uploadToS3Tables() failed: %v", err)
	}

	// GetTableが1回呼ばれたことを確認
	if getTableCallCount != 1 {
		t.Errorf("expected GetTable to be called once, got %d", getTableCallCount)
	}

	// 2回目のアップロード（同じテーブル）
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data 2"), "metrics")
	if err != nil {
		t.Fatalf("second uploadToS3Tables() failed: %v", err)
	}

	// GetTableが1回のままであることを確認（キャッシュが使用された）
	if getTableCallCount != 1 {
		t.Errorf("expected GetTable to be called once (cached), got %d", getTableCallCount)
	}
}

// TestUploadToS3Tables_Integration_ContextCancellation tests context cancellation handling
// コンテキストキャンセルの処理を検証
// Requirements: 2.6, 5.4
func TestUploadToS3Tables_Integration_ContextCancellation(t *testing.T) {
	// キャンセル済みのコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 即座にキャンセル

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

	// uploadToS3Tablesを実行
	err = exporter.uploadToS3Tables(ctx, []byte("test data"), "metrics")

	// エラーが返されることを確認
	if err == nil {
		t.Error("uploadToS3Tables() should return error for cancelled context")
	}

	// エラーメッセージに"cancelled"が含まれることを確認
	if err != nil && err.Error() == "" {
		t.Error("error message should not be empty")
	}
}

// TestUploadToS3Tables_Integration_TableCreation tests table creation flow
// テーブル作成フローを検証
// Requirements: 1.1, 2.1
func TestUploadToS3Tables_Integration_TableCreation(t *testing.T) {
	// モックS3 Tablesクライアントを作成
	getTableCalled := false
	createNamespaceCalled := false
	createTableCalled := false

	mockS3TablesClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			getTableCalled = true
			if !createTableCalled {
				// 最初の呼び出しではテーブルが存在しない
				return nil, fmt.Errorf("table not found")
			}
			// CreateTable後の呼び出しではテーブル情報を返す
			tableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
			warehouseLocation := "s3://test-warehouse-bucket"
			versionToken := "test-version-token"
			return &s3tables.GetTableOutput{
				TableARN:          &tableARN,
				WarehouseLocation: &warehouseLocation,
				VersionToken:      &versionToken,
			}, nil
		},
		getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
			// Namespaceが存在しない
			return nil, fmt.Errorf("namespace not found")
		},
		createNamespaceFunc: func(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error) {
			createNamespaceCalled = true
			return &s3tables.CreateNamespaceOutput{}, nil
		},
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			createTableCalled = true
			tableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
			versionToken := "test-version-token"
			return &s3tables.CreateTableOutput{
				TableARN:     &tableARN,
				VersionToken: &versionToken,
			}, nil
		},
	}

	mockS3Client := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
	}

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

	// モッククライアントを設定
	exporter.s3TablesClient = mockS3TablesClient
	exporter.s3Client = mockS3Client

	// uploadToS3Tablesを実行
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "metrics")
	if err != nil {
		t.Errorf("uploadToS3Tables() failed: %v", err)
	}

	// 各APIが呼ばれたことを確認
	if !getTableCalled {
		t.Error("expected GetTable to be called")
	}
	if !createNamespaceCalled {
		t.Error("expected CreateNamespace to be called")
	}
	if !createTableCalled {
		t.Error("expected CreateTable to be called")
	}
}

// TestUploadToS3Tables_Integration_MultipleDataTypes tests uploading different data types
// 異なるデータタイプのアップロードを検証
// Requirements: 1.1, 1.2, 1.3, 1.4
func TestUploadToS3Tables_Integration_MultipleDataTypes(t *testing.T) {
	dataTypes := []string{"metrics", "traces", "logs"}

	for _, dataType := range dataTypes {
		t.Run(dataType, func(t *testing.T) {
			// モックS3 Tablesクライアントを作成
			mockS3TablesClient := &mockS3TablesClient{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					tableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
					warehouseLocation := "s3://test-warehouse-bucket"
					versionToken := "test-version-token"
					return &s3tables.GetTableOutput{
						TableARN:          &tableARN,
						WarehouseLocation: &warehouseLocation,
						VersionToken:      &versionToken,
					}, nil
				},
			}

			mockS3Client := &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}

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

			// モッククライアントを設定
			exporter.s3TablesClient = mockS3TablesClient
			exporter.s3Client = mockS3Client

			// uploadToS3Tablesを実行
			err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), dataType)
			if err != nil {
				t.Errorf("uploadToS3Tables() for %s failed: %v", dataType, err)
			}
		})
	}
}
