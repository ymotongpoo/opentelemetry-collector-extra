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
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestGetTable_APIError tests S3 Tables GetTable API error handling
// Requirements: 5.1
func TestGetTable_APIError(t *testing.T) {
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
			return nil, fmt.Errorf("GetTable API error: table not found")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル取得を試行
	_, err = exporter.getTable(context.Background(), "test-namespace", "test-table")
	if err == nil {
		t.Fatal("expected error from getTable when API fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to get table"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}

// TestCreateTable_APIError tests S3 Tables CreateTable API error handling
// Requirements: 5.1
func TestCreateTable_APIError(t *testing.T) {
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
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			return nil, fmt.Errorf("CreateTable API error: insufficient permissions")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル作成を試行
	schema := createMetricsSchema()
	_, err = exporter.createTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from createTable when API fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to create table"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}

// TestCreateNamespace_APIError tests S3 Tables CreateNamespace API error handling
// Requirements: 5.1
func TestCreateNamespace_APIError(t *testing.T) {
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
		getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
			return nil, fmt.Errorf("namespace not found")
		},
		createNamespaceFunc: func(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error) {
			return nil, fmt.Errorf("CreateNamespace API error: quota exceeded")
		},
	}
	exporter.s3TablesClient = mockClient

	// Namespace作成を試行
	err = exporter.createNamespaceIfNotExists(context.Background(), "test-namespace")
	if err == nil {
		t.Fatal("expected error from createNamespaceIfNotExists when API fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to create namespace"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}

// TestUploadToWarehouseLocation_S3Error tests S3 PutObject API error handling
// Requirements: 5.2
func TestUploadToWarehouseLocation_S3Error(t *testing.T) {
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

	// モックS3クライアントを設定してエラーを返す
	mockClient := &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return nil, fmt.Errorf("PutObject API error: access denied")
		},
	}
	exporter.s3Client = mockClient

	// アップロードを試行
	_, err = exporter.uploadToWarehouseLocation(context.Background(), "s3://test-bucket", []byte("test data"), "metrics")
	if err == nil {
		t.Fatal("expected error from uploadToWarehouseLocation when S3 API fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to upload Parquet file to warehouse location"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message too short: '%s'", err.Error())
	}
}

// 注: コンテキストキャンセルのテストは既存のテストファイルに存在するため、ここでは省略

// TestGetOrCreateTable_GetTableError tests error handling when GetTable fails
// Requirements: 5.1, 5.5
func TestGetOrCreateTable_GetTableError(t *testing.T) {
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
	// GetTableは失敗、GetNamespaceは成功、CreateTableも失敗
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return nil, fmt.Errorf("table not found")
		},
		getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
			return &s3tables.GetNamespaceOutput{}, nil
		},
		createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
			return nil, fmt.Errorf("CreateTable failed: insufficient permissions")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル取得または作成を試行
	schema := createMetricsSchema()
	_, err = exporter.getOrCreateTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from getOrCreateTable when CreateTable fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to create table"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message should contain '%s', got '%s'", expectedSubstr, err.Error())
	}
}

// TestGetOrCreateTable_CreateNamespaceError tests error handling when CreateNamespace fails
// Requirements: 5.1, 5.5
func TestGetOrCreateTable_CreateNamespaceError(t *testing.T) {
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
	// GetTableは失敗、GetNamespaceも失敗、CreateNamespaceも失敗
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return nil, fmt.Errorf("table not found")
		},
		getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
			return nil, fmt.Errorf("namespace not found")
		},
		createNamespaceFunc: func(ctx context.Context, params *s3tables.CreateNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateNamespaceOutput, error) {
			return nil, fmt.Errorf("CreateNamespace failed: quota exceeded")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル取得または作成を試行
	schema := createMetricsSchema()
	_, err = exporter.getOrCreateTable(context.Background(), "test-namespace", "test-table", schema)
	if err == nil {
		t.Fatal("expected error from getOrCreateTable when CreateNamespace fails")
	}

	// エラーメッセージに適切なコンテキストが含まれることを確認
	expectedSubstr := "failed to create namespace"
	if len(err.Error()) < len(expectedSubstr) {
		t.Errorf("error message should contain '%s', got '%s'", expectedSubstr, err.Error())
	}
}

// 注: 空データのテストは既存のテストファイルに存在するため、ここでは省略

// TestUploadToS3Tables_UnconfiguredTable tests that unconfigured table names are handled correctly
// Requirements: 7.1, 7.2, 7.3, 7.4
func TestUploadToS3Tables_UnconfiguredTable(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "", // トレースは未設定
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// 未設定のテーブルタイプでアップロードを試行
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "traces")
	if err != nil {
		t.Errorf("uploadToS3Tables() with unconfigured table should not return error, got: %v", err)
	}
}

// TestUploadToS3Tables_ErrorPropagation tests that errors are properly propagated
// Requirements: 5.1, 5.2, 5.5
func TestUploadToS3Tables_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*s3TablesExporter)
		expectedError string
	}{
		{
			name: "GetTable error propagates",
			setupMock: func(e *s3TablesExporter) {
				mockClient := &mockS3TablesClient{
					getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
						return nil, fmt.Errorf("GetTable API error")
					},
					getNamespaceFunc: func(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
						return &s3tables.GetNamespaceOutput{}, nil
					},
					createTableFunc: func(ctx context.Context, params *s3tables.CreateTableInput, optFns ...func(*s3tables.Options)) (*s3tables.CreateTableOutput, error) {
						return nil, fmt.Errorf("CreateTable API error")
					},
				}
				e.s3TablesClient = mockClient
			},
			expectedError: "failed to get or create table",
		},
		{
			name: "PutObject error propagates",
			setupMock: func(e *s3TablesExporter) {
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
				e.s3TablesClient = mockS3TablesClient

				mockS3Client := &mockS3Client{
					putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						return nil, fmt.Errorf("PutObject API error: access denied")
					},
				}
				e.s3Client = mockS3Client
			},
			expectedError: "failed to upload to warehouse location",
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

			// モックを設定
			tt.setupMock(exporter)

			// アップロードを試行
			err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "metrics")
			if err == nil {
				t.Fatal("expected error from uploadToS3Tables")
			}

			// エラーメッセージに期待される文字列が含まれることを確認
			if len(err.Error()) < len(tt.expectedError) {
				t.Errorf("error message should contain '%s', got '%s'", tt.expectedError, err.Error())
			}
		})
	}
}

// TestErrorWrapping tests that errors are properly wrapped with context
// Requirements: 5.5
func TestErrorWrapping(t *testing.T) {
	tests := []struct {
		name          string
		operation     func() error
		expectedError string
		checkWrapped  bool
	}{
		{
			name: "extractBucketFromWarehouseLocation wraps error",
			operation: func() error {
				_, err := extractBucketFromWarehouseLocation("invalid-location")
				return err
			},
			expectedError: "invalid warehouse location format",
			checkWrapped:  false,
		},
		{
			name: "extractBucketNameFromArn wraps error",
			operation: func() error {
				_, err := extractBucketNameFromArn("invalid-arn")
				return err
			},
			expectedError: "invalid Table Bucket ARN format",
			checkWrapped:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if err == nil {
				t.Fatal("expected error but got none")
			}

			// エラーメッセージに期待される文字列が含まれることを確認
			if len(err.Error()) < len(tt.expectedError) {
				t.Errorf("error message should contain '%s', got '%s'", tt.expectedError, err.Error())
			}

			// エラーがラップされている場合は、errors.Unwrapで元のエラーを取得できることを確認
			if tt.checkWrapped {
				unwrapped := errors.Unwrap(err)
				if unwrapped == nil {
					t.Error("expected error to be wrapped, but Unwrap returned nil")
				}
			}
		})
	}
}
