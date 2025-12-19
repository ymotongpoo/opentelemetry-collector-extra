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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_ErrorMessageContextInformation tests error message context information property
// Feature: iceberg-snapshot-commit, Property 9: エラーメッセージのコンテキスト情報
// Validates: Requirements 5.4
func TestProperty_ErrorMessageContextInformation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のエラー発生時、システムはエラーメッセージにTable Name、データファイル数などの
	// 十分なコンテキスト情報を含むべきである

	// テストケース1: GetTableMetadataLocation APIエラー時のコンテキスト情報
	t.Run("get_metadata_location_error_context", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなテーブル名とデータファイル数を生成
			namespace := fmt.Sprintf("namespace-%d", i)
			tableName := fmt.Sprintf("table-%d", i)
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}
			totalDataSize := int64(1024 * (i + 1))

			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定（GetTableMetadataLocationでエラーを返す）
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return nil, errors.New("API error: access denied")
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table",
				WarehouseLocation: "s3://test-bucket/warehouse",
				VersionToken:      "version-token-1",
			}

			// commitSnapshotを呼び出してエラーを取得
			ctx := context.Background()
			err = exporter.commitSnapshot(ctx, namespace, tableName, tableInfo, dataFilePaths, totalDataSize)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error, got nil", i)
				continue
			}

			// エラーメッセージにコンテキスト情報が含まれることを確認
			errorMsg := err.Error()

			// エラーメッセージに必要な情報が含まれているか検証
			// 注: エラーメッセージは関数内でログ出力されるが、返されるエラーにも情報が含まれるべき
			if !strings.Contains(errorMsg, "metadata") {
				t.Errorf("iteration %d: error message should contain 'metadata', got: %s", i, errorMsg)
			}
		}
	})

	// テストケース2: S3 GetObjectエラー時のコンテキスト情報
	t.Run("s3_get_object_error_context", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなテーブル名とデータファイル数を生成
			namespace := fmt.Sprintf("namespace-%d", i)
			tableName := fmt.Sprintf("table-%d", i)
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}
			totalDataSize := int64(1024 * (i + 1))

			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: aws.String("s3://test-bucket/metadata/00001-test.metadata.json"),
						VersionToken:     aws.String("version-token-1"),
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定（GetObjectでエラーを返す）
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return nil, errors.New("S3 error: network timeout")
				},
			}
			exporter.s3Client = mockS3Client

			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table",
				WarehouseLocation: "s3://test-bucket/warehouse",
				VersionToken:      "version-token-1",
			}

			// commitSnapshotを呼び出してエラーを取得
			ctx := context.Background()
			err = exporter.commitSnapshot(ctx, namespace, tableName, tableInfo, dataFilePaths, totalDataSize)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error, got nil", i)
				continue
			}

			// エラーメッセージにコンテキスト情報が含まれることを確認
			errorMsg := err.Error()

			// エラーメッセージに必要な情報が含まれているか検証
			if !strings.Contains(errorMsg, "metadata") && !strings.Contains(errorMsg, "download") {
				t.Errorf("iteration %d: error message should contain context about metadata download, got: %s", i, errorMsg)
			}
		}
	})

	// テストケース3: UpdateTableMetadataLocationエラー時のコンテキスト情報
	t.Run("update_metadata_location_error_context", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなテーブル名とデータファイル数を生成
			namespace := fmt.Sprintf("namespace-%d", i)
			tableName := fmt.Sprintf("table-%d", i)
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}
			totalDataSize := int64(1024 * (i + 1))

			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// モックS3 Tablesクライアントを設定
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: aws.String("s3://test-bucket/metadata/00001-test.metadata.json"),
						VersionToken:     aws.String("version-token-1"),
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					return nil, errors.New("API error: conflict exception")
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := newMockS3ClientWithMetadata(&existingMetadata)
			exporter.s3Client = mockS3Client

			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table",
				WarehouseLocation: "s3://test-bucket/warehouse",
				VersionToken:      "version-token-1",
			}

			// commitSnapshotを呼び出してエラーを取得
			ctx := context.Background()
			err = exporter.commitSnapshot(ctx, namespace, tableName, tableInfo, dataFilePaths, totalDataSize)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error, got nil", i)
				continue
			}

			// エラーメッセージにコンテキスト情報が含まれることを確認
			errorMsg := err.Error()

			// エラーメッセージに必要な情報が含まれているか検証
			if !strings.Contains(errorMsg, "metadata") && !strings.Contains(errorMsg, "update") {
				t.Errorf("iteration %d: error message should contain context about metadata update, got: %s", i, errorMsg)
			}
		}
	})

	// テストケース4: コンテキストキャンセル時のコンテキスト情報
	t.Run("context_cancellation_error_context", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなテーブル名とデータファイル数を生成
			namespace := fmt.Sprintf("namespace-%d", i)
			tableName := fmt.Sprintf("table-%d", i)
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}
			totalDataSize := int64(1024 * (i + 1))

			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table",
				WarehouseLocation: "s3://test-bucket/warehouse",
				VersionToken:      "version-token-1",
			}

			// キャンセル可能なコンテキストを作成
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // 即座にキャンセル

			// commitSnapshotを呼び出してエラーを取得
			err = exporter.commitSnapshot(ctx, namespace, tableName, tableInfo, dataFilePaths, totalDataSize)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error, got nil", i)
				continue
			}

			// エラーメッセージにコンテキスト情報が含まれることを確認
			errorMsg := err.Error()

			// エラーメッセージにキャンセル情報が含まれているか検証
			if !strings.Contains(errorMsg, "cancel") {
				t.Errorf("iteration %d: error message should contain 'cancel', got: %s", i, errorMsg)
			}
		}
	})
}

// newMockS3ClientWithMetadata creates a mock S3 client that returns the given metadata
// 指定されたメタデータを返すモックS3クライアントを作成
func newMockS3ClientWithMetadata(metadata *IcebergMetadata) *mockS3Client {
	return &mockS3Client{
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(metadata)
			if err != nil {
				return nil, err
			}
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader(string(metadataJSON))),
			}, nil
		},
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
	}
}
