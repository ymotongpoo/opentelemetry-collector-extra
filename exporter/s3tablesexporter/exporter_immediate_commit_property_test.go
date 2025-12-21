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
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_ImmediateSnapshotCommit tests immediate snapshot commit property
// Feature: iceberg-snapshot-commit, Property 11: 即座のスナップショットコミット
// Validates: Requirements 7.1
//
// プロパティ: 任意のデータファイルアップロードに対して、
// システムは即座にスナップショットをコミットしなければならない
func TestProperty_ImmediateSnapshotCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// テストケース1: 単一のデータファイルアップロード時の即座のコミット
	t.Run("single_file_upload_commits_immediately", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなデータサイズを生成（1KB - 100KB）
			dataSize := 1024 + (i*1000%99000)
			testData := make([]byte, dataSize)
			for j := range testData {
				testData[j] = byte(j % 256)
			}

			// 各データタイプでテスト
			dataTypes := []string{"metrics", "traces", "logs"}
			dataType := dataTypes[i%len(dataTypes)]

			// commitSnapshotが呼び出されたかを追跡
			commitSnapshotCalled := false
			var capturedDataFilePaths []string
			var capturedTotalDataSize int64

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

			// モックS3 Tablesクライアントを設定
			tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
			warehouseLocation := "s3://test-warehouse-bucket"
			versionToken := "test-version-token"
			metadataLocation := "s3://test-warehouse-bucket/metadata/00001-test.metadata.json"

			// 既存のメタデータを作成
			existingMetadata := &IcebergMetadata{
				FormatVersion:      2,
				TableUUID:          "test-table-uuid",
				Location:           warehouseLocation,
				LastSequenceNumber: 0,
				LastUpdatedMS:      1234567890000,
				LastColumnID:       1,
				Schemas: []IcebergSchema{
					{
						SchemaID: 0,
						Fields: []IcebergSchemaField{
							{ID: 1, Name: "test_field", Required: true, Type: "string"},
						},
					},
				},
				CurrentSchemaID: 0,
				PartitionSpecs: []IcebergPartitionSpec{
					{SpecID: 0, Fields: []IcebergPartitionField{}},
				},
				DefaultSpecID:     0,
				LastPartitionID:   0,
				Properties:        map[string]string{},
				CurrentSnapshotID: -1,
				Snapshots:         []IcebergSnapshot{},
				SnapshotLog:       []IcebergSnapshotLog{},
				MetadataLog:       []IcebergMetadataLog{},
			}

			mockS3TablesClient := &mockS3TablesClient{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					return &s3tables.GetTableOutput{
						TableARN:          &tableArn,
						WarehouseLocation: &warehouseLocation,
						VersionToken:      &versionToken,
					}, nil
				},
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					// commitSnapshotが呼び出されたことを記録
					commitSnapshotCalled = true
					newVersionToken := "new-version-token"
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					// データファイルのアップロードを記録
					if params.Key != nil && len(*params.Key) > 5 && (*params.Key)[:5] == "data/" {
						s3Path := fmt.Sprintf("s3://%s/%s", *params.Bucket, *params.Key)
						capturedDataFilePaths = append(capturedDataFilePaths, s3Path)
						// データサイズを記録
						if params.Body != nil {
							bodyBytes, _ := io.ReadAll(params.Body)
							capturedTotalDataSize += int64(len(bodyBytes))
						}
					}
					return &s3.PutObjectOutput{}, nil
				},
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					// 既存のメタデータを返す
					metadataJSON, _ := json.Marshal(existingMetadata)
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// アップロードを実行
			err = exporter.uploadToS3Tables(context.Background(), testData, dataType)
			if err != nil {
				t.Errorf("iteration %d: uploadToS3Tables() failed: %v", i, err)
				continue
			}

			// プロパティ検証: commitSnapshotが即座に呼び出されたことを確認
			if !commitSnapshotCalled {
				t.Errorf("iteration %d: expected commitSnapshot to be called immediately after data file upload, but it was not called", i)
			}

			// データファイルがアップロードされたことを確認
			if len(capturedDataFilePaths) == 0 {
				t.Errorf("iteration %d: expected at least one data file to be uploaded", i)
			}

			// アップロードされたデータサイズが元のデータサイズと一致することを確認
			if capturedTotalDataSize != int64(dataSize) {
				t.Errorf("iteration %d: expected total data size %d, got %d", i, dataSize, capturedTotalDataSize)
			}
		}
	})

	// テストケース2: 異なるデータタイプでの即座のコミット
	t.Run("immediate_commit_across_data_types", func(t *testing.T) {
		dataTypes := []string{"metrics", "traces", "logs"}
		iterations := 30

		for _, dataType := range dataTypes {
			for i := 0; i < iterations; i++ {
				// ランダムなデータサイズを生成
				dataSize := 1024 + (i * 500)
				testData := make([]byte, dataSize)

				// commitSnapshotが呼び出されたかを追跡
				commitSnapshotCalled := false

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

				// モックS3 Tablesクライアントを設定
				tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
				warehouseLocation := "s3://test-warehouse-bucket"
				versionToken := "test-version-token"
				metadataLocation := "s3://test-warehouse-bucket/metadata/00001-test.metadata.json"

				existingMetadata := &IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          "test-table-uuid",
					Location:           warehouseLocation,
					LastSequenceNumber: 0,
					LastUpdatedMS:      1234567890000,
					LastColumnID:       1,
					Schemas: []IcebergSchema{
						{
							SchemaID: 0,
							Fields: []IcebergSchemaField{
								{ID: 1, Name: "test_field", Required: true, Type: "string"},
							},
						},
					},
					CurrentSchemaID: 0,
					PartitionSpecs: []IcebergPartitionSpec{
						{SpecID: 0, Fields: []IcebergPartitionField{}},
					},
					DefaultSpecID:     0,
					LastPartitionID:   0,
					Properties:        map[string]string{},
					CurrentSnapshotID: -1,
					Snapshots:         []IcebergSnapshot{},
					SnapshotLog:       []IcebergSnapshotLog{},
					MetadataLog:       []IcebergMetadataLog{},
				}

				mockS3TablesClient := &mockS3TablesClient{
					getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
						return &s3tables.GetTableOutput{
							TableARN:          &tableArn,
							WarehouseLocation: &warehouseLocation,
							VersionToken:      &versionToken,
						}, nil
					},
					getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
						return &s3tables.GetTableMetadataLocationOutput{
							MetadataLocation: &metadataLocation,
							VersionToken:     &versionToken,
						}, nil
					},
					updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
						commitSnapshotCalled = true
						newVersionToken := "new-version-token"
						return &s3tables.UpdateTableMetadataLocationOutput{
							VersionToken: &newVersionToken,
						}, nil
					},
				}
				exporter.s3TablesClient = mockS3TablesClient

				mockS3Client := &mockS3Client{
					putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						return &s3.PutObjectOutput{}, nil
					},
					getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						metadataJSON, _ := json.Marshal(existingMetadata)
						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(metadataJSON)),
						}, nil
					},
				}
				exporter.s3Client = mockS3Client

				// アップロードを実行
				err = exporter.uploadToS3Tables(context.Background(), testData, dataType)
				if err != nil {
					t.Errorf("dataType %s, iteration %d: uploadToS3Tables() failed: %v", dataType, i, err)
					continue
				}

				// プロパティ検証: commitSnapshotが即座に呼び出されたことを確認
				if !commitSnapshotCalled {
					t.Errorf("dataType %s, iteration %d: expected commitSnapshot to be called immediately", dataType, i)
				}
			}
		}
	})

	// テストケース3: 空データの場合はコミットされないことを確認
	t.Run("empty_data_does_not_commit", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			commitSnapshotCalled := false

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

			mockS3TablesClient := &mockS3TablesClient{
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					commitSnapshotCalled = true
					newVersionToken := "new-version-token"
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// 空データでアップロードを実行
			err = exporter.uploadToS3Tables(context.Background(), []byte{}, "metrics")
			if err != nil {
				t.Errorf("iteration %d: uploadToS3Tables() with empty data failed: %v", i, err)
				continue
			}

			// プロパティ検証: 空データの場合はcommitSnapshotが呼び出されないことを確認
			if commitSnapshotCalled {
				t.Errorf("iteration %d: expected commitSnapshot NOT to be called for empty data", i)
			}
		}
	})

	// テストケース4: UpdateTableMetadataLocation APIが正しいパラメータで呼び出されることを確認
	t.Run("commit_uses_correct_parameters", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			testData := make([]byte, 1024)
			var capturedParams *s3tables.UpdateTableMetadataLocationInput

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

			tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"
			warehouseLocation := "s3://test-warehouse-bucket"
			versionToken := "test-version-token"
			metadataLocation := "s3://test-warehouse-bucket/metadata/00001-test.metadata.json"

			existingMetadata := &IcebergMetadata{
				FormatVersion:      2,
				TableUUID:          "test-table-uuid",
				Location:           warehouseLocation,
				LastSequenceNumber: 0,
				LastUpdatedMS:      1234567890000,
				LastColumnID:       1,
				Schemas: []IcebergSchema{
					{
						SchemaID: 0,
						Fields: []IcebergSchemaField{
							{ID: 1, Name: "test_field", Required: true, Type: "string"},
						},
					},
				},
				CurrentSchemaID: 0,
				PartitionSpecs: []IcebergPartitionSpec{
					{SpecID: 0, Fields: []IcebergPartitionField{}},
				},
				DefaultSpecID:     0,
				LastPartitionID:   0,
				Properties:        map[string]string{},
				CurrentSnapshotID: -1,
				Snapshots:         []IcebergSnapshot{},
				SnapshotLog:       []IcebergSnapshotLog{},
				MetadataLog:       []IcebergMetadataLog{},
			}

			mockS3TablesClient := &mockS3TablesClient{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					return &s3tables.GetTableOutput{
						TableARN:          &tableArn,
						WarehouseLocation: &warehouseLocation,
						VersionToken:      &versionToken,
					}, nil
				},
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedParams = params
					newVersionToken := "new-version-token"
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			mockS3Client := &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					metadataJSON, _ := json.Marshal(existingMetadata)
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// アップロードを実行
			err = exporter.uploadToS3Tables(context.Background(), testData, "metrics")
			if err != nil {
				t.Errorf("iteration %d: uploadToS3Tables() failed: %v", i, err)
				continue
			}

			// プロパティ検証: UpdateTableMetadataLocation APIが正しいパラメータで呼び出されたことを確認
			if capturedParams == nil {
				t.Errorf("iteration %d: expected UpdateTableMetadataLocation to be called", i)
				continue
			}

			// TableBucketARNが正しいことを確認
			if capturedParams.TableBucketARN == nil || *capturedParams.TableBucketARN != cfg.TableBucketArn {
				t.Errorf("iteration %d: expected TableBucketARN '%s', got '%v'", i, cfg.TableBucketArn, capturedParams.TableBucketARN)
			}

			// Namespaceが正しいことを確認
			if capturedParams.Namespace == nil || *capturedParams.Namespace != cfg.Namespace {
				t.Errorf("iteration %d: expected Namespace '%s', got '%v'", i, cfg.Namespace, capturedParams.Namespace)
			}

			// Table Nameが正しいことを確認
			expectedTableName := cfg.Tables.Metrics
			if capturedParams.Name == nil || *capturedParams.Name != expectedTableName {
				t.Errorf("iteration %d: expected Name '%s', got '%v'", i, expectedTableName, capturedParams.Name)
			}

			// MetadataLocationが設定されていることを確認
			if capturedParams.MetadataLocation == nil || *capturedParams.MetadataLocation == "" {
				t.Errorf("iteration %d: expected MetadataLocation to be set", i)
			}

			// VersionTokenが設定されていることを確認
			if capturedParams.VersionToken == nil || *capturedParams.VersionToken == "" {
				t.Errorf("iteration %d: expected VersionToken to be set", i)
			}
		}
	})
}
