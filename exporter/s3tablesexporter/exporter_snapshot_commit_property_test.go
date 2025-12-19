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

// TestProperty_SnapshotCreationAndCommit tests snapshot creation and commit property
// Feature: iceberg-snapshot-commit, Property 1: スナップショット作成とコミット
// Validates: Requirements 1.1, 1.2, 1.3, 2.1
func TestProperty_SnapshotCreationAndCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のデータファイルアップロードに対して、システムは新しいIcebergスナップショットを作成し、
	// そのスナップショットをテーブルメタデータにコミットし、current-snapshot-idが-1以外の値になるべきである

	// テストケース1: 単一のデータファイルでのスナップショットコミット
	t.Run("single_data_file_snapshot_commit", func(t *testing.T) {
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

		// 既存のメタデータを作成（current-snapshot-id: -1）
		existingMetadata := IcebergMetadata{
			FormatVersion:      2,
			TableUUID:          "test-uuid",
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
			CurrentSnapshotID: -1, // スナップショットなし
			Snapshots:         []IcebergSnapshot{},
			SnapshotLog:       []IcebergSnapshotLog{},
			MetadataLog:       []IcebergMetadataLog{},
		}

		// メタデータをJSONにシリアライズ
		metadataJSON, err := json.Marshal(existingMetadata)
		if err != nil {
			t.Fatalf("failed to marshal metadata: %v", err)
		}

		// モックS3 Tablesクライアントを設定
		metadataLocation := "s3://test-bucket/metadata/00000-initial.metadata.json"
		versionToken := "version-token-1"
		newVersionToken := "version-token-2"

		var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput
		mockS3TablesClient := &mockS3TablesClient{
			getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
				return &s3tables.GetTableMetadataLocationOutput{
					MetadataLocation: &metadataLocation,
					VersionToken:     &versionToken,
				}, nil
			},
			updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
				capturedUpdateParams = params
				return &s3tables.UpdateTableMetadataLocationOutput{
					VersionToken: &newVersionToken,
				}, nil
			},
		}
		exporter.s3TablesClient = mockS3TablesClient

		// モックS3クライアントを設定
		uploadedMetadata := make(map[string][]byte)
		mockS3Client := &mockS3Client{
			getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(bytes.NewReader(metadataJSON)),
				}, nil
			},
			putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
				// アップロードされたデータを保存
				data, _ := io.ReadAll(params.Body)
				uploadedMetadata[*params.Key] = data
				return &s3.PutObjectOutput{}, nil
			},
		}
		exporter.s3Client = mockS3Client

		// テーブル情報を作成
		tableInfo := &TableInfo{
			TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
			WarehouseLocation: "s3://test-bucket",
			VersionToken:      versionToken,
		}

		// データファイルパス
		dataFilePaths := []string{"s3://test-bucket/data/file1.parquet"}

		// スナップショットをコミット
		err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
		if err != nil {
			t.Fatalf("commitSnapshot() failed: %v", err)
		}

		// UpdateTableMetadataLocation APIが呼び出されたことを確認
		if capturedUpdateParams == nil {
			t.Fatal("UpdateTableMetadataLocation was not called")
		}

		// バージョントークンが使用されたことを確認
		if *capturedUpdateParams.VersionToken != versionToken {
			t.Errorf("expected VersionToken '%s', got '%s'", versionToken, *capturedUpdateParams.VersionToken)
		}

		// 新しいメタデータロケーションが設定されたことを確認
		if capturedUpdateParams.MetadataLocation == nil || *capturedUpdateParams.MetadataLocation == "" {
			t.Error("MetadataLocation should not be empty")
		}

		// アップロードされたメタデータを検証
		var foundMetadata bool
		for key, data := range uploadedMetadata {
			if len(key) > 9 && key[len(key)-14:] == ".metadata.json" {
				// メタデータファイルを解析
				var newMetadata IcebergMetadata
				if err := json.Unmarshal(data, &newMetadata); err != nil {
					t.Fatalf("failed to unmarshal uploaded metadata: %v", err)
				}

				// current-snapshot-idが-1以外であることを確認
				if newMetadata.CurrentSnapshotID == -1 {
					t.Error("CurrentSnapshotID should not be -1 after commit")
				}

				// スナップショットが追加されたことを確認
				if len(newMetadata.Snapshots) != 1 {
					t.Errorf("expected 1 snapshot, got %d", len(newMetadata.Snapshots))
				}

				foundMetadata = true
				break
			}
		}

		if !foundMetadata {
			t.Error("metadata file was not uploaded")
		}

		// バージョントークンが更新されたことを確認
		if tableInfo.VersionToken != newVersionToken {
			t.Errorf("expected VersionToken to be updated to '%s', got '%s'", newVersionToken, tableInfo.VersionToken)
		}
	})

	// テストケース2: 複数のデータファイルでのスナップショットコミット
	t.Run("multiple_data_files_snapshot_commit", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（1-10個）
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
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

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)
			existingMetadata.CurrentSnapshotID = -1 // スナップショットなしの状態から開始

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-initial.metadata.json", i)
			versionToken := fmt.Sprintf("version-token-%d", i)
			newVersionToken := fmt.Sprintf("version-token-%d-new", i)

			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			uploadedMetadata := make(map[string][]byte)
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					// アップロードされたデータを保存
					data, _ := io.ReadAll(params.Body)
					uploadedMetadata[*params.Key] = data
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// UpdateTableMetadataLocation APIが呼び出されたことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("iteration %d: UpdateTableMetadataLocation was not called", i)
			}

			// アップロードされたメタデータを検証
			var foundMetadata bool
			for key, data := range uploadedMetadata {
				if len(key) > 9 && key[len(key)-14:] == ".metadata.json" {
					// メタデータファイルを解析
					var newMetadata IcebergMetadata
					if err := json.Unmarshal(data, &newMetadata); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded metadata: %v", i, err)
					}

					// current-snapshot-idが-1以外であることを確認
					if newMetadata.CurrentSnapshotID == -1 {
						t.Errorf("iteration %d: CurrentSnapshotID should not be -1 after commit", i)
					}

					// スナップショットが追加されたことを確認
					expectedSnapshotCount := len(existingMetadata.Snapshots) + 1
					if len(newMetadata.Snapshots) != expectedSnapshotCount {
						t.Errorf("iteration %d: expected %d snapshots, got %d", i, expectedSnapshotCount, len(newMetadata.Snapshots))
					}

					foundMetadata = true
					break
				}
			}

			if !foundMetadata {
				t.Errorf("iteration %d: metadata file was not uploaded", i)
			}
		}
	})

	// テストケース3: 既存のスナップショットがある状態でのコミット
	t.Run("commit_with_existing_snapshots", func(t *testing.T) {
		iterations := 30
		for i := 0; i < iterations; i++ {
			// ランダムな既存スナップショット数を生成（1-5個）
			numExistingSnapshots := 1 + (i % 5)

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

			// 既存のスナップショットを持つメタデータを作成
			existingMetadata := generateRandomMetadata(i * 100) // シード値を調整
			// 既存のスナップショット数を設定
			existingMetadata.Snapshots = make([]IcebergSnapshot, numExistingSnapshots)
			existingMetadata.SnapshotLog = make([]IcebergSnapshotLog, numExistingSnapshots)
			for j := 0; j < numExistingSnapshots; j++ {
				snapshotID := int64(1234567890000 + int64(j*1000))
				existingMetadata.Snapshots[j] = IcebergSnapshot{
					SnapshotID:     snapshotID,
					TimestampMS:    snapshotID,
					SequenceNumber: int64(j + 1),
					Summary: map[string]string{
						"operation":     "append",
						"added-files":   fmt.Sprintf("%d", j+1),
						"added-records": fmt.Sprintf("%d", (j+1)*100),
					},
					ManifestList: fmt.Sprintf("s3://test-bucket/metadata/snap-%d.avro", snapshotID),
				}
				existingMetadata.SnapshotLog[j] = IcebergSnapshotLog{
					TimestampMS: snapshotID,
					SnapshotID:  snapshotID,
				}
			}
			existingMetadata.CurrentSnapshotID = existingMetadata.Snapshots[numExistingSnapshots-1].SnapshotID
			existingMetadata.LastSequenceNumber = int64(numExistingSnapshots)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-existing.metadata.json", i)
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
			uploadedMetadata := make(map[string][]byte)
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					// アップロードされたデータを保存
					data, _ := io.ReadAll(params.Body)
					uploadedMetadata[*params.Key] = data
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// データファイルパス
			dataFilePaths := []string{fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i)}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// アップロードされたメタデータを検証
			var foundMetadata bool
			for key, data := range uploadedMetadata {
				if len(key) > 9 && key[len(key)-14:] == ".metadata.json" {
					// メタデータファイルを解析
					var newMetadata IcebergMetadata
					if err := json.Unmarshal(data, &newMetadata); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded metadata: %v", i, err)
					}

					// スナップショット数が増加したことを確認
					expectedSnapshotCount := numExistingSnapshots + 1
					if len(newMetadata.Snapshots) != expectedSnapshotCount {
						t.Errorf("iteration %d: expected %d snapshots, got %d", i, expectedSnapshotCount, len(newMetadata.Snapshots))
					}

					// current-snapshot-idが更新されたことを確認
					if newMetadata.CurrentSnapshotID == existingMetadata.CurrentSnapshotID {
						t.Errorf("iteration %d: CurrentSnapshotID should be updated", i)
					}

					// シーケンス番号が増加したことを確認
					if newMetadata.LastSequenceNumber <= existingMetadata.LastSequenceNumber {
						t.Errorf("iteration %d: LastSequenceNumber should increase", i)
					}

					foundMetadata = true
					break
				}
			}

			if !foundMetadata {
				t.Errorf("iteration %d: metadata file was not uploaded", i)
			}
		}
	})
}
