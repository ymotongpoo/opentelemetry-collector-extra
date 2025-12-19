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

// TestProperty_BatchCommitCompleteness tests batch commit completeness property
// Feature: iceberg-snapshot-commit, Property 7: バッチコミットの完全性
// Validates: Requirements 6.1, 6.2, 6.3
func TestProperty_BatchCommitCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の複数のデータファイルをアップロードした場合、システムはすべてのファイルを
	// 単一のスナップショットにまとめ、生成されたメタデータファイルにすべてのデータファイルの
	// パスとメタデータを含むべきである

	// テストケース1: 複数のデータファイルのバッチコミット
	t.Run("multiple_files_single_snapshot", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（2-20個）
			numFiles := 2 + (i % 19)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/batch-%d-file-%d.parquet", i, j)
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

			// 既存のメタデータを作成（スナップショットなし）
			existingMetadata := IcebergMetadata{
				FormatVersion:      2,
				TableUUID:          fmt.Sprintf("test-uuid-%d", i),
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
			}

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
			uploadedFiles := make(map[string][]byte)
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					// アップロードされたデータを保存
					data, _ := io.ReadAll(params.Body)
					uploadedFiles[*params.Key] = data
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

			// バッチコミットを実行
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths)
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// アップロードされたメタデータを検証
			var foundMetadata bool
			var foundManifest bool
			var metadataContent IcebergMetadata
			var manifestContent IcebergManifest

			for key, data := range uploadedFiles {
				if len(key) > 14 && key[len(key)-14:] == ".metadata.json" {
					// メタデータファイルを解析
					if err := json.Unmarshal(data, &metadataContent); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded metadata: %v", i, err)
					}
					foundMetadata = true
				} else if len(key) > 14 && key[len(key)-14:] == ".manifest.json" {
					// マニフェストファイルを解析
					if err := json.Unmarshal(data, &manifestContent); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded manifest: %v", i, err)
					}
					foundManifest = true
				}
			}

			if !foundMetadata {
				t.Errorf("iteration %d: metadata file was not uploaded", i)
				continue
			}

			if !foundManifest {
				t.Errorf("iteration %d: manifest file was not uploaded", i)
				continue
			}

			// プロパティ検証1: すべてのファイルが単一のスナップショットにまとめられている
			if len(metadataContent.Snapshots) != 1 {
				t.Errorf("iteration %d: expected 1 snapshot, got %d", i, len(metadataContent.Snapshots))
			}

			// プロパティ検証2: マニフェストにすべてのデータファイルが含まれている
			if len(manifestContent.Entries) != numFiles {
				t.Errorf("iteration %d: expected %d manifest entries, got %d", i, numFiles, len(manifestContent.Entries))
			}

			// プロパティ検証3: マニフェスト内のすべてのエントリが元のデータファイルパスを含んでいる
			foundPaths := make(map[string]bool)
			for _, entry := range manifestContent.Entries {
				foundPaths[entry.DataFile.FilePath] = true
			}

			for _, expectedPath := range dataFilePaths {
				if !foundPaths[expectedPath] {
					t.Errorf("iteration %d: data file path %s not found in manifest", i, expectedPath)
				}
			}

			// プロパティ検証4: すべてのマニフェストエントリが同じスナップショットIDを持つ
			if len(manifestContent.Entries) > 0 {
				expectedSnapshotID := manifestContent.Entries[0].SnapshotID
				for j, entry := range manifestContent.Entries {
					if entry.SnapshotID != expectedSnapshotID {
						t.Errorf("iteration %d: entry %d has different snapshot ID: expected %d, got %d",
							i, j, expectedSnapshotID, entry.SnapshotID)
					}
				}
			}
		}
	})

	// テストケース2: 異なるサイズのバッチでのコミット
	t.Run("various_batch_sizes", func(t *testing.T) {
		// 様々なバッチサイズをテスト（1, 5, 10, 50, 100個）
		batchSizes := []int{1, 5, 10, 50, 100}

		for _, batchSize := range batchSizes {
			t.Run(fmt.Sprintf("batch_size_%d", batchSize), func(t *testing.T) {
				dataFilePaths := make([]string, batchSize)
				for j := 0; j < batchSize; j++ {
					dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/size-%d-file-%d.parquet", batchSize, j)
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
					t.Fatalf("newS3TablesExporter() failed: %v", err)
				}

				// 既存のメタデータを作成（スナップショットなし）
				existingMetadata := IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          fmt.Sprintf("test-uuid-size-%d", batchSize),
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
				}

				// メタデータをJSONにシリアライズ
				metadataJSON, err := json.Marshal(existingMetadata)
				if err != nil {
					t.Fatalf("failed to marshal metadata: %v", err)
				}

				// モックS3 Tablesクライアントを設定
				metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/size-%d-initial.metadata.json", batchSize)
				versionToken := fmt.Sprintf("version-token-size-%d", batchSize)
				newVersionToken := fmt.Sprintf("version-token-size-%d-new", batchSize)

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
				uploadedFiles := make(map[string][]byte)
				mockS3Client := &mockS3Client{
					getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(metadataJSON)),
						}, nil
					},
					putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
						// アップロードされたデータを保存
						data, _ := io.ReadAll(params.Body)
						uploadedFiles[*params.Key] = data
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

				// バッチコミットを実行
				err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths)
				if err != nil {
					t.Fatalf("commitSnapshot() failed: %v", err)
				}

				// マニフェストを検証
				var manifestContent IcebergManifest
				var foundManifest bool

				for key, data := range uploadedFiles {
					if len(key) > 14 && key[len(key)-14:] == ".manifest.json" {
						if err := json.Unmarshal(data, &manifestContent); err != nil {
							t.Fatalf("failed to unmarshal uploaded manifest: %v", err)
						}
						foundManifest = true
						break
					}
				}

				if !foundManifest {
					t.Fatal("manifest file was not uploaded")
				}

				// すべてのデータファイルがマニフェストに含まれていることを確認
				if len(manifestContent.Entries) != batchSize {
					t.Errorf("expected %d manifest entries, got %d", batchSize, len(manifestContent.Entries))
				}
			})
		}
	})

	// テストケース3: 既存のスナップショットがある状態でのバッチコミット
	t.Run("batch_commit_with_existing_snapshots", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（2-15個）
			numFiles := 2 + (i % 14)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/existing-%d-file-%d.parquet", i, j)
			}

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
			existingMetadata := IcebergMetadata{
				FormatVersion:      2,
				TableUUID:          fmt.Sprintf("test-uuid-existing-%d", i),
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
				DefaultSpecID:   0,
				LastPartitionID: 0,
				Properties:      map[string]string{},
			}
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
			uploadedFiles := make(map[string][]byte)
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					// アップロードされたデータを保存
					data, _ := io.ReadAll(params.Body)
					uploadedFiles[*params.Key] = data
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

			// バッチコミットを実行
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths)
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// アップロードされたメタデータとマニフェストを検証
			var metadataContent IcebergMetadata
			var manifestContent IcebergManifest
			var foundMetadata, foundManifest bool

			for key, data := range uploadedFiles {
				if len(key) > 14 && key[len(key)-14:] == ".metadata.json" {
					if err := json.Unmarshal(data, &metadataContent); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded metadata: %v", i, err)
					}
					foundMetadata = true
				} else if len(key) > 14 && key[len(key)-14:] == ".manifest.json" {
					if err := json.Unmarshal(data, &manifestContent); err != nil {
						t.Fatalf("iteration %d: failed to unmarshal uploaded manifest: %v", i, err)
					}
					foundManifest = true
				}
			}

			if !foundMetadata {
				t.Errorf("iteration %d: metadata file was not uploaded", i)
				continue
			}

			if !foundManifest {
				t.Errorf("iteration %d: manifest file was not uploaded", i)
				continue
			}

			// 新しいスナップショットが追加されたことを確認
			expectedSnapshotCount := numExistingSnapshots + 1
			if len(metadataContent.Snapshots) != expectedSnapshotCount {
				t.Errorf("iteration %d: expected %d snapshots, got %d", i, expectedSnapshotCount, len(metadataContent.Snapshots))
			}

			// マニフェストにすべてのデータファイルが含まれていることを確認
			if len(manifestContent.Entries) != numFiles {
				t.Errorf("iteration %d: expected %d manifest entries, got %d", i, numFiles, len(manifestContent.Entries))
			}
		}
	})
}
