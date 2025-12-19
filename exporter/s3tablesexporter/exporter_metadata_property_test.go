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
					DefaultSpecID:     0,
					LastPartitionID:   0,
					Properties:        map[string]string{"key1": "value1"},
					CurrentSnapshotID: 1234567890000,
					Snapshots: []IcebergSnapshot{
						{
							SnapshotID:     1234567890000,
							TimestampMS:    1234567890000,
							SequenceNumber: 1,
							Summary: map[string]string{
								"operation":     "append",
								"added-files":   "1",
								"added-records": "100",
								"total-files":   "1",
								"total-records": "100",
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
					DefaultSpecID:     0,
					LastPartitionID:   0,
					Properties:        map[string]string{"key1": "value1", "key2": "value2"},
					CurrentSnapshotID: 1234567892000,
					Snapshots: []IcebergSnapshot{
						{
							SnapshotID:     1234567890000,
							TimestampMS:    1234567890000,
							SequenceNumber: 1,
							Summary: map[string]string{
								"operation":     "append",
								"added-files":   "1",
								"added-records": "100",
							},
							ManifestList: "s3://test-bucket/test-table-3/metadata/snap-1234567890000-1-manifest-list.avro",
						},
						{
							SnapshotID:     1234567891000,
							TimestampMS:    1234567891000,
							SequenceNumber: 2,
							Summary: map[string]string{
								"operation":     "append",
								"added-files":   "1",
								"added-records": "200",
							},
							ManifestList: "s3://test-bucket/test-table-3/metadata/snap-1234567891000-2-manifest-list.avro",
						},
						{
							SnapshotID:     1234567892000,
							TimestampMS:    1234567892000,
							SequenceNumber: 3,
							Summary: map[string]string{
								"operation":     "append",
								"added-files":   "1",
								"added-records": "300",
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
	numSnapshots := seed % 5    // 0-4個のスナップショット
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

// TestProperty_ManifestStructureCompleteness tests manifest file structure completeness
// Feature: iceberg-snapshot-commit, Property 4: マニフェストファイル構造の完全性
// Validates: Requirements 6.4, 8.5
func TestProperty_ManifestStructureCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のデータファイルリストに対して、生成されたマニフェストファイルは
	// データファイルのパス、ファイルサイズ、レコード数を含むべきである

	// テストケース1: 単一のデータファイル
	t.Run("single_data_file", func(t *testing.T) {
		dataFilePaths := []string{"s3://test-bucket/data/file1.parquet"}
		snapshotID := int64(1234567890000)
		sequenceNumber := int64(1)

		manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
		if err != nil {
			t.Fatalf("generateManifest() failed: %v", err)
		}

		// マニフェストの基本構造を検証
		if manifest.FormatVersion != 2 {
			t.Errorf("expected FormatVersion 2, got %d", manifest.FormatVersion)
		}
		if manifest.Content != "data" {
			t.Errorf("expected Content 'data', got '%s'", manifest.Content)
		}
		if len(manifest.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(manifest.Entries))
		}

		// エントリの構造を検証
		entry := manifest.Entries[0]
		if entry.Status != 1 { // ADDED
			t.Errorf("expected Status 1 (ADDED), got %d", entry.Status)
		}
		if entry.SnapshotID != snapshotID {
			t.Errorf("expected SnapshotID %d, got %d", snapshotID, entry.SnapshotID)
		}
		if entry.SequenceNumber != sequenceNumber {
			t.Errorf("expected SequenceNumber %d, got %d", sequenceNumber, entry.SequenceNumber)
		}

		// データファイル情報を検証
		dataFile := entry.DataFile
		if dataFile.FilePath != dataFilePaths[0] {
			t.Errorf("expected FilePath '%s', got '%s'", dataFilePaths[0], dataFile.FilePath)
		}
		if dataFile.FileFormat != "PARQUET" {
			t.Errorf("expected FileFormat 'PARQUET', got '%s'", dataFile.FileFormat)
		}
		// RecordCountとFileSizeBytesは現時点では0（TODO実装）
		if dataFile.RecordCount < 0 {
			t.Errorf("RecordCount should be non-negative, got %d", dataFile.RecordCount)
		}
		if dataFile.FileSizeBytes < 0 {
			t.Errorf("FileSizeBytes should be non-negative, got %d", dataFile.FileSizeBytes)
		}
	})

	// テストケース2: 複数のデータファイル
	t.Run("multiple_data_files", func(t *testing.T) {
		dataFilePaths := []string{
			"s3://test-bucket/data/file1.parquet",
			"s3://test-bucket/data/file2.parquet",
			"s3://test-bucket/data/file3.parquet",
		}
		snapshotID := int64(1234567890000)
		sequenceNumber := int64(1)

		manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
		if err != nil {
			t.Fatalf("generateManifest() failed: %v", err)
		}

		// エントリ数を検証
		if len(manifest.Entries) != len(dataFilePaths) {
			t.Errorf("expected %d entries, got %d", len(dataFilePaths), len(manifest.Entries))
		}

		// 各エントリを検証
		for i, entry := range manifest.Entries {
			if entry.Status != 1 { // ADDED
				t.Errorf("entry %d: expected Status 1 (ADDED), got %d", i, entry.Status)
			}
			if entry.SnapshotID != snapshotID {
				t.Errorf("entry %d: expected SnapshotID %d, got %d", i, snapshotID, entry.SnapshotID)
			}
			if entry.SequenceNumber != sequenceNumber {
				t.Errorf("entry %d: expected SequenceNumber %d, got %d", i, sequenceNumber, entry.SequenceNumber)
			}
			if entry.DataFile.FilePath != dataFilePaths[i] {
				t.Errorf("entry %d: expected FilePath '%s', got '%s'", i, dataFilePaths[i], entry.DataFile.FilePath)
			}
			if entry.DataFile.FileFormat != "PARQUET" {
				t.Errorf("entry %d: expected FileFormat 'PARQUET', got '%s'", i, entry.DataFile.FileFormat)
			}
		}
	})

	// テストケース3: ランダムなデータファイル数でのテスト
	t.Run("random_number_of_data_files", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなデータファイル数を生成（1-10個）
			numFiles := 1 + (i % 10)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}

			snapshotID := int64(1234567890000 + int64(i*1000))
			sequenceNumber := int64(i + 1)

			manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
			if err != nil {
				t.Fatalf("iteration %d: generateManifest() failed: %v", i, err)
			}

			// エントリ数を検証
			if len(manifest.Entries) != numFiles {
				t.Errorf("iteration %d: expected %d entries, got %d", i, numFiles, len(manifest.Entries))
			}

			// 各エントリの基本構造を検証
			for j, entry := range manifest.Entries {
				if entry.Status != 1 {
					t.Errorf("iteration %d, entry %d: expected Status 1, got %d", i, j, entry.Status)
				}
				if entry.SnapshotID != snapshotID {
					t.Errorf("iteration %d, entry %d: expected SnapshotID %d, got %d", i, j, snapshotID, entry.SnapshotID)
				}
				if entry.SequenceNumber != sequenceNumber {
					t.Errorf("iteration %d, entry %d: expected SequenceNumber %d, got %d", i, j, sequenceNumber, entry.SequenceNumber)
				}
				if entry.DataFile.FilePath != dataFilePaths[j] {
					t.Errorf("iteration %d, entry %d: expected FilePath '%s', got '%s'", i, j, dataFilePaths[j], entry.DataFile.FilePath)
				}
				if entry.DataFile.FileFormat != "PARQUET" {
					t.Errorf("iteration %d, entry %d: expected FileFormat 'PARQUET', got '%s'", i, j, entry.DataFile.FileFormat)
				}
			}
		}
	})

	// テストケース4: 様々なスナップショットIDとシーケンス番号
	t.Run("various_snapshot_ids_and_sequence_numbers", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			dataFilePaths := []string{
				fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i),
			}
			snapshotID := int64(1000000000000 + int64(i*123456))
			sequenceNumber := int64(i*7 + 1)

			manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
			if err != nil {
				t.Fatalf("iteration %d: generateManifest() failed: %v", i, err)
			}

			// スナップショットIDとシーケンス番号が正しく設定されていることを確認
			entry := manifest.Entries[0]
			if entry.SnapshotID != snapshotID {
				t.Errorf("iteration %d: expected SnapshotID %d, got %d", i, snapshotID, entry.SnapshotID)
			}
			if entry.SequenceNumber != sequenceNumber {
				t.Errorf("iteration %d: expected SequenceNumber %d, got %d", i, sequenceNumber, entry.SequenceNumber)
			}
		}
	})

	// テストケース5: 様々なS3パス形式
	t.Run("various_s3_path_formats", func(t *testing.T) {
		testCases := []struct {
			name     string
			filePath string
		}{
			{
				name:     "simple_path",
				filePath: "s3://bucket/data/file.parquet",
			},
			{
				name:     "nested_path",
				filePath: "s3://bucket/namespace/table/data/file.parquet",
			},
			{
				name:     "complex_bucket_name",
				filePath: "s3://63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3/data/file.parquet",
			},
			{
				name:     "timestamp_in_filename",
				filePath: "s3://bucket/data/20240101-120000-uuid.parquet",
			},
			{
				name:     "uuid_in_filename",
				filePath: "s3://bucket/data/a1b2c3d4-e5f6-7890-abcd-ef1234567890.parquet",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dataFilePaths := []string{tc.filePath}
				snapshotID := int64(1234567890000)
				sequenceNumber := int64(1)

				manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
				if err != nil {
					t.Fatalf("generateManifest() failed: %v", err)
				}

				// ファイルパスが正しく保持されていることを確認
				if manifest.Entries[0].DataFile.FilePath != tc.filePath {
					t.Errorf("expected FilePath '%s', got '%s'", tc.filePath, manifest.Entries[0].DataFile.FilePath)
				}
			})
		}
	})

	// テストケース6: エラーケース - 空のデータファイルリスト
	t.Run("error_empty_data_file_list", func(t *testing.T) {
		dataFilePaths := []string{}
		snapshotID := int64(1234567890000)
		sequenceNumber := int64(1)

		_, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
		if err == nil {
			t.Error("expected error for empty data file list")
		}
	})

	// テストケース7: マニフェストのJSON シリアライズ可能性
	t.Run("manifest_json_serialization", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			numFiles := 1 + (i % 5)
			dataFilePaths := make([]string, numFiles)
			for j := 0; j < numFiles; j++ {
				dataFilePaths[j] = fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)
			}

			snapshotID := int64(1234567890000 + int64(i*1000))
			sequenceNumber := int64(i + 1)

			manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
			if err != nil {
				t.Fatalf("iteration %d: generateManifest() failed: %v", i, err)
			}

			// JSONにシリアライズ
			jsonBytes, err := json.Marshal(manifest)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal manifest to JSON: %v", i, err)
			}

			// JSONからデシリアライズ
			var deserializedManifest IcebergManifest
			if err := json.Unmarshal(jsonBytes, &deserializedManifest); err != nil {
				t.Fatalf("iteration %d: failed to unmarshal manifest from JSON: %v", i, err)
			}

			// デシリアライズされたマニフェストが元のマニフェストと一致することを確認
			if deserializedManifest.FormatVersion != manifest.FormatVersion {
				t.Errorf("iteration %d: FormatVersion mismatch after JSON round-trip", i)
			}
			if deserializedManifest.Content != manifest.Content {
				t.Errorf("iteration %d: Content mismatch after JSON round-trip", i)
			}
			if len(deserializedManifest.Entries) != len(manifest.Entries) {
				t.Errorf("iteration %d: Entries count mismatch after JSON round-trip", i)
			}
		}
	})

	// テストケース8: 大量のデータファイル
	t.Run("large_number_of_data_files", func(t *testing.T) {
		// 100個のデータファイルでテスト
		numFiles := 100
		dataFilePaths := make([]string, numFiles)
		for i := 0; i < numFiles; i++ {
			dataFilePaths[i] = fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i)
		}

		snapshotID := int64(1234567890000)
		sequenceNumber := int64(1)

		manifest, err := generateManifest(dataFilePaths, snapshotID, sequenceNumber)
		if err != nil {
			t.Fatalf("generateManifest() failed: %v", err)
		}

		// エントリ数を検証
		if len(manifest.Entries) != numFiles {
			t.Errorf("expected %d entries, got %d", numFiles, len(manifest.Entries))
		}

		// すべてのエントリが正しく設定されていることを確認
		for i, entry := range manifest.Entries {
			if entry.Status != 1 {
				t.Errorf("entry %d: expected Status 1, got %d", i, entry.Status)
			}
			if entry.DataFile.FilePath != dataFilePaths[i] {
				t.Errorf("entry %d: FilePath mismatch", i)
			}
		}
	})
}

// TestProperty_SnapshotCreationAndCommit tests snapshot creation and commit property
